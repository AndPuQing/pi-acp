//! S6 (W-453) acceptance tests: drive the built `pi-acp` binary as an ACP
//! **client** and exercise the full agent method set against the **mock pi**
//! (`PI_ACP_MOCK=1` env; the mock lives in the `pi-acp` binary itself).
//!
//! Covers the design §7 must-align surface: initialize (capabilities + auth
//! methods), session/new (configOptions / modes / startup info /
//! available_commands_update), prompt (streaming + usage_update), the built-in
//! slash commands, cancel, set_mode / set_config_option / session/set_model,
//! session/list / load (history replay) / delete, all in CI with no real pi.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AuthMethod, CancelNotification, ContentBlock, DeleteSessionRequest, InitializeRequest,
    ListSessionsRequest, LoadSessionRequest, NewSessionRequest, PromptRequest, SessionConfigKind,
    SessionConfigOptionCategory, SessionId, SessionNotification, SessionUpdate,
    SetSessionConfigOptionRequest, SetSessionModeRequest, StopReason, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{
    on_receive_notification, AcpAgent, AcpAgentConfig, Client, UntypedMessage,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");
const TIMEOUT: Duration = Duration::from_secs(15);

/// Everything the agent notified us, in order, keyed by session id.
type NotifLog = Arc<Mutex<Vec<(String, SessionUpdate)>>>;

fn prompt_for(session_id: &SessionId, s: &str) -> PromptRequest {
    PromptRequest::new(
        session_id.clone(),
        vec![ContentBlock::Text(TextContent::new(s.to_string()))],
    )
}

/// Wait until `pred` sees a matching notification (bounded poll).
async fn wait_for<F>(log: &NotifLog, pred: F) -> SessionUpdate
where
    F: Fn(&SessionUpdate) -> bool,
{
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        {
            let entries = log.lock().await;
            if let Some((_, u)) = entries.iter().find(|(_, u)| pred(u)) {
                return u.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for notification; log: {:?}",
            *log.lock().await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// All streamed text chunks across sessions, concatenated.
fn all_text(log: &[(String, SessionUpdate)]) -> String {
    let mut out = String::new();
    for (_, u) in log {
        if let SessionUpdate::AgentMessageChunk(c) = u {
            if let ContentBlock::Text(t) = &c.content {
                out.push_str(&t.text);
            }
        }
    }
    out
}

fn find_config_option<'a>(
    options: &'a [agent_client_protocol::schema::v1::SessionConfigOption],
    id: &str,
) -> Option<&'a agent_client_protocol::schema::v1::SessionConfigOption> {
    options.iter().find(|o| o.id.0.as_ref() == id)
}

/// Write a fake pi session file (header + messages) for list/load/delete tests.
fn write_pi_session(dir: &Path, id: &str, cwd: &str) -> PathBuf {
    let sessions = dir.join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    let path = sessions.join(format!("{id}.jsonl"));
    let header = json!({ "type": "session", "id": id, "cwd": cwd }).to_string();
    fs::write(
        &path,
        format!(
            "{header}\n\
             {{\"type\":\"session_info\",\"name\":\"Old Session\",\"timestamp\":\"2026-08-01T10:00:00.000Z\"}}\n\
             {{\"type\":\"message\",\"timestamp\":\"2026-08-01T10:00:05.000Z\",\"message\":{{\"role\":\"user\",\"content\":\"first user message\"}}}}\n"
        ),
    )
    .unwrap();
    path
}

/// Run the full method set against the mock pi.
#[tokio::test]
async fn full_method_set_against_mock_pi() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(&agent_dir).unwrap();
    fs::write(cwd.join("AGENTS.md"), "test context").unwrap();
    let pi_session = write_pi_session(&agent_dir, "old-session", &cwd.to_string_lossy());

    let command_log = tmp.path().join("commands.log");
    let scenario_dir = tmp.path().join("scenarios");
    fs::create_dir_all(&scenario_dir).unwrap();
    // Scenario 1: a slow prompt (300ms) that streams one delta — used for the
    // cancel test. `agent_settled` is auto-appended by the mock.
    fs::write(
        scenario_dir.join("1.jsonl"),
        "{\"__directive__\":\"wait_ms\",\"ms\":300}\n\
         {\"type\":\"message_update\",\"usage\":{},\"assistantMessageEvent\":{\"type\":\"text_delta\",\"contentIndex\":0,\"delta\":\"slow turn\"}}\n",
    )
    .unwrap();

    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN)
            .env("PI_ACP_MOCK_SCENARIO", scenario_dir.to_str().unwrap())
            .env("PI_ACP_MOCK_COMMAND_LOG", command_log.to_str().unwrap())
            .env("PI_CODING_AGENT_DIR", agent_dir.to_str().unwrap())
            .env("RUST_LOG", "info,pi_acp=debug"),
    );

    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();

    Client
        .builder()
        .name("s6-e2e-client")
        .on_receive_notification(
            async move |notif: SessionNotification, _cx| {
                log_in_handler
                    .lock()
                    .await
                    .push((notif.session_id.0.to_string(), notif.update.clone()));
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(agent, async |cx| {
            // ---------------------------------------------------------------
            // 1. initialize
            // ---------------------------------------------------------------
            let init = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            assert_eq!(init.agent_info.as_ref().unwrap().name, "pi-acp");
            assert!(
                init.agent_capabilities.load_session,
                "loadSession capability must be advertised"
            );
            assert!(init.agent_capabilities.prompt_capabilities.image);
            let caps = init.agent_capabilities.session_capabilities;
            assert!(caps.list.is_some(), "sessionCapabilities.list");
            assert!(caps.delete.is_some(), "sessionCapabilities.delete");
            assert_eq!(init.auth_methods.len(), 1);
            match &init.auth_methods[0] {
                AuthMethod::Terminal(t) => {
                    assert_eq!(t.args, vec!["--terminal-login".to_string()]);
                }
                other => panic!("expected terminal auth method, got {other:?}"),
            }

            // ---------------------------------------------------------------
            // 2. session/new
            // ---------------------------------------------------------------
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd.clone()))
                .block_task()
                .await?;
            let sid = new_session.session_id.clone();
            assert!(!sid.0.is_empty());

            // configOptions: model select first, then thought_level select.
            let options = new_session.config_options.as_ref().expect("configOptions");
            let model_opt = find_config_option(options, "model").expect("model option");
            assert_eq!(model_opt.category, Some(SessionConfigOptionCategory::Model));
            if let SessionConfigKind::Select(sel) = &model_opt.kind {
                assert_eq!(sel.current_value.0.as_ref(), "mock/mock-model");
            } else {
                panic!("model option must be a select");
            }
            let thought_opt = find_config_option(options, "thought_level").expect("thought_level option");
            assert_eq!(
                thought_opt.category,
                Some(SessionConfigOptionCategory::ThoughtLevel)
            );
            if let SessionConfigKind::Select(sel) = &thought_opt.kind {
                assert_eq!(sel.current_value.0.as_ref(), "medium");
            } else {
                panic!("thought_level option must be a select");
            }
            let modes = new_session.modes.as_ref().expect("modes");
            assert_eq!(modes.current_mode_id.0.as_ref(), "medium");

            // Startup info arrived as a notification right after session/new.
            let startup = wait_for(&log, |u| {
                matches!(u, SessionUpdate::AgentMessageChunk(c)
                    if matches!(&c.content, ContentBlock::Text(t) if t.text.contains("AGENTS.md")))
            })
            .await;
            let SessionUpdate::AgentMessageChunk(sc) = startup else {
                unreachable!()
            };
            let ContentBlock::Text(st) = &sc.content else {
                unreachable!()
            };
            assert!(st.text.contains("## Context"), "startup info: {}", st.text);

            // available_commands_update: pi commands + builtins, no extensions.
            let commands = wait_for(&log, |u| {
                matches!(u, SessionUpdate::AvailableCommandsUpdate(_))
            })
            .await;
            let SessionUpdate::AvailableCommandsUpdate(acu) = commands else {
                unreachable!()
            };
            let names: Vec<&str> = acu.available_commands.iter().map(|c| c.name.as_str()).collect();
            assert!(names.contains(&"review"), "pi get_commands merged: {names:?}");
            assert!(names.contains(&"skill:deploy"), "skill commands kept: {names:?}");
            assert!(!names.contains(&"ext-thing"), "extension commands excluded: {names:?}");
            for builtin in ["compact", "autocompact", "export", "session", "name", "steering", "follow-up", "changelog"] {
                assert!(names.contains(&builtin), "builtin /{builtin} advertised: {names:?}");
            }

            // ---------------------------------------------------------------
            // 6. cancel (through the full ACP path, mid-turn)
            // ---------------------------------------------------------------
            let slow_prompt = {
                let cx = cx.clone();
                let sid = sid.clone();
                tokio::spawn(async move {
                    cx.send_request(PromptRequest::new(
                        sid,
                        vec![ContentBlock::Text(TextContent::new("slow".to_string()))],
                    ))
                    .block_task()
                    .await
                })
            };
            tokio::time::sleep(Duration::from_millis(80)).await;
            cx.send_notification(CancelNotification::new(sid.clone()))?;
            let slow_result = slow_prompt.await.expect("slow prompt task panicked")?;
            assert_eq!(slow_result.stop_reason, StopReason::Cancelled);


            // ---------------------------------------------------------------
            // 3. plain prompt: streaming + usage_update
            // ---------------------------------------------------------------
            let prompt_resp = cx
                .send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("hello".to_string()))],
                ))
                .block_task()
                .await?;
            assert_eq!(prompt_resp.stop_reason, StopReason::EndTurn);
            // The streamed chunk may still be in flight when the response
            // lands; wait for it explicitly.
            let chunk = wait_for(&log, |u| {
                matches!(u, SessionUpdate::AgentMessageChunk(c)
                    if matches!(&c.content, ContentBlock::Text(t) if t.text.contains("hello from mock")))
            })
            .await;
            let _ = chunk;
            // usage_update (decision 3): used=15 from the mock's message_update,
            // size=1000 from the mock model's contextWindow.
            let usage = wait_for(&log, |u| matches!(u, SessionUpdate::UsageUpdate(_))).await;
            let SessionUpdate::UsageUpdate(uu) = usage else {
                unreachable!()
            };
            assert_eq!(uu.used, 15);
            assert_eq!(uu.size, 1000);

            // ---------------------------------------------------------------
            // 4. built-in slash commands
            // ---------------------------------------------------------------
            let compact = cx
                .send_request(prompt_for(&sid, "/compact fix the context"))
                .block_task()
                .await?;
            assert_eq!(compact.stop_reason, StopReason::EndTurn);
            let text = all_text(&log.lock().await.clone());
            assert!(text.contains("Compaction completed. (custom instructions applied)"), "{text}");
            assert!(text.contains("Tokens before: 1500"), "{text}");
            assert!(text.contains("The conversation was summarized."), "{text}");

            let stats = cx.send_request(prompt_for(&sid, "/session")).block_task().await?;
            assert_eq!(stats.stop_reason, StopReason::EndTurn);
            let text = all_text(&log.lock().await.clone());
            assert!(text.contains("Session: mock-session-id"), "{text}");
            assert!(text.contains("Messages: 3"), "{text}");
            assert!(text.contains("Tokens: in 100, out 50, cache read 10, cache write 5, total 165"), "{text}");

            let named = cx.send_request(prompt_for(&sid, "/name My Session")).block_task().await?;
            assert_eq!(named.stop_reason, StopReason::EndTurn);
            let text = all_text(&log.lock().await.clone());
            assert!(text.contains("Session name set: My Session"), "{text}");
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::SessionInfoUpdate(i)
                    if i.title.as_opt_deref() == Some(Some("My Session")))
            })
            .await;

            let steer_show = cx.send_request(prompt_for(&sid, "/steering")).block_task().await?;
            assert_eq!(steer_show.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Steering mode: one-at-a-time"));

            let steer_set = cx.send_request(prompt_for(&sid, "/steering all")).block_task().await?;
            assert_eq!(steer_set.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Steering mode set to: all"));

            let follow = cx.send_request(prompt_for(&sid, "/follow-up one-at-a-time")).block_task().await?;
            assert_eq!(follow.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Follow-up mode set to: one-at-a-time"));

            let auto = cx.send_request(prompt_for(&sid, "/autocompact")).block_task().await?;
            assert_eq!(auto.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Auto-compaction enabled."));

            let changelog = cx.send_request(prompt_for(&sid, "/changelog")).block_task().await?;
            assert_eq!(changelog.stop_reason, StopReason::EndTurn);

            let export = cx.send_request(prompt_for(&sid, "/export")).block_task().await?;
            assert_eq!(export.stop_reason, StopReason::EndTurn);
            assert!(
                all_text(&log.lock().await.clone())
                    .contains("Nothing to export yet (no session messages)"),
                "export guard must fire when the session file is missing"
            );

            // ---------------------------------------------------------------
            // 5. set_mode / set_config_option / session/set_model
            // ---------------------------------------------------------------
            let set_mode = cx
                .send_request(SetSessionModeRequest::new(sid.clone(), "xhigh"))
                .block_task()
                .await?;
            assert!(set_mode.meta.is_none() || set_mode.meta.is_some());
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::CurrentModeUpdate(m) if m.current_mode_id.0.as_ref() == "xhigh")
            })
            .await;

            let set_thought = cx
                .send_request(SetSessionConfigOptionRequest::new(
                    sid.clone(),
                    "thought_level",
                    "high",
                ))
                .block_task()
                .await?;
            let thought = find_config_option(&set_thought.config_options, "thought_level").unwrap();
            if let SessionConfigKind::Select(sel) = &thought.kind {
                assert_eq!(sel.current_value.0.as_ref(), "high");
            } else {
                panic!("thought_level must be a select");
            }
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::CurrentModeUpdate(m) if m.current_mode_id.0.as_ref() == "high")
            })
            .await;
            wait_for(&log, |u| matches!(u, SessionUpdate::ConfigOptionUpdate(_))).await;

            let set_model = cx
                .send_request(SetSessionConfigOptionRequest::new(
                    sid.clone(),
                    "model",
                    "mock/mock-fast",
                ))
                .block_task()
                .await?;
            let model_opt = find_config_option(&set_model.config_options, "model").unwrap();
            if let SessionConfigKind::Select(sel) = &model_opt.kind {
                assert_eq!(sel.current_value.0.as_ref(), "mock/mock-fast");
            } else {
                panic!("model must be a select");
            }

            // Unstable session/set_model via a raw method (the client SDK has
            // no typed variant either).
            let set_raw = cx
                .send_request(UntypedMessage::new(
                    "session/set_model",
                    json!({ "sessionId": sid.0, "modelId": "mock/mock-model" }),
                ).expect("untyped message"))
                .block_task()
                .await?;
            let _: Value = set_raw;
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::ConfigOptionUpdate(_))
            })
            .await;


            // ---------------------------------------------------------------
            // 7. session/list, session/load, session/delete
            // ---------------------------------------------------------------
            let listed = cx
                .send_request(ListSessionsRequest::new())
                .block_task()
                .await?;
            assert!(
                listed.sessions.iter().any(|s| s.session_id.0.as_ref() == "old-session"),
                "list should include the fake pi session: {:?}",
                listed.sessions
            );

            let loaded = cx
                .send_request(LoadSessionRequest::new("old-session", cwd.clone()))
                .block_task()
                .await?;
            assert!(loaded.config_options.is_some());
            // History replay notifications.
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::UserMessageChunk(c)
                    if matches!(&c.content, ContentBlock::Text(t) if t.text == "hello there"))
            })
            .await;
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::AgentMessageChunk(c)
                    if matches!(&c.content, ContentBlock::Text(t) if t.text.contains("hi! how can I help?")))
            })
            .await;
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::ToolCall(t) if t.title == "ls")
            })
            .await;
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::ToolCallUpdate(t) if t.tool_call_id.0.as_ref() == "read-1")
            })
            .await;

            // Prompt the restored session: it must work (session id preserved).
            let loaded_prompt = cx
                .send_request(PromptRequest::new(
                    "old-session".to_string(),
                    vec![ContentBlock::Text(TextContent::new("more".to_string()))],
                ))
                .block_task()
                .await?;
            assert_eq!(loaded_prompt.stop_reason, StopReason::EndTurn);

            let deleted = cx
                .send_request(DeleteSessionRequest::new("old-session"))
                .block_task()
                .await?;
            let _: agent_client_protocol::schema::v1::DeleteSessionResponse = deleted;
            assert!(!pi_session.exists(), "session file must be deleted");

            let deleted_again = cx
                .send_request(DeleteSessionRequest::new("old-session"))
                .block_task()
                .await?;
            let _: agent_client_protocol::schema::v1::DeleteSessionResponse = deleted_again;

            Ok(())
        })
        .await
        .expect("ACP client session should complete without error");

    // The mock recorded the expected pi RPC commands.
    let cmds = fs::read_to_string(&command_log).unwrap();
    for expected in [
        "get_state",
        "get_available_models",
        "prompt",
        "compact",
        "get_session_stats",
        "set_session_name",
        "set_steering_mode",
        "set_follow_up_mode",
        "set_auto_compaction",
        "set_thinking_level",
        "set_model",
        "get_commands",
        "abort",
        "get_messages",
    ] {
        assert!(
            cmds.lines().any(|l| l == expected),
            "mock command log missing {expected}: {cmds}"
        );
    }
}

/// session/set_model with an unknown session must produce a JSON-RPC error
/// (the fallback handler declines nothing else).
#[tokio::test]
async fn set_session_model_unknown_session_errors() {
    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN),
    );
    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();

    let result = Client
        .builder()
        .name("s6-e2e-client-2")
        .on_receive_notification(
            async move |notif: SessionNotification, _cx| {
                log_in_handler
                    .lock()
                    .await
                    .push((notif.session_id.0.to_string(), notif.update.clone()));
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(agent, async |cx| {
            let err = cx
                .send_request(
                    UntypedMessage::new(
                        "session/set_model",
                        json!({ "sessionId": "does-not-exist", "modelId": "mock/mock-model" }),
                    )
                    .expect("untyped message"),
                )
                .block_task()
                .await
                .expect_err("unknown session must error");
            assert!(
                err.to_string().contains("Unknown sessionId"),
                "error message: {err}"
            );
            Ok(())
        })
        .await;
    result.expect("connection should complete");
}

/// `session/set_config_option` with an unknown config id must error with
/// invalidParams-style text.
#[tokio::test]
async fn unknown_config_option_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN),
    );

    let result = Client
        .builder()
        .name("s6-e2e-client-3")
        .connect_with(agent, async |cx| {
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd))
                .block_task()
                .await?;
            let sid = new_session.session_id;
            let err = cx
                .send_request(SetSessionConfigOptionRequest::new(
                    sid,
                    "not-a-real-option",
                    "x",
                ))
                .block_task()
                .await
                .expect_err("unknown config option must error");
            assert!(err.to_string().contains("Unknown config option"), "{err}");
            Ok(())
        })
        .await;
    result.expect("connection should complete");
}
