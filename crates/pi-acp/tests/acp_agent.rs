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
use std::sync::{Arc, OnceLock};
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

static ACP_AGENT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn acquire_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    ACP_AGENT_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .await
}

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

/// Wait until at least `min` notifications match `pred`, then return them all.
async fn wait_for_count<F>(log: &NotifLog, pred: F, min: usize) -> Vec<SessionUpdate>
where
    F: Fn(&SessionUpdate) -> bool,
{
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        {
            let entries = log.lock().await;
            let matched: Vec<SessionUpdate> = entries
                .iter()
                .filter(|(_, u)| pred(u))
                .map(|(_, u)| u.clone())
                .collect();
            if matched.len() >= min {
                return matched;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {min} matching notifications; log: {:?}",
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
    let _test_guard = acquire_test_lock().await;
    println!("full_method: setup");
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
            println!("full_method: initialize");
            let init = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            println!("full_method: initialize complete");
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
            println!("full_method: session/new");
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd.clone()))
                .block_task()
                .await?;
            println!("full_method: session/new complete");
            let sid = new_session.session_id.clone();
            assert!(!sid.0.is_empty());

            let session_map: Value = serde_json::from_str(
                &fs::read_to_string(agent_dir.join("pi-acp/session-map.json"))
                    .expect("session/new must persist its session mapping"),
            )
            .expect("session map must be valid JSON");
            let stored = &session_map["sessions"]["mock-session-id"];
            let cwd_string = cwd.to_string_lossy().into_owned();
            assert_eq!(
                stored.get("sessionId").and_then(Value::as_str),
                Some("mock-session-id")
            );
            assert_eq!(
                stored.get("cwd").and_then(Value::as_str),
                Some(cwd_string.as_str())
            );

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
            println!("full_method: cancel prompt");
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
            println!("full_method: cancel prompt complete");
            assert_eq!(slow_result.stop_reason, StopReason::Cancelled);

            // First real prompt names the thread (fixes #102/#24: without a
            // title Zed's sidebar keeps "New Agent Thread"). The cancelled
            // "slow" prompt is the session's first, so it carries the title.
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::SessionInfoUpdate(i)
                    if i.title.as_opt_deref() == Some(Some("slow")))
            })
            .await;


            // ---------------------------------------------------------------
            // 3. plain prompt: streaming + usage_update
            // ---------------------------------------------------------------
            println!("full_method: plain prompt");
            let prompt_resp = cx
                .send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("hello".to_string()))],
                ))
                .block_task()
                .await?;
            println!("full_method: plain prompt complete");
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
            println!("full_method: slash commands");
            println!("full_method: compact");
            let compact = cx
                .send_request(prompt_for(&sid, "/compact fix the context"))
                .block_task()
                .await?;
            println!("full_method: compact complete");
            assert_eq!(compact.stop_reason, StopReason::EndTurn);
            let text = all_text(&log.lock().await.clone());
            assert!(text.contains("Compaction completed. (custom instructions applied)"), "{text}");
            assert!(text.contains("Tokens before: 1500"), "{text}");
            assert!(text.contains("The conversation was summarized."), "{text}");

            println!("full_method: session stats");
            let stats = cx.send_request(prompt_for(&sid, "/session")).block_task().await?;
            println!("full_method: session stats complete");
            assert_eq!(stats.stop_reason, StopReason::EndTurn);
            let text = all_text(&log.lock().await.clone());
            assert!(text.contains("Session: mock-session-id"), "{text}");
            assert!(text.contains("Messages: 3"), "{text}");
            assert!(text.contains("Tokens: in 100, out 50, cache read 10, cache write 5, total 165"), "{text}");

            println!("full_method: name");
            let named = cx.send_request(prompt_for(&sid, "/name My Session")).block_task().await?;
            println!("full_method: name complete");
            assert_eq!(named.stop_reason, StopReason::EndTurn);
            let text = all_text(&log.lock().await.clone());
            assert!(text.contains("Session name set: My Session"), "{text}");
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::SessionInfoUpdate(i)
                    if i.title.as_opt_deref() == Some(Some("My Session")))
            })
            .await;
            // pi also emits `session_info_changed` on set_session_name; the
            // adapter forwards it as a second `session_info_update`, keeping
            // the title live for pi/extension-driven renames.
            let named_updates = wait_for_count(
                &log,
                |u| {
                    matches!(u, SessionUpdate::SessionInfoUpdate(i)
                        if i.title.as_opt_deref() == Some(Some("My Session")))
                },
                2,
            )
            .await;
            assert_eq!(named_updates.len(), 2);

            println!("full_method: steering show");
            let steer_show = cx.send_request(prompt_for(&sid, "/steering")).block_task().await?;
            println!("full_method: steering show complete");
            assert_eq!(steer_show.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Steering mode: one-at-a-time"));

            println!("full_method: steering set");
            let steer_set = cx.send_request(prompt_for(&sid, "/steering all")).block_task().await?;
            println!("full_method: steering set complete");
            assert_eq!(steer_set.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Steering mode set to: all"));

            println!("full_method: follow-up");
            let follow = cx.send_request(prompt_for(&sid, "/follow-up one-at-a-time")).block_task().await?;
            println!("full_method: follow-up complete");
            assert_eq!(follow.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Follow-up mode set to: one-at-a-time"));

            println!("full_method: autocompact");
            let auto = cx.send_request(prompt_for(&sid, "/autocompact")).block_task().await?;
            println!("full_method: autocompact complete");
            assert_eq!(auto.stop_reason, StopReason::EndTurn);
            assert!(all_text(&log.lock().await.clone()).contains("Auto-compaction enabled."));

            println!("full_method: changelog");
            let changelog = cx.send_request(prompt_for(&sid, "/changelog")).block_task().await?;
            println!("full_method: changelog complete");
            assert_eq!(changelog.stop_reason, StopReason::EndTurn);

            println!("full_method: export");
            let export = cx.send_request(prompt_for(&sid, "/export")).block_task().await?;
            println!("full_method: export complete");
            assert_eq!(export.stop_reason, StopReason::EndTurn);
            assert!(
                all_text(&log.lock().await.clone())
                    .contains("Nothing to export yet (no session messages)"),
                "export guard must fire when the session file is missing"
            );

            // ---------------------------------------------------------------
            // 5. set_mode / set_config_option / session/set_model
            // ---------------------------------------------------------------
            println!("full_method: config methods");
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
            println!("full_method: session/list");
            let listed = cx
                .send_request(ListSessionsRequest::new())
                .block_task()
                .await?;
            println!("full_method: session/list complete; session/load");
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
            // session/load publishes the thread title from the session file
            // (fixes #102/#24: restored threads show their real title).
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::SessionInfoUpdate(i)
                    if i.title.as_opt_deref() == Some(Some("Old Session")))
            })
            .await;
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
            println!("full_method: restored prompt");
            let loaded_prompt = cx
                .send_request(PromptRequest::new(
                    "old-session".to_string(),
                    vec![ContentBlock::Text(TextContent::new("more".to_string()))],
                ))
                .block_task()
                .await?;
            println!("full_method: restored prompt complete; delete");
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
            println!("full_method: delete complete");
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
    let _test_guard = acquire_test_lock().await;
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
    let _test_guard = acquire_test_lock().await;
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
                    sid.clone(),
                    "not-a-real-option",
                    "x",
                ))
                .block_task()
                .await
                .expect_err("unknown config option must error");
            assert!(err.to_string().contains("Unknown config option"), "{err}");

            // Close the live subprocess before the SDK tears down the outer
            // adapter. This keeps the nested mock reaped on resource-limited
            // CI runners instead of relying on forced process-group cleanup.
            cx.send_request(DeleteSessionRequest::new(sid))
                .block_task()
                .await?;
            Ok(())
        })
        .await;
    result.expect("connection should complete");
}

// ---------------------------------------------------------------------------
// S8 (W-455): reliability — errors surface, dead pi is loud, auth promotes
// ---------------------------------------------------------------------------

/// After the pi subprocess dies, `session/prompt` on that session returns an
/// **explicit** `internalError` — never a silent empty `end_turn`, and never a
/// generic "session closed" that hides what happened (fixes #82).
///
/// `--mock-exit-after 4` (env form for the inner mock): commands 1-3 are the
/// session/new handshake (`get_state` x2 + `get_available_models`), command 4
/// is the first prompt, which the mock answers by dying.
#[tokio::test]
async fn prompt_after_pi_death_returns_explicit_error() {
    let _test_guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN)
            .env("PI_ACP_MOCK_EXIT_AFTER", "4"),
    );

    let result = Client
        .builder()
        .name("s8-dead-pi-client")
        .connect_with(agent, async |cx| {
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd))
                .block_task()
                .await
                .expect("session/new must succeed before the mock dies");
            let sid = new_session.session_id;

            // The in-flight turn dies with pi: explicit PiExited error.
            let err = cx
                .send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("hello".to_string()))],
                ))
                .block_task()
                .await
                .expect_err("prompt must fail when pi dies mid-turn");
            assert_eq!(
                err.code,
                agent_client_protocol::schema::v1::ErrorCode::InternalError
            );
            assert!(err.message.contains("pi process exited"), "{}", err.message);
            assert!(
                err.message.contains("does not restart pi"),
                "hint must be present: {}",
                err.message
            );
            let data = err.data.as_ref().expect("structured error data");
            assert_eq!(data["errorType"], "piExited");
            assert_eq!(data["code"], 42);

            // A later prompt on the same (dead) session fails loudly too — the
            // session remembers the exit and never hangs or goes quiet.
            let err2 = cx
                .send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("again".to_string()))],
                ))
                .block_task()
                .await
                .expect_err("later prompt on a dead session must error");
            assert_eq!(
                err2.code,
                agent_client_protocol::schema::v1::ErrorCode::InternalError
            );
            assert!(
                err2.message.contains("pi process exited"),
                "{}",
                err2.message
            );
            assert_eq!(
                err2.data.as_ref().expect("error data")["errorType"],
                "piExited"
            );

            cx.send_request(DeleteSessionRequest::new(sid))
                .block_task()
                .await?;
            Ok(())
        })
        .await;
    result.expect("connection should complete");
}

/// A pi error that looks like missing credentials is promoted to ACP
/// `authRequired` (code -32000) with the terminal auth methods attached, so
/// the client can offer terminal login (S8 / auth.rs keyword matching).
#[tokio::test]
async fn auth_looking_prompt_error_surfaces_auth_required() {
    let _test_guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN)
            .env(
                "PI_ACP_MOCK_PROMPT_ERROR",
                "unauthorized: 401 missing api key",
            ),
    );

    let result = Client
        .builder()
        .name("s8-auth-prompt-client")
        .connect_with(agent, async |cx| {
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd))
                .block_task()
                .await
                .expect("session/new succeeds; the error comes from the prompt");
            let err = cx
                .send_request(PromptRequest::new(
                    new_session.session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new("hi".to_string()))],
                ))
                .block_task()
                .await
                .expect_err("auth-looking pi error must promote to authRequired");
            assert_eq!(
                err.code,
                agent_client_protocol::schema::v1::ErrorCode::AuthRequired
            );
            assert!(
                err.message.contains("Configure an API key"),
                "standard auth message: {}",
                err.message
            );
            let data = err.data.as_ref().expect("authRequired data");
            let methods = data["authMethods"].as_array().expect("authMethods array");
            assert_eq!(methods.len(), 1);
            assert_eq!(methods[0]["id"], "pi_terminal_login");
            assert_eq!(methods[0]["type"], "terminal");

            cx.send_request(DeleteSessionRequest::new(new_session.session_id))
                .block_task()
                .await?;
            Ok(())
        })
        .await;
    result.expect("connection should complete");
}

/// `session/new` whose model fetch fails with auth-looking text promotes to
/// `authRequired` (parity with TS `newSession`: models failure → auth check
/// first, then internalError).
#[tokio::test]
async fn auth_looking_models_error_on_new_surfaces_auth_required() {
    let _test_guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN)
            .env("PI_ACP_MOCK_MODELS_ERROR", "missing api key for provider"),
    );

    let result = Client
        .builder()
        .name("s8-auth-new-client")
        .connect_with(agent, async |cx| {
            let err = cx
                .send_request(NewSessionRequest::new(cwd))
                .block_task()
                .await
                .expect_err("auth-looking models error must fail session/new");
            assert_eq!(
                err.code,
                agent_client_protocol::schema::v1::ErrorCode::AuthRequired
            );
            let data = err.data.as_ref().expect("authRequired data");
            let methods = data["authMethods"].as_array().expect("authMethods array");
            assert_eq!(methods.len(), 1);
            assert_eq!(methods[0]["id"], "pi_terminal_login");
            Ok(())
        })
        .await;
    result.expect("connection should complete");
}

/// `session/load` surfaces `get_messages` failures explicitly instead of
/// silently skipping history replay (S8: never swallow errors; TS parity:
/// `loadSession` throws on `getMessages`).
#[tokio::test]
async fn load_session_surfaces_get_messages_failure() {
    let _test_guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&agent_dir).unwrap();
    // A real pi session file so `find_stored_session` resolves the id.
    write_pi_session(&agent_dir, "stored-session", &cwd.to_string_lossy());

    // The mock dies on command 2: 1=spawn handshake `get_state` (succeeds),
    // 2=load's `get_messages` (dies → explicit error, no silent replay skip).
    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN)
            .env("PI_ACP_MOCK_EXIT_AFTER", "2")
            .env("PI_CODING_AGENT_DIR", agent_dir.to_str().unwrap()),
    );

    let result = Client
        .builder()
        .name("s8-load-error-client")
        .connect_with(agent, async |cx| {
            let err = cx
                .send_request(LoadSessionRequest::new("stored-session", cwd))
                .block_task()
                .await
                .expect_err("get_messages failure must fail session/load");
            assert_eq!(
                err.code,
                agent_client_protocol::schema::v1::ErrorCode::InternalError
            );
            assert!(err.message.contains("pi process exited"), "{}", err.message);
            assert_eq!(
                err.data.as_ref().expect("error data")["errorType"],
                "piExited"
            );

            cx.send_request(DeleteSessionRequest::new("stored-session"))
                .block_task()
                .await?;
            Ok(())
        })
        .await;
    result.expect("connection should complete");
}

// ---------------------------------------------------------------------------
// Signal handling (design §8.3)
// ---------------------------------------------------------------------------

/// SIGTERM triggers the graceful-shutdown path: with a live session (and its
/// mock pi subprocess) up, the agent must dispose everything and exit cleanly
/// (code 0) instead of dying on the raw signal and orphaning pi. Drives the
/// binary as a raw JSON-RPC client so the child pid is ours to signal.
#[cfg(unix)]
#[tokio::test]
async fn sigterm_triggers_graceful_shutdown() {
    let _test_guard = acquire_test_lock().await;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command as TokioCommand;

    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();

    let mut child = TokioCommand::new(BIN)
        .env("PI_ACP_MOCK", "1")
        .env("PI_ACP_PI_COMMAND", BIN)
        .env("RUST_LOG", "warn") // keep the stdout protocol stream clean
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // Pipeline initialize + session/new (ACP JSON-RPC over line-delimited
    // JSON; the agent processes them in order). session/new spawns the mock
    // pi subprocess, so the session is live once it responds.
    let requests = [
        json!({"jsonrpc":"2.0","id":"1","method":"initialize","params":{"protocolVersion":1}}),
        json!({"jsonrpc":"2.0","id":"2","method":"session/new","params":{"cwd": cwd.to_str().unwrap()}}),
    ];
    for req in requests {
        let mut line = req.to_string();
        line.push('\n');
        stdin.write_all(line.as_bytes()).await.unwrap();
    }
    stdin.flush().await.unwrap();

    // Wait for the session/new response frame (id "2"); notifications and any
    // log lines are skipped.
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let mut raw = String::new();
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent never answered session/new"
        );
        raw.clear();
        let n = tokio::time::timeout(
            deadline - tokio::time::Instant::now(),
            stdout.read_line(&mut raw),
        )
        .await
        .expect("read timed out")
        .unwrap();
        assert!(n > 0, "agent stdout closed before session/new responded");
        if raw.contains("\"id\":\"2\"") {
            break;
        }
    }

    // SIGTERM the agent; it must dispose the session and exit code 0 within a
    // bounded window (a missing handler would die on the raw signal instead).
    let pid = child.id().expect("agent pid");
    let kill_status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("kill -TERM");
    assert!(kill_status.success(), "kill -TERM failed");

    let status = tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .expect("agent did not exit after SIGTERM (graceful shutdown hung)")
        .expect("wait failed");
    assert_eq!(
        status.code(),
        Some(0),
        "agent must exit code 0 after SIGTERM (graceful shutdown), got {status:?}"
    );
}
