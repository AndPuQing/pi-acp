//! W-562: ACP **v2** end-to-end, driven **in-process** over `Channel::duplex`.
//!
//! One pi-acp binary serves both versions: the SDK's `AgentProtocolRouter`
//! negotiates from `initialize` and then becomes a raw-frame pass-through. The
//! v1 and v2 implementations are native and separate (no conversion), so these
//! tests exercise the whole v2 surface:
//!
//! - a real v2 `initialize` / `session/new` / `session/prompt` handshake;
//! - `message_id` grouping across a message's chunks;
//! - the v2-only `agent_message` patch built from pi's authoritative
//!   `message_end`;
//! - v2's replacement of the prompt response's `stopReason` with `state_update`;
//! - `CurrentModeUpdate` being **skipped** (not an error) on v2;
//! - `session/resume` replay-cursor semantics, including rejecting an unknown
//!   cursor rather than guessing a position;
//! - the same in-process path still working for a v1 client when the feature is
//!   compiled in.
//!
//! Everything runs against the in-binary mock pi (`PI_ACP_MOCK=1`), so CI needs
//! no real pi and no network. The agent runs **inside** this test process, so
//! the environment it reads is this process's own — the tests therefore mutate
//! shared process env and are serialized by `ENV_LOCK`.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::schema::{InitializeRequest, NewSessionRequest};
use agent_client_protocol::v2::schema as v2;
use agent_client_protocol::{on_receive_notification, Channel, Client};
use pi_acp::agent::AcpAgent;
use pi_acp::config::Config;
use tokio::sync::Mutex;

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");
const TIMEOUT: Duration = Duration::from_secs(20);

/// Insertion ACKs are independent of model completion. Concurrent retries,
/// replay and a replacement native subprocess must preserve message identity.
#[tokio::test]
async fn v2_insertion_keys_and_retained_ids_are_truthful() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();
    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let received = log.clone();
    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = tokio::spawn(async move { agent.run_with(agent_end).await });
    Client
        .v2()
        .on_receive_notification(
            async move |notification: v2::UpdateSessionNotification, _cx| {
                received
                    .lock()
                    .await
                    .push(serde_json::to_value(notification).unwrap());
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(client_end, async move |cx| {
            cx.send_request(v2::InitializeRequest::new(
                ProtocolVersion::V2,
                v2::Implementation::new("identity-test", "1"),
            ))
            .block_task()
            .await?;
            // Invalid identity cannot produce a fictional insertion echo.
            assert!(cx
                .send_request(v2::PromptRequest::new(
                    "missing-session",
                    vec![v2::ContentBlock::Text(v2::TextContent::new("invalid"))]
                ))
                .block_task()
                .await
                .is_err());
            assert!(!log
                .lock()
                .await
                .iter()
                .any(|v| kind(v) == Some("user_message")));
            let session = cx
                .send_request(v2::NewSessionRequest::new(&env.cwd))
                .block_task()
                .await?;
            let key = v2::IdempotencyKey::new("concurrent-insertion-0001").unwrap();
            let request = v2::PromptRequest::new(
                session.session_id.clone(),
                vec![v2::ContentBlock::Text(v2::TextContent::new("one turn"))],
            )
            .idempotency_key(key.clone());
            let (first, duplicate) = tokio::join!(
                cx.send_request(request.clone()).block_task(),
                cx.send_request(request.clone()).block_task()
            );
            let first = first?;
            let duplicate = duplicate?;
            assert_eq!(first.message_id, duplicate.message_id);
            wait_for_raw(
                &log,
                |v| kind(v) == Some("state_update") && update(v)["state"] == "idle",
                "first owned turn",
            )
            .await;
            let before = log.lock().await.clone();
            let users: Vec<_> = before
                .iter()
                .filter(|v| kind(v) == Some("user_message"))
                .collect();
            assert_eq!(
                users.len(),
                1,
                "concurrent same-key retry must neither echo nor execute twice"
            );
            assert_eq!(update(users[0])["messageId"], first.message_id.0.as_ref());
            assert_eq!(
                before
                    .iter()
                    .filter(|v| kind(v) == Some("state_update") && update(v)["state"] == "running")
                    .count(),
                1
            );
            let assistant = before
                .iter()
                .find(|v| kind(v) == Some("agent_message"))
                .expect("complete assistant snapshot");
            let assistant_id = update(assistant)["messageId"].clone();
            log.lock().await.clear();
            // Resume replaces the native subprocess, replaying the ACP-owned IDs.
            cx.send_request(
                v2::ResumeSessionRequest::new(session.session_id.clone(), &env.cwd)
                    .replay_from(v2::ReplayFrom::Start(v2::ReplayFromStart::new())),
            )
            .block_task()
            .await?;
            let replay = log.lock().await.clone();
            assert!(replay.iter().any(|v| kind(v) == Some("user_message")
                && update(v)["messageId"] == first.message_id.0.as_ref()));
            assert!(replay.iter().any(
                |v| kind(v) == Some("agent_message") && update(v)["messageId"] == assistant_id
            ));
            log.lock().await.clear();
            assert_eq!(
                cx.send_request(request.clone())
                    .block_task()
                    .await?
                    .message_id,
                first.message_id,
                "retained retry must reuse recorded insertion"
            );
            assert!(!log
                .lock()
                .await
                .iter()
                .any(|v| kind(v) == Some("user_message") || kind(v) == Some("agent_message")));
            let second = cx
                .send_request(v2::PromptRequest::new(
                    session.session_id.clone(),
                    vec![v2::ContentBlock::Text(v2::TextContent::new("new turn"))],
                ))
                .block_task()
                .await?;
            assert_ne!(second.message_id, first.message_id);
            wait_for_raw(
                &log,
                |v| kind(v) == Some("state_update") && update(v)["state"] == "idle",
                "new turn after restored history",
            )
            .await;
            assert!(
                log.lock()
                    .await
                    .iter()
                    .filter(|v| kind(v) == Some("agent_message"))
                    .all(|v| update(v)["messageId"] != assistant_id),
                "new assistant IDs cannot overwrite retained history"
            );
            log.lock().await.clear();
            cx.send_request(v2::DeleteSessionRequest::new(session.session_id.clone()))
                .block_task()
                .await?;
            assert!(
                pi_acp::conversation::replay(session.session_id.0.as_ref())
                    .unwrap()
                    .is_empty(),
                "delete removes retained content"
            );
            assert!(
                pi_acp::conversation::lookup(
                    session.session_id.0.as_ref(),
                    Some("concurrent-insertion-0001")
                )
                .unwrap()
                .is_none(),
                "delete removes retained keys"
            );
            pi_acp::conversation::record(&v2::UpdateSessionNotification::new(
                session.session_id.clone(),
                v2::SessionUpdate::UserMessage(
                    v2::UserMessage::new(first.message_id.clone()).content(vec![
                        v2::ContentBlock::Text(v2::TextContent::new("late frame")),
                    ]),
                ),
            ))
            .unwrap();
            assert!(
                pi_acp::conversation::replay(session.session_id.0.as_ref())
                    .unwrap()
                    .is_empty(),
                "late frames cannot restore deleted content"
            );
            assert!(
                cx.send_request(request).block_task().await.is_err(),
                "deleted session cannot ACK an old key"
            );
            assert!(!log
                .lock()
                .await
                .iter()
                .any(|v| kind(v) == Some("user_message")));
            Ok(())
        })
        .await
        .expect("v2 insertion identity flow");
    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// Serializes the process-env mutation these in-process tests require.
static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Everything the agent notified us, in v2 form, in order.
type NotifLog = Arc<Mutex<Vec<serde_json::Value>>>;

/// Wait until `pred` sees a matching raw notification (bounded poll).
async fn wait_for_raw<F>(log: &NotifLog, pred: F, what: &str) -> serde_json::Value
where
    F: Fn(&serde_json::Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        {
            let entries = log.lock().await;
            if let Some(v) = entries.iter().find(|v| pred(v)) {
                return v.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}; log: {:#?}",
            *log.lock().await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The `sessionUpdate` discriminator of a raw `session/update` notification.
fn kind(v: &serde_json::Value) -> Option<&str> {
    v.get("update")?.get("sessionUpdate")?.as_str()
}

/// The `session/update` payload (the typed notification serializes its params
/// directly — the JSON-RPC `params` envelope belongs to the transport).
fn update(v: &serde_json::Value) -> &serde_json::Value {
    v.get("update").unwrap_or(&serde_json::Value::Null)
}

/// Point the in-process agent (and the mock pi it spawns) at the mock, with an
/// isolated agent dir so tests never share a session map.
struct MockEnv {
    _tmp: tempfile::TempDir,
    cwd: std::path::PathBuf,
}

impl MockEnv {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let cwd = tmp.path().join("project");
        let agent_dir = tmp.path().join("agent");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::create_dir_all(&agent_dir).unwrap();

        std::env::set_var("PI_ACP_MOCK", "1");
        std::env::set_var("PI_ACP_PI_COMMAND", BIN);
        std::env::set_var("PI_CODING_AGENT_DIR", &agent_dir);
        std::env::set_var(
            "PI_ACP_MOCK_SESSION_FILE",
            tmp.path().join("sessions/mock-session.jsonl"),
        );
        Self { _tmp: tmp, cwd }
    }
}

/// The full v2 flow: `initialize` -> `session/new` -> `session/prompt` ->
/// updates -> terminal state, asserting the v2-only behaviors along the way.
///
/// `client` is the v2 client component; the caller decides which side drives.
async fn drive_v2(
    agent: Arc<AcpAgent>,
    cwd: std::path::PathBuf,
    prompt_text: &'static str,
) -> (NotifLog, v2::InitializeResponse, v2::NewSessionResponse) {
    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();

    let (agent_end, client_end) = Channel::duplex();

    let agent_task = {
        let agent = agent.clone();
        tokio::spawn(async move { agent.run_with(agent_end).await })
    };

    let init_out: Arc<Mutex<Option<v2::InitializeResponse>>> = Arc::new(Mutex::new(None));
    let new_out: Arc<Mutex<Option<v2::NewSessionResponse>>> = Arc::new(Mutex::new(None));
    let init_slot = init_out.clone();
    let new_slot = new_out.clone();
    let log_in_client = log.clone();

    Client
        .v2()
        .name("w562-v2-client")
        .on_receive_notification(
            async move |notif: v2::UpdateSessionNotification, _cx| {
                // v2-typed: `state_update` / `agent_message` have no v1
                // equivalent and would fail to parse as v1 types.
                let raw = serde_json::to_value(&notif).unwrap_or(serde_json::Value::Null);
                log_in_handler.lock().await.push(raw);
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(client_end, async move |cx| {
            // 1. initialize — the router must answer with the requested version.
            let init = cx
                .send_request(v2::InitializeRequest::new(
                    ProtocolVersion::V2,
                    v2::Implementation::new("w562-test-client", "1.0.0"),
                ))
                .block_task()
                .await?;
            assert_eq!(
                init.protocol_version,
                ProtocolVersion::V2,
                "the agent must answer with the requested version"
            );
            assert_eq!(init.info.name, "pi-acp");
            // v2 requires auth methods to imply auth/login + auth/logout; the
            // adapter advertises none rather than RPCs it would fail.
            assert!(
                init.auth_methods.is_empty(),
                "v2 must not advertise auth methods pi-acp cannot serve: {:?}",
                init.auth_methods
            );
            *init_slot.lock().await = Some(init);

            // 2. session/new
            let new_session = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            assert_eq!(new_session.session_id.0.as_ref(), "mock-session-id");
            assert!(
                !new_session.config_options.is_empty(),
                "v2 carries configOptions (where v2 expresses modes)"
            );
            let sid = new_session.session_id.clone();
            *new_slot.lock().await = Some(new_session);

            // 3. session/prompt — this endpoint waits for settlement before
            // responding. The terminal reason is a separate state_update;
            // the response/notification delivery order is not a guarantee.
            let prompt_resp = cx
                .send_request(v2::PromptRequest::new(
                    sid.clone(),
                    vec![v2::ContentBlock::Text(v2::TextContent::new(prompt_text))],
                ))
                .block_task()
                .await?;
            let _ = prompt_resp; // v2 PromptResponse carries only _meta.

            // 4. wait for the v2-only terminal `state_update` so the turn is
            // known to be over before asserting on the streamed updates.
            let idle = wait_for_raw(
                &log_in_client,
                |v| kind(v) == Some("state_update") && update(v)["state"] == "idle",
                "a terminal v2 state_update",
            )
            .await;
            assert_eq!(
                update(&idle)["state"],
                "idle",
                "v2 reports completion through state_update"
            );

            Ok(())
        })
        .await
        .expect("v2 in-process ACP client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish after the client disconnected")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");

    let init = init_out.lock().await.clone().expect("initialize response");
    let new = new_out.lock().await.clone().expect("session/new response");
    (log, init, new)
}

#[tokio::test]
async fn v2_end_to_end_handshake_prompt_and_patch_object() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let (log, init, _new) = drive_v2(
        Arc::new(AcpAgent::new(Config::from_env())),
        env.cwd.clone(),
        "hello",
    )
    .await;

    // The v2 capability block exists and no v1-only auth surface leaked.
    assert!(init.capabilities.session.is_some());
    assert!(init.auth_methods.is_empty());

    let entries = log.lock().await.clone();

    // Every streamed chunk of the answer carries the same messageId (grouping).
    //
    // Scoped to the answer's own message: the session also publishes a startup
    // prelude as its own `agent_message_chunk`, which is a genuinely different
    // message and so carries its own id. Lumping the two together would assert
    // that every chunk in the session belongs to one message, which was only
    // true while the prelude's chunk was being dropped on the way out.
    let all_chunk_ids: Vec<String> = entries
        .iter()
        .filter(|v| matches!(kind(v), Some("agent_message_chunk") | Some("agent_message")))
        .filter_map(|v| update(v)["messageId"].as_str().map(str::to_string))
        .collect();
    assert!(
        !all_chunk_ids.is_empty(),
        "v2 must tag streamed chunks with a messageId: {entries:#?}"
    );

    // The message the completion patch names is the answer, and its chunks are
    // contiguous and identically tagged.
    let answer_id = entries
        .iter()
        .find(|v| kind(v) == Some("agent_message"))
        .map(|v| {
            update(v)["messageId"]
                .as_str()
                .expect("patch messageId")
                .to_string()
        })
        .unwrap_or_else(|| panic!("expected an agent_message patch: {entries:#?}"));
    let answer_chunks: Vec<&String> = all_chunk_ids
        .iter()
        .filter(|id| **id == answer_id)
        .collect();
    assert!(
        !answer_chunks.is_empty(),
        "the answer's chunks must carry the patch's id {answer_id}: {all_chunk_ids:?}"
    );
    // Every chunk tag in the session is either the answer's or a distinct
    // message's, never an ungrouped stream: a run of one message's chunks is
    // contiguous, so no id may reappear after a different one.
    let mut seen: Vec<&String> = Vec::new();
    for id in &all_chunk_ids {
        if seen.last() != Some(&id) {
            assert!(
                !seen.contains(&id),
                "a message's chunks must be contiguous: {all_chunk_ids:?}"
            );
            seen.push(id);
        }
    }

    // The v2-only complete-message patch, carrying the authoritative content
    // under the same id so the client can replace what it accumulated.
    let patch = entries
        .iter()
        .find(|v| kind(v) == Some("agent_message"))
        .unwrap_or_else(|| panic!("expected an agent_message patch: {entries:#?}"));
    let patch_id = update(patch)["messageId"]
        .as_str()
        .expect("patch messageId");
    assert_eq!(
        patch_id, answer_id,
        "the patch must reuse the streamed message's id"
    );
    let content = update(patch)["content"]
        .as_array()
        .expect("patch content is a concrete array (v2 patch semantics)");
    assert!(
        content
            .iter()
            .any(|b| b["type"] == "text" && b["text"] == "hello from mock"),
        "patch carries the authoritative complete content: {content:#?}"
    );

    // v2 has no current_mode_update: it is skipped, never an error.
    assert!(
        !entries
            .iter()
            .any(|v| kind(v) == Some("current_mode_update")),
        "the v2 path must not emit current_mode_update"
    );
    // ...while the mode information v2 *does* have still arrives.
    assert!(
        entries
            .iter()
            .any(|v| kind(v) == Some("config_option_update")
                || kind(v) == Some("available_commands_update")),
        "v2 still publishes config options / commands"
    );
}

/// The v2 path must survive the mode-update-shaped events that v1 emits, i.e.
/// skipping `CurrentModeUpdate` rather than failing the turn.
#[tokio::test]
async fn v2_skips_current_mode_update_without_failing_the_turn() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let (log, _init, new) = drive_v2(
        Arc::new(AcpAgent::new(Config::from_env())),
        env.cwd.clone(),
        "hello",
    )
    .await;

    // The turn reached its terminal state despite the v1 session core having
    // emitted a mode update along the way (session/new publishes the thinking
    // mode; v2 drops exactly that frame).
    let entries = log.lock().await.clone();
    assert!(
        entries
            .iter()
            .any(|v| kind(v) == Some("state_update") && update(v)["state"] == "idle"),
        "the turn must still reach idle after the skipped mode update"
    );
    assert!(
        !entries
            .iter()
            .any(|v| kind(v) == Some("current_mode_update")),
        "current_mode_update must never reach a v2 client"
    );
    // The session is usable afterwards: its config options were published.
    assert!(!new.config_options.is_empty());
}

/// `session/resume` with `replayFrom: {"type":"start"}` replays the stored
/// conversation, exactly like v1 `session/load`.
#[tokio::test]
async fn v2_resume_with_start_replays_history() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();
    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = {
        let agent = agent.clone();
        tokio::spawn(async move { agent.run_with(agent_end).await })
    };

    let cwd = env.cwd.clone();
    Client
        .v2()
        .name("w562-v2-resume-client")
        .on_receive_notification(
            async move |notif: v2::UpdateSessionNotification, _cx| {
                let raw = serde_json::to_value(&notif).unwrap_or(serde_json::Value::Null);
                log_in_handler.lock().await.push(raw);
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(client_end, async move |cx| {
            cx.send_request(v2::InitializeRequest::new(
                ProtocolVersion::V2,
                v2::Implementation::new("w562-resume-client", "1.0.0"),
            ))
            .block_task()
            .await?;

            // Create a session so there is a stored conversation to resume.
            let created = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            let sid = created.session_id.clone();
            cx.send_request(v2::PromptRequest::new(
                sid.clone(),
                vec![v2::ContentBlock::Text(v2::TextContent::new("hello"))],
            ))
            .block_task()
            .await?;
            wait_for_raw(
                &log,
                |v| kind(v) == Some("state_update") && update(v)["state"] == "idle",
                "the first turn to settle",
            )
            .await;

            // Resume replaying from the start.
            let resumed = cx
                .send_request(
                    v2::ResumeSessionRequest::new(sid.clone(), &cwd)
                        .replay_from(v2::ReplayFrom::Start(v2::ReplayFromStart::new())),
                )
                .block_task()
                .await?;
            assert!(
                !resumed.config_options.is_empty(),
                "resume returns the session's config options"
            );

            // The replay shows up as ordinary session updates.
            wait_for_raw(
                &log,
                |v| {
                    let k = kind(v);
                    k == Some("user_message_chunk") || k == Some("agent_message_chunk")
                },
                "replayed history after session/resume",
            )
            .await;
            Ok(())
        })
        .await
        .expect("v2 resume client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");
}

/// `session/resume` publishes the replayed history **before** its response, so
/// the response is the client's completion boundary rather than a race.
///
/// The ACP SDK holds its dispatch loop for each registered callback before
/// routing the next frame, so "already in the log when the response returns"
/// means "already applied by the client". A client can therefore treat the
/// response as "history is complete" instead of guessing with a timeout.
#[tokio::test]
async fn v2_resume_history_is_complete_when_the_response_returns() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();
    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = {
        let agent = agent.clone();
        tokio::spawn(async move { agent.run_with(agent_end).await })
    };

    let cwd = env.cwd.clone();
    Client
        .v2()
        .name("w562-v2-boundary-client")
        .on_receive_notification(
            async move |notif: v2::UpdateSessionNotification, _cx| {
                let raw = serde_json::to_value(&notif).unwrap_or(serde_json::Value::Null);
                log_in_handler.lock().await.push(raw);
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(client_end, async move |cx| {
            cx.send_request(v2::InitializeRequest::new(
                ProtocolVersion::V2,
                v2::Implementation::new("w562-boundary-client", "1.0.0"),
            ))
            .block_task()
            .await?;

            let created = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            let sid = created.session_id.clone();
            cx.send_request(v2::PromptRequest::new(
                sid.clone(),
                vec![v2::ContentBlock::Text(v2::TextContent::new("hello"))],
            ))
            .block_task()
            .await?;
            wait_for_raw(
                &log,
                |v| kind(v) == Some("state_update") && update(v)["state"] == "idle",
                "the first turn to settle",
            )
            .await;

            // Everything from here on is what the resume publishes.
            let boundary = log.lock().await.len();

            cx.send_request(
                v2::ResumeSessionRequest::new(sid.clone(), &cwd)
                    .replay_from(v2::ReplayFrom::Start(v2::ReplayFromStart::new())),
            )
            .block_task()
            .await?;

            // Deliberately no `wait_for_raw`: the response is the boundary.
            let entries = log.lock().await;
            let after: Vec<&serde_json::Value> = entries.iter().skip(boundary).collect();
            let snapshot = after.iter().find(|v| kind(v) == Some("state_update")).expect("resume must synchronize current execution state before responding");
            assert_eq!(update(snapshot)["state"], "idle");
            assert!(update(snapshot).get("background").is_none(), "unrestricted sessions must retain unknown background coverage");
            let user = after
                .iter()
                .position(|v| matches!(kind(v),Some("user_message_chunk") | Some("user_message")))
                .expect(
                    "the replayed user message must already be applied when the response returns",
                );
            let assistant = after
                .iter()
                .position(|v| matches!(kind(v),Some("agent_message_chunk") | Some("agent_message")))
                .expect(
                    "the replayed assistant message must already be applied when the response returns",
                );
            assert!(
                user < assistant,
                "replay preserves the stored order (user message before the answer)"
            );
            let markers: Vec<_> = after.iter().enumerate()
                .filter(|(_, value)| kind(value) == Some("replay_complete"))
                .map(|(index, _)| index).collect();
            assert_eq!(markers.len(), 1, "one replay_complete must precede the response");
            let snapshot_index = after.iter().position(|value| kind(value) == Some("state_update")).unwrap();
            assert!(assistant < markers[0] && markers[0] < snapshot_index,
                "history, replay_complete, and the current snapshot must be ordered");
            assert_eq!(
                after
                    .iter()
                    .filter(|v| matches!(kind(v),Some("agent_message_chunk") | Some("agent_message")))
                    .count(),
                1,
                "loading history is not a turn: exactly the stored answer is replayed, \
                 with no second response"
            );
            Ok(())
        })
        .await
        .expect("v2 resume boundary client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");
}

/// `session/resume` that omits `replayFrom` resumes **without** replaying.
#[tokio::test]
async fn v2_resume_without_replay_from_does_not_replay() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();
    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = {
        let agent = agent.clone();
        tokio::spawn(async move { agent.run_with(agent_end).await })
    };

    let cwd = env.cwd.clone();
    let replayed = Arc::new(Mutex::new(false));
    let replayed_in_handler = replayed.clone();
    Client
        .v2()
        .name("w562-v2-no-replay-client")
        .on_receive_notification(
            async move |notif: v2::UpdateSessionNotification, _cx| {
                let raw = serde_json::to_value(&notif).unwrap_or(serde_json::Value::Null);
                log_in_handler.lock().await.push(raw);
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(client_end, async move |cx| {
            cx.send_request(v2::InitializeRequest::new(
                ProtocolVersion::V2,
                v2::Implementation::new("w562-no-replay-client", "1.0.0"),
            ))
            .block_task()
            .await?;
            let created = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            let sid = created.session_id.clone();
            cx.send_request(v2::PromptRequest::new(
                sid.clone(),
                vec![v2::ContentBlock::Text(v2::TextContent::new("hello"))],
            ))
            .block_task()
            .await?;
            wait_for_raw(
                &log,
                |v| kind(v) == Some("state_update") && update(v)["state"] == "idle",
                "the first turn to settle",
            )
            .await;

            // Record the history-replay updates seen *before* the resume, so
            // the assertion below compares against the post-resume delta.
            let before = log.lock().await.len();

            let resumed = cx
                .send_request(v2::ResumeSessionRequest::new(sid.clone(), &cwd))
                .block_task()
                .await?;
            assert!(!resumed.config_options.is_empty());

            cx.send_request(agent_client_protocol::UntypedMessage::new("session/resume",
                serde_json::json!({"sessionId": sid.0.as_ref(), "cwd": cwd.to_string_lossy(), "replayFrom": null}))
                .expect("null replay request"))
                .block_task().await?;

            // Give the (absent) replay a chance to arrive, then prove no
            // history chunks followed the resume response.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let entries = log.lock().await.clone();
            let history_after = entries[before..].iter().any(|v| {
                matches!(
                    kind(v),
                    Some("user_message_chunk") | Some("agent_message_chunk") |
                    Some("user_message") | Some("agent_message") | Some("replay_complete")
                )
            });
            *replayed_in_handler.lock().await = history_after;
            Ok(())
        })
        .await
        .expect("v2 no-replay client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");

    assert!(
        !*replayed.lock().await,
        "omitting replayFrom must resume without replaying history"
    );
}

/// An unknown `replayFrom` cursor is rejected, never guessed at.
#[tokio::test]
async fn v2_resume_rejects_unknown_replay_cursor() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = {
        let agent = agent.clone();
        tokio::spawn(async move { agent.run_with(agent_end).await })
    };

    let cwd = env.cwd.clone();
    Client
        .v2()
        .name("w562-v2-bad-cursor-client")
        .connect_with(client_end, async move |cx| {
            cx.send_request(v2::InitializeRequest::new(
                ProtocolVersion::V2,
                v2::Implementation::new("w562-bad-cursor-client", "1.0.0"),
            ))
            .block_task()
            .await?;
            let created = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            let sid = created.session_id.clone();

            // A future ACP variant (no `_` prefix): must be rejected.
            let raw = serde_json::json!({
                "sessionId": sid.0.as_ref(),
                "cwd": cwd.to_string_lossy(),
                "replayFrom": { "type": "someFutureCursor", "offset": 3 },
            });
            let err = cx
                .send_request(
                    agent_client_protocol::UntypedMessage::new("session/resume", raw)
                        .expect("untyped request"),
                )
                .block_task()
                .await
                .expect_err("an unknown replay cursor must be rejected");
            assert!(
                err.to_string().contains("someFutureCursor"),
                "the error must name the cursor it did not understand: {err}"
            );

            // An implementation-private `_`-prefixed cursor: also rejected
            // (it is not ours, and guessing a position is forbidden).
            let raw = serde_json::json!({
                "sessionId": sid.0.as_ref(),
                "cwd": cwd.to_string_lossy(),
                "replayFrom": { "type": "_privateCursor" },
            });
            let err = cx
                .send_request(
                    agent_client_protocol::UntypedMessage::new("session/resume", raw)
                        .expect("untyped request"),
                )
                .block_task()
                .await
                .expect_err("a private replay cursor must be rejected");
            assert!(
                err.to_string().contains("_privateCursor"),
                "the error must name the private cursor: {err}"
            );
            Ok(())
        })
        .await
        .expect("v2 bad-cursor client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");
}

/// Negotiation: a v2 client talking to a **v1-only** agent is told so
/// explicitly, instead of silently running a mismatched protocol.
///
/// This simulates the mismatch with a v1-only agent component, so it asserts
/// the negotiation failure path rather than the v2 success path.
#[tokio::test]
async fn v2_client_against_v1_only_agent_is_rejected() {
    let (agent_end, client_end) = Channel::duplex();
    let agent_task = tokio::spawn(async move {
        agent_client_protocol::Agent
            .v1()
            .on_receive_request(
                async |_init: InitializeRequest, responder, _cx| {
                    responder.respond(agent_client_protocol::v1::schema::InitializeResponse::new(
                        ProtocolVersion::V1,
                    ))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .connect_to(agent_end)
            .await
    });

    Client
        .v2()
        .name("w562-v2-client-vs-v1-agent")
        .connect_with(client_end, async |cx| {
            let err = cx
                .send_request(v2::InitializeRequest::new(
                    ProtocolVersion::V2,
                    v2::Implementation::new("w562-v2-client-vs-v1-agent", "1.0.0"),
                ))
                .block_task()
                .await
                .expect_err("a v1-only agent cannot serve a v2 client");
            let text = err.to_string();
            assert!(
                text.contains("version") || text.contains("protocol"),
                "the rejection must name the version the agent supports: {text}"
            );
            Ok(())
        })
        .await
        .expect("client driver failed");

    let _ = tokio::time::timeout(TIMEOUT, agent_task).await;
}

/// The v2 capability block advertises `session/close`, so the method must be
/// real: closing disposes the session and is idempotent for unknown ids.
#[tokio::test]
async fn v2_session_close_disposes_and_is_idempotent() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = tokio::spawn(async move { agent.run_with(agent_end).await });

    let cwd = env.cwd.clone();
    Client
        .v2()
        .name("w562-v2-close-client")
        .connect_with(client_end, async move |cx| {
            let init = cx
                .send_request(v2::InitializeRequest::new(
                    ProtocolVersion::V2,
                    v2::Implementation::new("w562-close-client", "1.0.0"),
                ))
                .block_task()
                .await?;
            // v2 requires a session capability block; `session/close` is
            // advertised and this test proves the method behind it is real.
            assert!(
                init.capabilities.session.is_some(),
                "v2 requires the session capability block"
            );

            let created = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            let sid = created.session_id.clone();

            // Closing a live session succeeds...
            cx.send_request(v2::CloseSessionRequest::new(sid.clone()))
                .block_task()
                .await?;
            // ...and closing it again is idempotent (the goal is already met).
            cx.send_request(v2::CloseSessionRequest::new(sid.clone()))
                .block_task()
                .await?;
            // An unknown id also succeeds: nothing to close is not an error.
            cx.send_request(v2::CloseSessionRequest::new("never-existed"))
                .block_task()
                .await?;
            Ok(())
        })
        .await
        .expect("v2 close client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");
}

/// The native v2 handlers for `session/set_config_option`, `session/list` and
/// `session/delete` answer in v2 types directly (no v1↔v2 conversion). The
/// config-option response in particular must use v2's `configId` key.
#[tokio::test]
async fn v2_config_option_list_and_delete_are_native() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = tokio::spawn(async move { agent.run_with(agent_end).await });

    let cwd = env.cwd.clone();
    Client
        .v2()
        .name("w562-v2-config-client")
        .connect_with(client_end, async move |cx| {
            cx.send_request(v2::InitializeRequest::new(
                ProtocolVersion::V2,
                v2::Implementation::new("w562-config-client", "1.0.0"),
            ))
            .block_task()
            .await?;

            let created = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            let sid = created.session_id.clone();

            // v2 expresses the thinking level as a config option; setting it
            // must answer with the refreshed v2 option set.
            let updated = cx
                .send_request(v2::SetSessionConfigOptionRequest::new(
                    sid.clone(),
                    v2::SessionConfigId::new("thought_level"),
                    v2::SessionConfigOptionValue::Id {
                        value: v2::SessionConfigValueId::new("low"),
                    },
                ))
                .block_task()
                .await?;
            assert!(
                !updated.config_options.is_empty(),
                "the v2 set_config_option response carries the refreshed options"
            );

            // `session/list` is native; the cwd filter round-trips through the
            // v2 `AbsolutePath` type.
            cx.send_request(v2::ListSessionsRequest::new().cwd(cwd.clone()))
                .block_task()
                .await?;

            // `session/delete` is idempotent, like v1's.
            cx.send_request(v2::DeleteSessionRequest::new(sid.clone()))
                .block_task()
                .await?;
            cx.send_request(v2::DeleteSessionRequest::new(sid.clone()))
                .block_task()
                .await?;
            Ok(())
        })
        .await
        .expect("v2 config client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");
}

#[tokio::test]
async fn v2_tip_fork_uses_a_distinct_persisted_branch() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();
    let source_file = env._tmp.path().join("agent/sessions/source-session.jsonl");
    std::fs::create_dir_all(source_file.parent().unwrap()).unwrap();
    let original = format!(
        "{{\"type\":\"session\",\"id\":\"source-session\",\"cwd\":\"{}\"}}\n{{\"type\":\"message\",\"message\":{{\"role\":\"user\",\"content\":\"before fork\"}}}}\n",
        env.cwd.display()
    );
    std::fs::write(&source_file, &original).unwrap();
    let (source_message_id, _) = pi_acp::conversation::insert(
        "source-session",
        Some("parent-insertion-key"),
        vec![v2::ContentBlock::Text(v2::TextContent::new("before fork"))],
    )
    .unwrap();
    let cwd = env.cwd.clone();
    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let notifications = log.clone();
    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = tokio::spawn(async move { agent.run_with(agent_end).await });

    Client
        .v2()
        .name("tip-fork-v2")
        .on_receive_notification(
            async move |notif: v2::UpdateSessionNotification, _cx| {
                notifications
                    .lock()
                    .await
                    .push(serde_json::to_value(notif).unwrap());
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(client_end, async move |cx| {
            let init = cx
                .send_request(v2::InitializeRequest::new(
                    ProtocolVersion::V2,
                    v2::Implementation::new("tip-fork-v2", "1"),
                ))
                .block_task()
                .await?;
            assert!(init.capabilities.session.as_ref().unwrap().fork.is_some());
            let forked = cx
                .send_request(v2::ForkSessionRequest::new("source-session", &cwd))
                .block_task()
                .await?;
            let child = forked.session_id;
            assert_ne!(child.0.as_ref(), "source-session");
            assert!(!forked.config_options.is_empty());
            assert!(
                pi_acp::conversation::lookup(child.0.as_ref(), Some("parent-insertion-key"))
                    .unwrap()
                    .is_none(),
                "fork does not copy parent-scoped insertion keys"
            );
            assert_eq!(
                pi_acp::conversation::lookup("source-session", Some("parent-insertion-key"))
                    .unwrap(),
                Some(source_message_id.clone())
            );
            cx.send_request(
                v2::ResumeSessionRequest::new(child.clone(), &cwd)
                    .replay_from(v2::ReplayFrom::Start(v2::ReplayFromStart::new())),
            )
            .block_task()
            .await?;
            assert!(
                log.lock()
                    .await
                    .iter()
                    .any(|v| kind(v) == Some("user_message")
                        && update(v)["messageId"] == source_message_id.0.as_ref()),
                "tip fork retains original ACP message identities"
            );
            let child_file = source_file.with_file_name(format!("{}.jsonl", child.0));
            assert!(std::fs::read_to_string(&child_file)
                .unwrap()
                .contains("before fork"));
            let raw = std::fs::read_to_string(&child_file).unwrap();
            let header: serde_json::Value =
                serde_json::from_str(raw.lines().next().unwrap()).unwrap();
            assert_eq!(
                header["parentSession"],
                source_file.to_string_lossy().as_ref()
            );
            assert_eq!(std::fs::read_to_string(&source_file).unwrap(), original);
            cx.send_request(v2::PromptRequest::new(
                child.clone(),
                vec![v2::ContentBlock::Text(v2::TextContent::new("continue"))],
            ))
            .block_task()
            .await?;
            wait_for_raw(
                &log,
                |v| kind(v) == Some("state_update") && update(v)["state"] == "idle",
                "forked prompt completion",
            )
            .await;
            cx.send_request(v2::ResumeSessionRequest::new(child.clone(), &cwd))
                .block_task()
                .await?;
            cx.send_request(v2::ResumeSessionRequest::new("source-session", &cwd))
                .block_task()
                .await?;
            assert_eq!(std::fs::read_to_string(&source_file).unwrap(), original);
            assert!(cx
                .send_request(v2::ForkSessionRequest::new("unknown-session", &cwd))
                .block_task()
                .await
                .is_err());
            Ok(())
        })
        .await
        .expect("v2 fork client failed");
    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// A v1 client still gets v1 from the same agent that serves v2: registering a
/// v2 implementation does not change v1 behavior.
#[tokio::test]
async fn v1_client_still_negotiates_v1_when_the_agent_serves_both() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    let env = MockEnv::new();

    let (agent_end, client_end) = Channel::duplex();
    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    let agent_task = tokio::spawn(async move { agent.run_with(agent_end).await });

    Client
        .v1()
        .name("w562-v1-client")
        .connect_with(client_end, async move |cx| {
            let init = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            assert_eq!(
                init.protocol_version,
                ProtocolVersion::V1,
                "a v1 client must still negotiate v1"
            );
            // The v1 auth surface is unchanged.
            assert!(!init.auth_methods.is_empty());

            let created = cx
                .send_request(NewSessionRequest::new(env.cwd.clone()))
                .block_task()
                .await?;
            assert_eq!(created.session_id.0.as_ref(), "mock-session-id");
            Ok(())
        })
        .await
        .expect("v1 client failed");

    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish")
        .expect("agent run_with task panicked")
        .expect("agent run_with returned an error");
}

/// Empty requested history still ends explicitly; unreadable history must not.
#[tokio::test]
async fn v2_resume_empty_or_unreadable_history_has_truthful_replay_boundary() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    for corrupt in [false, true] {
        let env = MockEnv::new();
        let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
        let received = log.clone();
        let (agent_end, client_end) = Channel::duplex();
        let agent = Arc::new(AcpAgent::new(Config::from_env()));
        let agent_task = tokio::spawn(async move { agent.run_with(agent_end).await });
        Client
            .v2()
            .on_receive_notification(
                async move |notification: v2::UpdateSessionNotification, _cx| {
                    received
                        .lock()
                        .await
                        .push(serde_json::to_value(notification).unwrap());
                    Ok(())
                },
                on_receive_notification!(),
            )
            .connect_with(client_end, async move |cx| {
                cx.send_request(v2::InitializeRequest::new(
                    ProtocolVersion::V2,
                    v2::Implementation::new("replay-boundary", "1"),
                ))
                .block_task()
                .await?;
                let created = cx
                    .send_request(v2::NewSessionRequest::new(&env.cwd))
                    .block_task()
                    .await?;
                // Seed native persistence, then control the retained ACP history.
                // The mock provider is local and never contacts a model service.
                cx.send_request(v2::PromptRequest::new(
                    created.session_id.clone(),
                    vec![v2::ContentBlock::Text(v2::TextContent::new("seed"))],
                ))
                .block_task()
                .await?;
                wait_for_raw(
                    &log,
                    |value| kind(value) == Some("state_update") && update(value)["state"] == "idle",
                    "seed turn settlement",
                )
                .await;
                let encoded = created
                    .session_id
                    .0
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                let directory = env._tmp.path().join("agent/pi-acp/conversations");
                std::fs::create_dir_all(&directory).unwrap();
                let retained = if corrupt {
                    b"{".as_slice()
                } else {
                    br#"{"keys":{},"updates":[]}"#.as_slice()
                };
                std::fs::write(directory.join(format!("{encoded}.json")), retained).unwrap();
                let boundary = log.lock().await.len();
                let resumed = cx
                    .send_request(
                        v2::ResumeSessionRequest::new(created.session_id, &env.cwd)
                            .replay_from(v2::ReplayFrom::Start(v2::ReplayFromStart::new())),
                    )
                    .block_task()
                    .await;
                let entries = log.lock().await;
                let after: Vec<_> = entries.iter().skip(boundary).collect();
                let markers: Vec<_> = after
                    .iter()
                    .enumerate()
                    .filter(|(_, value)| kind(value) == Some("replay_complete"))
                    .map(|(index, _)| index)
                    .collect();
                if corrupt {
                    assert!(
                        resumed.is_err(),
                        "unreadable retained history must reject replay"
                    );
                    assert!(
                        markers.is_empty(),
                        "failed replay cannot emit replay_complete"
                    );
                } else {
                    assert!(resumed.is_ok());
                    assert_eq!(
                        markers.len(),
                        1,
                        "empty successful replay needs exactly one marker"
                    );
                    let snapshot = after
                        .iter()
                        .position(|value| kind(value) == Some("state_update"))
                        .unwrap();
                    assert!(
                        markers[0] < snapshot,
                        "current execution state follows the replay marker"
                    );
                }
                Ok(())
            })
            .await
            .expect("replay boundary client");
        tokio::time::timeout(TIMEOUT, agent_task)
            .await
            .expect("agent shutdown")
            .expect("agent task")
            .expect("agent result");
    }
}
