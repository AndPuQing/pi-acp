//! W-559: drive [`AcpAgent`] **in-process** over an injected transport.
//!
//! `AcpAgent::run_with` replaces the hardcoded `Stdio` transport of
//! [`AcpAgent::run`] with a caller-supplied one, so a host application (loom)
//! can reuse pi-acp's translation stack directly instead of spawning a second
//! process. These tests wire the agent to a test ACP client over
//! [`Channel::duplex`] — an in-process, frame-preserving transport pair — and
//! prove the full handshake works: `initialize` → `session/new` →
//! `session/prompt` → streamed `SessionUpdate` notifications → the turn's
//! terminal state.
//!
//! The default test runs against the in-binary **mock pi** (`PI_ACP_MOCK=1`),
//! so CI needs no real pi. `in_process_against_real_pi` is the same flow
//! against a real `pi` and is `#[ignore]`d (it needs auth + a live backend).
//!
//! Unlike `acp_agent.rs` (which drives the built binary as a child process),
//! the agent runs **inside** this test process, so the environment it reads
//! (`PI_ACP_PI_COMMAND`, `PI_CODING_AGENT_DIR`, …) is this process's own. The
//! two tests here mutate shared process env and are serialized by `ENV_LOCK`.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, NewSessionRequest, PromptRequest, SessionNotification,
    SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{on_receive_notification, Channel, Client};
use pi_acp::agent::AcpAgent;
use pi_acp::config::Config;
use tokio::sync::Mutex;

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");
const TIMEOUT: Duration = Duration::from_secs(15);

/// Serializes the process-env mutation the in-process tests require.
static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Everything the agent notified us, in order.
type NotifLog = Arc<Mutex<Vec<SessionUpdate>>>;

/// Wait until `pred` sees a matching notification (bounded poll).
async fn wait_for<F>(log: &NotifLog, pred: F) -> SessionUpdate
where
    F: Fn(&SessionUpdate) -> bool,
{
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        {
            let entries = log.lock().await;
            if let Some(u) = entries.iter().find(|u| pred(u)) {
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

/// The in-process handshake shared by both tests: connect a test ACP client to
/// `agent` over a `Channel::duplex` pair and run the four-step flow.
///
/// Returns the agent task's result, which the caller asserts on — a clean
/// client disconnect must end `run_with` with `Ok`.
async fn drive_in_process(
    agent: Arc<AcpAgent>,
    cwd: std::path::PathBuf,
    // `Some(text)` requires a streamed text chunk containing `text`; `None`
    // accepts any agent message chunk (real-pi turn content is not
    // predictable, only its presence is).
    expect_text: Option<&'static str>,
) -> pi_acp::error::Result<()> {
    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();

    let (agent_end, client_end) = Channel::duplex();

    let agent_task = {
        let agent = agent.clone();
        tokio::spawn(async move { agent.run_with(agent_end).await })
    };

    Client
        .builder()
        .name("w559-in-process-client")
        .on_receive_notification(
            async move |notif: SessionNotification, _cx| {
                log_in_handler.lock().await.push(notif.update.clone());
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(client_end, async move |cx| {
            // 1. initialize — handler registered on the injected transport.
            let init = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            assert_eq!(init.agent_info.as_ref().unwrap().name, "pi-acp");

            // 2. session/new — spawns the (mock) pi child, publishes startup
            // info + available commands + the initial usage snapshot.
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd.clone()))
                .block_task()
                .await?;
            let sid = new_session.session_id.clone();
            assert!(
                new_session.config_options.is_some(),
                "configOptions must survive the injected transport"
            );

            // Notifications flow over the in-process channel in both cases:
            // before the first prompt the agent publishes the empty context
            // state (usage_update) for the session.
            let usage = wait_for(
                &log,
                |u| matches!(u, SessionUpdate::UsageUpdate(uu) if uu.used == 0 && uu.size == 1000),
            )
            .await;
            let SessionUpdate::UsageUpdate(uu) = usage else {
                unreachable!()
            };
            assert_eq!(uu.used, 0);

            // 3. session/prompt — the mock streams one text delta and only then
            // signals `agent_settled`; `StopReason::EndTurn` is therefore the
            // ACP-visible terminal state produced by that settled event.
            let prompt_resp = cx
                .send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new("hello".to_string()))],
                ))
                .block_task()
                .await?;
            assert_eq!(prompt_resp.stop_reason, StopReason::EndTurn);

            // 4. the streamed SessionUpdate notification arrived on this side.
            wait_for(&log, |u| {
                matches!(u, SessionUpdate::AgentMessageChunk(c)
                    if matches!(&c.content, ContentBlock::Text(t)
                        if expect_text.is_none_or(|want| t.text.contains(want))))
            })
            .await;

            Ok(())
        })
        .await
        .expect("in-process ACP client failed");

    // Dropping the client end must end `run_with` cleanly, not with a transport
    // error: prove the agent task terminates on its own.
    tokio::time::timeout(TIMEOUT, agent_task)
        .await
        .expect("agent run_with did not finish after the client disconnected")
        .expect("agent run_with task panicked")
}

/// The default (dependency-free) proof: a full handshake between `AcpAgent` and
/// an ACP client inside one process, against the in-binary mock pi.
#[tokio::test]
async fn in_process_channel_against_mock_pi() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    let agent_dir = tmp.path().join("agent");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&agent_dir).unwrap();

    // Read by the in-process agent (`Config::from_env`, `settings::agent_dir`)
    // and inherited by the mock pi child it spawns.
    std::env::set_var("PI_ACP_MOCK", "1");
    std::env::set_var("PI_ACP_PI_COMMAND", BIN);
    std::env::set_var("PI_CODING_AGENT_DIR", &agent_dir);
    std::env::set_var(
        "PI_ACP_MOCK_SESSION_FILE",
        tmp.path().join("sessions/mock-session.jsonl"),
    );

    let agent = Arc::new(AcpAgent::new(Config::from_env()));
    drive_in_process(agent, cwd, Some("hello from mock"))
        .await
        .unwrap();
}

/// Same flow against a **real** `pi` (auth + backend required), proving the
/// injected transport carries a genuine turn end to end. Ignored by default:
/// run with `cargo test -- --ignored in_process_against_real_pi`.
///
/// Deliberately keeps the caller's real pi agent dir (that is where the auth
/// configuration lives), so a real session mapping is written there.
#[tokio::test]
#[ignore = "requires a real pi binary with configured auth"]
async fn in_process_against_real_pi() {
    let _env_guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();

    // This test must drive real pi: clear the mock wiring the sibling test sets.
    std::env::remove_var("PI_ACP_MOCK");
    std::env::remove_var("PI_ACP_PI_COMMAND");
    std::env::remove_var("PI_ACP_MOCK_SESSION_FILE");
    std::env::remove_var("PI_CODING_AGENT_DIR");

    let agent = Arc::new(AcpAgent::new(Config::default()));
    // Real-pi turn content is not predictable; assert that a streamed
    // assistant chunk arrives, whatever it says.
    drive_in_process(agent, cwd, None).await.unwrap();
}
