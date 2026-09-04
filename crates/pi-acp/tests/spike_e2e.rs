//! S2 runtime spike e2e: drive the built `pi-acp` binary as an ACP client and
//! run one real text round-trip through it to a live `pi --mode rpc` subprocess.
//!
//! This is the acceptance test for the runtime decision (design D9 / §5.3):
//! the ACP SDK's `Stdio` transport — which internally uses `blocking`/`async-io`
//! — must be driven correctly under a `tokio` multi-thread runtime, and a real
//! pi round-trip must flow through both bridges (client ⇄ pi-acp ⇄ pi).
//!
//! It is `#[ignore]`d because it needs `pi` on PATH plus a working LLM backend,
//! none of which exist in CI. Run it locally with:
//!
//! ```sh
//! cargo test -p pi-acp --test spike_e2e -- --ignored --nocapture
//! ```
//!
//! [`full_chain_against_real_pi`] covers the pre-release manual gate the design
//! §9 asks for (initialize → new → prompt → load → list → delete) against a
//! real pi + LLM backend, isolated to a temp agent dir so the real `~/.pi` is
//! never touched.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    ContentBlock, DeleteSessionRequest, InitializeRequest, ListSessionsRequest, LoadSessionRequest,
    NewSessionRequest, PromptRequest, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{on_receive_notification, AcpAgent, AcpAgentConfig};

/// The prompt we send; the LLM is instructed to echo exactly one word so the
/// assertion is robust across models.
const PROMPT: &str = "Reply with exactly the single word: pong";

/// Fail early with a clear message if `pi` is not launchable.
fn assert_pi_available() -> String {
    let pi = pi_acp::config::Config::from_env().pi_command;
    if let Err(e) = std::process::Command::new(&pi).arg("--version").output() {
        panic!(
            "`{pi}` could not be launched (needed for the e2e spike): {e}. \
             Install pi or set PI_ACP_PI_COMMAND."
        );
    }
    pi
}

/// Append streamed assistant text chunks to the shared buffer.
async fn record_text(streamed: Arc<tokio::sync::Mutex<String>>, notif: SessionNotification) {
    if let SessionUpdate::AgentMessageChunk(chunk) = &notif.update {
        if let ContentBlock::Text(text) = &chunk.content {
            streamed.lock().await.push_str(&text.text);
        }
    }
}

/// A temp pi agent dir with the real one's provider/model/auth config mirrored
/// in, so pi works against the LLM backend while session files land under the
/// temp dir — the real `~/.pi` is never written to. Returns the temp dir
/// (kept alive for the test's duration).
fn isolated_agent_dir() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let agent_dir = tmp.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    // Best-effort: a local pi without these files is fine.
    let real_agent_dir = pi_acp::settings::agent_dir();
    for name in [
        "auth.json",
        "models.json",
        "models-store.json",
        "settings.json",
    ] {
        let src = real_agent_dir.join(name);
        if src.exists() {
            let _ = std::fs::copy(src, agent_dir.join(name));
        }
    }
    tmp
}

#[tokio::test]
#[ignore = "needs `pi` on PATH and a working LLM backend (run locally, see module docs)"]
async fn one_real_text_round_trip() {
    assert_pi_available();

    // Collected assistant text, streamed to the client as session/update
    // notifications during the prompt turn.
    let streamed = Arc::new(tokio::sync::Mutex::new(String::new()));
    let streamed_in_handler = streamed.clone();

    let agent = AcpAgent::new(
        AcpAgentConfig::new(env!("CARGO_BIN_EXE_pi-acp")).env("RUST_LOG", "info,pi_acp=debug"),
    );

    agent_client_protocol::Client
        .builder()
        .name("spike-client")
        .on_receive_notification(
            async move |notif: SessionNotification, _cx| {
                record_text(streamed_in_handler.clone(), notif).await;
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(agent, async |cx| {
            // 1. initialize
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            eprintln!("[e2e] initialized");

            // 2. session/new (agent spawns pi here)
            let cwd = std::env::temp_dir();
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd))
                .block_task()
                .await?;
            eprintln!("[e2e] session: {}", new_session.session_id);

            // 3. prompt — the agent forwards to pi, streams deltas, and only
            //    responds once pi has settled.
            let prompt_response = cx
                .send_request(PromptRequest::new(
                    new_session.session_id,
                    vec![ContentBlock::Text(TextContent::new(PROMPT.to_string()))],
                ))
                .block_task()
                .await?;
            eprintln!(
                "[e2e] prompt stop_reason: {:?}",
                prompt_response.stop_reason
            );

            assert_eq!(prompt_response.stop_reason, StopReason::EndTurn);
            Ok(())
        })
        .await
        .expect("ACP client session should complete without error");

    let text = streamed.lock().await.clone();
    eprintln!("[e2e] streamed assistant text: {text:?}");
    assert!(
        text.contains("pong"),
        "streamed text should contain 'pong', got: {text:?}"
    );
}

/// The full pre-release chain (design §9): initialize → session/new → prompt
/// → load → list → delete against a **real** pi + LLM backend. Runs with a
/// temp `PI_CODING_AGENT_DIR` so session files land there and `session/delete`
/// cleans up; the real `~/.pi` is never touched.
#[tokio::test]
#[ignore = "needs `pi` on PATH and a working LLM backend (run locally, see module docs)"]
async fn full_chain_against_real_pi() {
    assert_pi_available();

    let tmp = isolated_agent_dir();
    let agent_dir = tmp.path().join("agent");
    // pi (spawned by the agent) inherits the agent's process cwd, and
    // `session/list` filters on the session header's cwd — so use the process
    // cwd, not a temp dir, or the list filter would drop the session.
    let cwd = std::env::current_dir().unwrap();

    let streamed = Arc::new(tokio::sync::Mutex::new(String::new()));
    let streamed_in_handler = streamed.clone();

    let agent = AcpAgent::new(
        AcpAgentConfig::new(env!("CARGO_BIN_EXE_pi-acp"))
            .env("PI_CODING_AGENT_DIR", agent_dir.to_str().unwrap())
            .env("RUST_LOG", "info,pi_acp=debug"),
    );

    agent_client_protocol::Client
        .builder()
        .name("spike-full-chain-client")
        .on_receive_notification(
            async move |notif: SessionNotification, _cx| {
                record_text(streamed_in_handler.clone(), notif).await;
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(agent, async |cx| {
            // 1. initialize
            let init = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            assert_eq!(init.agent_info.as_ref().unwrap().name, "pi-acp");
            eprintln!("[e2e-full] initialized");

            // 2. session/new — agent spawns real pi; the session file is
            //    persisted under the temp agent dir.
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd.clone()))
                .block_task()
                .await?;
            let sid = new_session.session_id.clone();
            eprintln!("[e2e-full] session: {sid}");

            // 3. prompt — one real LLM round trip; the turn settles only on
            //    pi's `agent_settled`.
            let prompt_response = cx
                .send_request(PromptRequest::new(
                    sid.clone(),
                    vec![ContentBlock::Text(TextContent::new(PROMPT.to_string()))],
                ))
                .block_task()
                .await?;
            eprintln!(
                "[e2e-full] prompt stop_reason: {:?}",
                prompt_response.stop_reason
            );
            assert_eq!(prompt_response.stop_reason, StopReason::EndTurn);

            // 4. session/load — resume the session just created (pi re-reads
            //    the persisted session file).
            let load = cx
                .send_request(LoadSessionRequest::new(sid.clone(), cwd.clone()))
                .block_task()
                .await?;
            eprintln!("[e2e-full] load: {} configOptions", {
                load.config_options.as_ref().map(Vec::len).unwrap_or(0)
            });

            // 5. session/list — the persisted session must show up.
            let list = cx
                .send_request(ListSessionsRequest::new())
                .block_task()
                .await?;
            eprintln!("[e2e-full] list: {} sessions", list.sessions.len());
            assert!(
                list.sessions.iter().any(|s| s.session_id == sid),
                "session/list must contain {sid}: {:?}",
                list.sessions
                    .iter()
                    .map(|s| s.session_id.0.clone())
                    .collect::<Vec<_>>()
            );

            // 6. session/delete — idempotent removal of the session file.
            cx.send_request(DeleteSessionRequest::new(sid.clone()))
                .block_task()
                .await?;
            eprintln!("[e2e-full] deleted {sid}");

            Ok(())
        })
        .await
        .expect("full-chain ACP session should complete without error");

    // The prompt's streamed text carried the LLM echo through both bridges.
    let text = streamed.lock().await.clone();
    eprintln!("[e2e-full] streamed assistant text: {text:?}");
    assert!(
        text.contains("pong"),
        "streamed text should contain 'pong', got: {text:?}"
    );
}
