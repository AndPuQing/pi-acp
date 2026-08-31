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

use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, NewSessionRequest, PromptRequest, SessionNotification,
    SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{on_receive_notification, AcpAgent, AcpAgentConfig};

/// The prompt we send; the LLM is instructed to echo exactly one word so the
/// assertion is robust across models.
const PROMPT: &str = "Reply with exactly the single word: pong";

#[tokio::test]
#[ignore = "needs `pi` on PATH and a working LLM backend (run locally, see module docs)"]
async fn one_real_text_round_trip() {
    // The agent spawns `pi`; fail early with a clear message if it is missing.
    let pi = pi_acp::config::Config::from_env().pi_command;
    if let Err(e) = std::process::Command::new(&pi).arg("--version").output() {
        panic!(
            "`{pi}` could not be launched (needed for the e2e spike): {e}. \
             Install pi or set PI_ACP_PI_COMMAND."
        );
    }

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
                if let SessionUpdate::AgentMessageChunk(chunk) = &notif.update {
                    if let ContentBlock::Text(text) = &chunk.content {
                        streamed_in_handler.lock().await.push_str(&text.text);
                    }
                }
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
