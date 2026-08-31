//! ACP `Agent` role implementation (S2 spike).
//!
//! Serves an ACP client over stdio and bridges the three minimal methods to a
//! real `pi --mode rpc` child process:
//!
//! - `initialize`   — advertise the (empty) agent capabilities.
//! - `session/new`  — spawn `pi`, learn its session id via `get_state`.
//! - `session/prompt` — send a `prompt` to pi, stream assistant text deltas back
//!   as `session/update` notifications, and finish the turn on the
//!   `agent_settled` event (not the early `prompt` response).
//!
//! This is the **runtime spike**: it proves the official ACP SDK's `Stdio`
//! transport (which internally uses `blocking`/`async-io`) is driven correctly
//! under a `tokio` multi-thread runtime, and that a real pi text round-trip
//! flows through both bridges (design D9 / §5.3). S6 replaces this minimal
//! wiring with the full method set and moves the logic into `session` +
//! `translate`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, Implementation, InitializeRequest,
    InitializeResponse, NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse,
    SessionId, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{on_receive_request, Agent, Result, Stdio};
use tokio::sync::Mutex;

use crate::config::Config;
use crate::pi::process::{text_delta_of, PiProcess};

/// ACP `invalidParams` JSON-RPC code, used for an unknown session id.
const ACP_INVALID_PARAMS: i32 = -32602;

/// Session state shared across the per-method handlers.
type Sessions = Arc<Mutex<HashMap<SessionId, PiProcess>>>;

/// Run the ACP agent over stdio until the client disconnects (stdin EOF).
///
/// This future is meant to be driven by a `tokio` runtime (see `main.rs`); the
/// spike validates exactly that this works with the SDK's `Stdio` transport.
pub async fn run() -> Result<()> {
    let cfg = Config::from_env();
    let timeout = Duration::from_secs(cfg.rpc_timeout_secs);
    let pi_command = cfg.pi_command.clone();
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    // Each `async move` handler captures the map by move; give the two that
    // need it their own clones (the Arc is not Copy).
    let sessions_new = sessions.clone();
    let sessions_prompt = sessions.clone();

    Agent
        .builder()
        .name("pi-acp")
        .on_receive_request(
            async move |_req: InitializeRequest, responder, _cx| {
                tracing::info!("ACP initialize");
                let resp = InitializeResponse::new(_req.protocol_version)
                    .agent_capabilities(AgentCapabilities::new())
                    .agent_info(Implementation::new("pi-acp", env!("CARGO_PKG_VERSION")));
                responder.respond(resp)
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: NewSessionRequest, responder, _cx| {
                tracing::info!(cwd = %req.cwd.display(), "ACP session/new");
                let mut pi = PiProcess::spawn(&pi_command, timeout).await?;
                let state = pi.get_state().await?;
                tracing::info!(session_id = %state.session_id, "pi session ready");
                let session_id: SessionId = state.session_id.into();
                sessions_new.lock().await.insert(session_id.clone(), pi);
                responder.respond(NewSessionResponse::new(session_id))
            },
            on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest, responder, cx| {
                let text = prompt_text(&req);
                tracing::info!(
                    session = %req.session_id,
                    chars = text.len(),
                    "ACP session/prompt"
                );

                // Take the pi handle out so its guard lifetime stays local to
                // the critical section; re-insert it after the pump.
                let pi = {
                    let mut sessions = sessions_prompt.lock().await;
                    sessions.remove(&req.session_id)
                };
                let mut pi = match pi {
                    Some(pi) => pi,
                    None => {
                        return responder.respond_with_error(agent_client_protocol::Error::new(
                            ACP_INVALID_PARAMS,
                            format!("unknown session: {}", req.session_id),
                        ));
                    }
                };

                // Stream assistant text deltas to the client as session/update
                // notifications while pumping pi events until `agent_settled`.
                let sid_for_stream = req.session_id.clone();
                let sid_for_reinsert = req.session_id.clone();
                let pump = pi.prompt_until_settled(&text, move |event| {
                    if let Some(delta) = text_delta_of(event) {
                        let notif = SessionNotification::new(
                            sid_for_stream.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(delta.to_string())),
                            )),
                        );
                        let _ = cx.send_notification(notif);
                    }
                });
                let result = pump.await;

                {
                    let mut sessions = sessions_prompt.lock().await;
                    sessions.insert(sid_for_reinsert, pi);
                }

                result?;
                tracing::info!("prompt turn settled");
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            on_receive_request!(),
        )
        .connect_to(Stdio::new())
        .await
}

/// Concatenate the text blocks of an ACP prompt (the spike only needs text; S4
/// adds images / resource blocks).
fn prompt_text(req: &PromptRequest) -> String {
    req.prompt
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(t) => Some(t.text.clone()),
            _ => None,
        })
        .collect()
}
