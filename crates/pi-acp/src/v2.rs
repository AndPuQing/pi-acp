//! Native ACP v2 inbound handlers.
//!
//! One binary serves both versions. In `initialize` the SDK's
//! [`AgentProtocolRouter`](agent_client_protocol::AgentProtocolRouter) decides
//! which of two **native** implementations owns the connection; this module is
//! the v2 one's request surface. It parses v2 method names and v2 types, drives
//! the same shared engine as the v1 handlers (through the protocol-neutral
//! parameters and facts in [`crate::agent`]), and builds v2 responses directly.
//! No v1 frame is ever converted into a v2 one.
//!
//! Two v2 design changes are handled here rather than by a converter:
//!
//! - **`replayFrom`**: v2's resume cursor has exactly two shapes, `start` and
//!   an unknown future cursor. An unknown cursor is *rejected* rather than
//!   guessed at (see [`plan_resume`]).
//! - **`session/set_mode`**: v2 removed session modes, so it stays
//!   `methodNotFound` rather than silently accepting a method v2 does not
//!   define.

use std::sync::Arc;

use agent_client_protocol::{ConnectionTo, Dispatch, Handled, UntypedMessage};
use agent_client_protocol_schema::v2;
use serde_json::Value;

use crate::agent::AcpAgent;

/// `ConnectionTo<Client>` alias (the Agent role's counterpart).
type Client = agent_client_protocol::Client;

/// ACP `methodNotFound` JSON-RPC code.
const ACP_METHOD_NOT_FOUND: i32 = -32601;
/// ACP `invalidParams` JSON-RPC code.
const ACP_INVALID_PARAMS: i32 = -32602;

fn method_not_found(method: &str, protocol: &str) -> agent_client_protocol::Error {
    agent_client_protocol::Error::new(
        ACP_METHOD_NOT_FOUND,
        format!("`{method}` is not an ACP {protocol} method"),
    )
}

fn invalid_params(message: impl ToString) -> agent_client_protocol::Error {
    agent_client_protocol::Error::new(ACP_INVALID_PARAMS, message.to_string())
}

fn parse<T: serde::de::DeserializeOwned>(
    params: &Value,
    method: &str,
) -> Result<T, agent_client_protocol::Error> {
    serde_json::from_value(params.clone()).map_err(|e| invalid_params(format!("{method}: {e}")))
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, agent_client_protocol::Error> {
    serde_json::to_value(value)
        .map_err(|e| invalid_params(format!("failed to serialize response: {e}")))
}

/// Handle one raw ACP v2 request by dispatching it to the native v2 handler.
///
/// Returns [`Handled::No`] for a method this adapter does not implement, so the
/// SDK answers with `methodNotFound` rather than the adapter inventing one.
pub async fn handle_dispatch(
    agent: &Arc<AcpAgent>,
    dispatch: Dispatch<UntypedMessage, UntypedMessage>,
    cx: &ConnectionTo<Client>,
) -> Result<Handled<Dispatch<UntypedMessage, UntypedMessage>>, agent_client_protocol::Error> {
    let Dispatch::Request(message, responder) = dispatch else {
        // Notifications are handled by their own typed registrations.
        return Ok(Handled::No {
            message: dispatch,
            retry: false,
        });
    };

    let method = message.method.clone();
    let params = message.params.clone();

    match method.as_str() {
        "session/new" => {
            let request: v2::NewSessionRequest = parse(&params, &method)?;
            match agent.handle_new_session_v2(&request, cx).await {
                Ok((response, post)) => {
                    // Publish the empty context state before the handler
                    // returns, so a client cannot race the post-response task
                    // with its first prompt (same ordering as v1).
                    let _ = post.session.publish_initial_usage().await;
                    responder.respond(to_value(response)?)?;
                    let cx_for_task = cx.clone();
                    let protocol = agent.protocol();
                    cx.spawn(async move {
                        post.send(&cx_for_task, protocol).await;
                        Ok(())
                    })?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/prompt" => {
            let request: v2::PromptRequest = parse(&params, &method)?;
            let agent = agent.clone();
            let cx_for_task = cx.clone();
            // The handler runs the turn; keep the SDK dispatch loop free for
            // `session/cancel` by spawning.
            cx.spawn(async move {
                match agent.handle_prompt_v2(&request, &cx_for_task).await {
                    Ok((response, persisted)) => {
                        let answered = responder.respond(to_value(response)?);
                        if let Some(session) = persisted {
                            agent.persist_session_if_ready(&session).await;
                        }
                        answered
                    }
                    Err(e) => responder.respond_with_error(e),
                }
            })?;
            Ok(Handled::Yes)
        }
        "session/resume" => {
            let request = parse::<v2::ResumeSessionRequest>(&params, &method)?;
            let replay = match plan_resume(&request) {
                Ok(replay) => replay,
                Err(e) => {
                    responder.respond_with_error(e)?;
                    return Ok(Handled::Yes);
                }
            };
            match agent.handle_resume_session_v2(request, replay, cx).await {
                Ok((response, post)) => {
                    // Publish before responding so the response is the client's
                    // completion boundary, matching the v1 `session/load`
                    // handler.
                    let protocol = agent.protocol();
                    post.send(cx, protocol).await;
                    responder.respond(to_value(response)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/close" => {
            // v2's capability model has no `close` flag, but the method exists
            // and closing is idempotent.
            let request: v2::CloseSessionRequest = parse(&params, &method)?;
            match agent.handle_close_session_v2(&request).await {
                Ok(response) => {
                    responder.respond(to_value(response)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/list" => {
            let request: v2::ListSessionsRequest = parse(&params, &method)?;
            match agent.handle_list_sessions_v2(&request).await {
                Ok(response) => {
                    responder.respond(to_value(response)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/delete" => {
            let request: v2::DeleteSessionRequest = parse(&params, &method)?;
            match agent.handle_delete_session_v2(&request).await {
                Ok(response) => {
                    responder.respond(to_value(response)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/set_config_option" => {
            let request: v2::SetSessionConfigOptionRequest = parse(&params, &method)?;
            match agent.handle_set_config_option_v2(&request, cx).await {
                Ok(response) => {
                    responder.respond(to_value(response)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/set_model" => {
            // v2 has no typed request for this unstable method either, so the
            // shared raw-JSON handler serves both versions.
            let agent = agent.clone();
            let cx_for_task = cx.clone();
            cx.spawn(async move {
                match agent.handle_set_session_model(&params, &cx_for_task).await {
                    Ok(()) => responder.respond(serde_json::json!({})),
                    Err(e) => responder.respond_with_error(e),
                }
            })?;
            Ok(Handled::Yes)
        }
        "auth/login" => {
            // v2's successor to `authenticate`. pi-acp's is a no-op: real auth
            // runs out-of-band through the terminal-login flow.
            let _request: v2::LoginAuthRequest = parse(&params, &method)?;
            responder.respond(to_value(v2::LoginAuthResponse::new())?)?;
            Ok(Handled::Yes)
        }
        // v2 removed session modes entirely, so a conforming client never sends
        // this. Answering `methodNotFound` states that plainly instead of
        // silently accepting a method the version does not define.
        "session/set_mode" => {
            responder.respond_with_error(method_not_found(&method, "v2"))?;
            Ok(Handled::Yes)
        }
        _ => Ok(Handled::No {
            message: Dispatch::Request(message, responder),
            retry: false,
        }),
    }
}

/// Enforce v2's replay-cursor semantics, returning whether to replay history.
///
/// v2's `ReplayFrom` has exactly two shapes: `Start` (replay everything) and an
/// untagged `Other` for cursors this version does not define. There is no
/// "resume from an arbitrary position" variant, so:
///
/// - `replayFrom: {type: "start"}` → replay the whole conversation (the v1
///   `session/load` equivalent);
/// - `replayFrom` omitted/`null` → resume without replaying;
/// - anything else — a future ACP variant *or* an implementation-private
///   `_`-prefixed extension — is rejected. The schema is explicit that a
///   receiver which does not understand a cursor must *"reject the request
///   rather than guessing where to replay from"*.
fn plan_resume(request: &v2::ResumeSessionRequest) -> Result<bool, agent_client_protocol::Error> {
    match &request.replay_from {
        Some(v2::ReplayFrom::Start(_)) => Ok(true),
        Some(other_cursor) => Err(invalid_params(format!(
            "session/resume: unknown replayFrom cursor `{}`; this agent supports only \
             `{{\"type\":\"start\"}}` or no replayFrom at all",
            match other_cursor {
                v2::ReplayFrom::Other(other) => other.type_.clone(),
                v2::ReplayFrom::Start(_) => "start".to_string(),
                _ => "unknown".to_string(),
            }
        ))),
        None => Ok(false),
    }
}
