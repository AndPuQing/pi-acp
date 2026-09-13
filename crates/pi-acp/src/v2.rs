//! ACP v2 boundary adapter (W-562).
//!
//! On a v2 connection the SDK speaks **v2 method names and v2 types**, while
//! pi-acp's whole session core is v1. Rather than re-typing the core (which the
//! issue forbids), this module adapts at the edge: it takes the raw v2 request,
//! maps it to the equivalent v1 request, calls the *same* v1 handler, and
//! converts the v1 response back to v2. Outbound `session/update` notifications
//! take the mirror-image path in [`crate::protocol`].
//!
//! Two v1→v2 conversions are lossy by design and are resolved here rather than
//! by guessing:
//!
//! - **`modes`**: v2 removed session modes (`session/set_mode` is gone; modes are
//!   expressed through config options). A v1 response carrying `modes` cannot be
//!   converted, so the v2 response drops that field — the decision table's "v2
//!   expresses mode via config options" — and keeps `configOptions`, which is
//!   where pi-acp already publishes its model and thinking selectors.
//! - **`authMethods`**: v2 requires that advertising any auth method implies
//!   implementing the `auth/login` *and* `auth/logout` pair. pi-acp implements
//!   `authenticate` as a no-op and has no logout; its real auth mechanism is the
//!   v1 terminal-login affordance that v2 replaced. The v2 `initialize`
//!   therefore omits auth methods instead of advertising RPCs it would fail.

use std::sync::Arc;

use agent_client_protocol::schema::v1;
use agent_client_protocol::{ConnectionTo, Dispatch, Handled, UntypedMessage};
use agent_client_protocol_schema::v2;
use agent_client_protocol_schema::v2::conversion::{try_v1_to_v2, try_v2_to_v1};
use serde_json::Value;

use crate::agent::AcpAgent;

/// `ConnectionTo<Client>` alias (the Agent role's counterpart).
type Client = agent_client_protocol::Client;

/// ACP `methodNotFound` JSON-RPC code.
const ACP_METHOD_NOT_FOUND: i32 = -32601;
/// ACP `invalidParams` JSON-RPC code.
const ACP_INVALID_PARAMS: i32 = -32602;
/// ACP `internalError` JSON-RPC code.
const ACP_INTERNAL_ERROR: i32 = -32603;

fn method_not_found(method: &str, protocol: &str) -> agent_client_protocol::Error {
    agent_client_protocol::Error::new(
        ACP_METHOD_NOT_FOUND,
        format!("`{method}` is not an ACP {protocol} method"),
    )
}

fn invalid_params(message: impl ToString) -> agent_client_protocol::Error {
    agent_client_protocol::Error::new(ACP_INVALID_PARAMS, message.to_string())
}

fn internal_error(message: impl ToString) -> agent_client_protocol::Error {
    agent_client_protocol::Error::new(ACP_INTERNAL_ERROR, message.to_string())
}

fn parse<T: serde::de::DeserializeOwned>(
    params: &Value,
    method: &str,
) -> Result<T, agent_client_protocol::Error> {
    serde_json::from_value(params.clone()).map_err(|e| invalid_params(format!("{method}: {e}")))
}

/// v2 request -> v1 request (the schema crate's own conversion).
fn to_v1<T, U>(value: T, method: &str) -> Result<U, agent_client_protocol::Error>
where
    U: TryFrom<T>,
    agent_client_protocol_schema::v2::conversion::ProtocolConversionError:
        From<<U as TryFrom<T>>::Error>,
{
    try_v2_to_v1(value).map_err(|e| invalid_params(format!("{method}: {e}")))
}

/// v1 response -> v2 response (the schema crate's own conversion).
fn to_v2<T, U>(value: T, what: &str) -> Result<U, agent_client_protocol::Error>
where
    U: TryFrom<T>,
    agent_client_protocol_schema::v2::conversion::ProtocolConversionError:
        From<<U as TryFrom<T>>::Error>,
{
    try_v1_to_v2(value).map_err(|e| internal_error(format!("{what}: {e}")))
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, agent_client_protocol::Error> {
    serde_json::to_value(value).map_err(internal_error)
}

/// Handle one raw ACP v2 request by adapting it to the v1 core.
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
            let request: v1::NewSessionRequest =
                to_v1(parse::<v2::NewSessionRequest>(&params, &method)?, &method)?;
            match agent.handle_new_session(&request, cx).await {
                Ok((response, post)) => {
                    // v2 has no `modes`; drop it before converting.
                    let mut response = response;
                    response.modes = None;
                    let converted: v2::NewSessionResponse =
                        to_v2(response, "session/new response")?;
                    // Publish the empty context state before the handler
                    // returns, so a client cannot race the post-response task
                    // with its first prompt (same ordering as v1).
                    let _ = post.session.publish_initial_usage().await;
                    responder.respond(to_value(converted)?)?;
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
            let request: v1::PromptRequest =
                to_v1(parse::<v2::PromptRequest>(&params, &method)?, &method)?;
            let agent = agent.clone();
            let cx_for_task = cx.clone();
            // The v1 handler runs the turn and responds when it settles; keep
            // the SDK dispatch loop free for `session/cancel` by spawning.
            cx.spawn(async move {
                match agent.handle_prompt(&request, &cx_for_task).await {
                    Ok((_response, persisted)) => {
                        // v2 reports completion through `state_update`
                        // (published by the session pump) and its
                        // `PromptResponse` carries only `_meta`, so the v1
                        // `stopReason` is intentionally not carried over.
                        let answered = responder.respond(to_value(v2::PromptResponse::new())?);
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
            match plan_resume(request) {
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
                Ok(plan) => {
                    let load: v1::LoadSessionRequest = match to_v1(plan.request, &method) {
                        Ok(load) => load,
                        Err(e) => {
                            responder.respond_with_error(e)?;
                            return Ok(Handled::Yes);
                        }
                    };
                    match agent.handle_load_session(&load, cx).await {
                        Ok((response, mut post)) => {
                            let mut response = response;
                            response.modes = None;
                            let converted: v2::ResumeSessionResponse =
                                match to_v2(response, "session/resume response") {
                                    Ok(converted) => converted,
                                    Err(e) => {
                                        responder.respond_with_error(e)?;
                                        return Ok(Handled::Yes);
                                    }
                                };
                            responder.respond(to_value(converted)?)?;
                            // `replayFrom` omitted means "resume without
                            // replaying": restore the session and publish the
                            // title / commands, but no history.
                            post.replay = plan.replay;
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
            }
        }
        "session/close" => {
            // v2 advertises this capability, so it is implemented (not just
            // marked): closing disposes the session and is idempotent.
            let request: v2::CloseSessionRequest = parse(&params, &method)?;
            let session_id: v1::SessionId = to_v1(request.session_id, &method)?;
            let response = agent.handle_close_session(&session_id).await;
            let converted: v2::CloseSessionResponse = to_v2(response, "session/close response")?;
            responder.respond(to_value(converted)?)?;
            Ok(Handled::Yes)
        }
        "session/list" => {
            let request: v1::ListSessionsRequest =
                to_v1(parse::<v2::ListSessionsRequest>(&params, &method)?, &method)?;
            match agent.handle_list_sessions(&request).await {
                Ok(response) => {
                    let converted: v2::ListSessionsResponse =
                        to_v2(response, "session/list response")?;
                    responder.respond(to_value(converted)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/delete" => {
            let request: v1::DeleteSessionRequest = to_v1(
                parse::<v2::DeleteSessionRequest>(&params, &method)?,
                &method,
            )?;
            match agent.handle_delete_session(&request).await {
                Ok(response) => {
                    let converted: v2::DeleteSessionResponse =
                        to_v2(response, "session/delete response")?;
                    responder.respond(to_value(converted)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
        }
        "session/set_config_option" => {
            let request: v1::SetSessionConfigOptionRequest = to_v1(
                parse::<v2::SetSessionConfigOptionRequest>(&params, &method)?,
                &method,
            )?;
            match agent.handle_set_config_option(&request, cx).await {
                Ok(response) => {
                    let converted: v2::SetSessionConfigOptionResponse =
                        to_v2(response, "session/set_config_option response")?;
                    responder.respond(to_value(converted)?)?;
                    Ok(Handled::Yes)
                }
                Err(e) => {
                    responder.respond_with_error(e)?;
                    Ok(Handled::Yes)
                }
            }
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

/// What a v2 `session/resume` request maps onto.
struct ResumePlan {
    /// The equivalent v1 load request.
    request: v2::ResumeSessionRequest,
    /// Whether the historical conversation should be replayed.
    replay: bool,
}

/// Enforce v2's replay-cursor semantics.
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
fn plan_resume(
    request: v2::ResumeSessionRequest,
) -> Result<ResumePlan, agent_client_protocol::Error> {
    match &request.replay_from {
        Some(v2::ReplayFrom::Start(_)) => Ok(ResumePlan {
            request,
            replay: true,
        }),
        Some(other_cursor) => Err(invalid_params(format!(
            "session/resume: unknown replayFrom cursor `{}`; this agent supports only \
             `{{\"type\":\"start\"}}` or no replayFrom at all",
            match other_cursor {
                v2::ReplayFrom::Other(other) => other.type_.clone(),
                v2::ReplayFrom::Start(_) => "start".to_string(),
                _ => "unknown".to_string(),
            }
        ))),
        None => Ok(ResumePlan {
            // The schema conversion from v2 `ResumeSessionRequest` to v1
            // `LoadSessionRequest` only accepts `replayFrom: start`, so supply
            // it to make the mapping total and carry the real intent in
            // `replay` — the caller suppresses the history publication.
            request: {
                let mut request = request;
                request.replay_from = Some(v2::ReplayFrom::Start(v2::ReplayFromStart::new()));
                request
            },
            replay: false,
        }),
    }
}
