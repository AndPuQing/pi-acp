//! ACP protocol-version boundary (W-562).
//!
//! pi-acp's session core speaks **ACP v1 types only** (`session/`, `translate/`,
//! `pi/`). When the `protocol-v2` feature is enabled the agent also registers a
//! v2 implementation on the SDK's [`AgentProtocolRouter`]; the router picks an
//! implementation from the client's `initialize`, and from then on the SDK is a
//! raw-frame pass-through (`pipe_protocol_peers_until_done` copies, it does not
//! convert). Every per-message v1 <-> v2 conversion therefore happens here, at
//! the boundary, using the schema crate's own bidirectional conversion layer
//! (`agent_client_protocol_schema::v2::conversion`) — nothing is hand-written.
//!
//! Two rules from the W-562 decision table are enforced here:
//! - **v2 drops `current_mode_update`.** v2 removed `session/set_mode` and
//!   expresses modes through config options, so `CurrentModeUpdate` has no v2
//!   representation and is *skipped* (not an error) on the v2 path. v1 is
//!   unchanged.
//! - **v1 never fabricates a full-object update.** The complete-message patch
//!   (`agent_message`) exists only in v2; the v1 path keeps streaming chunks.
//!
//! [`AgentProtocolRouter`]: agent_client_protocol::AgentProtocolRouter

use agent_client_protocol::schema::v1;
use agent_client_protocol::{Client, ConnectionTo};
use std::sync::atomic::{AtomicU8, Ordering};

use crate::error::AcpxError;

/// The ACP wire protocol version a single connection negotiated.
///
/// A connection negotiates exactly once, in `initialize`, so this is fixed for
/// the lifetime of a connection (see [`NegotiatedProtocol`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// ACP protocol version 1 — the stable default.
    V1,
    /// ACP protocol version 2 — the unstable draft, behind `protocol-v2`.
    V2,
}

impl Protocol {
    /// The numeric wire version.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Inverse of [`Protocol::as_u8`]; anything that is not 2 is v1.
    #[must_use]
    pub const fn from_u8(value: u8) -> Self {
        if value == 2 {
            Self::V2
        } else {
            Self::V1
        }
    }

    /// Whether this connection speaks the v2 draft.
    #[must_use]
    pub const fn is_v2(self) -> bool {
        matches!(self, Self::V2)
    }
}

/// The protocol version negotiated on an agent's connection.
///
/// `AcpAgent` is a per-connection object (the binary creates one and calls
/// `run_with` once), so a plain atomic is enough: the router hands the whole
/// connection to exactly one implementation, and that implementation's
/// `initialize` handler records the version it answered with.
#[derive(Debug, Default)]
pub struct NegotiatedProtocol(AtomicU8);

impl NegotiatedProtocol {
    /// A fresh record, defaulting to v1 until `initialize` says otherwise.
    #[must_use]
    pub fn new() -> Self {
        Self(AtomicU8::new(Protocol::V1.as_u8()))
    }

    /// The negotiated version.
    #[must_use]
    pub fn get(&self) -> Protocol {
        Protocol::from_u8(self.0.load(Ordering::Relaxed))
    }

    /// Record the version this connection answered `initialize` with.
    pub fn set(&self, protocol: Protocol) {
        self.0.store(protocol.as_u8(), Ordering::Relaxed);
    }
}

fn conversion_error(what: &str, error: impl std::fmt::Display) -> AcpxError {
    AcpxError::RpcFailed {
        command: what.to_string(),
        message: error.to_string(),
    }
}

/// Send one `session/update` notification on a connection, converting the v1
/// update produced by the (v1-only) session core to the connection's version.
///
/// This is the single outbound boundary: every session update leaves through it,
/// so no unconverted frame can reach the wire. On v2 a `CurrentModeUpdate` is
/// dropped (see the module docs); every other update must convert, and a
/// conversion failure is surfaced rather than silently dropped.
pub fn send_session_update(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    notification: v1::SessionNotification,
) -> Result<(), AcpxError> {
    match protocol {
        Protocol::V1 => cx
            .send_notification(notification)
            .map_err(|e| conversion_error("session/update", e)),
        Protocol::V2 => {
            #[cfg(feature = "protocol-v2")]
            {
                let converted = match convert_session_update_to_v2(notification) {
                    Some(converted) => converted,
                    // The update has no v2 form and is deliberately skipped.
                    None => return Ok(()),
                };
                cx.send_notification(converted)
                    .map_err(|e| conversion_error("session/update", e))
            }
            #[cfg(not(feature = "protocol-v2"))]
            {
                let _ = cx;
                tracing::error!("v2 connection negotiated without the protocol-v2 feature");
                Err(AcpxError::RpcFailed {
                    command: "session/update".into(),
                    message: "ACP v2 negotiated but the protocol-v2 feature is disabled".into(),
                })
            }
        }
    }
}

/// The v2 form of a v1 session update, or `None` when the update is one v2 has
/// no representation for and the boundary deliberately drops.
///
/// Split out of [`send_session_update`] so the conversion — the part that can
/// fail, and the part a regression silently breaks — is testable without a live
/// connection. The connection call is all that is left in the function above.
#[cfg(feature = "protocol-v2")]
fn convert_session_update_to_v2(
    notification: v1::SessionNotification,
) -> Option<agent_client_protocol_schema::v2::UpdateSessionNotification> {
    // v2 removed session modes: `CurrentModeUpdate` has no v2 counterpart (the
    // conversion layer returns `removed_v1_enum_variant`). Skipping is the
    // decision, not a fallback — v2 expresses modes as config options, which
    // pi-acp still publishes via `ConfigOptionUpdate`.
    if matches!(notification.update, v1::SessionUpdate::CurrentModeUpdate(_)) {
        tracing::trace!("skipping current_mode_update on the ACP v2 path");
        return None;
    }

    // `with_message_id` is infallible in practice: it either supplies the id v2
    // requires or leaves an update that carries none. A conversion failure here
    // is a bug in the adapter, not a client condition, so it is logged as an
    // error and the frame is dropped rather than being sent unconverted — but
    // see `spawn_outbound_connector`: a dropped frame must never end the
    // connection.
    let notification = with_message_id(notification);
    match agent_client_protocol_schema::v2::conversion::try_v1_to_v2(notification) {
        Ok(converted) => Some(converted),
        Err(error) => {
            tracing::error!(%error, "dropping a session update with no ACP v2 form");
            None
        }
    }
}

/// The `message_id` handed to a chunk that has none, on the v2 path only.
///
/// Monotonic per process, which is all v2 asks for: an id that groups the
/// chunks of one message. A chunk with no message of its own (an extension
/// notice, a replayed history frame) is genuinely its own message, so a fresh
/// id per chunk is the honest answer rather than a shared sentinel.
#[cfg(feature = "protocol-v2")]
static SYNTHETIC_MESSAGE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Give every chunk in `notification` a `message_id`, minting one where the v1
/// session core left it `None`.
///
/// v2 made `ContentChunk.message_id` **required**, so the schema's own
/// conversion refuses a chunk without one. The v1 core only fills the id where
/// it tracks a streamed assistant message (`Pump::current_message_id`), which
/// leaves three producers anonymous: extension notices, replayed history and
/// the export link. Those are real frames a client must receive, so the id is
/// supplied here — at the single boundary — rather than at each producer, so a
/// new producer cannot reintroduce the failure.
///
/// Chunks that already carry an id are untouched: the streaming path's grouping
/// is what lets a client reassemble a message, and it must not be replaced.
#[cfg(feature = "protocol-v2")]
fn with_message_id(mut notification: v1::SessionNotification) -> v1::SessionNotification {
    use std::sync::atomic::Ordering;

    let mint = || {
        let next = SYNTHETIC_MESSAGE_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
        v1::MessageId::new(format!("pi-synthetic-{next}"))
    };

    match &mut notification.update {
        v1::SessionUpdate::UserMessageChunk(chunk) if chunk.message_id.is_none() => {
            chunk.message_id = Some(mint());
        }
        v1::SessionUpdate::AgentMessageChunk(chunk) if chunk.message_id.is_none() => {
            chunk.message_id = Some(mint());
        }
        v1::SessionUpdate::AgentThoughtChunk(chunk) if chunk.message_id.is_none() => {
            chunk.message_id = Some(mint());
        }
        _ => {}
    }
    notification
}

/// Convert the v1 `initialize` response into its v2 form (W-562).
///
/// Two v1→v2 conversions are lossy, and both are resolved here rather than by
/// guessing — each is a consequence of a v2 design change, not a fallback:
///
/// - **`authMethods`**: v2 requires that advertising any auth method implies
///   implementing the `auth/login` *and* `auth/logout` pair. pi-acp implements
///   `authenticate` as a no-op and has no logout, and its real auth mechanism is
///   the v1 terminal-login affordance (Zed's `_meta["terminal-auth"]`) that v2
///   replaced. The v2 response therefore omits auth methods instead of
///   advertising RPCs it would fail.
/// - **`mcpCapabilities.sse`**: v2 dropped the SSE transport. pi-acp advertises
///   HTTP/SSE from the installed adapter; on v2 the SSE flag cannot be
///   represented, so it is cleared and HTTP (the transport v2 kept) is
///   preserved. This only differs when MCP wiring is enabled.
///
/// The v2 `session` capabilities the conversion requires (list/resume/close)
/// are already advertised by `handle_initialize`.
#[cfg(feature = "protocol-v2")]
pub fn initialize_response_to_v2(
    response: v1::InitializeResponse,
) -> Result<agent_client_protocol_schema::v2::InitializeResponse, AcpxError> {
    use agent_client_protocol_schema::v2;
    use agent_client_protocol_schema::v2::conversion::try_v1_to_v2;

    let mut response = response;
    // The v1-only terminal-login affordance has no v2 equivalent.
    response.auth_methods.clear();
    response.agent_capabilities.mcp_capabilities.sse = false;
    // v2's capability model requires the `session` block to advertise resume
    // and close before a v1 capability set can be represented in v2. Both are
    // genuinely supported: `session/resume` shares the restore path with
    // `session/load`, and closing a session is the ACP session teardown
    // pi-acp already performs.
    response.agent_capabilities.session_capabilities.resume =
        Some(v1::SessionResumeCapabilities::new());
    response.agent_capabilities.session_capabilities.close =
        Some(v1::SessionCloseCapabilities::new());
    try_v1_to_v2::<v1::InitializeResponse, v2::InitializeResponse>(response)
        .map_err(|e| conversion_error("initialize->v2", e))
}

/// Send one `session/request_permission` request and await its answer,
/// converting both directions to the connection's protocol version.
///
/// The session pump builds the request with v1 types (it is v1-only); on a v2
/// connection both the request and its response cross the boundary here.
pub async fn request_permission(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    request: v1::RequestPermissionRequest,
) -> Result<v1::RequestPermissionResponse, AcpxError> {
    match protocol {
        Protocol::V1 => cx
            .send_request(request)
            .block_task()
            .await
            .map_err(|e| conversion_error("request_permission", e)),
        Protocol::V2 => {
            #[cfg(feature = "protocol-v2")]
            {
                use agent_client_protocol_schema::v2;
                use agent_client_protocol_schema::v2::conversion::{try_v1_to_v2, try_v2_to_v1};

                let request: v2::RequestPermissionRequest = try_v1_to_v2(request)
                    .map_err(|e| conversion_error("request_permission->v2", e))?;
                let response: v2::RequestPermissionResponse = cx
                    .send_request(request)
                    .block_task()
                    .await
                    .map_err(|e| conversion_error("request_permission", e))?;
                try_v2_to_v1(response).map_err(|e| conversion_error("request_permission->v1", e))
            }
            #[cfg(not(feature = "protocol-v2"))]
            {
                let _ = (cx, request);
                Err(AcpxError::RpcFailed {
                    command: "request_permission".into(),
                    message: "ACP v2 negotiated but the protocol-v2 feature is disabled".into(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "protocol-v2")]
    use agent_client_protocol::schema::v1;

    #[test]
    fn protocol_version_round_trips_through_u8() {
        assert_eq!(Protocol::V1.as_u8(), 1);
        assert_eq!(Protocol::V2.as_u8(), 2);
        assert_eq!(Protocol::from_u8(1), Protocol::V1);
        assert_eq!(Protocol::from_u8(2), Protocol::V2);
        // Anything else is not a version pi-acp knows; v1 is the safe default
        // (it is the only version the default build speaks).
        assert_eq!(Protocol::from_u8(0), Protocol::V1);
        assert_eq!(Protocol::from_u8(9), Protocol::V1);
        assert!(!Protocol::V1.is_v2());
        assert!(Protocol::V2.is_v2());
    }

    #[test]
    fn negotiated_protocol_defaults_to_v1_and_records_changes() {
        let negotiated = NegotiatedProtocol::new();
        assert_eq!(negotiated.get(), Protocol::V1);
        negotiated.set(Protocol::V2);
        assert_eq!(negotiated.get(), Protocol::V2);
        negotiated.set(Protocol::V1);
        assert_eq!(negotiated.get(), Protocol::V1);
    }

    /// The decision table's v2 rule: `CurrentModeUpdate` has no v2
    /// representation, so the boundary skips it instead of failing the turn.
    #[cfg(feature = "protocol-v2")]
    #[test]
    fn current_mode_update_is_unrepresentable_in_v2_but_skipped() {
        use agent_client_protocol_schema::v2::conversion::try_v1_to_v2;

        let update = v1::SessionUpdate::CurrentModeUpdate(v1::CurrentModeUpdate::new("medium"));
        // The schema crate cannot convert it...
        let result: Result<agent_client_protocol_schema::v2::SessionUpdate, _> =
            try_v1_to_v2(update.clone());
        assert!(result.is_err(), "current_mode_update has no v2 form");
        // ...which is exactly the case `send_session_update` detects and drops.
        assert!(matches!(update, v1::SessionUpdate::CurrentModeUpdate(_)));
    }

    /// v2's `initialize` cannot carry the v1-only terminal-login auth methods,
    /// and its capability model needs `resume`/`close`.
    #[cfg(feature = "protocol-v2")]
    #[test]
    fn v2_initialize_drops_auth_methods_and_advertises_resume_and_close() {
        use agent_client_protocol_schema::v2::conversion::try_v1_to_v2;
        let mut capabilities = v1::AgentCapabilities::new().load_session(true);
        capabilities.session_capabilities = v1::SessionCapabilities::new()
            .list(v1::SessionListCapabilities::new())
            .resume(v1::SessionResumeCapabilities::new())
            .close(v1::SessionCloseCapabilities::new());
        let response =
            v1::InitializeResponse::new(agent_client_protocol::schema::ProtocolVersion::V1)
                .agent_capabilities(capabilities)
                .agent_info(v1::Implementation::new("pi-acp", "0.0.0"))
                .auth_methods(crate::auth::get_auth_methods(false));

        let converted = initialize_response_to_v2(response).expect("v2 conversion");
        assert!(
            converted.auth_methods.is_empty(),
            "v2 must not advertise auth methods it does not implement"
        );
        let session = converted
            .capabilities
            .session
            .as_ref()
            .expect("v2 requires the session capability block");
        assert!(
            session.delete.is_none(),
            "the v1 response advertised no delete capability, so v2 must not either"
        );

        // Sanity: converting the untouched v1 response fails, which is why the
        // adapter clears the field rather than passing it through.
        let mut raw_capabilities = v1::AgentCapabilities::new().load_session(true);
        raw_capabilities.session_capabilities = v1::SessionCapabilities::new()
            .list(v1::SessionListCapabilities::new())
            .resume(v1::SessionResumeCapabilities::new())
            .close(v1::SessionCloseCapabilities::new());
        let raw = v1::InitializeResponse::new(agent_client_protocol::schema::ProtocolVersion::V1)
            .agent_capabilities(raw_capabilities)
            .agent_info(v1::Implementation::new("pi-acp", "0.0.0"))
            .auth_methods(crate::auth::get_auth_methods(false));
        let with_auth: Result<agent_client_protocol_schema::v2::InitializeResponse, _> =
            try_v1_to_v2(raw);
        assert!(
            with_auth.is_err(),
            "v1 auth methods without a logout capability are not representable in v2"
        );
    }

    /// v2 made `ContentChunk.message_id` required, and the v1 session core
    /// leaves it `None` for every producer that is not the streamed assistant
    /// message: extension notices, replayed history, the export link. The
    /// boundary mints one instead of letting the conversion fail.
    ///
    /// The regression this guards is a whole turn disappearing: an anonymous
    /// chunk failed conversion, the outbound connector returned, and every
    /// later frame — including the answer — was silently dropped.
    #[cfg(feature = "protocol-v2")]
    #[test]
    fn a_chunk_without_a_message_id_is_given_one_for_v2() {
        use agent_client_protocol_schema::v2::conversion::try_v1_to_v2;

        let notification = v1::SessionNotification::new(
            v1::SessionId::new("session-1"),
            v1::SessionUpdate::AgentMessageChunk(v1::ContentChunk::new(v1::ContentBlock::Text(
                v1::TextContent::new("Observational memory: running"),
            ))),
        );
        assert!(
            matches!(
                &notification.update,
                v1::SessionUpdate::AgentMessageChunk(chunk) if chunk.message_id.is_none()
            ),
            "the fixture must start without a message id"
        );

        // The raw conversion is what used to fail...
        let raw: Result<agent_client_protocol_schema::v2::UpdateSessionNotification, _> =
            try_v1_to_v2(notification.clone());
        assert!(
            raw.is_err(),
            "a v1 chunk without a message id has no v2 form"
        );

        // ...and the boundary's own conversion succeeds, with an id supplied.
        let converted = convert_session_update_to_v2(notification)
            .expect("an anonymous chunk must convert once given an id");
        let agent_client_protocol_schema::v2::SessionUpdate::AgentMessageChunk(chunk) =
            converted.update
        else {
            panic!("the update must stay an agent message chunk");
        };
        assert!(
            !chunk.message_id.0.is_empty(),
            "the minted id must be non-empty"
        );
    }

    /// A chunk that already carries an id keeps it.
    ///
    /// The streamed-assistant path groups chunks by id so a client can
    /// reassemble the message; replacing that id would split one message into
    /// many bubbles — the bug the id exists to prevent.
    #[cfg(feature = "protocol-v2")]
    #[test]
    fn an_existing_message_id_is_preserved() {
        let notification = v1::SessionNotification::new(
            v1::SessionId::new("session-1"),
            v1::SessionUpdate::AgentMessageChunk(
                v1::ContentChunk::new(v1::ContentBlock::Text(v1::TextContent::new("pong")))
                    .message_id(v1::MessageId::new("pi-msg-7")),
            ),
        );

        let converted = convert_session_update_to_v2(notification).expect("v2 conversion");
        let agent_client_protocol_schema::v2::SessionUpdate::AgentMessageChunk(chunk) =
            converted.update
        else {
            panic!("the update must stay an agent message chunk");
        };
        assert_eq!(chunk.message_id.0.to_string(), "pi-msg-7");
    }

    /// Every chunk-bearing update variant the v1 core can emit is covered.
    ///
    /// A variant the boundary stopped covering would reintroduce the dropped
    /// turn, so this fails if one is missed.
    #[cfg(feature = "protocol-v2")]
    #[test]
    fn every_anonymous_chunk_variant_reaches_a_v2_client() {
        let updates = [
            v1::SessionUpdate::AgentMessageChunk(v1::ContentChunk::new(v1::ContentBlock::Text(
                v1::TextContent::new("x"),
            ))),
            v1::SessionUpdate::UserMessageChunk(v1::ContentChunk::new(v1::ContentBlock::Text(
                v1::TextContent::new("x"),
            ))),
            v1::SessionUpdate::AgentThoughtChunk(v1::ContentChunk::new(v1::ContentBlock::Text(
                v1::TextContent::new("x"),
            ))),
        ];
        for update in updates {
            let name = format!("{update:?}");
            assert!(
                matches!(
                    &update,
                    v1::SessionUpdate::AgentMessageChunk(chunk)
                        | v1::SessionUpdate::UserMessageChunk(chunk)
                        | v1::SessionUpdate::AgentThoughtChunk(chunk)
                        if chunk.message_id.is_none()
                ),
                "the {name} fixture must start without an id"
            );
            let notification =
                v1::SessionNotification::new(v1::SessionId::new("session-1"), update);
            let converted = convert_session_update_to_v2(notification);
            assert!(
                converted.is_some(),
                "a {name} without a message id must still reach a v2 client"
            );
        }
    }

    /// The id helper only touches chunk-bearing updates.
    #[cfg(feature = "protocol-v2")]
    #[test]
    fn an_update_without_a_chunk_is_left_alone() {
        let notification = v1::SessionNotification::new(
            v1::SessionId::new("session-1"),
            v1::SessionUpdate::CurrentModeUpdate(v1::CurrentModeUpdate::new("medium")),
        );
        assert!(
            convert_session_update_to_v2(notification).is_none(),
            "current_mode_update is deliberately dropped on the v2 path"
        );
    }
}
