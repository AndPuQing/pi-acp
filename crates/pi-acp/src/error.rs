//! Unified adapter error type.
//!
//! Every fallible path in the crate returns [`AcpxError`] (design §8.2). At the
//! ACP boundary it is mapped to an ACP `RequestError`
//! (`internalError` / `invalidParams` / `authRequired`, with `data`).
//!
//! Policy: **never swallow errors.** Each variant below corresponds to a class
//! of failure that must reach the ACP client (fixes pi-acp #98 / #92 / #43).

use thiserror::Error;

/// Adapter-level error.
#[derive(Debug, Error)]
pub enum AcpxError {
    /// Could not spawn the `pi` subprocess (ENOENT / EACCES / ...).
    #[error("failed to spawn pi: {0}")]
    PiSpawn(String),

    /// A pi RPC request did not complete within its deadline (fixes #94).
    #[error("pi RPC request '{cmd}' timed out after {secs}s")]
    RpcTimeout {
        /// Human-readable command name (e.g. `prompt`).
        cmd: String,
        /// Deadline in seconds.
        secs: u64,
    },

    /// The pi subprocess exited while requests were in flight (fixes #82).
    #[error("pi process exited (code={code:?}, signal={signal:?})")]
    PiExited {
        code: Option<i32>,
        signal: Option<i32>,
    },

    /// pi answered a command with `success: false`.
    #[error("pi RPC command '{command}' failed: {message}")]
    RpcFailed { command: String, message: String },

    /// Missing/invalid credentials; surfaced as ACP `AuthRequired` so clients
    /// can offer terminal login.
    #[error("authentication required: {0}")]
    AuthRequired(String),

    /// Referenced a session id that is not known.
    #[error("unknown session: {0}")]
    UnknownSession(String),

    /// A session's pump task is no longer running (session closed / pi exited),
    /// so commands targeted at it cannot be serviced.
    #[error("session {0} is closed")]
    SessionClosed(String),

    /// Standard I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parse/serialize failure.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, AcpxError>;

/// JSON-RPC error code for an internal server error (JSON-RPC 2.0).
const ACP_INTERNAL_ERROR: i32 = -32603;
/// JSON-RPC error code for ACP `authRequired` (reserved range).
const ACP_AUTH_REQUIRED: i32 = -32000;

/// Map an [`AcpxError`] onto the ACP `RequestError` the client should receive.
///
/// `AuthRequired` is surfaced with the ACP `authRequired` code so clients can
/// offer Terminal Auth; everything else is an internal error carrying the
/// diagnostic as the message (design D5 / §8.2).
impl From<AcpxError> for agent_client_protocol::Error {
    fn from(e: AcpxError) -> Self {
        use agent_client_protocol::Error as AcpError;
        match e {
            AcpxError::AuthRequired(msg) => AcpError::new(ACP_AUTH_REQUIRED, msg),
            other => AcpError::new(ACP_INTERNAL_ERROR, other.to_string()),
        }
    }
}
