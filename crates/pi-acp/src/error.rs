//! Unified adapter error type.
//!
//! Every fallible path in the crate returns [`AcpxError`] (design §8.2). At the
//! ACP boundary it is mapped to an ACP `RequestError`
//! (`internalError` / `invalidParams` / `authRequired`, with `data`).
//!
//! Policy: **never swallow errors.** Each variant below corresponds to a class
//! of failure that must reach the ACP client (fixes pi-acp #98 / #92 / #43).
//!
//! S8 (W-455) additions: every mapping carries a structured `data` payload so
//! clients can distinguish failure classes programmatically; `PiExited` and
//! `PiSpawn` messages carry actionable hints (no-auto-respawn per decision 1,
//! install hint per design §8.2).

use serde_json::{json, Value};
use thiserror::Error;

/// Adapter-level error.
#[derive(Debug, Error)]
pub enum AcpxError {
    /// Could not spawn the `pi` subprocess (ENOENT / EACCES / ...).
    ///
    /// The message carries an actionable, cross-platform install hint: the
    /// npm package, the `PI_ACP_PI_COMMAND` override, and the Windows note
    /// that pi's entry point is the npm global `pi.cmd` (fixes pi-acp #27).
    #[error(
        "failed to spawn pi: {0}. Install pi with `npm i -g \
         @earendil-works/pi-coding-agent` (on Windows the entry point is the npm \
         global `pi.cmd`), or set PI_ACP_PI_COMMAND to the full path."
    )]
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
    ///
    /// The message carries a hint: pi-acp does **not** restart pi
    /// automatically (decision 1); the client should start a new session.
    #[error(
        "pi process exited (code={code:?}, signal={signal:?}) — pi-acp does not restart pi \
         automatically; start a new session (session/new) or restart pi-acp to recover"
    )]
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

impl AcpxError {
    /// The stable `errorType` discriminator for this variant (carried in the
    /// ACP error `data` so clients can branch programmatically).
    pub fn error_type(&self) -> &'static str {
        match self {
            AcpxError::PiSpawn(_) => "piSpawn",
            AcpxError::RpcTimeout { .. } => "rpcTimeout",
            AcpxError::PiExited { .. } => "piExited",
            AcpxError::RpcFailed { .. } => "rpcFailed",
            AcpxError::AuthRequired(_) => "authRequired",
            AcpxError::UnknownSession(_) => "unknownSession",
            AcpxError::SessionClosed(_) => "sessionClosed",
            AcpxError::Io(_) => "io",
            AcpxError::Json(_) => "json",
        }
    }

    /// Structured `data` attached to the ACP `RequestError` (design D5:
    /// errors carry data, not just a message). Variant-specific fields plus
    /// the stable `errorType` discriminator.
    pub fn detail(&self) -> Value {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "errorType".to_string(),
            Value::String(self.error_type().to_string()),
        );
        match self {
            AcpxError::PiSpawn(msg) => {
                obj.insert("message".to_string(), json!(msg));
            }
            AcpxError::RpcTimeout { cmd, secs } => {
                obj.insert("command".to_string(), json!(cmd));
                obj.insert("secs".to_string(), json!(secs));
            }
            AcpxError::PiExited { code, signal } => {
                obj.insert("code".to_string(), json!(code));
                obj.insert("signal".to_string(), json!(signal));
                obj.insert(
                    "hint".to_string(),
                    json!("pi-acp does not restart pi automatically; start a new session (session/new) or restart pi-acp to recover"),
                );
            }
            AcpxError::RpcFailed { command, message } => {
                obj.insert("command".to_string(), json!(command));
                obj.insert("message".to_string(), json!(message));
            }
            AcpxError::AuthRequired(msg) => {
                obj.insert("message".to_string(), json!(msg));
            }
            AcpxError::UnknownSession(id) | AcpxError::SessionClosed(id) => {
                obj.insert("sessionId".to_string(), json!(id));
            }
            AcpxError::Io(e) => {
                obj.insert("kind".to_string(), json!(e.kind().to_string()));
            }
            AcpxError::Json(e) => {
                obj.insert("message".to_string(), json!(e.to_string()));
            }
        }
        Value::Object(obj)
    }
}

/// The ACP `authRequired` error `data` payload: the terminal auth methods the
/// client can offer (spec: `data.authMethods`).
pub fn auth_required_data() -> Value {
    json!({ "authMethods": crate::auth::get_auth_methods(true) })
}

/// Map an [`AcpxError`] onto the ACP `RequestError` the client should receive.
///
/// `AuthRequired` is surfaced with the ACP `authRequired` code (with the
/// `authMethods` data so clients can offer Terminal Auth); everything else is
/// an internal error carrying the diagnostic as the message **and** a
/// structured `data` payload (design D5 / §8.2).
impl From<AcpxError> for agent_client_protocol::Error {
    fn from(e: AcpxError) -> Self {
        use agent_client_protocol::Error as AcpError;
        match e {
            AcpxError::AuthRequired(_) => AcpError::new(
                ACP_AUTH_REQUIRED,
                "Configure an API key or log in with an OAuth provider.",
            )
            .data(auth_required_data()),
            other => AcpError::new(ACP_INTERNAL_ERROR, other.to_string()).data(other.detail()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::Error as AcpError;

    #[test]
    fn pi_exited_display_carries_no_respawn_hint() {
        let msg = AcpxError::PiExited {
            code: Some(42),
            signal: None,
        }
        .to_string();
        assert!(msg.contains("code=Some(42)"), "{msg}");
        assert!(msg.contains("does not restart pi"), "{msg}");
        assert!(msg.contains("session/new"), "{msg}");
    }

    #[test]
    fn pi_spawn_display_carries_install_hint() {
        let msg = AcpxError::PiSpawn("pi: No such file".into()).to_string();
        assert!(
            msg.contains("npm i -g @earendil-works/pi-coding-agent"),
            "{msg}"
        );
        assert!(msg.contains("PI_ACP_PI_COMMAND"), "{msg}");
    }

    #[test]
    fn rpc_timeout_detail_is_structured() {
        let err: AcpError = AcpxError::RpcTimeout {
            cmd: "prompt".into(),
            secs: 30,
        }
        .into();
        assert_eq!(err.code, ACP_INTERNAL_ERROR.into());
        let data = err.data.as_ref().expect("error data");
        assert_eq!(data["errorType"], "rpcTimeout");
        assert_eq!(data["command"], "prompt");
        assert_eq!(data["secs"], 30);
    }

    #[test]
    fn pi_exited_detail_carries_code_signal_and_hint() {
        let err: AcpError = AcpxError::PiExited {
            code: Some(42),
            signal: None,
        }
        .into();
        let data = err.data.as_ref().expect("error data");
        assert_eq!(data["errorType"], "piExited");
        assert_eq!(data["code"], 42);
        assert_eq!(data["signal"], serde_json::Value::Null);
        assert!(
            data["hint"]
                .as_str()
                .unwrap()
                .contains("does not restart pi"),
            "hint: {data}"
        );
    }

    #[test]
    fn rpc_failed_detail_carries_command_and_message() {
        let err: AcpError = AcpxError::RpcFailed {
            command: "prompt".into(),
            message: "boom".into(),
        }
        .into();
        let data = err.data.as_ref().expect("error data");
        assert_eq!(data["errorType"], "rpcFailed");
        assert_eq!(data["command"], "prompt");
        assert_eq!(data["message"], "boom");
    }

    #[test]
    fn auth_required_maps_to_auth_required_code_with_auth_methods() {
        let err: AcpError = AcpxError::AuthRequired("no key".into()).into();
        assert_eq!(err.code, ACP_AUTH_REQUIRED.into());
        assert!(err.message.contains("Configure an API key"));
        let data = err.data.as_ref().expect("authRequired data");
        let methods = data["authMethods"].as_array().expect("authMethods array");
        assert_eq!(methods.len(), 1);
        assert_eq!(methods[0]["id"], "pi_terminal_login");
        assert_eq!(methods[0]["type"], "terminal");
    }

    #[test]
    fn session_closed_detail_carries_session_id() {
        let err: AcpError = AcpxError::SessionClosed("s1".into()).into();
        let data = err.data.as_ref().expect("error data");
        assert_eq!(data["errorType"], "sessionClosed");
        assert_eq!(data["sessionId"], "s1");
    }
}
