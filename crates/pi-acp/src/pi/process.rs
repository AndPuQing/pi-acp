//! pi subprocess transport (S2 spike).
//!
//! This is the **minimal** piece of the pi RPC client needed to validate the
//! ACP SDK × tokio runtime premise (design D9 / §5.3): spawn `pi --mode rpc`,
//! send a `prompt` command, stream the resulting events back to the ACP client
//! as `session/update` notifications, and finish the turn on `agent_settled`.
//!
//! S3 (W-450) replaces this with the full client: pending-request map, per-request
//! deadlines, an event channel, a child-exit watcher, prelude capture, and
//! SIGTERM→SIGKILL teardown. What is built here is deliberately runtime-agnostic
//! logic on top of `tokio::process`, so swapping the runtime (the spike's only
//! open question) would touch only the few `tokio::` primitives below.

use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::error::{AcpxError, Result};

/// Default per-request pi RPC deadline (seconds). Overridable via
/// `PI_ACP_RPC_TIMEOUT_SECS`; threaded here from [`crate::config::Config`].
pub const DEFAULT_RPC_TIMEOUT_SECS: u64 = 30;

/// A spawned `pi --mode rpc` child process plus its JSONL request/response
/// plumbing. Only one command is in flight at a time (the spike sends a single
/// `prompt` per session); S3 generalizes this to a pending-id map.
pub struct PiProcess {
    child: Option<Child>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    /// Monotonic id source for pi RPC requests (matches the response by `id`).
    next_id: AtomicU64,
    /// Per-request deadline.
    timeout: Duration,
}

/// Identity of the pi session a [`PiProcess`] owns, learned via `get_state`.
#[derive(Debug, Clone)]
pub struct PiSessionState {
    /// pi's own session id (used as the ACP session id so the two align).
    pub session_id: String,
    /// Path to the pi session file, if known (used later for load/list, S7).
    pub session_file: Option<String>,
}

impl PiProcess {
    /// Spawn `pi --mode rpc --no-themes` with piped stdio.
    ///
    /// Inherits the current environment so pi picks up its configured
    /// provider/model/credentials (e.g. `PI_PROVIDER` / `PI_MODEL`).
    pub async fn spawn(pi_command: &str, timeout: Duration) -> Result<Self> {
        let mut child = Command::new(pi_command)
            .args(["--mode", "rpc", "--no-themes"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // pi writes diagnostics to stderr; keep it out of the JSONL stream.
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| AcpxError::PiSpawn(format!("{pi_command}: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AcpxError::PiSpawn("pi stdin not piped".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AcpxError::PiSpawn("pi stdout not piped".to_string()))?;

        tracing::info!(pid = ?child.id(), pi_command, "spawned pi --mode rpc");

        Ok(Self {
            child: Some(child),
            stdin,
            stdout: BufReader::new(stdout),
            next_id: AtomicU64::new(0),
            timeout,
        })
    }

    fn next_id(&self) -> String {
        self.next_id.fetch_add(1, Ordering::Relaxed).to_string()
    }

    /// Write a single JSON line to pi's stdin.
    async fn write_line(&mut self, value: &Value) -> Result<()> {
        let mut line = value.to_string();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Read the next JSON line from pi's stdout, skipping blank and
    /// non-JSON (human-readable prelude) lines. Returns EOF as [`AcpxError::PiExited`].
    pub async fn read_event(&mut self) -> Result<Value> {
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.stdout.read_line(&mut line).await?;
            if n == 0 {
                return Err(AcpxError::PiExited {
                    code: None,
                    signal: None,
                });
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(trimmed) {
                Ok(v) => return Ok(v),
                // Prelude (context / skills / extension banner) — S6 captures
                // these for the startup info; for the spike we just skip.
                Err(_) => tracing::trace!(line = %trimmed, "pi prelude line (skipped)"),
            }
        }
    }

    /// Send a command (identified by a freshly-allocated id) and wait for the
    /// matching `response` line, discarding any interleaved events. Bounded by
    /// the per-request deadline.
    pub async fn request(&mut self, command: &Value) -> Result<Value> {
        let id = self.next_id();
        let mut msg = command.clone();
        msg["id"] = Value::String(id.clone());
        self.write_line(&msg).await?;
        self.wait_response(&id).await
    }

    /// Wait for the `response` line whose `id` matches, skipping events.
    pub async fn wait_response(&mut self, id: &str) -> Result<Value> {
        let timeout = self.timeout;
        let secs = timeout.as_secs();
        match tokio::time::timeout(timeout, async {
            loop {
                let v = self.read_event().await?;
                let is_response = v.get("type").and_then(Value::as_str) == Some("response");
                let matches = v.get("id").and_then(Value::as_str) == Some(id);
                if is_response && matches {
                    return Ok(v);
                }
            }
        })
        .await
        {
            Ok(res) => res,
            Err(_) => Err(AcpxError::RpcTimeout {
                cmd: "response".into(),
                secs,
            }),
        }
    }

    /// Return the pi session identity for this process via `get_state`.
    pub async fn get_session_state(&mut self) -> Result<PiSessionState> {
        let resp = self
            .request(&serde_json::json!({ "type": "get_state" }))
            .await?;
        if resp.get("success").and_then(Value::as_bool) != Some(true) {
            let message = resp
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
                .to_string();
            return Err(AcpxError::RpcFailed {
                command: "get_state".into(),
                message,
            });
        }
        let session_id = resp
            .pointer("/data/sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| AcpxError::RpcFailed {
                command: "get_state".into(),
                message: "response missing data.sessionId".into(),
            })?
            .to_string();
        let session_file = resp
            .pointer("/data/sessionFile")
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok(PiSessionState {
            session_id,
            session_file,
        })
    }

    /// Send a `prompt` command and stream each pi event to `on_event` until the
    /// turn settles (`agent_settled`). The `prompt` command's `response` line
    /// arrives early (before the events) and is simply forwarded to `on_event`;
    /// the turn is complete only at `agent_settled`.
    pub async fn prompt_until_settled<F: FnMut(&Value)>(
        &mut self,
        message: &str,
        mut on_event: F,
    ) -> Result<()> {
        let id = self.next_id();
        let msg = serde_json::json!({ "id": id, "type": "prompt", "message": message });
        self.write_line(&msg).await?;

        let timeout = self.timeout;
        let secs = timeout.as_secs();
        match tokio::time::timeout(timeout, async {
            loop {
                let v = self.read_event().await?;
                let t = v.get("type").and_then(Value::as_str);
                if t == Some("agent_settled") {
                    return Ok(());
                }
                // A `response` line for our prompt can arrive early; ignore it
                // here and keep pumping events until the turn actually settles.
                on_event(&v);
            }
        })
        .await
        {
            Ok(res) => res,
            Err(_) => Err(AcpxError::RpcTimeout {
                cmd: "prompt".into(),
                secs,
            }),
        }
    }
}

impl Drop for PiProcess {
    /// Best-effort teardown. `Drop` cannot await, so send the kill signal
    /// without waiting for the exit (S3 upgrades this to SIGTERM→SIGKILL with a
    /// reaper task and a process-group kill for wrapper launchers).
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

/// Extract the streaming assistant text delta from a pi `message_update` event,
/// if any. pi emits `assistantMessageEvent` as a discriminated object; only the
/// `text_delta` variant carries user-visible text (thinking/tool deltas are
/// handled by the full state machine in S5).
pub fn text_delta_of(event: &Value) -> Option<&str> {
    if event.get("type").and_then(Value::as_str) != Some("message_update") {
        return None;
    }
    let ame = event.get("assistantMessageEvent")?;
    (ame.get("type").and_then(Value::as_str) == Some("text_delta"))
        .then(|| ame.get("delta"))
        .flatten()
        .and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn extracts_text_delta() {
        let ev = v(
            r#"{"type":"message_update","usage":{},"assistantMessageEvent":{"type":"text_delta","contentIndex":1,"delta":"pong"}}"#,
        );
        assert_eq!(text_delta_of(&ev), Some("pong"));
    }

    #[test]
    fn ignores_thinking_and_non_update_events() {
        let thinking = v(
            r#"{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","contentIndex":0,"delta":"hmm"}}"#,
        );
        assert_eq!(text_delta_of(&thinking), None);

        let settled = v(r#"{"type":"agent_settled"}"#);
        assert_eq!(text_delta_of(&settled), None);

        let no_delta = v(
            r#"{"type":"message_update","assistantMessageEvent":{"type":"text_end","contentIndex":1,"content":"pong"}}"#,
        );
        assert_eq!(text_delta_of(&no_delta), None);
    }
}
