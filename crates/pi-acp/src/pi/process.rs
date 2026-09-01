//! pi subprocess RPC client — built in S3 (W-450).
//!
//! Spawns `pi --mode rpc --no-themes [--session <path>]` and owns the JSONL
//! plumbing on top of it (corresponds to the TS reference `pi-rpc/process.ts`):
//!
//! - [`PiProcess::request`] writes one JSON line and awaits the `response`
//!   whose `id` matches, bounded by an outer `tokio::time::timeout`
//!   (default 30s, configurable) — fixes #94 (requests could hang forever).
//! - A dedicated **reader task** consumes `Child::stdout` line by line:
//!   responses resolve the pending map, everything else is dispatched to the
//!   event channel. Non-JSON lines before/around NDJSON are stripped of ANSI
//!   and collected as the *prelude* (context/skills/extension banner).
//! - A **watcher task** awaits `Child::wait()`; on exit it rejects every
//!   pending request with [`AcpxError::PiExited`] and marks the process dead —
//!   so a dead pi is detected loudly instead of a silent empty `end_turn`
//!   (fixes #82).
//! - Teardown is SIGTERM → grace → SIGKILL against the whole process group
//!   (pi is spawned in its own group on unix), with a reaper task; `Drop` is
//!   the sync emergency path. Never relies on stdin-EOF alone (S2 constraint 4).
//!
//! The S2 spike's minimal transport (`spawn` / `write_line` / `wait_response` /
//! `get_session_state` / `prompt_until_settled` / `text_delta_of`) is extended
//! here rather than rewritten: the single-in-flight model becomes the
//! pending-map + event-channel client.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::error::{AcpxError, Result};
use crate::pi::resolve::resolve_current_env;
use crate::pi::rpc::{
    ExtensionUiResponse, Model, QueueMode, RpcCommand, RpcEvent, RpcResponse, RpcSessionState,
    ThinkingLevel,
};

/// Default per-request pi RPC deadline (seconds). Overridable via
/// `PI_ACP_RPC_TIMEOUT_SECS`; threaded here from [`crate::config::Config`].
pub const DEFAULT_RPC_TIMEOUT_SECS: u64 = 30;

/// How long [`PiProcess::dispose`] waits after SIGTERM before escalating to
/// SIGKILL (pi runs a graceful shutdown handler on SIGTERM).
const SIGTERM_GRACE: Duration = Duration::from_secs(3);
/// How long [`PiProcess::Drop`]'s off-thread SIGTERM→SIGKILL escalation waits.
const DROP_ESCALATION_DELAY: Duration = Duration::from_millis(250);
/// Bounded event channel capacity. The reader task awaits sends (backpressure),
/// so a slow event pump stalls the stream rather than unboundedly buffering.
const EVENT_CHANNEL_CAPACITY: usize = 1024;
/// Upper bound on collected prelude lines (defensive; pi's banner is ~10).
const PRELUDE_CAP: usize = 200;

/// Shared state between the client handle and the two background tasks
/// (reader + watcher).
struct Shared {
    /// In-flight requests: `id` → oneshot back to the awaiting `request()`.
    pending: tokio::sync::Mutex<HashMap<String, oneshot::Sender<Result<Value>>>>,
    /// Exit info once the watcher observes the child exit; `None` while alive.
    /// `(code, signal)` — signal is unix-only (`None` elsewhere).
    exit: Mutex<Option<(Option<i32>, Option<i32>)>>,
    /// Human-readable stdout lines (ANSI-stripped) seen before/instead of NDJSON.
    prelude: Mutex<Vec<String>>,
}

/// A spawned `pi --mode rpc` child process plus its JSONL request/response
/// plumbing.
pub struct PiProcess {
    stdin: ChildStdin,
    /// Monotonic id source for pi RPC requests (matched back by `id`).
    next_id: AtomicU64,
    /// Per-request deadline.
    timeout: Duration,
    shared: Arc<Shared>,
    /// Event channel receiver; taken once by the session event pump (S5) or
    /// consumed incrementally via [`PiProcess::next_event`].
    events: Option<mpsc::Receiver<RpcEvent>>,
    /// Reaper task (`Child::wait()` → reject pending + mark dead).
    watcher: JoinHandle<()>,
    /// Child pid for teardown signaling; cleared by [`PiProcess::dispose`] so
    /// `Drop` does not re-signal a process we already terminated.
    pid: Option<u32>,
}

impl PiProcess {
    /// Spawn `pi --mode rpc --no-themes` with piped stdio.
    ///
    /// Inherits the current environment so pi picks up its configured
    /// provider/model/credentials (e.g. `PI_PROVIDER` / `PI_MODEL`).
    pub async fn spawn(pi_command: &str, timeout: Duration) -> Result<Self> {
        Self::spawn_with_session(pi_command, None, timeout).await
    }

    /// [`PiProcess::spawn`] with an explicit `--session <path>` (pi persists the
    /// session to that file; used by session load/switch in later stages).
    pub async fn spawn_with_session(
        pi_command: &str,
        session_path: Option<&Path>,
        timeout: Duration,
    ) -> Result<Self> {
        Self::spawn_inner(pi_command, &[], session_path, timeout).await
    }

    /// [`PiProcess::spawn_with_session`] with extra pi CLI flags appended after
    /// the standard arguments (e.g. test fixtures like `--mock-rpc`).
    pub async fn spawn_with_args(
        pi_command: &str,
        extra_args: &[&str],
        session_path: Option<&Path>,
        timeout: Duration,
    ) -> Result<Self> {
        Self::spawn_inner(pi_command, extra_args, session_path, timeout).await
    }

    async fn spawn_inner(
        pi_command: &str,
        extra_args: &[&str],
        session_path: Option<&Path>,
        timeout: Duration,
    ) -> Result<Self> {
        // Resolve the configured command to a launchable program. On Windows
        // this expands a bare `pi` to the npm `pi.cmd` wrapper and routes it
        // through `cmd.exe /d /s /c` (fixes pi-acp #27); on unix it is a no-op
        // for the common `pi` name. `cmd_args` carry any shell prefix.
        let resolved = resolve_current_env(pi_command);
        let mut cmd = Command::new(&resolved.program);
        cmd.args(&resolved.cmd_args);
        cmd.args(["--mode", "rpc", "--no-themes"]);
        if let Some(path) = session_path {
            cmd.arg("--session").arg(path);
        }
        cmd.args(extra_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // pi writes diagnostics to stderr; keep it out of the JSONL stream.
            .stderr(Stdio::null());
        // Put pi in its own process group so teardown can signal the whole tree
        // (wrapper launchers like pi.cmd may spawn grandchildren).
        #[cfg(unix)]
        cmd.process_group(0);

        let mut child = cmd
            .spawn()
            .map_err(|e| AcpxError::PiSpawn(spawn_error(pi_command, &resolved, e)))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AcpxError::PiSpawn("pi stdin not piped".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AcpxError::PiSpawn("pi stdout not piped".to_string()))?;

        tracing::info!(
            pid = ?child.id(),
            pi_command,
            has_session = session_path.is_some(),
            "spawned pi --mode rpc"
        );

        let shared = Arc::new(Shared {
            pending: tokio::sync::Mutex::new(HashMap::new()),
            exit: Mutex::new(None),
            prelude: Mutex::new(Vec::new()),
        });

        let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

        let reader_shared = shared.clone();
        tokio::spawn(read_loop(stdout, reader_shared, events_tx));

        let pid = child.id();
        let watcher_shared = shared.clone();
        let watcher = tokio::spawn(wait_loop(child, watcher_shared));

        Ok(Self {
            stdin,
            next_id: AtomicU64::new(0),
            timeout,
            shared,
            events: Some(events_rx),
            watcher,
            pid,
        })
    }

    fn next_id(&self) -> String {
        self.next_id.fetch_add(1, Ordering::Relaxed).to_string()
    }

    /// Write a single JSON line to pi's stdin.
    async fn write_line(&mut self, value: &Value) -> std::io::Result<()> {
        let mut line = value.to_string();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Send a command and await its `response`, bounded by the per-request
    /// deadline (outer `tokio::time::timeout`; fixes #94). On success returns
    /// the response `data` (or `Null`); pi's `success: false` maps to
    /// [`AcpxError::RpcFailed`]. A child exit while awaiting rejects the
    /// request with [`AcpxError::PiExited`] (fixes #82).
    pub async fn request(&mut self, command: &RpcCommand) -> Result<Value> {
        self.request_response(command).await?.ok()
    }

    /// [`PiProcess::request`] without unwrapping `data` — callers that need the
    /// full response envelope (e.g. to distinguish error payloads) use this.
    pub async fn request_response(&mut self, command: &RpcCommand) -> Result<RpcResponse> {
        // Fail fast on a dead child instead of writing into a broken pipe and
        // hoping for a timeout (design D3; fixes #82's silent empty end_turn).
        if let Some((code, signal)) = self.exit_status() {
            return Err(AcpxError::PiExited { code, signal });
        }

        let id = self.next_id();
        let mut msg = serde_json::to_value(command)?;
        msg["id"] = Value::String(id.clone());

        let (tx, rx) = oneshot::channel();
        self.shared.pending.lock().await.insert(id.clone(), tx);

        if let Err(e) = self.write_line(&msg).await {
            // Write failed (EPIPE etc.): the request never reached pi — drop the
            // pending entry and surface the io error.
            self.shared.pending.lock().await.remove(&id);
            return Err(e.into());
        }

        let timeout = self.timeout;
        let secs = timeout.as_secs();
        match tokio::time::timeout(timeout, rx).await {
            // The pending map stores `oneshot::Sender<Result<Value>>`: the outer
            // `Ok` is the oneshot completing, the inner is the reader's/watcher's
            // payload (`Ok(response)` / `Err(PiExited)`).
            Ok(Ok(Ok(res))) => serde_json::from_value(res).map_err(Into::into),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => {
                // Sender dropped without a payload (stream ended before the
                // watcher rejected pending) — treat as a dead process.
                self.shared.pending.lock().await.remove(&id);
                Err(AcpxError::PiExited {
                    code: None,
                    signal: None,
                })
            }
            Err(_) => {
                // Deadline hit; drop the pending entry so a late response is
                // routed to the event channel as `UnmatchedResponse` instead of
                // resolving a vanished request.
                self.shared.pending.lock().await.remove(&id);
                Err(AcpxError::RpcTimeout {
                    cmd: command.name().to_string(),
                    secs,
                })
            }
        }
    }

    /// The next pi event, or `None` once the event stream has ended (reader
    /// task exited / receiver closed). Consumes the channel receiver lazily;
    /// either use this or [`PiProcess::take_event_receiver`], not both.
    pub async fn next_event(&mut self) -> Option<RpcEvent> {
        let rx = self.events.as_mut()?;
        rx.recv().await
    }

    /// Take the event channel receiver (once). The session event pump (S5)
    /// owns it for the session lifetime; [`PiProcess::next_event`] is the
    /// single-consumer convenience.
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<RpcEvent>> {
        self.events.take()
    }

    /// ANSI-stripped human-readable lines pi emitted on stdout before the
    /// NDJSON stream began (context / skills / extension banner). Consumed on
    /// read; see `startup` for how these feed the startup info (S6).
    pub fn consume_prelude_lines(&self) -> Vec<String> {
        let mut lines = self.shared.prelude.lock().unwrap();
        std::mem::take(&mut *lines)
    }

    /// Whether the watcher has observed the child exit.
    pub fn is_dead(&self) -> bool {
        self.exit_status().is_some()
    }

    /// The child's exit `(code, signal)` once it has exited; `None` while alive.
    /// `signal` is populated on unix only (windows reports `None`).
    pub fn exit_status(&self) -> Option<(Option<i32>, Option<i32>)> {
        *self.shared.exit.lock().unwrap()
    }

    /// Wait (bounded) for the watcher to observe the child's exit, then return
    /// the settled exit status. The reader may observe stdout EOF a moment
    /// before the watcher reaps the child; sessions call this when the event
    /// stream ends so the death error carries the real exit code (S8 / #82).
    pub async fn wait_exited(&mut self, timeout: Duration) -> Option<(Option<i32>, Option<i32>)> {
        let _ = tokio::time::timeout(timeout, &mut self.watcher).await;
        self.exit_status()
    }

    /// Send a `prompt` command and stream each pi event to `on_event` until the
    /// turn settles (`agent_settled`). The `prompt` `response` line arrives
    /// early (before the events) and is consumed by [`PiProcess::request`]; the
    /// turn is complete only at `agent_settled` (S2 constraint 2) — awaiting
    /// only the response would truncate streaming output.
    pub async fn prompt_until_settled<F: FnMut(&RpcEvent)>(
        &mut self,
        message: &str,
        mut on_event: F,
    ) -> Result<()> {
        self.request(&RpcCommand::Prompt {
            message: message.to_string(),
            images: None,
            streaming_behavior: None,
        })
        .await?;

        let timeout = self.timeout;
        let secs = timeout.as_secs();
        tokio::time::timeout(timeout, async {
            loop {
                match self.next_event().await {
                    Some(event) => {
                        if matches!(event, RpcEvent::AgentSettled) {
                            return Ok(());
                        }
                        on_event(&event);
                    }
                    // Stream ended without settling (pi died mid-turn).
                    None => {
                        let (code, signal) = self.exit_status().unwrap_or((None, None));
                        return Err(AcpxError::PiExited { code, signal });
                    }
                }
            }
        })
        .await
        .map_err(|_| AcpxError::RpcTimeout {
            cmd: "prompt".to_string(),
            secs,
        })?
    }

    // --- typed command wrappers (parity with TS `PiRpcProcess`) ---

    /// `get_state` → current session state.
    pub async fn get_state(&mut self) -> Result<RpcSessionState> {
        let data = self.request(&RpcCommand::GetState).await?;
        serde_json::from_value(data).map_err(Into::into)
    }

    /// `abort` — cancel the in-flight turn.
    pub async fn abort(&mut self) -> Result<()> {
        self.request(&RpcCommand::Abort).await?;
        Ok(())
    }

    /// `get_available_models` → all models pi can switch to.
    pub async fn get_available_models(&mut self) -> Result<Vec<Model>> {
        let data = self.request(&RpcCommand::GetAvailableModels).await?;
        let models = data
            .get("models")
            .cloned()
            .ok_or_else(|| AcpxError::RpcFailed {
                command: "get_available_models".into(),
                message: "response missing data.models".into(),
            })?;
        serde_json::from_value(models).map_err(Into::into)
    }

    /// `set_model` → the active model after the switch.
    pub async fn set_model(&mut self, provider: &str, model_id: &str) -> Result<Model> {
        let data = self
            .request(&RpcCommand::SetModel {
                provider: provider.to_string(),
                model_id: model_id.to_string(),
            })
            .await?;
        serde_json::from_value(data).map_err(Into::into)
    }

    /// `set_thinking_level`.
    pub async fn set_thinking_level(&mut self, level: ThinkingLevel) -> Result<()> {
        self.request(&RpcCommand::SetThinkingLevel { level })
            .await?;
        Ok(())
    }

    /// `set_follow_up_mode`.
    pub async fn set_follow_up_mode(&mut self, mode: QueueMode) -> Result<()> {
        self.request(&RpcCommand::SetFollowUpMode { mode }).await?;
        Ok(())
    }

    /// `set_steering_mode`.
    pub async fn set_steering_mode(&mut self, mode: QueueMode) -> Result<()> {
        self.request(&RpcCommand::SetSteeringMode { mode }).await?;
        Ok(())
    }

    /// `compact` — manual compaction; returns pi's `CompactionResult` payload.
    pub async fn compact(&mut self, custom_instructions: Option<&str>) -> Result<Value> {
        self.request(&RpcCommand::Compact {
            custom_instructions: custom_instructions.map(str::to_string),
        })
        .await
    }

    /// `set_auto_compaction`.
    pub async fn set_auto_compaction(&mut self, enabled: bool) -> Result<()> {
        self.request(&RpcCommand::SetAutoCompaction { enabled })
            .await?;
        Ok(())
    }

    /// `get_session_stats`.
    pub async fn get_session_stats(&mut self) -> Result<Value> {
        self.request(&RpcCommand::GetSessionStats).await
    }

    /// `set_session_name` (fixes the Zed sidebar title, #102/#24).
    pub async fn set_session_name(&mut self, name: &str) -> Result<()> {
        self.request(&RpcCommand::SetSessionName {
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    /// `export_html` → the written file path.
    pub async fn export_html(&mut self, output_path: Option<&str>) -> Result<String> {
        let data = self
            .request(&RpcCommand::ExportHtml {
                output_path: output_path.map(str::to_string),
            })
            .await?;
        data.get("path")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| AcpxError::RpcFailed {
                command: "export_html".into(),
                message: "response missing data.path".into(),
            })
    }

    /// `switch_session` — point this process at another pi session file.
    pub async fn switch_session(&mut self, session_path: &str) -> Result<()> {
        self.request(&RpcCommand::SwitchSession {
            session_path: session_path.to_string(),
        })
        .await?;
        Ok(())
    }

    /// `get_messages` — the session transcript (used for replay in S7).
    pub async fn get_messages(&mut self) -> Result<Value> {
        self.request(&RpcCommand::GetMessages).await
    }

    /// `get_commands` — available slash / skill / extension commands.
    pub async fn get_commands(&mut self) -> Result<Value> {
        self.request(&RpcCommand::GetCommands).await
    }

    /// Answer a pending [`crate::pi::rpc::ExtensionUiRequest`]. This is a
    /// fire-and-forget write (no `id`, no response) — pi matches it by the
    /// request's own id.
    pub async fn send_extension_ui_response(
        &mut self,
        response: ExtensionUiResponse,
    ) -> Result<()> {
        let mut msg = serde_json::to_value(response)?;
        msg["type"] = Value::String("extension_ui_response".into());
        self.write_line(&msg).await?;
        Ok(())
    }

    /// Graceful teardown: SIGTERM → grace → SIGKILL, then await the reaper.
    /// Prefer this over `Drop` (which is the sync emergency path); sessions
    /// call this on dispose. Also closes the event channel so pumps see EOF.
    pub async fn dispose(&mut self) {
        let pid = self.pid.take();
        if let Some(pid) = pid {
            tracing::debug!(pid, "dispose: sending SIGTERM");
            signal_pi(pid, /* term */ true);
        }
        if tokio::time::timeout(SIGTERM_GRACE, &mut self.watcher)
            .await
            .is_err()
        {
            if let Some(pid) = pid {
                tracing::debug!(pid, "dispose: SIGTERM ignored, escalating to SIGKILL");
                signal_pi(pid, /* term */ false);
            }
            // The watcher must reap the now-killed child; give it a bounded wait.
            let _ = tokio::time::timeout(SIGTERM_GRACE, &mut self.watcher).await;
        }
        // Close the event stream: any pump sees `None` and stops.
        self.events = None;
    }
}

impl Drop for PiProcess {
    /// Sync emergency teardown (used when `dispose()` was not called, e.g.
    /// panic unwinding or session-map eviction without an async context).
    /// SIGTERM immediately, then an off-thread SIGKILL escalation — `Drop`
    /// cannot await, so the grace period happens on a detached thread. The
    /// watcher task still owns the `Child` and reaps it.
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            signal_pi(pid, /* term */ true);
            std::thread::spawn(move || {
                std::thread::sleep(DROP_ESCALATION_DELAY);
                signal_pi(pid, /* term */ false);
            });
        }
    }
}

/// Signal a pi child, covering its whole process group when possible.
///
/// Unix: pi is spawned in its own group (`process_group(0)`), so first signal
/// the group (`-pid`), then fall back to the direct child — one of the two
/// necessarily works whether the group still exists or not. Uses the standard
/// `kill` binary (always present on POSIX) to avoid an unsafe libc binding.
/// Windows: no SIGTERM exists; `taskkill /T /F` force-terminates the tree.
#[cfg(unix)]
fn signal_pi(pid: u32, term: bool) {
    let sig = if term { "TERM" } else { "KILL" };
    for target in [format!("-{pid}"), pid.to_string()] {
        let _ = std::process::Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(&target)
            .output();
    }
}

#[cfg(windows)]
fn signal_pi(pid: u32, _term: bool) {
    let _ = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .output();
}

/// Build an actionable spawn-failure message. A `NotFound` (ENOENT) gets an
/// explicit install hint; other failures carry the raw error so the cause
/// (EACCES, bad path, ...) is not lost.
fn spawn_error(
    pi_command: &str,
    resolved: &crate::pi::resolve::ResolvedPi,
    e: std::io::Error,
) -> String {
    match e.kind() {
        std::io::ErrorKind::NotFound => format!(
            "`{pi_command}` not found on PATH (resolved to {})",
            resolved.program
        ),
        _ => format!("{pi_command}: {e}"),
    }
}

/// Background task: read pi's stdout line by line until EOF, routing responses
/// to the pending map and everything else to the event channel. Non-JSON lines
/// are treated as prelude (ANSI-stripped). A closed event channel (no consumer)
/// stops event forwarding but *keeps reading* so pending responses still land.
async fn read_loop(stdout: ChildStdout, shared: Arc<Shared>, events_tx: mpsc::Sender<RpcEvent>) {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF (child exited or closed stdout)
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "pi stdout read error; ending event stream");
                break;
            }
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                // Human-readable prelude (context/skills/extension banner):
                // strip ANSI and collect for the startup info.
                let cleaned = strip_ansi(trimmed).trim_end().to_string();
                if !cleaned.is_empty() {
                    let mut prelude = shared.prelude.lock().unwrap();
                    if prelude.len() < PRELUDE_CAP {
                        prelude.push(cleaned);
                    }
                }
                continue;
            }
        };

        if value.get("type").and_then(Value::as_str) == Some("response") {
            let matched = match value.get("id").and_then(Value::as_str) {
                Some(id) => shared.pending.lock().await.remove(id),
                None => None,
            };
            match matched {
                Some(tx) => {
                    let _ = tx.send(Ok(value));
                }
                None => {
                    // Late response (after a timeout) or unsolicited — route to
                    // the event stream rather than dropping (TS parity).
                    let _ = events_tx
                        .send(RpcEvent::UnmatchedResponse { raw: value })
                        .await;
                }
            }
            continue;
        }

        let event = match serde_json::from_value::<RpcEvent>(value) {
            Ok(ev) => ev,
            Err(e) => {
                tracing::warn!(error = %e, "unparseable pi event; forwarding raw");
                // Need the raw value back for the Unknown variant.
                let raw = serde_json::from_str(trimmed).unwrap_or(Value::Null);
                RpcEvent::Unknown { raw }
            }
        };

        if events_tx.send(event).await.is_err() {
            // No event consumer (yet) — the session pump may not have started.
            // Keep reading so pending responses still resolve; events are dropped.
            tracing::trace!("pi event channel closed; dropping events");
        }
    }

    // Stream ended: signal consumers by dropping the sender. The watcher task
    // independently marks the process dead / rejects pending.
    tracing::debug!("pi stdout stream ended");
}

/// Background task: reap the child and propagate its exit.
/// On exit: record `(code, signal)`, reject every pending request with
/// [`AcpxError::PiExited`], and clear the pending map.
async fn wait_loop(mut child: Child, shared: Arc<Shared>) {
    let status = child.wait().await.ok();
    let (code, signal) = match &status {
        Some(s) => (s.code(), exit_signal(s)),
        None => (None, None),
    };
    tracing::info!(code, signal, "pi subprocess exited");

    {
        let mut exit = shared.exit.lock().unwrap();
        *exit = Some((code, signal));
    }

    let pending = {
        let mut map = shared.pending.lock().await;
        std::mem::take(&mut *map)
    };
    let count = pending.len();
    for (_, tx) in pending {
        let _ = tx.send(Err(AcpxError::PiExited { code, signal }));
    }
    if count > 0 {
        tracing::warn!(count, "rejected pending pi RPC requests after child exit");
    }
}

/// `ExitStatus::signal()` is unix-only.
#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

/// Strip ANSI escape sequences (colors, cursor movement, OSC titles) from a
/// line, mirroring the TS reference's regex-based `stripAnsi`. Best-effort:
/// CSI (`ESC [ ... final-byte`) and OSC (`ESC ] ... BEL|ST`) are removed;
/// stray lone ESC sequences drop the ESC and the following char.
pub fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut it = input.chars().peekable();
    while let Some(c) = it.next() {
        match c {
            '\u{1b}' => match it.peek() {
                Some('[') => {
                    it.next(); // consume '['
                    for esc in it.by_ref() {
                        if ('\u{40}'..='\u{7e}').contains(&esc) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    // OSC: consume until BEL (0x07) or ST (ESC \).
                    it.next(); // consume ']'
                    for esc in it.by_ref() {
                        match esc {
                            '\u{07}' => break,
                            '\u{1b}' => {
                                let _ = it.next(); // consume '\' of ST
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    // Lone ESC: drop it and the next char (best-effort).
                    it.next();
                }
            },
            // 8-bit CSI (0x9b) — treat like ESC [.
            '\u{9b}' => {
                for esc in it.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&esc) {
                        break;
                    }
                }
            }
            other => out.push(other),
        }
    }
    out
}

/// Extract the streaming assistant text delta from a pi event, if any (S2
/// constraint 3: text arrives chunk-by-chunk in `text_delta`).
pub fn text_delta_of(event: &RpcEvent) -> Option<&str> {
    match event {
        RpcEvent::MessageUpdate {
            assistant_message_event,
            ..
        } => assistant_message_event.text_delta(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pi::rpc::AssistantMessageEvent;

    #[test]
    fn strips_csi_color_codes() {
        let input = "\u{1b}[32mContext\u{1b}[0m: mock context";
        assert_eq!(strip_ansi(input), "Context: mock context");
    }

    #[test]
    fn strips_8bit_csi_and_osx_title() {
        assert_eq!(strip_ansi("\u{9b}1mplain\u{9b}0m"), "plain");
        assert_eq!(strip_ansi("\u{1b}]0;my title\u{07}body"), "body");
    }

    #[test]
    fn leaves_plain_text_alone() {
        assert_eq!(
            strip_ansi("plain line with no escapes"),
            "plain line with no escapes"
        );
        assert_eq!(strip_ansi(""), "");
    }

    #[test]
    fn extracts_text_delta_from_typed_event() {
        let ev: RpcEvent = serde_json::from_str(
            r#"{"type":"message_update","usage":{},"assistantMessageEvent":{"type":"text_delta","contentIndex":1,"delta":"pong"}}"#,
        )
        .unwrap();
        assert_eq!(text_delta_of(&ev), Some("pong"));

        let thinking: RpcEvent = serde_json::from_str(
            r#"{"type":"message_update","usage":{},"assistantMessageEvent":{"type":"thinking_delta","contentIndex":0,"delta":"hmm"}}"#,
        )
        .unwrap();
        assert_eq!(text_delta_of(&thinking), None);

        let settled: RpcEvent = serde_json::from_str(r#"{"type":"agent_settled"}"#).unwrap();
        assert_eq!(text_delta_of(&settled), None);

        // Direct sub-event accessor.
        let ev: AssistantMessageEvent =
            serde_json::from_str(r#"{"type":"text_delta","contentIndex":2,"delta":"x"}"#).unwrap();
        assert_eq!(ev.content_index(), 2);
        assert_eq!(ev.text_delta(), Some("x"));
    }
}
