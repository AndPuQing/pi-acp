//! `PiAcpSession` — the per-session state machine (S5, W-452).
//!
//! Ports `acp/session.ts` (`PiAcpSession`) onto tokio. One session = one pi
//! `--mode rpc` subprocess plus a **pump task** that owns the process handle
//! and the pi event stream:
//!
//! - **TurnQueue**: client-side `one-at-a-time` queueing. A `prompt` while a
//!   turn is running is queued; the queue is drained one turn at a time, each
//!   completing only on pi's `agent_settled` event (**not** `agent_end`, which
//!   pi may emit repeatedly for retries/compaction/continuations). `cancel()`
//!   clears the queue (each queued turn resolves `Cancelled`) and aborts the
//!   in-flight turn. A settle timeout (pi accepted the prompt but never
//!   settled) does **not** immediately retire the session: the stuck turn fails
//!   with `SettleTimeout`, pi is aborted, and new prompts queue behind a
//!   bounded drain until the abort response and late `agent_settled` arrive —
//!   so neither can affect a fresh turn (W-480).
//! - **Event pump**: a single `tokio::select!` loop consumes pi events and
//!   session commands; every outbound ACP notification goes through one
//!   ordered channel (`[`OutboundMessage`]`), so `session/update` frames leave
//!   in the exact order the state machine produced them (design D4).
//! - **Monotonic tool status**: `pending -> in_progress -> completed/failed`
//!   never downgrades. pi events can arrive out of order (late `toolcall_*`
//!   deltas after execution started); a tool already marked `in_progress`
//!   stays `in_progress` when a streaming event re-surfaces it.
//! - **edit/write diff**: on `tool_execution_start` the file is snapshotted
//!   *before* the mutation; on `tool_execution_end` the new content is read
//!   back and emitted as ACP `ToolCallContent::Diff` (old/new text) instead of
//!   plain text. `edit` calls also resolve a 1-based line number from the
//!   first uniquely-located `oldText` (S4 `find_unique_line_number`).
//! - **Extension UI bridge**: pi `select`/`confirm` requests are bridged to
//!   ACP `session/request_permission` (options map to `PermissionOption`s);
//!   `input`/`editor`/`notify` and anything else are answered with a v1
//!   `cancelled` response (parity with the TS reference).
//!
//! The pump task is created by [`PiAcpSession::spawn`]; external callers
//! (`prompt` / `cancel` / `dispose`) talk to it over a command channel. The
//! outbound channel is consumed either by a test recorder or by
//! [`spawn_outbound_connector`], which bridges it to the ACP SDK connection
//! (wired in S6).

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    ConfigOptionUpdate, ContentBlock, ContentChunk, Cost, CurrentModeUpdate, Diff,
    PermissionOption, PermissionOptionKind, RequestPermissionOutcome, RequestPermissionRequest,
    RequestPermissionResponse, SessionId, SessionInfoUpdate, SessionNotification, SessionUpdate,
    TextContent, ToolCall, ToolCallContent, ToolCallId, ToolCallStatus, ToolCallUpdate,
    ToolCallUpdateFields, ToolKind, UsageUpdate,
};
use agent_client_protocol::{Client, ConnectionTo};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::error::{AcpxError, Result};
use crate::pi::process::{PiProcess, RpcClient};
use crate::pi::rpc::{
    supported_thinking_levels, AssistantMessageEvent, CompactionReason, ExtensionUiRequest,
    ExtensionUiResponse, ImageContent, Model, RpcCommand, RpcEvent, RpcSessionState, ThinkingLevel,
    Usage,
};
use crate::time::utc_now_iso8601;
use crate::translate::bash::{
    bash_command, bash_exit_code, bash_output_delta, bash_result_text, bash_terminal_content,
    bash_terminal_exit_meta, bash_terminal_info_meta, bash_terminal_output_meta, is_bash_tool,
};
use crate::translate::tools::{
    edit_old_texts, find_unique_line_number, to_tool_call_locations, to_tool_kind, tool_path,
    tool_result_to_text,
};

/// How a turn ended (mirrors TS `StopReason`, minus `'error'` — the Rust
/// rewrite surfaces failures as explicit `Err(AcpxError)`, design D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The turn completed normally (`agent_settled`).
    EndTurn,
    /// The turn was cancelled by the client (`session/cancel`).
    Cancelled,
}

/// The pi subprocess's exit `(code, signal)` once it has exited (`None` while
/// alive); shared between the session handle and the pump task so a dead pi is
/// reported loudly on every later command (S8 / #82).
type PiExitStatus = Option<(Option<i32>, Option<i32>)>;

/// How long after our own thinking/model set a `thinking_level_changed` event
/// counts as that set's echo (W-479 P1). The echoing event follows the set
/// response by milliseconds; 2s is generous headroom for loaded runners while
/// keeping genuinely pi-initiated later changes on the full-refresh path.
const THINKING_SET_ECHO_WINDOW: Duration = Duration::from_secs(2);

/// How long a settle-timeout recovery waits for pi to confirm that the
/// aborted turn is over (W-480). A healthy pi settles promptly after `abort`;
/// an unresponsive one is retired instead of letting stale events race a
/// fresh turn.
const STALE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether a `thinking_level_changed` event observed at `now` is the echo of
/// our own set at `last_set` (pure predicate for the pump's echo suppression).
fn thinking_event_is_echo(last_set: Option<std::time::Instant>, now: std::time::Instant) -> bool {
    last_set.is_some_and(|t| now.saturating_duration_since(t) <= THINKING_SET_ECHO_WINDOW)
}

/// Clear a thinking-set stamp recorded before a failed set — but only when it
/// is still ours, so a concurrent newer set's stamp is never wiped.
fn clear_thinking_stamp(
    slot: &Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    stamp: std::time::Instant,
) {
    let mut guard = slot.lock().unwrap();
    if guard.as_ref() == Some(&stamp) {
        *guard = None;
    }
}

/// Outbound ACP messages produced by the session pump.
///
/// Everything the session sends to the ACP client travels this single ordered
/// channel (design §8.1: *all `sessionUpdate` notifications through one
/// ordered sink*). Production bridges it to the SDK connection via
/// [`spawn_outbound_connector`]; tests record and answer it directly.
#[derive(Debug)]
pub enum OutboundMessage {
    /// A `session/update` notification (full frame, including the session id).
    Notify(SessionNotification),
    /// A `session/request_permission` request; the answer is delivered back on
    /// the oneshot. If the responder is dropped without sending, the request
    /// is treated as cancelled by the caller.
    RequestPermission(
        RequestPermissionRequest,
        oneshot::Sender<std::result::Result<RequestPermissionResponse, AcpxError>>,
    ),
    /// Ordering barrier (S8 / D4): acknowledged once everything sent before it
    /// has been forwarded to the connection. The pump awaits this before
    /// resolving a turn so streamed notifications are never overtaken by the
    /// `session/prompt` response (TS `flushEmits` parity).
    Flush(oneshot::Sender<()>),
}

/// Parameters for spawning a session (S6 agent wiring / tests).
pub struct SessionParams {
    /// `pi` executable to spawn.
    pub pi_command: String,
    /// Extra CLI flags appended after the standard pi RPC arguments
    /// (test fixtures like `--mock-rpc`; empty in production).
    pub extra_args: Vec<String>,
    /// Per-request pi RPC deadline.
    pub timeout: Duration,
    /// Deadline for a turn's `agent_settled` after pi accepts the prompt
    /// (design §11 risk #84 mitigation). When it elapses the pending turn is
    /// resolved with [`AcpxError::SettleTimeout`] instead of hanging
    /// `session/prompt` forever. `Duration::ZERO` disables the fallback.
    pub settle_timeout: Duration,
    /// Working directory of the session (resolves relative tool paths).
    pub cwd: PathBuf,
    /// Outbound ACP message sink (see [`OutboundMessage`]).
    pub outbound: mpsc::Sender<OutboundMessage>,
    /// Optional pi session file to resume (`--session <path>`; used by
    /// `session/load`).
    pub session_path: Option<PathBuf>,
    /// Optional ACP session id override (used by `session/load`). It must match
    /// the id reported by pi for `session_path`; it is only an ACP registration
    /// value, never a replacement for pi's native/provider session id.
    pub session_id_override: Option<SessionId>,
    /// File-based slash commands to expand in `prompt` (pi RPC mode disables
    /// its own slash expansion, so pi-acp does it — TS `session.ts`).
    pub file_commands: Vec<crate::commands::FileSlashCommand>,
    /// Per-child environment overrides for the pi subprocess (inherits the
    /// rest). W-483 passes the session-scoped `PI_ACP_MCP_SERVERS_JSON`
    /// payload here; empty everywhere else.
    pub extra_env: Vec<(String, String)>,
}

/// A handle to a running session. The heavy lifting lives in the pump task;
/// this handle is cheaply cloneable (`Arc`) so the `SessionManager` can share
/// it with the ACP agent handlers.
#[derive(Debug)]
pub struct PiAcpSession {
    session_id: SessionId,
    cwd: PathBuf,
    cmd_tx: mpsc::Sender<SessionCommand>,
    /// Whether a real (non-slash) prompt has already been sent; the first one
    /// derives the thread's provisional title (fixes #102/#24).
    first_prompt: AtomicBool,
    /// File-based slash commands for this session's cwd (expanded in
    /// [`PiAcpSession::prompt`]; pi RPC mode disables its own expansion).
    file_commands: Vec<crate::commands::FileSlashCommand>,
    /// The pi subprocess's exit `(code, signal)` when it died **unexpectedly**
    /// (stream end, not graceful dispose). Commands issued after death fail
    /// with [`AcpxError::PiExited`] (code/signal + hint) instead of a generic
    /// "session closed" (S8 / fixes #82 — a dead pi is always loud).
    death: Arc<std::sync::Mutex<PiExitStatus>>,
    /// The `get_state` snapshot taken during [`PiAcpSession::spawn`]. No pi
    /// mutation happens between spawn and the `session/new` handshake, so the
    /// agent reuses this instead of re-fetching state (W-479: saves one pi
    /// round-trip on the session/new critical path). Later reads use the
    /// live [`PiAcpSession::get_state`] RPC.
    initial_state: RpcSessionState,
    /// When our own `set_thinking_level` / `set_model` last succeeded.
    /// A `thinking_level_changed` event arriving inside
    /// [`THINKING_SET_ECHO_WINDOW`] is that set's echo: the agent's explicit
    /// config refresh (which runs right after the set) already re-read the
    /// same post-set state, so the pump skips its own re-read and emits only
    /// the authoritative mode update (W-479 P1).
    thinking_set_at: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

/// Commands the pump task accepts from the outside world.
enum SessionCommand {
    /// Start (or queue) a turn.
    Prompt {
        message: String,
        images: Vec<ImageContent>,
        respond: oneshot::Sender<Result<StopReason>>,
    },
    /// Clear the queue and abort the in-flight turn.
    Cancel {
        respond: oneshot::Sender<Result<()>>,
    },
    /// Run an arbitrary pi RPC command on the session's process and return the
    /// response `data` (thin delegation for the agent's method handlers —
    /// `set_model`, `compact`, `get_commands`, ...).
    Rpc {
        command: RpcCommand,
        respond: oneshot::Sender<Result<Value>>,
    },
    /// Refresh the cached context window after a successful `set_model` (feeds
    /// ACP `usage_update.size`).
    SetContextWindow {
        window: Option<u64>,
        respond: oneshot::Sender<()>,
    },
    /// Publish the empty initial context usage once the ACP session is known
    /// to the client (`session/new` response has been sent).
    PublishInitialUsage { respond: oneshot::Sender<()> },
    /// Snapshot the MCP registrar markers scraped from pi's stderr (W-483
    /// handshake gate input).
    McpMarkerSnapshot {
        respond: oneshot::Sender<Vec<String>>,
    },
    /// Graceful teardown: dispose the pi process, then signal completion.
    Shutdown { done: oneshot::Sender<()> },
}

/// A queued (not yet started) turn.
struct QueuedTurn {
    message: String,
    images: Vec<ImageContent>,
    resolve: oneshot::Sender<Result<StopReason>>,
}

/// The currently running turn.
struct PendingTurn {
    resolve: oneshot::Sender<Result<StopReason>>,
    /// Monotonic identity for the prompt RPC that owns this turn. Pi events do
    /// not carry a turn id, but prompt responses do arrive through a separate
    /// channel, so this prevents a late response from an earlier turn from
    /// mutating the current turn's deadline or result.
    turn_id: u64,
    /// Whether pi has acknowledged the prompt RPC. Normally this precedes all
    /// events, but separate response/event channels can be observed in either
    /// order by the pump.
    prompt_accepted: bool,
    /// An `agent_settled` observed before the prompt response. Keep it until
    /// the matching response confirms that it belongs to this turn.
    settled_before_accept: bool,
    /// Completes once the prompt JSON line is written. Cancellation waits for
    /// this before sending abort so abort cannot overtake a prompt that has not
    /// reached pi yet.
    prompt_started: Option<oneshot::Receiver<()>>,
}

/// Result of the early prompt RPC, tagged with the turn that sent it.
struct PromptResult {
    turn_id: u64,
    result: std::result::Result<(), AcpxError>,
}

/// Monotonic tool-call status tracked by the session. pi may surface a tool
/// multiple times (streaming `toolcall_*` deltas, then `tool_execution_*`);
/// the tracked status only ever moves forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackedStatus {
    Pending,
    InProgress,
}

/// Pre-mutation snapshot of a file an `edit`/`write` tool call is about to
/// touch, used to emit an ACP structured diff on completion.
#[derive(Debug, Clone)]
struct FileSnapshot {
    /// The path as given in the tool args (kept verbatim — TS parity: the
    /// diff's `path` field carries the raw path).
    path: String,
    /// Prior content; `None` when the file did not exist / could not be read
    /// (a new file, so the diff's `oldText` is `None`).
    old_text: Option<String>,
}

/// Per-session state machine state, owned by the pump task.
struct Pump {
    proc: Arc<Mutex<PiProcess>>,
    /// Shared request transport. Request waits do not hold `proc`, allowing
    /// cancellation to send `abort` while the prompt response is pending.
    rpc: Arc<RpcClient>,
    outbound: mpsc::Sender<OutboundMessage>,
    session_id: SessionId,
    cwd: PathBuf,
    event_rx: mpsc::Receiver<RpcEvent>,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    /// Extension-UI answers written back to pi (fire-and-forget writes). Kept
    /// on their own channel so the *command* channel closes — and the pump
    /// shuts down — the moment every [`PiAcpSession`] handle is dropped.
    extension_rx: mpsc::Receiver<ExtensionUiResponse>,
    /// Held so `extension_rx.recv()` parks when no answer is pending; cloned
    /// into the spawned extension-UI tasks.
    extension_tx: mpsc::Sender<ExtensionUiResponse>,
    /// Receives the result of the in-flight `prompt` RPC (the *early*
    /// acceptance response — the turn itself completes at `agent_settled`).
    prompt_rx: mpsc::Receiver<PromptResult>,
    /// Held open so `prompt_rx.recv()` parks when no prompt is in flight.
    _prompt_tx: mpsc::Sender<PromptResult>,
    /// Receives the result of the recovery `abort` RPC. The abort runs in a
    /// separate task so the pump can continue consuming pi events while the
    /// request is pending.
    recovery_abort_rx: mpsc::Receiver<Result<()>>,
    /// Held open so `recovery_abort_rx.recv()` parks while no recovery abort is
    /// in flight; cloned into the recovery abort task.
    recovery_abort_tx: mpsc::Sender<Result<()>>,
    /// Shared death record; set at teardown when pi exited unexpectedly so the
    /// [`PiAcpSession`] handle fails later commands with [`AcpxError::PiExited`].
    death: Arc<std::sync::Mutex<PiExitStatus>>,
    /// Shared with the [`PiAcpSession`] handle: when our own thinking/model
    /// set last succeeded (see the handle's field docs; W-479 P1).
    thinking_set_at: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    /// True when the pump loop exited because pi's stdout ended (process died).
    pi_died: bool,

    /// Client-side one-at-a-time turn queue.
    queue: VecDeque<QueuedTurn>,
    pending_turn: Option<PendingTurn>,
    next_turn_id: u64,
    /// Maps abort semantics to the ACP stop reason for the running turn.
    cancel_requested: bool,
    /// A turn failure that leaves pi's event stream ambiguous with no bounded
    /// recovery (a rejected `prompt` RPC, or an `abort` that never confirmed)
    /// retires the session: later prompts fail with `SessionClosed` until the
    /// session is disposed and recreated. A settle timeout is NOT poisoned —
    /// it recovers through [`Pump::recovering`] instead (W-480).
    poisoned: bool,
    /// Settle-timeout recovery (W-480): pi accepted a prompt but never
    /// settled, so the turn failed with `SettleTimeout` and pi was aborted.
    /// The aborted turn's late `agent_settled` carries no turn id and must
    /// never complete a fresh turn, so new prompts queue until the stale
    /// settle arrives (absorbed) or [`Pump::recover_deadline`] elapses; an
    /// unconfirmed recovery is then poisoned rather than starting a fresh turn.
    recovering: bool,
    /// End of the stale-settle drain (see [`Pump::recovering`]). `None` while
    /// not recovering.
    recover_deadline: Option<tokio::time::Instant>,
    /// The recovery `abort` RPC has returned successfully. A successful RPC
    /// alone is insufficient: pi must also emit the stale turn's settle event.
    recovery_abort_confirmed: bool,
    /// The stale turn's `agent_settled` has been consumed during recovery.
    recovery_settled: bool,
    /// True while pi's agent loop is running (`agent_start` .. `agent_end`).
    in_agent_loop: bool,
    /// Deadline by which the in-flight turn's `agent_settled` must arrive
    /// (design §11 risk #84 mitigation: a pi that accepts a prompt but never
    /// settles must not hang `session/prompt` forever). Armed when the prompt
    /// is accepted; cleared at resolution. `None` = no deadline (disabled or
    /// no turn in flight).
    settle_deadline: Option<tokio::time::Instant>,
    /// The settle deadline duration (from [`SessionParams::settle_timeout`]).
    settle_timeout: Duration,

    /// Monotonic tool statuses (`tool_call_id` -> status).
    current_tool_calls: HashMap<String, TrackedStatus>,
    /// Tool call ids that mutate files (`edit` / `write`).
    file_mutation_tool_call_ids: HashSet<String>,
    file_snapshots: HashMap<String, FileSnapshot>,
    bash_tool_call_ids: HashSet<String>,
    bash_output_snapshots: HashMap<String, String>,
    /// The active model's context window (tokens), from `get_state` at spawn
    /// and refreshed on `set_model`. Feeds ACP `usage_update.size` (S6).
    context_window: Option<u64>,
}

impl PiAcpSession {
    /// Spawn `pi --mode rpc`, learn its session id, and start the pump task.
    pub async fn spawn(params: SessionParams) -> Result<Arc<Self>> {
        let extra: Vec<&str> = params.extra_args.iter().map(String::as_str).collect();
        let session_path = params.session_path.as_deref();
        let mut proc = PiProcess::spawn_with_args_in_dir_and_env(
            &params.pi_command,
            &extra,
            session_path,
            &params.cwd,
            &params.extra_env,
            params.timeout,
        )
        .await?;
        let state = match proc.get_state().await {
            Ok(state) => state,
            Err(err) => {
                // The process is owned locally until the pump is installed;
                // dispose it here so a failed startup cannot orphan the pi
                // child while the caller handles the handshake error.
                proc.dispose().await;
                return Err(err);
            }
        };
        let session_id = match params.session_id_override {
            Some(expected) => {
                if expected.0.as_ref() != state.session_id.as_str() {
                    let expected_id = expected.0.to_string();
                    let actual_id = state.session_id.clone();
                    // Do not leave a pi child running after rejecting a stale
                    // map entry or a missing session file.
                    proc.dispose().await;
                    return Err(AcpxError::SessionIdMismatch {
                        expected: expected_id,
                        actual: actual_id,
                    });
                }
                expected
            }
            None => state.session_id.clone().into(),
        };
        tracing::info!(session_id = %session_id.0, "pi session ready");
        let rpc = proc.request_client();
        let event_rx = match proc.take_event_receiver() {
            Some(event_rx) => event_rx,
            None => {
                proc.dispose().await;
                return Err(AcpxError::RpcFailed {
                    command: "session".into(),
                    message: "pi event channel already taken".into(),
                });
            }
        };

        // Shared set-timestamp for thinking-echo suppression (W-479 P1).
        let thinking_set_at = Arc::new(std::sync::Mutex::new(None));
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (prompt_tx, prompt_rx) = mpsc::channel(4);
        let (recovery_abort_tx, recovery_abort_rx) = mpsc::channel(1);
        let (extension_tx, extension_rx) = mpsc::channel(8);
        let death = Arc::new(std::sync::Mutex::new(None));
        let pump = Pump {
            proc: Arc::new(Mutex::new(proc)),
            rpc,
            outbound: params.outbound.clone(),
            session_id: session_id.clone(),
            cwd: params.cwd.clone(),
            event_rx,
            cmd_rx,
            extension_rx,
            extension_tx,
            prompt_rx,
            _prompt_tx: prompt_tx,
            recovery_abort_rx,
            recovery_abort_tx,
            death: death.clone(),
            thinking_set_at: thinking_set_at.clone(),
            pi_died: false,
            queue: VecDeque::new(),
            pending_turn: None,
            next_turn_id: 0,
            cancel_requested: false,
            poisoned: false,
            recovering: false,
            recover_deadline: None,
            recovery_abort_confirmed: false,
            recovery_settled: false,
            in_agent_loop: false,
            settle_deadline: None,
            settle_timeout: params.settle_timeout,
            current_tool_calls: HashMap::new(),
            file_mutation_tool_call_ids: HashSet::new(),
            file_snapshots: HashMap::new(),
            bash_tool_call_ids: HashSet::new(),
            bash_output_snapshots: HashMap::new(),
            context_window: state.model.as_ref().and_then(|m| m.context_window),
        };
        tokio::spawn(pump_loop(pump));

        Ok(Arc::new(Self {
            session_id,
            cwd: params.cwd,
            cmd_tx,
            first_prompt: AtomicBool::new(false),
            file_commands: params.file_commands,
            death,
            initial_state: state,
            thinking_set_at,
        }))
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// The spawn-time `get_state` snapshot (see the field docs). The
    /// `session/new` handshake consumes this instead of a second `get_state`
    /// round-trip (W-479).
    pub fn initial_state(&self) -> &RpcSessionState {
        &self.initial_state
    }

    /// The error to surface when the session's pump is gone: [`AcpxError::PiExited`]
    /// when pi died unexpectedly (carrying code/signal so the client gets the
    /// "pi is dead" diagnosis + hint), else [`AcpxError::SessionClosed`].
    fn death_error(&self) -> AcpxError {
        if let Some((code, signal)) = *self.death.lock().unwrap() {
            AcpxError::PiExited { code, signal }
        } else {
            AcpxError::SessionClosed(self.session_id.0.to_string())
        }
    }

    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    /// Atomically claim the first-prompt slot. Returns `true` on the first
    /// call and `false` forever after; drives the provisional-title emission
    /// in the agent's `session/prompt` handler (fixes #102/#24).
    pub fn mark_first_prompt(&self) -> bool {
        !self.first_prompt.swap(true, Ordering::SeqCst)
    }

    /// Start a turn (or queue it behind the running one) and await its
    /// completion — which happens at pi's `agent_settled`, **not** at the
    /// early `prompt` response (S2 constraint 2).
    ///
    /// File-based slash commands are expanded first (pi RPC mode disables its
    /// own expansion; TS `session.prompt` does the same).
    ///
    /// Returns [`StopReason::EndTurn`] for a normal settle, [`StopReason::Cancelled`]
    /// when `cancel()` was requested, or `Err` when the turn failed (pi error /
    /// process death) — surfaced explicitly per design D5.
    pub async fn prompt(&self, message: String, images: Vec<ImageContent>) -> Result<StopReason> {
        let expanded = crate::commands::expand_slash_command(&message, &self.file_commands);
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Prompt {
                message: expanded,
                images,
                respond: tx,
            })
            .await
            .map_err(|_| self.death_error())?;
        rx.await.map_err(|_| self.death_error())?
    }

    /// Cancel the running turn and clear all queued turns (each resolves
    /// `Cancelled`). Mirrors TS `PiAcpSession.cancel`: also sends `abort` to
    /// pi, which then settles and resolves the in-flight turn as `Cancelled`.
    pub async fn cancel(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Cancel { respond: tx })
            .await
            .map_err(|_| self.death_error())?;
        rx.await.map_err(|_| self.death_error())?
    }

    /// Gracefully tear the session down: dispose the pi process (SIGTERM →
    /// SIGKILL) and stop the pump. Pending/queued turns fail with
    /// [`AcpxError::PiExited`].
    pub async fn dispose(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(SessionCommand::Shutdown { done: tx })
            .await;
        let _ = rx.await;
    }

    // --- thin pi RPC delegation (agent method handlers, S6) ---
    //
    // The pump owns the `PiProcess`; these send a [`SessionCommand::Rpc`] and
    // await the response. Keeps the ACP handlers thin while the process stays
    // inside the session.

    async fn rpc(&self, command: RpcCommand) -> Result<Value> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Rpc {
                command,
                respond: tx,
            })
            .await
            .map_err(|_| self.death_error())?;
        rx.await.map_err(|_| self.death_error())?
    }

    /// `get_state`.
    pub async fn get_state(&self) -> Result<crate::pi::rpc::RpcSessionState> {
        let data = self.rpc(RpcCommand::GetState).await?;
        serde_json::from_value(data).map_err(Into::into)
    }

    /// `get_available_models`.
    pub async fn get_available_models(&self) -> Result<Vec<crate::pi::rpc::Model>> {
        let data = self.rpc(RpcCommand::GetAvailableModels).await?;
        let models = data
            .get("models")
            .cloned()
            .ok_or_else(|| AcpxError::RpcFailed {
                command: "get_available_models".into(),
                message: "response missing data.models".into(),
            })?;
        serde_json::from_value(models).map_err(Into::into)
    }

    /// `set_model`; refreshes the cached context window for `usage_update`.
    /// A model switch can clamp thinking and emit `thinking_level_changed`,
    /// so it shares `set_thinking_level`'s echo bookkeeping (W-479 P1).
    pub async fn set_model(&self, provider: &str, model_id: &str) -> Result<()> {
        let stamp = std::time::Instant::now();
        *self.thinking_set_at.lock().unwrap() = Some(stamp);
        let result = self.set_model_inner(provider, model_id).await;
        if result.is_err() {
            clear_thinking_stamp(&self.thinking_set_at, stamp);
        }
        result
    }

    async fn set_model_inner(&self, provider: &str, model_id: &str) -> Result<()> {
        let data = self
            .rpc(RpcCommand::SetModel {
                provider: provider.to_string(),
                model_id: model_id.to_string(),
            })
            .await?;
        let window = serde_json::from_value::<crate::pi::rpc::Model>(data)
            .ok()
            .and_then(|m| m.context_window);
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::SetContextWindow {
                window,
                respond: tx,
            })
            .await
            .map_err(|_| self.death_error())?;
        let _ = rx.await;
        Ok(())
    }

    /// Publish an empty ACP context usage update for a newly-created session.
    ///
    /// pi only reports usage after a model turn. Zed needs the model's context
    /// window before that first turn to render its context indicator.
    pub async fn publish_initial_usage(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::PublishInitialUsage { respond: tx })
            .await
            .map_err(|_| self.death_error())?;
        rx.await.map_err(|_| self.death_error())?;
        Ok(())
    }

    /// Snapshot the MCP registrar markers scraped from pi's stderr (W-483
    /// handshake gate input; a dead pump surfaces as the session's death
    /// error).
    pub async fn mcp_marker_snapshot(&self) -> Result<Vec<String>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::McpMarkerSnapshot { respond: tx })
            .await
            .map_err(|_| self.death_error())?;
        rx.await.map_err(|_| self.death_error())
    }

    /// `set_thinking_level`. Records the set timestamp *before* sending the
    /// RPC so a fast `thinking_level_changed` echo can never overtake the
    /// stamp (the pump processes events concurrently); a failed set clears
    /// its own stamp again so it can never suppress a later genuine refresh
    /// (W-479 P1 echo suppression).
    pub async fn set_thinking_level(&self, level: crate::pi::rpc::ThinkingLevel) -> Result<()> {
        let stamp = std::time::Instant::now();
        *self.thinking_set_at.lock().unwrap() = Some(stamp);
        let result = self.rpc(RpcCommand::SetThinkingLevel { level }).await;
        if result.is_err() {
            clear_thinking_stamp(&self.thinking_set_at, stamp);
        }
        result?;
        Ok(())
    }

    /// Native per-model thinking levels for the ACP selector (W-478).
    ///
    /// Queries pi's `get_available_thinking_levels` (the same source pi's
    /// own TUI uses), so Zed offers exactly the levels the active model
    /// supports. Falls back to the local `supported_thinking_levels`
    /// computation from the current model (pi-ai `getSupportedThinkingLevels`
    /// parity, for older pi builds without the RPC), then to the full ladder.
    pub async fn available_thinking_levels(&self) -> Vec<crate::pi::rpc::ThinkingLevel> {
        if let Ok(data) = self.rpc(RpcCommand::GetAvailableThinkingLevels).await {
            if let Some(levels) = data.get("levels").and_then(Value::as_array) {
                let parsed: Vec<crate::pi::rpc::ThinkingLevel> = levels
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(crate::pi::rpc::ThinkingLevel::parse)
                    .collect();
                if !parsed.is_empty() {
                    return parsed;
                }
            }
        }
        match self.get_state().await {
            Ok(state) => supported_thinking_levels(state.model.as_ref()),
            Err(_) => crate::pi::rpc::ThinkingLevel::all().to_vec(),
        }
    }

    /// `set_steering_mode`.
    pub async fn set_steering_mode(&self, mode: crate::pi::rpc::QueueMode) -> Result<()> {
        self.rpc(RpcCommand::SetSteeringMode { mode }).await?;
        Ok(())
    }

    /// `set_follow_up_mode`.
    pub async fn set_follow_up_mode(&self, mode: crate::pi::rpc::QueueMode) -> Result<()> {
        self.rpc(RpcCommand::SetFollowUpMode { mode }).await?;
        Ok(())
    }

    /// `compact`.
    pub async fn compact(&self, custom_instructions: Option<&str>) -> Result<Value> {
        self.rpc(RpcCommand::Compact {
            custom_instructions: custom_instructions.map(str::to_string),
        })
        .await
    }

    /// `get_session_stats`.
    pub async fn get_session_stats(&self) -> Result<Value> {
        self.rpc(RpcCommand::GetSessionStats).await
    }

    /// `set_session_name`.
    pub async fn set_session_name(&self, name: &str) -> Result<()> {
        self.rpc(RpcCommand::SetSessionName {
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    /// `set_auto_compaction`.
    pub async fn set_auto_compaction(&self, enabled: bool) -> Result<()> {
        self.rpc(RpcCommand::SetAutoCompaction { enabled }).await?;
        Ok(())
    }

    /// `export_html` → the written file path.
    pub async fn export_html(&self, output_path: &str) -> Result<String> {
        let data = self
            .rpc(RpcCommand::ExportHtml {
                output_path: Some(output_path.to_string()),
            })
            .await?;
        data.get("path")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| AcpxError::RpcFailed {
                command: "export_html".into(),
                message: "response missing data.path".into(),
            })
    }

    /// `get_messages` (session/load history replay).
    pub async fn get_messages(&self) -> Result<Value> {
        self.rpc(RpcCommand::GetMessages).await
    }

    /// `get_commands` (slash / skill / extension command list).
    pub async fn get_commands(&self) -> Result<Value> {
        self.rpc(RpcCommand::GetCommands).await
    }
}

// ---------------------------------------------------------------------------
// Pump task
// ---------------------------------------------------------------------------

async fn pump_loop(mut pump: Pump) {
    let mut shutdown_done: Option<oneshot::Sender<()>> = None;

    loop {
        tokio::select! {
            cmd = pump.cmd_rx.recv() => {
                match cmd {
                    Some(SessionCommand::Prompt { message, images, respond }) => {
                        pump.on_prompt(message, images, respond).await;
                    }
                    Some(SessionCommand::Cancel { respond }) => {
                        pump.on_cancel(respond).await;
                    }
                    Some(SessionCommand::Rpc { command, respond }) => {
                        pump.on_rpc(command, respond).await;
                    }
                    Some(SessionCommand::SetContextWindow { window, respond }) => {
                        if let Some(window) = window {
                            pump.context_window = Some(window);
                        }
                        let _ = respond.send(());
                    }
                    Some(SessionCommand::PublishInitialUsage { respond }) => {
                        pump.emit_initial_usage_update().await;
                        let _ = respond.send(());
                    }
                    Some(SessionCommand::McpMarkerSnapshot { respond }) => {
                        let lines = pump.proc.lock().await.mcp_marker_snapshot();
                        let _ = respond.send(lines);
                    }
                    Some(SessionCommand::Shutdown { done }) => {
                        shutdown_done = Some(done);
                        break;
                    }
                    // Every handle was dropped (session no longer referenced):
                    // end the pump; the teardown below disposes pi.
                    None => break,
                }
            }
            ext = pump.extension_rx.recv() => {
                if let Some(resp) = ext {
                    pump.on_extension_ui_response(resp).await;
                }
            }
            ev = pump.event_rx.recv() => {
                match ev {
                    Some(ev) => pump.on_event(ev).await,
                    // pi's stdout ended (process exit) — fail the in-flight turn.
                    None => {
                        pump.on_stream_end().await;
                        pump.pi_died = true;
                        break;
                    }
                }
            }
            prompt_result = pump.prompt_rx.recv() => {
                pump.on_prompt_result(prompt_result).await;
            }
            recovery_abort_result = pump.recovery_abort_rx.recv() => {
                pump.on_recovery_abort_result(recovery_abort_result).await;
            }
            // Settle deadline (design §11 risk #84): pi accepted the prompt
            // but never emitted `agent_settled` — resolve the turn with an
            // explicit error instead of hanging `session/prompt` forever. The
            // deadline is copied out of the pump so the async block only
            // borrows the copy, not the pump (the other arms mutate it).
            _settle_deadline = {
                let deadline = pump.settle_deadline;
                async move {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                }
            } => {
                pump.on_settle_timeout().await;
            }
            // Stale-settle drain (W-480 recovery): bound the wait for both the
            // abort confirmation and the aborted turn's late `agent_settled`
            // before queued retries run or the session is poisoned.
            // Same borrow discipline as the settle deadline above.
            _recover_deadline = {
                let deadline = pump.recover_deadline;
                async move {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                }
            } => {
                pump.on_recover_timeout().await;
            }
        }
    }

    // Teardown: resolve anything still pending, dispose the process.
    if let Some(pending) = pump.pending_turn.take() {
        let _ = pending.resolve.send(Err(AcpxError::PiExited {
            code: None,
            signal: None,
        }));
    }
    while let Some(t) = pump.queue.pop_front() {
        let _ = t.resolve.send(Err(AcpxError::PiExited {
            code: None,
            signal: None,
        }));
    }
    // pi died unexpectedly: record the (settled) exit status so later commands
    // on this session fail with `PiExited` (code/signal + hint) instead of a
    // generic "session closed" (S8 / fixes #82). Graceful shutdowns skip this.
    if pump.pi_died {
        let status = {
            let mut proc = pump.proc.lock().await;
            proc.wait_exited(std::time::Duration::from_millis(200))
                .await
        };
        if let Some(status) = status {
            *pump.death.lock().unwrap() = Some(status);
        }
    }
    pump.proc.lock().await.dispose().await;
    if let Some(done) = shutdown_done {
        let _ = done.send(());
    }
}

impl Pump {
    // --- commands ---

    fn session_closed_error(&self) -> AcpxError {
        AcpxError::SessionClosed(self.session_id.0.to_string())
    }

    fn fail_queued_turns(&mut self) {
        while let Some(turn) = self.queue.pop_front() {
            let _ = turn.resolve.send(Err(self.session_closed_error()));
        }
    }

    async fn on_prompt(
        &mut self,
        message: String,
        images: Vec<ImageContent>,
        respond: oneshot::Sender<Result<StopReason>>,
    ) {
        if self.poisoned {
            let _ = respond.send(Err(self.session_closed_error()));
            return;
        }
        let queued = QueuedTurn {
            message,
            images,
            resolve: respond,
        };
        if self.pending_turn.is_some() || self.recovering {
            // One-at-a-time: a turn is running, queue this one. While
            // recovering from a settle timeout (W-480) no turn is running,
            // but queued prompts still wait for the aborted turn's stale
            // `agent_settled` so it can never complete a fresh turn.
            self.queue.push_back(queued);
            self.emit_text(&format!("Queued message (position {}).", self.queue.len()))
                .await;
            self.emit_queue_depth(self.pending_turn.is_some()).await;
        } else {
            self.start_turn(queued).await;
        }
    }

    async fn on_cancel(&mut self, respond: oneshot::Sender<Result<()>>) {
        if self.poisoned {
            let _ = respond.send(Err(self.session_closed_error()));
            return;
        }
        if self.pending_turn.is_none() {
            self.cancel_requested = false;
            // While recovering from a settle timeout (W-480), retries wait
            // queued with no turn running: cancel clears them so a stray
            // cancel never leaves stale retries behind. The recovery itself
            // still runs to absorb the aborted turn's late settle.
            let had_queue = !self.queue.is_empty();
            while let Some(t) = self.queue.pop_front() {
                let _ = t.resolve.send(Ok(StopReason::Cancelled));
            }
            if had_queue {
                self.emit_text("Cleared queued prompts.").await;
                self.emit_queue_depth(false).await;
            }
            let _ = respond.send(Ok(()));
            return;
        }
        self.cancel_requested = true;

        // Clear the queue; each queued turn resolves as cancelled.
        let had_queue = !self.queue.is_empty();
        while let Some(t) = self.queue.pop_front() {
            let _ = t.resolve.send(Ok(StopReason::Cancelled));
        }
        if had_queue {
            self.emit_text("Cleared queued prompts.").await;
            self.emit_queue_depth(self.pending_turn.is_some()).await;
        }

        // The prompt is sent from a spawned task so the pump can service
        // cancellation while its response is pending. Preserve the wire
        // ordering by waiting only for the write/flush milestone, never for
        // the prompt response itself.
        let prompt_started = self
            .pending_turn
            .as_mut()
            .and_then(|pending| pending.prompt_started.take());
        if let Some(prompt_started) = prompt_started {
            let _ = prompt_started.await;
        }

        // Complete the abort before the pump handles another command/event.
        // This keeps a delayed abort from acquiring the process mutex after a
        // queued turn has already started. The resulting agent_settled event
        // is buffered by the event channel while the RPC is in flight.
        let result = self.rpc.request(&RpcCommand::Abort).await.map(|_| ());
        if result.is_err() {
            // Without a confirmed abort, pi may still settle asynchronously;
            // retire the session so that event cannot resolve a later turn.
            self.poisoned = true;
            self.cancel_requested = false;
            if let Some(pending) = self.pending_turn.take() {
                let _ = pending.resolve.send(Err(self.session_closed_error()));
            }
            self.fail_queued_turns();
            self.in_agent_loop = false;
            self.settle_deadline = None;
            self.emit_queue_depth(false).await;
        }
        let _ = respond.send(result);
    }

    async fn on_extension_ui_response(&mut self, resp: ExtensionUiResponse) {
        if let Err(e) = self.rpc.send_extension_ui_response(resp).await {
            tracing::warn!(error = %e, "failed to write extension_ui_response to pi");
        }
    }

    /// Run one delegated pi RPC command (thin agent delegation).
    async fn on_rpc(&mut self, command: RpcCommand, respond: oneshot::Sender<Result<Value>>) {
        let result = self.rpc.request(&command).await;
        let _ = respond.send(result);
    }

    /// The early `prompt` RPC response. `Ok` means pi accepted the turn (it
    /// completes at `agent_settled`); `Err` means the prompt was rejected or
    /// pi died — resolve the pending turn explicitly (TS parity), surfacing
    /// the error per design D5, and do **not** auto-start queued turns (pi may
    /// be unhealthy).
    async fn on_prompt_result(&mut self, prompt: Option<PromptResult>) {
        let Some(PromptResult { turn_id, result }) = prompt else {
            return;
        };

        let Some(pending) = self.pending_turn.as_ref() else {
            tracing::debug!(
                turn_id,
                "late prompt response with no pending turn; ignoring"
            );
            return;
        };
        if pending.turn_id != turn_id {
            tracing::debug!(
                turn_id,
                current_turn_id = pending.turn_id,
                "late prompt response; ignoring"
            );
            return;
        }

        match result {
            Ok(()) => {
                // The response and event channels are separate. A fast pi can
                // therefore put agent_settled in the event channel before the
                // pump observes this response even though pi wrote the
                // response first. Mark the response before deciding whether a
                // buffered settle can complete this turn.
                let settled_before_accept = {
                    let pending = self
                        .pending_turn
                        .as_mut()
                        .expect("pending turn checked above");
                    pending.prompt_accepted = true;
                    pending.settled_before_accept
                };
                if settled_before_accept {
                    self.settle_pending_turn().await;
                } else if self.settle_timeout > Duration::ZERO {
                    // Accepted: arm the settle fallback (design §11 risk #84).
                    // The turn completes at `agent_settled`; the per-request
                    // RPC timeout only bounds the early response.
                    self.settle_deadline = Some(tokio::time::Instant::now() + self.settle_timeout);
                }
            }
            Err(result) => {
                self.settle_deadline = None;
                let Some(pending) = self.pending_turn.take() else {
                    return;
                };
                self.flush_outbound().await;
                let _ = pending.resolve.send(Err(result));
                self.fail_queued_turns();
                self.poisoned = true;
                self.cancel_requested = false;
                self.in_agent_loop = false;
                self.emit_queue_depth(false).await;
            }
        }
    }

    async fn start_turn(&mut self, queued: QueuedTurn) {
        self.cancel_requested = false;
        self.in_agent_loop = false;
        let turn_id = self.next_turn_id;
        self.next_turn_id += 1;
        let (prompt_started_tx, prompt_started_rx) = oneshot::channel();
        self.pending_turn = Some(PendingTurn {
            resolve: queued.resolve,
            turn_id,
            prompt_accepted: false,
            settled_before_accept: false,
            prompt_started: Some(prompt_started_rx),
        });
        self.emit_queue_depth(true).await;

        // Send the prompt in a spawned task so the pump keeps servicing
        // commands (cancel) and events while the early response is in flight.
        let rpc = self.rpc.clone();
        let tx = self._prompt_tx.clone();
        tokio::spawn(async move {
            let result = rpc
                .request_with_started(
                    &RpcCommand::Prompt {
                        message: queued.message,
                        images: Some(queued.images),
                        streaming_behavior: None,
                    },
                    prompt_started_tx,
                )
                .await;
            let _ = tx
                .send(PromptResult {
                    turn_id,
                    result: result.map(|_| ()),
                })
                .await;
        });
    }

    // --- outbound helpers ---

    async fn emit(&mut self, update: SessionUpdate) {
        let notif = SessionNotification::new(self.session_id.clone(), update);
        if self
            .outbound
            .send(OutboundMessage::Notify(notif))
            .await
            .is_err()
        {
            tracing::debug!("outbound sink closed; dropping session update");
        }
    }

    /// Ordering barrier: block until everything already sent on the outbound
    /// channel has been forwarded to the connection (S8 / D4; TS `flushEmits`).
    /// Resolving a turn only after this guarantees streamed notifications are
    /// never overtaken by the `session/prompt` response frame.
    async fn flush_outbound(&mut self) {
        let (tx, rx) = oneshot::channel();
        if self
            .outbound
            .send(OutboundMessage::Flush(tx))
            .await
            .is_err()
        {
            return; // sink closed; nothing to flush
        }
        let _ = rx.await;
    }

    async fn emit_text(&mut self, text: &str) {
        let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text.to_string())));
        self.emit(SessionUpdate::AgentMessageChunk(chunk)).await;
    }

    /// Publish the client-side queue depth via `session_info_update._meta`
    /// (the `piAcp.queueDepth` contract; invisible in Zed today, kept for
    /// TS parity).
    async fn emit_queue_depth(&mut self, running: bool) {
        let meta = json!({
            "piAcp": { "queueDepth": self.queue.len(), "running": running }
        })
        .as_object()
        .expect("static queueDepth meta")
        .clone();
        let update = SessionUpdate::SessionInfoUpdate(SessionInfoUpdate::new().meta(meta));
        self.emit(update).await;
    }

    /// Emit an ACP `usage_update` from pi's assistant-message usage (decision
    /// 3: first release includes the standard notification, aligning #106).
    ///
    /// `used` = pi's cumulative context token count (`totalTokens`, falling
    /// back to the component sum); `size` = the active model's context window;
    /// `cost` = the cumulative USD cost when pi reports one. Skipped when the
    /// usage is all-zero or the model's context window is unknown (a
    /// `usage_update` without a meaningful `size` would be misleading).
    async fn emit_usage_update(&mut self, usage: &Usage) {
        let Some(size) = self.context_window else {
            return;
        };
        let used = if usage.total_tokens > 0 {
            usage.total_tokens
        } else {
            usage.input + usage.output + usage.cache_read + usage.cache_write
        };
        if used == 0 && !usage.cost.as_ref().is_some_and(|c| c.total > 0.0) {
            return;
        }
        let mut update = UsageUpdate::new(used, size);
        if let Some(cost) = &usage.cost {
            if cost.total > 0.0 {
                update = update.cost(Cost::new(cost.total, "USD"));
            }
        }
        self.emit(SessionUpdate::UsageUpdate(update)).await;
    }

    /// Emit the initial zero-use context window. This is separate from
    /// [`Self::emit_usage_update`] because pi's first usage event may not
    /// arrive until after the first model turn.
    async fn emit_initial_usage_update(&mut self) {
        let Some(size) = self.context_window else {
            return;
        };
        self.emit(SessionUpdate::UsageUpdate(UsageUpdate::new(0, size)))
            .await;
    }

    /// pi's streaming `message_update` can carry an empty usage snapshot. The
    /// final assistant `message_end` contains the authoritative usage, so only
    /// extract usage from assistant messages here.
    fn usage_from_assistant_message(message: &Value) -> Option<Usage> {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            return None;
        }
        serde_json::from_value(message.get("usage")?.clone()).ok()
    }

    // --- pi events ---

    /// Forward pi's `thinking_level_changed` to the client so Zed's thinking
    /// selectors follow pi-initiated changes (e.g. alongside a model switch).
    ///
    /// The mode update is authoritative from the event and always emitted.
    /// The config-option refresh re-reads pi state (best-effort): a failed
    /// round-trip must not turn an informational event into a turn failure,
    /// so the mode update still stands on its own. The thought-level options
    /// are the model's native available levels (W-478), not a static ladder.
    ///
    /// Echo suppression (W-479 P1): when this event closely follows our own
    /// `set_thinking_level` / `set_model`, the agent's explicit refresh has
    /// already re-read the same post-set state — skip the duplicate re-read.
    async fn on_thinking_level_changed(&mut self, level: ThinkingLevel) {
        self.emit(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
            level.id(),
        )))
        .await;
        if thinking_event_is_echo(
            *self.thinking_set_at.lock().unwrap(),
            std::time::Instant::now(),
        ) {
            return;
        }
        let (state_res, models_res, levels_res) = tokio::join!(
            self.rpc.request(&RpcCommand::GetState),
            self.rpc.request(&RpcCommand::GetAvailableModels),
            self.rpc.request(&RpcCommand::GetAvailableThinkingLevels),
        );
        let (Ok(state_data), Ok(models_data)) = (state_res, models_res) else {
            return;
        };
        let state_model = serde_json::from_value::<RpcSessionState>(state_data)
            .ok()
            .and_then(|s| s.model);
        let current_model = state_model.as_ref().and_then(|m| {
            let provider = m.provider.trim();
            let id = m.id.trim();
            if provider.is_empty() || id.is_empty() {
                None
            } else {
                Some(format!("{provider}/{id}"))
            }
        });
        let available: Vec<(String, String)> = models_data
            .get("models")
            .cloned()
            .and_then(|v| serde_json::from_value::<Vec<Model>>(v).ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|m| {
                let provider = m.provider.trim();
                let id = m.id.trim();
                if provider.is_empty() || id.is_empty() {
                    None
                } else {
                    Some((format!("{provider}/{id}"), format!("{provider}/{}", m.name)))
                }
            })
            .collect();
        // Native levels first; fall back to the local per-model computation
        // so an older pi without the RPC still yields a dynamic list.
        let levels: Vec<ThinkingLevel> = levels_res
            .ok()
            .and_then(|data| data.get("levels").cloned())
            .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok())
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| ThinkingLevel::parse(id))
                    .collect()
            })
            .filter(|levels: &Vec<ThinkingLevel>| !levels.is_empty())
            .unwrap_or_else(|| supported_thinking_levels(state_model.as_ref()));
        let mut options = vec![crate::agent::thought_level_config_option(
            level.id(),
            &levels,
        )];
        let current_model_id = current_model
            .or_else(|| available.first().map(|(id, _)| id.clone()))
            .unwrap_or_default();
        if let Some(model_option) = crate::agent::model_config_option(&current_model_id, &available)
        {
            options.insert(0, model_option);
        }
        self.emit(SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(
            options,
        )))
        .await;
    }

    async fn on_event(&mut self, ev: RpcEvent) {
        match ev {
            RpcEvent::MessageUpdate {
                usage,
                assistant_message_event,
            } => {
                self.emit_usage_update(&usage).await;
                self.on_message_update(&assistant_message_event).await;
            }
            RpcEvent::MessageEnd { message } => {
                if let Some(usage) = Self::usage_from_assistant_message(&message) {
                    self.emit_usage_update(&usage).await;
                }
            }
            RpcEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            } => {
                self.on_tool_execution_start(&tool_call_id, &tool_name, &args)
                    .await
            }
            RpcEvent::ToolExecutionUpdate {
                tool_call_id,
                partial_result,
                ..
            } => {
                self.on_tool_execution_update(&tool_call_id, &partial_result)
                    .await
            }
            RpcEvent::ToolExecutionEnd {
                tool_call_id,
                result,
                is_error,
                ..
            } => {
                self.on_tool_execution_end(&tool_call_id, &result, is_error)
                    .await
            }
            RpcEvent::BashExecutionUpdate { id, delta } => {
                self.on_bash_execution_update(id.as_deref(), &delta).await
            }
            RpcEvent::AgentStart => self.in_agent_loop = true,
            // pi emits `agent_end` for every low-level run (retry, compaction,
            // queued continuation) — the ACP turn stays open until `agent_settled`.
            RpcEvent::AgentEnd { .. } => self.in_agent_loop = false,
            // `turn_end` marks sub-steps (e.g. a tool_use turn). Never resolves
            // the ACP prompt here.
            RpcEvent::TurnEnd { .. } => {}
            RpcEvent::AgentSettled => self.on_agent_settled().await,
            RpcEvent::ExtensionUiRequest { inner } => self.spawn_extension_ui(inner),
            RpcEvent::AutoRetryStart {
                attempt,
                max_attempts,
                delay_ms,
                ..
            } => {
                self.emit_text(&format_auto_retry_message(attempt, max_attempts, delay_ms))
                    .await;
            }
            RpcEvent::AutoRetryEnd { .. } => {
                self.emit_text("Retry finished, resuming.").await;
            }
            RpcEvent::CompactionStart { reason } => {
                if matches!(
                    reason,
                    CompactionReason::Threshold | CompactionReason::Overflow
                ) {
                    self.emit_text("Context nearing limit, running automatic compaction...")
                        .await;
                }
            }
            RpcEvent::CompactionEnd {
                reason, aborted, ..
            } => {
                if matches!(
                    reason,
                    CompactionReason::Threshold | CompactionReason::Overflow
                ) && !aborted
                {
                    self.emit_text(
                        "Automatic compaction finished; context was summarized to continue the session.",
                    )
                    .await;
                }
            }
            // pi-initiated renames (e.g. an extension calling `setSessionName`)
            // are forwarded as `session_info_update` so the client's thread
            // title stays live (fixes #102/#24).
            RpcEvent::SessionInfoChanged { name } => {
                if let Some(name) = name {
                    let update = SessionInfoUpdate::new()
                        .title(name)
                        .updated_at(utc_now_iso8601());
                    self.emit(SessionUpdate::SessionInfoUpdate(update)).await;
                }
            }
            // pi changed the thinking level itself: push both selectors so
            // Zed's mode picker and thinking dropdown follow.
            RpcEvent::ThinkingLevelChanged { level } => {
                self.on_thinking_level_changed(level).await;
            }
            // Not wired (logged): QueueUpdate / EntryAppended /
            // UnmatchedResponse / ExtensionError / summarization retries /
            // unknown future events.
            other => {
                tracing::trace!(?other, "unhandled pi event");
            }
        }
    }

    /// Handle pi's turn-completion event. The event does not carry a turn id,
    /// so an event observed before the matching prompt response is held until
    /// that response confirms the turn. During cancellation, the abort is the
    /// confirmation that an early response is no longer required.
    async fn on_agent_settled(&mut self) {
        if self.pending_turn.is_none() && self.recovering {
            // The aborted turn's late settle (W-480 recovery): absorb it —
            // it belongs to the timed-out turn, never to a fresh one. The
            // recovery also waits for the abort RPC response before starting
            // a retry, so an in-flight abort can never hit the fresh turn.
            tracing::debug!("absorbed stale agent_settled during settle-timeout recovery");
            self.recovery_settled = true;
            self.finish_recovery_if_confirmed().await;
            return;
        }
        if let Some(pending) = self.pending_turn.as_mut() {
            if !pending.prompt_accepted && !self.cancel_requested {
                pending.settled_before_accept = true;
                tracing::debug!(
                    turn_id = pending.turn_id,
                    "agent_settled arrived before prompt response; buffering"
                );
                return;
            }
        } else {
            tracing::debug!("agent_settled with no pending turn; ignoring");
            return;
        }
        self.settle_pending_turn().await;
    }

    /// The turn is truly over. Resolve the pending ACP prompt, then either
    /// start the next queued turn or publish the idle queue depth.
    async fn settle_pending_turn(&mut self) {
        self.settle_deadline = None;
        let Some(pending) = self.pending_turn.take() else {
            tracing::debug!("settling with no pending turn; ignoring");
            return;
        };
        // All streamed updates derived from pi events are delivered before the
        // response frame (TS `flushEmits` parity; S8 / D4 ordering).
        self.flush_outbound().await;
        let reason = if self.cancel_requested {
            StopReason::Cancelled
        } else {
            StopReason::EndTurn
        };
        let _ = pending.resolve.send(Ok(reason));
        self.in_agent_loop = false;

        if let Some(next) = self.queue.pop_front() {
            self.emit_text(&format!(
                "Starting queued message. ({} remaining)",
                self.queue.len()
            ))
            .await;
            self.start_turn(next).await;
        } else {
            self.emit_queue_depth(false).await;
        }
        self.cancel_requested = false;
    }

    /// pi's stdout ended without a settle: the process exited mid-turn. Fail
    /// the pending turn and all queued turns with [`AcpxError::PiExited`]
    /// (fixes #82 — a dead pi is never a silent empty `end_turn`).
    async fn on_stream_end(&mut self) {
        self.settle_deadline = None;
        // The reader can observe stdout EOF just before Child::wait() records
        // the exit status. Give the watcher a short polling window before
        // rejecting turns so a real exit code is not lost in that race. The
        // helper polls shared state, leaving the JoinHandle for teardown.
        let status = {
            let mut proc = self.proc.lock().await;
            proc.wait_exited(Duration::from_millis(200)).await
        };
        let (code, signal) = status.unwrap_or((None, None));
        let pending_err = AcpxError::PiExited { code, signal };
        self.flush_outbound().await;
        if let Some(p) = self.pending_turn.take() {
            let _ = p.resolve.send(Err(pending_err));
        }
        while let Some(t) = self.queue.pop_front() {
            let _ = t.resolve.send(Err(AcpxError::PiExited { code, signal }));
        }
        self.in_agent_loop = false;
    }

    /// The settle deadline fired: pi accepted the prompt but never emitted
    /// `agent_settled` (design §11 risk #84). Resolve the pending turn with an
    /// explicit [`AcpxError::SettleTimeout`] so `session/prompt` can never
    /// hang forever, and fire an `abort` to unstick pi.
    ///
    /// Unlike a rejected `prompt` RPC, this path stays recoverable (W-480):
    /// the session is NOT poisoned. Queued turns fail with the same
    /// `SettleTimeout` (they never ran — the client retries them), and new
    /// prompts queue behind a bounded drain until both the abort response and
    /// the aborted turn's late `agent_settled` arrive, so neither an in-flight
    /// abort nor a stale settle can affect a fresh turn. `cancel` keeps working
    /// (it clears the drain queue). A healthy pi retries on the same ACP
    /// session id; an abort failure or an unconfirmed drain poisons the
    /// session, which is the safe fallback for an unhealthy pi.
    async fn on_settle_timeout(&mut self) {
        self.settle_deadline = None;
        let Some(pending) = self.pending_turn.take() else {
            tracing::debug!("settle deadline fired with no pending turn; ignoring");
            return;
        };
        self.flush_outbound().await;
        let secs = self.settle_timeout.as_secs();
        let _ = pending.resolve.send(Err(AcpxError::SettleTimeout { secs }));
        // Queued turns never reached pi: fail them with the same timeout so
        // the client retries them on this same session (they are not
        // session-closed — the session recovers below).
        while let Some(t) = self.queue.pop_front() {
            let _ = t.resolve.send(Err(AcpxError::SettleTimeout { secs }));
        }
        self.cancel_requested = false;
        self.in_agent_loop = false;
        self.recovering = true;
        self.recover_deadline = Some(tokio::time::Instant::now() + STALE_DRAIN_TIMEOUT);
        self.recovery_abort_confirmed = false;
        self.recovery_settled = false;
        self.emit_queue_depth(false).await;

        // Tell pi to stop whatever it accepted. The pump waits for both this
        // RPC result and the resulting late `agent_settled` before starting a
        // retry; otherwise the abort could still affect the fresh turn.
        let rpc = self.rpc.clone();
        let result_tx = self.recovery_abort_tx.clone();
        tokio::spawn(async move {
            let result = rpc.request(&RpcCommand::Abort).await.map(|_| ());
            let _ = result_tx.send(result).await;
        });
    }

    /// Record the result of the recovery abort. A successful RPC is necessary
    /// but not sufficient: the event pump must also consume the old turn's
    /// settle event before a retry can start.
    async fn on_recovery_abort_result(&mut self, result: Option<Result<()>>) {
        let Some(result) = result else {
            return;
        };
        if !self.recovering {
            tracing::debug!("late recovery abort result after recovery ended; ignoring");
            return;
        }
        match result {
            Ok(()) => {
                tracing::debug!("recovery abort confirmed");
                self.recovery_abort_confirmed = true;
                self.finish_recovery_if_confirmed().await;
            }
            Err(error) => {
                tracing::warn!(error = %error, "abort after settle timeout failed");
                self.poison_recovery().await;
            }
        }
    }

    /// Start the queued retry only after both sides of the recovery handshake
    /// are confirmed. Keeping these confirmations separate handles either
    /// event order without allowing an abort response or settle event to race
    /// the next prompt.
    async fn finish_recovery_if_confirmed(&mut self) {
        if !self.recovering || !self.recovery_abort_confirmed || !self.recovery_settled {
            return;
        }
        self.recovering = false;
        self.recover_deadline = None;
        self.recovery_abort_confirmed = false;
        self.recovery_settled = false;
        if let Some(next) = self.queue.pop_front() {
            self.emit_text(&format!(
                "Starting queued message. ({} remaining)",
                self.queue.len()
            ))
            .await;
            self.start_turn(next).await;
        } else {
            self.emit_queue_depth(false).await;
        }
    }

    /// Retire a recovery that cannot prove the old turn is gone. Once poisoned,
    /// any late settle is ignored because no new turn will be started on this
    /// process, preserving the no-turn-id safety invariant.
    async fn poison_recovery(&mut self) {
        self.recovering = false;
        self.recover_deadline = None;
        self.recovery_abort_confirmed = false;
        self.recovery_settled = false;
        self.poisoned = true;
        self.cancel_requested = false;
        self.in_agent_loop = false;
        self.fail_queued_turns();
        self.emit_queue_depth(false).await;
    }

    /// The stale-settle drain elapsed without confirming both the abort RPC
    /// and the old turn's settle (W-480). Drain events that are already buffered
    /// at the deadline, then retry only if the complete handshake is present;
    /// otherwise poison the session so a late stale event cannot complete a
    /// future prompt.
    async fn on_recover_timeout(&mut self) {
        if !self.recovering {
            return;
        }

        // A result or event buffered alongside the timer may complete the
        // handshake. Consume all currently available values before deciding.
        while let Ok(result) = self.recovery_abort_rx.try_recv() {
            self.on_recovery_abort_result(Some(result)).await;
            if !self.recovering {
                return;
            }
        }
        while let Ok(ev) = self.event_rx.try_recv() {
            if matches!(ev, RpcEvent::AgentSettled) {
                tracing::debug!(
                    "absorbed buffered stale agent_settled at end of settle-timeout recovery"
                );
                self.recovery_settled = true;
            } else {
                // Other buffered events still belong to the timed-out turn.
                // Preserve existing notification behavior, but never allow
                // them to change the recovery decision.
                self.on_event(ev).await;
            }
            if !self.recovering {
                return;
            }
        }
        while let Ok(result) = self.recovery_abort_rx.try_recv() {
            self.on_recovery_abort_result(Some(result)).await;
            if !self.recovering {
                return;
            }
        }

        if self.recovery_abort_confirmed && self.recovery_settled {
            self.finish_recovery_if_confirmed().await;
        } else {
            tracing::warn!(
                abort_confirmed = self.recovery_abort_confirmed,
                settled = self.recovery_settled,
                "settle-timeout recovery drain expired without confirmation"
            );
            self.poison_recovery().await;
        }
    }

    // --- streaming assistant messages ---

    async fn on_message_update(&mut self, ame: &AssistantMessageEvent) {
        match ame {
            AssistantMessageEvent::TextDelta { delta, .. } => {
                self.emit_text(delta).await;
            }
            AssistantMessageEvent::ThinkingDelta { delta, .. } => {
                let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(delta.clone())));
                self.emit(SessionUpdate::AgentThoughtChunk(chunk)).await;
            }
            AssistantMessageEvent::ToolcallStart { id, tool_name, .. } => {
                // Modern pi strips the partial message and injects id/toolName;
                // args are not yet streamed, so rawInput/locations are absent.
                self.surface_tool_call(id, tool_name, None).await;
            }
            AssistantMessageEvent::ToolcallEnd { tool_call, .. } => {
                let id = tool_call
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let name = tool_call
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("tool")
                    .to_string();
                let raw_input = raw_input_of_tool_call(tool_call);
                self.surface_tool_call(&id, &name, raw_input).await;
            }
            // toolcall_delta carries no id on the modern wire form (partial is
            // stripped) — no-op, matching TS. Other sub-events are not streamed.
            _ => {}
        }
    }

    /// Surface a tool call as early as possible (while the model is still
    /// streaming args). Never downgrades an already-tracked status — if a
    /// `tool_execution_start` already marked the tool `in_progress`, a later
    /// streaming event keeps `in_progress` instead of going back to `pending`
    /// (clients hide progress on downgrades).
    async fn surface_tool_call(
        &mut self,
        tool_call_id: &str,
        tool_name: &str,
        raw_input: Option<Value>,
    ) {
        if tool_call_id.is_empty() {
            return;
        }
        let existing = self.current_tool_calls.get(tool_call_id).copied();
        // Monotonic: keep the existing status, else `pending`.
        let status = existing.unwrap_or(TrackedStatus::Pending);
        let locations = raw_input
            .as_ref()
            .map(|ri| to_tool_call_locations(ri, &self.cwd, None))
            .unwrap_or_default();

        if is_bash_tool(tool_name) {
            if existing.is_none() {
                self.current_tool_calls
                    .insert(tool_call_id.to_string(), TrackedStatus::Pending);
            }
            self.emit_bash_tool_call(
                tool_call_id,
                tool_name,
                raw_input.as_ref(),
                status,
                locations,
                existing.is_none(),
            )
            .await;
        } else if existing.is_none() {
            self.current_tool_calls
                .insert(tool_call_id.to_string(), TrackedStatus::Pending);
            let mut call = ToolCall::new(tool_call_id.to_string(), tool_name.to_string())
                .kind(to_tool_kind(tool_name))
                .status(acp_status(status))
                .locations(locations);
            if let Some(input) = raw_input {
                call = call.raw_input(input);
            }
            self.emit(SessionUpdate::ToolCall(call)).await;
        } else {
            let fields = ToolCallUpdateFields::new()
                .status(Some(acp_status(status)))
                .locations(Some(locations))
                .raw_input(raw_input);
            self.emit(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                tool_call_id.to_string(),
                fields,
            )))
            .await;
        }
    }

    // --- tool execution ---

    async fn on_tool_execution_start(&mut self, tool_call_id: &str, tool_name: &str, args: &Value) {
        let existing = self.current_tool_calls.get(tool_call_id).copied();
        self.current_tool_calls
            .insert(tool_call_id.to_string(), TrackedStatus::InProgress);

        if is_bash_tool(tool_name) {
            let locations = to_tool_call_locations(args, &self.cwd, None);
            self.emit_bash_tool_call(
                tool_call_id,
                tool_name,
                Some(args),
                TrackedStatus::InProgress,
                locations,
                existing.is_none(),
            )
            .await;
            return;
        }

        // Capture pre-mutation file contents so we can emit a structured ACP
        // diff on completion. For `edit`, resolve the 1-based line number of
        // the first uniquely-located oldText (S4 helper) for the ACP location.
        let mut line: Option<u32> = None;
        if matches!(tool_name, "edit" | "write") {
            self.file_mutation_tool_call_ids
                .insert(tool_call_id.to_string());
            if let Some(p) = tool_path(args) {
                let abs = resolve_path(&self.cwd, &p);
                let read = std::fs::read_to_string(&abs).ok();
                if tool_name == "edit" {
                    if let Some(text) = &read {
                        for needle in edit_old_texts(args) {
                            if let Some(n) = find_unique_line_number(text, &needle) {
                                line = Some(n);
                                break;
                            }
                        }
                    }
                }
                self.file_snapshots.insert(
                    tool_call_id.to_string(),
                    FileSnapshot {
                        path: p,
                        old_text: read,
                    },
                );
            }
        }

        let locations = to_tool_call_locations(args, &self.cwd, line);
        if existing.is_none() {
            let call = ToolCall::new(tool_call_id.to_string(), tool_name.to_string())
                .status(ToolCallStatus::InProgress)
                .locations(locations)
                .raw_input(args.clone());
            self.emit(SessionUpdate::ToolCall(call)).await;
        } else {
            let fields = ToolCallUpdateFields::new()
                .status(Some(ToolCallStatus::InProgress))
                .locations(Some(locations))
                .raw_input(Some(args.clone()));
            self.emit(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                tool_call_id.to_string(),
                fields,
            )))
            .await;
        }
    }

    async fn on_tool_execution_update(&mut self, tool_call_id: &str, partial_result: &Value) {
        if tool_call_id.is_empty() {
            return;
        }
        if self.bash_tool_call_ids.contains(tool_call_id) {
            self.emit_bash_output_update(
                tool_call_id,
                ToolCallStatus::InProgress,
                partial_result,
                false,
            )
            .await;
            return;
        }
        // File mutations suppress content/rawOutput while running (the diff is
        // emitted at completion); other tools stream their partial text.
        let is_file_mutation = self.file_mutation_tool_call_ids.contains(tool_call_id);
        let text = if is_file_mutation {
            String::new()
        } else {
            tool_result_to_text(partial_result)
        };
        let content = if text.is_empty() {
            None
        } else {
            Some(vec![ToolCallContent::Content(
                agent_client_protocol::schema::v1::Content::new(ContentBlock::Text(
                    TextContent::new(text),
                )),
            )])
        };
        let fields = ToolCallUpdateFields::new()
            .status(Some(ToolCallStatus::InProgress))
            .content(content)
            .raw_output(if is_file_mutation {
                None
            } else {
                Some(partial_result.clone())
            });
        self.emit(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            tool_call_id.to_string(),
            fields,
        )))
        .await;
    }

    async fn on_tool_execution_end(&mut self, tool_call_id: &str, result: &Value, is_error: bool) {
        if tool_call_id.is_empty() {
            return;
        }
        if self.bash_tool_call_ids.contains(tool_call_id) {
            self.emit_bash_output_update(
                tool_call_id,
                if is_error {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                },
                result,
                is_error,
            )
            .await;
            self.cleanup_tool_call(tool_call_id);
            return;
        }

        let text = tool_result_to_text(result);
        let snapshot = self.file_snapshots.get(tool_call_id).cloned();
        let mut content: Vec<ToolCallContent> = Vec::new();
        let mut has_structured_diff = false;

        if !is_error {
            if let Some(snap) = &snapshot {
                let abs = resolve_path(&self.cwd, &snap.path);
                if let Ok(new_text) = std::fs::read_to_string(&abs) {
                    if snap.old_text.is_none()
                        || Some(new_text.as_str()) != snap.old_text.as_deref()
                    {
                        has_structured_diff = true;
                        content = vec![ToolCallContent::Diff(
                            Diff::new(snap.path.clone(), new_text).old_text(snap.old_text.clone()),
                        )];
                    }
                }
            }
        }

        if !has_structured_diff && !text.is_empty() {
            content = vec![ToolCallContent::Content(
                agent_client_protocol::schema::v1::Content::new(ContentBlock::Text(
                    TextContent::new(text),
                )),
            )];
        }

        let fields = ToolCallUpdateFields::new()
            .status(Some(if is_error {
                ToolCallStatus::Failed
            } else {
                ToolCallStatus::Completed
            }))
            .content(if content.is_empty() {
                None
            } else {
                Some(content)
            })
            .raw_output(if has_structured_diff {
                None
            } else {
                Some(result.clone())
            });
        self.emit(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            tool_call_id.to_string(),
            fields,
        )))
        .await;

        self.cleanup_tool_call(tool_call_id);
    }

    /// Newer pi streams bash output through `bash_execution_update` deltas;
    /// append them to the tool's terminal.
    async fn on_bash_execution_update(&mut self, tool_call_id: Option<&str>, delta: &str) {
        let Some(id) = tool_call_id else { return };
        if !self.bash_tool_call_ids.contains(id) || delta.is_empty() {
            return;
        }
        let prev = self
            .bash_output_snapshots
            .get(id)
            .cloned()
            .unwrap_or_default();
        self.bash_output_snapshots
            .insert(id.to_string(), prev + delta);
        let fields = ToolCallUpdateFields::new().status(Some(ToolCallStatus::InProgress));
        let update =
            ToolCallUpdate::new(id.to_string(), fields).meta(bash_terminal_output_meta(id, delta));
        self.emit(SessionUpdate::ToolCallUpdate(update)).await;
    }

    // --- bash terminal rendering ---

    /// Emit (or update) a bash tool call as an ACP `execute` tool with an
    /// embedded terminal. The terminal (content + `terminal_info` meta) is
    /// attached on the *first* emission; later transitions only carry status.
    async fn emit_bash_tool_call(
        &mut self,
        tool_call_id: &str,
        tool_name: &str,
        args: Option<&Value>,
        status: TrackedStatus,
        locations: Vec<agent_client_protocol::schema::v1::ToolCallLocation>,
        include_terminal: bool,
    ) {
        self.bash_tool_call_ids.insert(tool_call_id.to_string());
        let title = args
            .and_then(bash_command)
            .unwrap_or_else(|| tool_name.to_string());
        if include_terminal {
            let call = ToolCall::new(tool_call_id.to_string(), title)
                .kind(ToolKind::Execute)
                .status(acp_status(status))
                .locations(locations)
                .content(bash_terminal_content(tool_call_id))
                .meta(bash_terminal_info_meta(
                    tool_call_id,
                    &self.cwd.to_string_lossy(),
                ));
            self.emit(SessionUpdate::ToolCall(call)).await;
        } else {
            let fields = ToolCallUpdateFields::new()
                .kind(Some(ToolKind::Execute))
                .title(Some(title))
                .status(Some(acp_status(status)))
                .locations(Some(locations));
            self.emit(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                tool_call_id.to_string(),
                fields,
            )))
            .await;
        }
    }

    /// Stream a delta of the accumulated bash output into the tool's terminal,
    /// and close it with an exit code on completion/failure.
    async fn emit_bash_output_update(
        &mut self,
        tool_call_id: &str,
        status: ToolCallStatus,
        result: &Value,
        is_error: bool,
    ) {
        let text = bash_result_text(result);
        let previous = self
            .bash_output_snapshots
            .get(tool_call_id)
            .cloned()
            .unwrap_or_default();
        let delta = bash_output_delta(&previous, &text);
        self.bash_output_snapshots
            .insert(tool_call_id.to_string(), text);

        let mut meta = serde_json::Map::new();
        if !delta.is_empty() {
            meta.extend(bash_terminal_output_meta(tool_call_id, &delta));
        }
        if matches!(status, ToolCallStatus::Completed | ToolCallStatus::Failed) {
            meta.extend(bash_terminal_exit_meta(
                tool_call_id,
                bash_exit_code(result, is_error),
            ));
        }
        let fields = ToolCallUpdateFields::new().status(Some(status));
        let update = if meta.is_empty() {
            ToolCallUpdate::new(tool_call_id.to_string(), fields)
        } else {
            ToolCallUpdate::new(tool_call_id.to_string(), fields).meta(meta)
        };
        self.emit(SessionUpdate::ToolCallUpdate(update)).await;
    }

    fn cleanup_tool_call(&mut self, tool_call_id: &str) {
        self.current_tool_calls.remove(tool_call_id);
        self.file_snapshots.remove(tool_call_id);
        self.file_mutation_tool_call_ids.remove(tool_call_id);
        self.bash_tool_call_ids.remove(tool_call_id);
        self.bash_output_snapshots.remove(tool_call_id);
    }

    // --- extension UI bridge ---

    /// Bridge a pi extension UI request to the ACP client. `select`/`confirm`
    /// become `session/request_permission` (answered in a spawned task — pi
    /// blocks its turn on the answer, so the pump must not wait); the rest are
    /// answered with a v1 `cancelled` (TS parity).
    fn spawn_extension_ui(&self, req: ExtensionUiRequest) {
        let outbound = self.outbound.clone();
        let extension_tx = self.extension_tx.clone();
        let session_id = self.session_id.clone();
        tokio::spawn(async move {
            let response = handle_extension_ui_request(&session_id, &req, &outbound).await;
            if let Some(resp) = response {
                let _ = extension_tx.send(resp).await;
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Outbound connector (S6 wiring)
// ---------------------------------------------------------------------------

/// Bridge a session's outbound channel to the ACP SDK connection: forwards
/// `session/update` notifications in order and answers
/// `session/request_permission` requests. Runs on a spawned task — outside the
/// SDK dispatch loop, so `block_task()` is safe there.
pub fn spawn_outbound_connector(
    conn: ConnectionTo<Client>,
    mut rx: mpsc::Receiver<OutboundMessage>,
) -> std::result::Result<(), AcpxError> {
    let _: tokio::task::JoinHandle<std::result::Result<(), AcpxError>> = tokio::spawn(async move {
        let run: std::result::Result<(), agent_client_protocol::Error> = async {
            while let Some(msg) = rx.recv().await {
                match msg {
                    OutboundMessage::Notify(notif) => {
                        conn.send_notification(notif)?;
                    }
                    OutboundMessage::RequestPermission(request, respond) => {
                        let response = conn.send_request(request).block_task().await.map_err(|e| {
                            AcpxError::RpcFailed {
                                command: "request_permission".into(),
                                message: e.to_string(),
                            }
                        });
                        let _ = respond.send(response);
                    }
                    OutboundMessage::Flush(ack) => {
                        // Everything before this marker is now enqueued on the
                        // connection's outgoing channel; release the pump.
                        let _ = ack.send(());
                    }
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = run {
            tracing::warn!(error = %e, "session outbound connector stopped");
        }
        Ok(())
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Extension UI helpers (pure, unit-tested)
// ---------------------------------------------------------------------------

const CHOICE_PREFIX: &str = "choice-";
const CONFIRM_YES: &str = "yes";
const CONFIRM_NO: &str = "no";

impl ExtensionUiRequest {
    /// The wire `method` of this request.
    fn method_name(&self) -> &'static str {
        match self {
            ExtensionUiRequest::Select { .. } => "select",
            ExtensionUiRequest::Confirm { .. } => "confirm",
            ExtensionUiRequest::Input { .. } => "input",
            ExtensionUiRequest::Editor { .. } => "editor",
            ExtensionUiRequest::Notify { .. } => "notify",
            ExtensionUiRequest::SetStatus { .. } => "setStatus",
            ExtensionUiRequest::SetWidget { .. } => "setWidget",
            ExtensionUiRequest::SetTitle { .. } => "setTitle",
            ExtensionUiRequest::SetEditorText { .. } => "set_editor_text",
        }
    }

    fn id(&self) -> &str {
        match self {
            ExtensionUiRequest::Select { id, .. }
            | ExtensionUiRequest::Confirm { id, .. }
            | ExtensionUiRequest::Input { id, .. }
            | ExtensionUiRequest::Editor { id, .. }
            | ExtensionUiRequest::Notify { id, .. }
            | ExtensionUiRequest::SetStatus { id, .. }
            | ExtensionUiRequest::SetWidget { id, .. }
            | ExtensionUiRequest::SetTitle { id, .. }
            | ExtensionUiRequest::SetEditorText { id, .. } => id,
        }
    }

    fn title(&self) -> Option<&str> {
        match self {
            ExtensionUiRequest::Select { title, .. }
            | ExtensionUiRequest::Confirm { title, .. }
            | ExtensionUiRequest::Input { title, .. }
            | ExtensionUiRequest::Editor { title, .. } => Some(title),
            _ => None,
        }
    }

    fn timeout_ms(&self) -> Option<u64> {
        match self {
            ExtensionUiRequest::Select { timeout, .. }
            | ExtensionUiRequest::Confirm { timeout, .. }
            | ExtensionUiRequest::Input { timeout, .. } => *timeout,
            _ => None,
        }
    }
}

/// Handle one extension UI request and return the answer to send back to pi
/// (`None` when nothing should be sent). The permission round-trip goes
/// through the outbound channel so tests can drive it.
async fn handle_extension_ui_request(
    session_id: &SessionId,
    req: &ExtensionUiRequest,
    outbound: &mpsc::Sender<OutboundMessage>,
) -> Option<ExtensionUiResponse> {
    match req {
        ExtensionUiRequest::Select {
            id, title, options, ..
        } => handle_extension_select(session_id, id, title, options, req, outbound).await,
        ExtensionUiRequest::Confirm { id, title, .. } => {
            handle_extension_confirm(session_id, id, title, req, outbound).await
        }
        ExtensionUiRequest::Input { id, .. } | ExtensionUiRequest::Editor { id, .. } => {
            let method = req.method_name();
            send_extension_notice(
                outbound,
                session_id,
                &format!("Pi {method} UI request is not supported in ACP yet; cancelling it."),
                None,
            )
            .await;
            Some(cancelled(id))
        }
        ExtensionUiRequest::Notify {
            id,
            message,
            notify_type,
            ..
        } => {
            let level = notify_type.clone().unwrap_or_else(|| "info".to_string());
            let meta = json!({ "piAcp": { "notify": { "level": level } } })
                .as_object()
                .expect("static notify meta")
                .clone();
            send_extension_notice(outbound, session_id, message, Some(meta)).await;
            Some(cancelled(id))
        }
        // setStatus / setWidget / setTitle / set_editor_text: display-only —
        // answer cancelled (TS parity).
        _ => Some(cancelled(req.id())),
    }
}

/// `select` -> ACP `session/request_permission` with one `PermissionOption`
/// per choice (`choice-<index>`); the chosen option's index maps back to the
/// value pi receives.
async fn handle_extension_select(
    session_id: &SessionId,
    id: &str,
    title: &str,
    raw_options: &[String],
    req: &ExtensionUiRequest,
    outbound: &mpsc::Sender<OutboundMessage>,
) -> Option<ExtensionUiResponse> {
    if raw_options.is_empty() {
        return Some(cancelled(id));
    }
    let permission_options: Vec<PermissionOption> = raw_options
        .iter()
        .enumerate()
        .map(|(i, name)| {
            PermissionOption::new(
                format!("{CHOICE_PREFIX}{i}"),
                name.clone(),
                PermissionOptionKind::AllowOnce,
            )
        })
        .collect();

    match request_permission(session_id, id, title, req, permission_options, outbound).await {
        Ok(RequestPermissionOutcome::Selected(selected)) => {
            let idx = option_index(selected.option_id.0.as_ref());
            match idx.and_then(|i| raw_options.get(i)) {
                Some(value) => Some(ExtensionUiResponse::Value {
                    id: id.to_string(),
                    value: value.clone(),
                }),
                None => Some(cancelled(id)),
            }
        }
        _ => Some(cancelled(id)),
    }
}

/// `confirm` -> ACP `session/request_permission` with the fixed Yes/No options.
async fn handle_extension_confirm(
    session_id: &SessionId,
    id: &str,
    title: &str,
    req: &ExtensionUiRequest,
    outbound: &mpsc::Sender<OutboundMessage>,
) -> Option<ExtensionUiResponse> {
    let permission_options = vec![
        PermissionOption::new(CONFIRM_YES, "Yes", PermissionOptionKind::AllowOnce),
        PermissionOption::new(CONFIRM_NO, "No", PermissionOptionKind::RejectOnce),
    ];
    match request_permission(session_id, id, title, req, permission_options, outbound).await {
        Ok(RequestPermissionOutcome::Selected(selected)) => Some(ExtensionUiResponse::Confirmed {
            id: id.to_string(),
            confirmed: selected.option_id.0.as_ref() == CONFIRM_YES,
        }),
        _ => Some(cancelled(id)),
    }
}

/// Send one `session/request_permission` over the outbound channel and await
/// the client's decision.
async fn request_permission(
    session_id: &SessionId,
    id: &str,
    title: &str,
    req: &ExtensionUiRequest,
    options: Vec<PermissionOption>,
    outbound: &mpsc::Sender<OutboundMessage>,
) -> Result<RequestPermissionOutcome> {
    let fields = ToolCallUpdateFields::new()
        .kind(Some(ToolKind::Other))
        .status(Some(ToolCallStatus::Pending))
        .title(Some(title.to_string()))
        .raw_input(Some(extension_ui_raw_input(req)));
    let tool_call = ToolCallUpdate::new(ToolCallId::new(format!("pi-ui-{id}")), fields);
    let request = RequestPermissionRequest::new(session_id.clone(), tool_call, options);

    let (tx, rx) = oneshot::channel();
    outbound
        .send(OutboundMessage::RequestPermission(request, tx))
        .await
        .map_err(|_| AcpxError::RpcFailed {
            command: "request_permission".into(),
            message: "outbound sink closed".into(),
        })?;
    let response = if let Some(timeout_ms) = req.timeout_ms() {
        tokio::time::timeout(Duration::from_millis(timeout_ms), rx)
            .await
            .map_err(|_| AcpxError::RpcFailed {
                command: "request_permission".into(),
                message: format!("client did not respond within {timeout_ms}ms"),
            })?
    } else {
        rx.await
    }
    .map_err(|_| AcpxError::RpcFailed {
        command: "request_permission".into(),
        message: "permission responder dropped".into(),
    })??;
    Ok(response.outcome)
}

async fn send_extension_notice(
    outbound: &mpsc::Sender<OutboundMessage>,
    session_id: &SessionId,
    text: &str,
    meta: Option<agent_client_protocol::schema::v1::Meta>,
) {
    let mut chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text.to_string())));
    if let Some(meta) = meta {
        chunk = chunk.meta(meta);
    }
    let _ = outbound
        .send(OutboundMessage::Notify(SessionNotification::new(
            session_id.clone(),
            SessionUpdate::AgentMessageChunk(chunk),
        )))
        .await;
}

/// Build the `rawInput` carried by the permission tool call (the extension
/// UI request's salient fields, TS `EXTENSION_UI_RAW_INPUT_KEYS`).
fn extension_ui_raw_input(req: &ExtensionUiRequest) -> Value {
    let mut m = serde_json::Map::new();
    m.insert(
        "method".to_string(),
        Value::String(req.method_name().to_string()),
    );
    if let Some(title) = req.title() {
        m.insert("title".to_string(), Value::String(title.to_string()));
    }
    match req {
        ExtensionUiRequest::Select { options, .. } => {
            m.insert("options".to_string(), json!(options));
        }
        ExtensionUiRequest::Confirm { message, .. } => {
            m.insert("message".to_string(), Value::String(message.clone()));
        }
        ExtensionUiRequest::Input {
            placeholder: Some(p),
            ..
        } => {
            m.insert("placeholder".to_string(), Value::String(p.clone()));
        }
        ExtensionUiRequest::Input { .. } => {}
        ExtensionUiRequest::Editor {
            prefill: Some(p), ..
        } => {
            m.insert("prefill".to_string(), Value::String(p.clone()));
        }
        ExtensionUiRequest::Editor { .. } => {}
        _ => {}
    }
    Value::Object(m)
}

fn cancelled(id: &str) -> ExtensionUiResponse {
    ExtensionUiResponse::Cancelled {
        id: id.to_string(),
        cancelled: true,
    }
}

/// Map a `choice-<index>` permission option id back to its index (TS
/// `optionIndex`: safe integer, canonical decimal form).
fn option_index(option_id: &str) -> Option<usize> {
    let raw = option_id.strip_prefix(CHOICE_PREFIX)?;
    if raw.is_empty() {
        return None;
    }
    let index: usize = raw.parse().ok()?;
    (index.to_string() == raw).then_some(index)
}

/// Streaming rawInput extraction from a `toolcall_end` tool call value: prefer
/// `arguments` (object), else parse `partialArgs` (TS parity).
fn raw_input_of_tool_call(tool_call: &Value) -> Option<Value> {
    match tool_call.get("arguments") {
        Some(a) if a.is_object() => Some(a.clone()),
        _ => {
            let s = tool_call
                .get("partialArgs")
                .and_then(Value::as_str)
                .unwrap_or("");
            if s.is_empty() {
                None
            } else {
                serde_json::from_str(s)
                    .ok()
                    .or_else(|| Some(json!({ "partialArgs": s })))
            }
        }
    }
}

/// Auto-retry notice text (TS `formatAutoRetryMessage`).
fn format_auto_retry_message(attempt: u32, max_attempts: u32, delay_ms: u64) -> String {
    if attempt == 0 || max_attempts == 0 {
        return "Retrying...".to_string();
    }
    let mut delay_seconds = delay_ms / 1000;
    if delay_ms > 0 && delay_seconds == 0 {
        delay_seconds = 1;
    }
    format!("Retrying (attempt {attempt}/{max_attempts}, waiting {delay_seconds}s)...")
}

fn acp_status(status: TrackedStatus) -> ToolCallStatus {
    match status {
        TrackedStatus::Pending => ToolCallStatus::Pending,
        TrackedStatus::InProgress => ToolCallStatus::InProgress,
    }
}

/// Resolve a tool-arg path against the session cwd (TS `toToolCallLocations`).
fn resolve_path(cwd: &Path, p: &str) -> PathBuf {
    let path = PathBuf::from(p);
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn thinking_echo_window_classifies_set_echoes() {
        let now = std::time::Instant::now();
        // No set yet: never an echo (genuine pi-initiated change refreshes).
        assert!(!thinking_event_is_echo(None, now));
        // Fresh set: echo, skip the redundant re-read.
        assert!(thinking_event_is_echo(Some(now), now));
        assert!(thinking_event_is_echo(
            Some(now - THINKING_SET_ECHO_WINDOW),
            now
        ));
        // Stale set: full refresh again.
        assert!(!thinking_event_is_echo(
            Some(now - THINKING_SET_ECHO_WINDOW - Duration::from_millis(1)),
            now
        ));
    }

    #[test]
    fn option_index_parses_choice_ids() {
        assert_eq!(option_index("choice-0"), Some(0));
        assert_eq!(option_index("choice-12"), Some(12));
        assert_eq!(option_index("choice-"), None);
        assert_eq!(option_index("choice"), None);
        assert_eq!(option_index("choice-01"), None); // non-canonical decimal
        assert_eq!(option_index("choice-1x"), None);
        assert_eq!(option_index("other"), None);
    }

    #[test]
    fn format_auto_retry_message_rounds_delays() {
        assert_eq!(
            format_auto_retry_message(1, 3, 5000),
            "Retrying (attempt 1/3, waiting 5s)..."
        );
        // sub-second delays round up to 1s
        assert_eq!(
            format_auto_retry_message(2, 4, 300),
            "Retrying (attempt 2/4, waiting 1s)..."
        );
        assert_eq!(
            format_auto_retry_message(1, 2, 0),
            "Retrying (attempt 1/2, waiting 0s)..."
        );
        // missing/zero fields fall back
        assert_eq!(format_auto_retry_message(0, 3, 1000), "Retrying...");
        assert_eq!(format_auto_retry_message(1, 0, 1000), "Retrying...");
    }

    #[test]
    fn raw_input_prefers_arguments_object() {
        let tc = json!({ "id": "t1", "name": "read", "arguments": { "path": "a.txt" } });
        assert_eq!(
            raw_input_of_tool_call(&tc),
            Some(json!({ "path": "a.txt" }))
        );
    }

    #[test]
    fn raw_input_parses_partial_args() {
        let tc = json!({ "id": "t1", "name": "read", "partialArgs": "{\"path\":\"x\"}" });
        assert_eq!(raw_input_of_tool_call(&tc), Some(json!({ "path": "x" })));
        // unparseable partial args are kept verbatim
        let tc = json!({ "id": "t1", "partialArgs": "not json {" });
        assert_eq!(
            raw_input_of_tool_call(&tc),
            Some(json!({ "partialArgs": "not json {" }))
        );
        // empty/missing -> None
        assert_eq!(raw_input_of_tool_call(&json!({ "id": "t1" })), None);
    }

    #[test]
    fn extension_ui_raw_input_carries_salient_fields() {
        let req: ExtensionUiRequest = serde_json::from_value(json!({
            "id": "ui-1", "method": "select", "title": "Pick", "options": ["a", "b"]
        }))
        .unwrap();
        let input = extension_ui_raw_input(&req);
        assert_eq!(input["method"], "select");
        assert_eq!(input["title"], "Pick");
        assert_eq!(input["options"], json!(["a", "b"]));

        let req: ExtensionUiRequest = serde_json::from_value(json!({
            "id": "ui-2", "method": "input", "title": "Type", "placeholder": "hint"
        }))
        .unwrap();
        let input = extension_ui_raw_input(&req);
        assert_eq!(input["method"], "input");
        assert_eq!(input["placeholder"], "hint");
        assert!(input.get("options").is_none());
    }

    #[test]
    fn extension_ui_timeout_is_read_from_interactive_requests() {
        let select: ExtensionUiRequest = serde_json::from_value(json!({
            "id": "ui-1",
            "method": "select",
            "title": "Pick",
            "options": ["a"],
            "timeout": 250
        }))
        .unwrap();
        assert_eq!(select.timeout_ms(), Some(250));

        let notify: ExtensionUiRequest = serde_json::from_value(json!({
            "id": "ui-2",
            "method": "notify",
            "message": "hello"
        }))
        .unwrap();
        assert_eq!(notify.timeout_ms(), None);
    }

    #[tokio::test]
    async fn request_permission_honors_extension_timeout() {
        let (outbound, mut outbound_rx) = mpsc::channel(1);
        let session_id: SessionId = "session".into();
        let request: ExtensionUiRequest = serde_json::from_value(json!({
            "id": "ui-1",
            "method": "select",
            "title": "Pick",
            "options": ["a"],
            "timeout": 10
        }))
        .unwrap();

        let permission =
            request_permission(&session_id, "ui-1", "Pick", &request, Vec::new(), &outbound);
        // Receive the request and then never answer: park forever holding the
        // responder so the extension timeout — not a racy drop — is the only
        // way the permission future can resolve. The old 25ms-drop raced the
        // 10ms timeout under coarse OS timer granularity (Windows ~15.6ms),
        // flaking with "permission responder dropped".
        let waiter = tokio::spawn(async move {
            let Some(OutboundMessage::RequestPermission(_, responder)) = outbound_rx.recv().await
            else {
                panic!("permission request was not sent");
            };
            let _hold = responder;
            std::future::pending::<()>().await;
        });
        let result = permission.await;
        waiter.abort();

        match result {
            Err(AcpxError::RpcFailed { command, message }) => {
                assert_eq!(command, "request_permission");
                assert!(message.contains("10ms"), "{message}");
            }
            other => panic!("expected permission timeout, got {other:?}"),
        }
    }

    #[test]
    fn resolve_path_handles_relative_and_absolute() {
        let cwd = Path::new("/work");
        assert_eq!(resolve_path(cwd, "a.txt"), PathBuf::from("/work/a.txt"));
        assert_eq!(resolve_path(cwd, "/abs/x"), PathBuf::from("/abs/x"));
    }
}
