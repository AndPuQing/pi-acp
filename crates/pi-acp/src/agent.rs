//! ACP `Agent` role implementation — full method set (S6, W-453).
//!
//! Serves an ACP client over stdio and bridges every v1 method to a real
//! `pi --mode rpc` child process through the [`SessionManager`] + per-session
//! state machine (S5). Ports `acp/agent.ts` with the design's "thin handler"
//! rule: handlers delegate to `session` / `translate`; the heavy logic lives
//! in [`PiAcpSession`] (turn queue, event pump, tool tracking) and the pure
//! translate layer.
//!
//! Methods:
//! - `initialize` — agent info, capabilities (load/list/delete, image prompts,
//!   embedded context), terminal auth methods.
//! - `session/new` — spawn pi, fetch state + models, configOptions (model +
//!   thought_level selects) + modes, startup info metadata, `closeAllExcept`
//!   policy, then post-response startup and `available_commands_update`
//!   notifications.
//! - `session/prompt` — restore-or-use session, expand file slash commands,
//!   handle built-in slash commands headlessly, else run the turn through the
//!   session queue. The handler **spawns** the turn and responds from the task
//!   so the SDK dispatch loop stays free for `session/cancel` (the SDK's
//!   dispatch loop blocks while a handler runs).
//! - `session/cancel` — notification → session cancel.
//! - `session/load` — restore a stored session (pi `--session`), return
//!   configOptions + modes, then replay history and publish
//!   `available_commands_update`.
//! - `session/list` / `session/delete` — pi session-file scanning, cwd filter,
//!   cursor pagination, idempotent delete.
//! - `session/set_mode` / `session/set_config_option` — thinking level / model,
//!   `current_mode_update` + `config_option_update`.
//! - `unstable/set_session_model` (`session/set_model` wire name) — handled via
//!   an untyped dispatch fallback: the pinned ACP schema (1.5.0) has no typed
//!   variant for this unstable method, so the SDK's `_`-prefixed extension
//!   fallback does not cover it either. The last handler in the chain claims
//!   the raw JSON-RPC request and delegates to the same model-set path.
//! - `authenticate` — no-op (terminal auth runs out-of-band).
//!
//! ACP `usage_update` notifications (decision 3 / #106) are emitted by the
//! session pump from pi's assistant-message usage (see [`PiAcpSession`]).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, AuthenticateRequest, AuthenticateResponse, AvailableCommandsUpdate,
    CancelNotification, ConfigOptionUpdate, ContentBlock, ContentChunk, CurrentModeUpdate,
    DeleteSessionRequest, DeleteSessionResponse, Implementation, InitializeRequest,
    InitializeResponse, ListSessionsRequest, ListSessionsResponse, LoadSessionRequest,
    LoadSessionResponse, McpCapabilities, NewSessionRequest, NewSessionResponse,
    PromptCapabilities, PromptRequest, PromptResponse, ResourceLink, SessionCapabilities,
    SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelectOption, SessionId,
    SessionInfo, SessionInfoUpdate, SessionMode, SessionModeId, SessionModeState,
    SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, SetSessionModeRequest, SetSessionModeResponse, StopReason,
    TextContent, ToolCall, ToolCallContent, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
    ToolKind,
};
use agent_client_protocol::{
    on_receive_dispatch, on_receive_notification, on_receive_request, Agent, ConnectionTo,
    Dispatch, Error as AcpError, Handled, Stdio, UntypedMessage,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::auth::{get_auth_methods, maybe_auth_required_error};
use crate::commands::{self, FileSlashCommand};
use crate::config::Config;
use crate::error::{AcpxError, Result};
use crate::pi::rpc::{ImageContent, Model, QueueMode, RpcSessionState, ThinkingLevel};
use crate::pi::sessions::{
    find_pi_session, list_pi_sessions, session_file_matches_id, title_from_session_file,
};
use crate::session::{
    spawn_outbound_connector, PiAcpSession, SessionManager, SessionParams,
    StopReason as SessionStopReason,
};
use crate::session_store::SessionStore;
use crate::settings::{get_enable_skill_commands, get_quiet_startup};
use crate::startup::{build_startup_info, build_update_notice, fetch_pi_version};
use crate::time::utc_now_iso8601;
use crate::translate::bash::{
    bash_exit_code, bash_terminal_content, bash_terminal_exit_meta, bash_terminal_info_meta,
    bash_terminal_output_meta,
};
use crate::translate::messages::{replay_message, ReplayMessage};
use crate::translate::prompt::prompt_to_pi_message;
use crate::translate::tools::{to_tool_kind, tool_result_to_text};

/// `configOptions` id for the model selector.
const MODEL_CONFIG_ID: &str = "model";
/// `configOptions` id for the thought-level selector.
const THOUGHT_LEVEL_CONFIG_ID: &str = "thought_level";

/// ACP `invalidParams` JSON-RPC code.
const ACP_INVALID_PARAMS: i32 = -32602;
/// ACP `authRequired` JSON-RPC code (reserved range).
const ACP_AUTH_REQUIRED: i32 = -32000;

/// The unstable `session/set_model` wire method (Zed's `unstable_setSessionModel`).
const SESSION_SET_MODEL_METHOD: &str = "session/set_model";

/// `ConnectionTo<Client>` alias for handlers (the Agent role's counterpart).
type Client = agent_client_protocol::Client;

/// ACP `internalError` JSON-RPC code.
const ACP_INTERNAL_ERROR: i32 = -32603;

/// `session/list` page size (mirrors the TS reference).
const LIST_PAGE_SIZE: usize = 50;

/// A model advertised to the client (`provider/id` + `provider/name` labels).
#[derive(Debug, Clone)]
struct AdvertisedModel {
    model_id: String,
    name: String,
}

/// Model state for a session (available models + the active one).
#[derive(Debug, Clone)]
struct ModelState {
    available_models: Vec<AdvertisedModel>,
    current_model_id: String,
}

/// Thought-level mode state (the model's native available levels, W-478).
#[derive(Debug, Clone)]
struct ModeState {
    current_mode_id: String,
    available_modes: Vec<SessionMode>,
    /// The native level list behind `available_modes` (shared with the
    /// `thought_level` config option so both selectors describe one ladder).
    levels: Vec<ThinkingLevel>,
}

/// Notifications that must be sent only after the `session/new` response is
/// queued. ACP clients such as Zed ignore session updates for an id they have
/// not registered yet.
struct NewSessionPostResponse {
    session: Arc<PiAcpSession>,
    prelude_text: String,
    enable_skill_commands: bool,
    file_commands: Vec<FileSlashCommand>,
}

impl NewSessionPostResponse {
    async fn send(self, cx: &ConnectionTo<Client>) {
        let session_id = self.session.session_id().clone();
        if !self.prelude_text.is_empty() {
            send_text_chunk(cx, &session_id, &self.prelude_text).await;
        }
        advertise_commands(
            cx,
            &self.session,
            self.enable_skill_commands,
            &self.file_commands,
        )
        .await;
    }
}

/// Notifications that must be sent only after the `session/load` response is
/// queued. This keeps replay and command discovery attached to a known session.
struct LoadSessionPostResponse {
    session: Arc<PiAcpSession>,
    history: Value,
    title: Option<String>,
    enable_skill_commands: bool,
    file_commands: Vec<FileSlashCommand>,
}

impl LoadSessionPostResponse {
    async fn send(self, cx: &ConnectionTo<Client>) {
        let session_id = self.session.session_id().clone();
        if let Some(title) = self.title {
            let update = SessionInfoUpdate::new()
                .title(title)
                .updated_at(utc_now_iso8601());
            let _ = cx.send_notification(SessionNotification::new(
                session_id.clone(),
                SessionUpdate::SessionInfoUpdate(update),
            ));
        }
        replay_history(cx, &self.session, &self.history).await;
        advertise_commands(
            cx,
            &self.session,
            self.enable_skill_commands,
            &self.file_commands,
        )
        .await;
    }
}

/// Cached startup version-check state (design D6: the npm registry probe runs
/// at most once per agent process, so repeated `session/new` never re-hit the
/// network). `Done(None)` means "checked, pi is up to date".
enum VersionCheck {
    /// Not checked yet.
    Pending,
    /// Checked; the cached notice text (if any).
    Done(Option<Arc<str>>),
}

/// Await startup probes that were launched before a session handshake. Error
/// paths must join them too: dropping a Tokio `JoinHandle` detaches its task,
/// which can otherwise leave a short-lived probe child running into the next
/// process-backed ACP fixture.
async fn drain_startup_tasks(
    notice_task: Option<tokio::task::JoinHandle<Option<String>>>,
    version_task: Option<tokio::task::JoinHandle<Option<String>>>,
) {
    if let Some(task) = notice_task {
        let _ = task.await;
    }
    if let Some(task) = version_task {
        let _ = task.await;
    }
}

/// The per-connection agent: shared state behind every handler.
pub struct AcpAgent {
    cfg: Config,
    sessions: SessionManager,
    store: SessionStore,
    /// Serializes session replacement policies (`new`/`load`) so concurrent
    /// requests cannot close each other's freshly-created subprocess.
    session_lifecycle: Mutex<()>,
    /// Most recent session cwd, used as the default `session/list` filter
    /// (TS parity: Zed sends `{}` and expects the project-scoped picker).
    last_session_cwd: Mutex<Option<PathBuf>>,
    /// Cached startup update notice (D6: async + cache, off the critical path).
    version_check: Mutex<VersionCheck>,
}

impl AcpAgent {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            sessions: SessionManager::new(),
            store: SessionStore::new(),
            session_lifecycle: Mutex::new(()),
            last_session_cwd: Mutex::new(None),
            version_check: Mutex::new(VersionCheck::Pending),
        }
    }

    /// Run the ACP agent over stdio until the client disconnects; dispose all
    /// sessions on the way out.
    pub async fn run(self: &Arc<Self>) -> Result<()> {
        let agent = self.clone();
        let a_init = agent.clone();
        let a_new = agent.clone();
        let a_prompt = agent.clone();
        let a_cancel = agent.clone();
        let a_load = agent.clone();
        let a_list = agent.clone();
        let a_delete = agent.clone();
        let a_set_mode = agent.clone();
        let a_set_config = agent.clone();
        let a_set_model = agent.clone();

        let result = Agent
            .builder()
            .name("pi-acp")
            .on_receive_request(
                async move |req: InitializeRequest, responder, _cx| {
                    let resp = a_init.handle_initialize(&req);
                    responder.respond(resp)
                },
                on_receive_request!(),
            )
            .on_receive_request(
                async move |req: NewSessionRequest, responder, cx| {
                    let agent = a_new.clone();
                    let result = agent.handle_new_session(&req, &cx).await;
                    match result {
                        Ok((resp, post_response)) => {
                            responder.respond(resp)?;
                            // Publish the empty context state before the
                            // handler returns, so a client cannot race the
                            // post-response task with its first prompt.
                            let _ = post_response.session.publish_initial_usage().await;
                            let cx_for_task = cx.clone();
                            cx.spawn(async move {
                                post_response.send(&cx_for_task).await;
                                Ok(())
                            })
                        }
                        Err(e) => responder.respond_with_error(e),
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                async move |req: PromptRequest, responder, cx| {
                    // Spawn the turn: the SDK dispatch loop blocks while a
                    // handler runs, and `session/cancel` must stay processable
                    // during a long turn. The responder travels into the task
                    // and answers when the turn settles.
                    let agent = a_prompt.clone();
                    let cx_for_task = cx.clone();
                    cx.spawn(async move {
                        let result = agent.handle_prompt(&req, &cx_for_task).await;
                        match result {
                            Ok((resp, persisted)) => {
                                let answered = responder.respond(resp);
                                // Persist after the response is queued: the
                                // session-map write (+ its `get_state`
                                // round-trip) must not hold up prompt latency
                                // (W-479 P1). Failures stay best-effort inside.
                                if let Some(session) = persisted {
                                    agent.persist_session_if_ready(&session).await;
                                }
                                answered
                            }
                            Err(e) => responder.respond_with_error(e),
                        }
                    })?;
                    Ok(())
                },
                on_receive_request!(),
            )
            .on_receive_notification(
                async move |notif: CancelNotification, _cx| {
                    a_cancel.handle_cancel(&notif).await;
                    Ok(())
                },
                on_receive_notification!(),
            )
            .on_receive_request(
                async move |req: LoadSessionRequest, responder, cx| {
                    let agent = a_load.clone();
                    let result = agent.handle_load_session(&req, &cx).await;
                    match result {
                        Ok((resp, post_response)) => {
                            responder.respond(resp)?;
                            let cx_for_task = cx.clone();
                            cx.spawn(async move {
                                post_response.send(&cx_for_task).await;
                                Ok(())
                            })
                        }
                        Err(e) => responder.respond_with_error(e),
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                async move |req: ListSessionsRequest, responder, _cx| {
                    let agent = a_list.clone();
                    let result = agent.handle_list_sessions(&req).await;
                    match result {
                        Ok(resp) => responder.respond(resp),
                        Err(e) => responder.respond_with_error(e),
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                async move |req: DeleteSessionRequest, responder, _cx| {
                    let agent = a_delete.clone();
                    let result = agent.handle_delete_session(&req).await;
                    match result {
                        Ok(resp) => responder.respond(resp),
                        Err(e) => responder.respond_with_error(e),
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                async move |req: SetSessionModeRequest, responder, cx| {
                    let agent = a_set_mode.clone();
                    let result = agent.handle_set_mode(&req, &cx).await;
                    match result {
                        Ok(resp) => responder.respond(resp),
                        Err(e) => responder.respond_with_error(e),
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                async move |req: SetSessionConfigOptionRequest, responder, cx| {
                    let agent = a_set_config.clone();
                    let result = agent.handle_set_config_option(&req, &cx).await;
                    match result {
                        Ok(resp) => responder.respond(resp),
                        Err(e) => responder.respond_with_error(e),
                    }
                },
                on_receive_request!(),
            )
            .on_receive_request(
                async move |req: AuthenticateRequest, responder, _cx| {
                    tracing::info!(method_id = %req.method_id, "ACP authenticate (no-op; terminal auth runs out-of-band)");
                    responder.respond(AuthenticateResponse::new())
                },
                on_receive_request!(),
            )
            // LAST handler: the unstable `session/set_model` method, which the
            // pinned schema cannot type. Claims only that method; everything
            // else passes through untouched.
            .on_receive_dispatch(
                async move |dispatch: Dispatch<UntypedMessage, UntypedMessage>, cx| {
                    let Dispatch::Request(message, responder) = dispatch else {
                        return Ok(Handled::No {
                            message: dispatch,
                            retry: false,
                        });
                    };
                    if message.method != SESSION_SET_MODEL_METHOD {
                        return Ok(Handled::No {
                            message: Dispatch::Request(message, responder),
                            retry: false,
                        });
                    }
                    let agent = a_set_model.clone();
                    let cx_for_task = cx.clone();
                    cx.spawn(async move {
                        let result = agent.handle_set_session_model(&message.params, &cx_for_task).await;
                        match result {
                            Ok(()) => responder.respond(json!({})),
                            Err(e) => responder.respond_with_error(e),
                        }
                    })?;
                    Ok(Handled::Yes)
                },
                on_receive_dispatch!(),
            )
            .connect_to(Stdio::new())
            .await
            .map_err(|e| AcpxError::RpcFailed {
                command: "acp-stdio".into(),
                message: e.to_string(),
            });

        self.sessions.dispose_all().await;
        result
    }

    /// Gracefully shut down (design §8.3): dispose every live pi session.
    /// Invoked on SIGINT/SIGTERM so a Ctrl+C / `kill` on pi-acp never orphans
    /// its pi subprocesses (they run in their own process group and would not
    /// receive the terminal's signal).
    pub async fn shutdown(&self) {
        self.sessions.dispose_all().await;
    }

    // -----------------------------------------------------------------------
    // initialize
    // -----------------------------------------------------------------------

    fn handle_initialize(&self, req: &InitializeRequest) -> InitializeResponse {
        tracing::info!(protocol_version = ?req.protocol_version, "ACP initialize");
        // We currently only support ACP protocol version 1.
        let protocol_version = agent_client_protocol::schema::ProtocolVersion::V1;

        let supports_terminal_auth_meta = req
            .client_capabilities
            .meta
            .as_ref()
            .and_then(|m| m.get("terminal-auth"))
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let capabilities = AgentCapabilities::new()
            .load_session(true)
            .mcp_capabilities(McpCapabilities::new().http(false).sse(false))
            .prompt_capabilities(
                PromptCapabilities::new()
                    .image(true)
                    .audio(false)
                    .embedded_context(self.cfg.enable_embedded_context),
            )
            .session_capabilities(
                SessionCapabilities::new()
                    .list(agent_client_protocol::schema::v1::SessionListCapabilities::new())
                    .delete(agent_client_protocol::schema::v1::SessionDeleteCapabilities::new()),
            );

        InitializeResponse::new(protocol_version)
            .agent_capabilities(capabilities)
            .agent_info(
                Implementation::new("pi-acp", env!("CARGO_PKG_VERSION")).title("pi ACP adapter"),
            )
            .auth_methods(get_auth_methods(supports_terminal_auth_meta))
    }

    // -----------------------------------------------------------------------
    // session/new
    // -----------------------------------------------------------------------

    async fn handle_new_session(
        &self,
        req: &NewSessionRequest,
        cx: &ConnectionTo<Client>,
    ) -> std::result::Result<(NewSessionResponse, NewSessionPostResponse), AcpError> {
        if !req.cwd.is_absolute() {
            return Err(invalid_params(&format!(
                "cwd must be an absolute path: {}",
                req.cwd.display()
            )));
        }
        let _lifecycle = self.session_lifecycle.lock().await;
        *self.last_session_cwd.lock().await = Some(req.cwd.clone());

        // Kick off the npm update check as early as possible so it overlaps the
        // session handshake below (design D6: async + cached). Await it before
        // building the response so its startup metadata is ready; the matching
        // notification is queued only after the response below.
        let mut notice_task: Option<tokio::task::JoinHandle<Option<String>>> = None;
        if self.cfg.enable_version_check {
            let pending = matches!(*self.version_check.lock().await, VersionCheck::Pending);
            if pending {
                let pi_command = self.cfg.pi_command.clone();
                notice_task = Some(tokio::spawn(async move {
                    build_update_notice(&pi_command).await
                }));
            }
        }

        let file_commands = commands::load_slash_commands(&req.cwd);
        let enable_skill_commands = get_enable_skill_commands(&req.cwd);
        let quiet_startup = get_quiet_startup(&req.cwd);

        // D6: the `pi --version` probe runs on a spawned task **overlapping**
        // the handshake below, so a slow probe never *adds* to the session/new
        // critical path (the response waits only max(handshake, probe), never
        // handshake + probe). Skipped under quietStartup — the prelude is then
        // just the update notice, which carries no version header.
        let mut version_task: Option<tokio::task::JoinHandle<Option<String>>> = None;
        if !quiet_startup {
            let pi_command = self.cfg.pi_command.clone();
            version_task = Some(tokio::spawn(
                async move { fetch_pi_version(&pi_command).await },
            ));
        }

        let session = match self
            .spawn_session(Some(&req.cwd), None, None, cx, file_commands.clone())
            .await
        {
            Ok(session) => session,
            Err(err) => {
                drain_startup_tasks(notice_task.take(), version_task.take()).await;
                return Err(err);
            }
        };
        let session_id = session.session_id().clone();

        // Reuse the spawn-time state: `PiAcpSession::spawn` already fetched
        // `get_state`, and nothing mutates pi between spawn and here, so only
        // the model list costs a round-trip on this path (W-479: one fewer
        // pi RPC on the session/new critical path).
        let state = session.initial_state().clone();
        let models_res = session.get_available_models().await;
        let available_models = models_res.as_ref().ok().cloned();

        // Auth checks (parity with TS): a model-list failure that smells like
        // missing credentials, or zero models, both mean "authenticate first".
        if let Some(err) = models_res.as_ref().err() {
            if maybe_auth_required_error(&err.to_string()).is_some() {
                self.cleanup_failed_new_session(&session, Some(&state))
                    .await;
                drain_startup_tasks(notice_task.take(), version_task.take()).await;
                return Err(auth_required());
            }
            self.cleanup_failed_new_session(&session, Some(&state))
                .await;
            drain_startup_tasks(notice_task.take(), version_task.take()).await;
            return Err(AcpError::new(ACP_INTERNAL_ERROR, err.to_string()));
        }
        let raw_models_count = available_models.as_ref().map(Vec::len).unwrap_or(0);
        if raw_models_count == 0 {
            self.cleanup_failed_new_session(&session, Some(&state))
                .await;
            drain_startup_tasks(notice_task.take(), version_task.take()).await;
            return Err(auth_required());
        }

        let (config_options, _models, modes) =
            get_session_configuration(&session, Some(&state), available_models.as_ref()).await;

        if let Some(session_file) = state
            .session_file
            .as_deref()
            .filter(|path| !path.trim().is_empty())
        {
            let path = resolve_session_file_path(&req.cwd, session_file);
            if session_file_matches_id(&path, &session_id.0) {
                self.store
                    .upsert(&session_id.0, &req.cwd.to_string_lossy(), session_file);
            } else {
                tracing::debug!(
                    session = %session_id,
                    ?path,
                    "session file is not persisted yet; delaying session-map entry"
                );
            }
        }

        let update_notice = if let Some(task) = notice_task.take() {
            // The check has been running in parallel with the handshake; await
            // it (its internal timeouts bound the wait) and cache the result.
            let notice = task.await.unwrap_or(None);
            *self.version_check.lock().await = VersionCheck::Done(notice.clone().map(Arc::from));
            notice
        } else {
            match &*self.version_check.lock().await {
                VersionCheck::Done(notice) => notice.as_ref().map(|s| s.to_string()),
                VersionCheck::Pending => None,
            }
        };
        let prelude_text = {
            let pi_version = match version_task.take() {
                Some(task) => task.await.unwrap_or(None),
                None => None,
            };
            build_startup_prelude(
                &req.cwd,
                pi_version.as_deref(),
                quiet_startup,
                update_notice.as_deref(),
            )
        };

        // Policy: within a single ACP connection keep only one live pi
        // subprocess (TS parity — avoids leaking subprocesses when clients
        // start new sessions without closing old ones).
        self.sessions.close_all_except(&session_id).await;

        let mut meta = serde_json::Map::new();
        if !prelude_text.is_empty() {
            meta.insert("piAcp".to_string(), json!({ "startupInfo": prelude_text }));
        }

        let response = NewSessionResponse::new(session_id.clone())
            .modes(mode_state_to_acp(&modes))
            .config_options(config_options.clone())
            .meta(meta);

        tracing::info!(session = %session_id, "session/new complete");
        Ok((
            response,
            NewSessionPostResponse {
                session,
                prelude_text,
                enable_skill_commands,
                file_commands,
            },
        ))
    }

    /// Close a failed `session/new`: dispose the session, remove its session
    /// file (when known) and store entry (TS `cleanupFailedNewSession`).
    async fn cleanup_failed_new_session(
        &self,
        session: &Arc<PiAcpSession>,
        state: Option<&RpcSessionState>,
    ) {
        let session_id = session.session_id().clone();
        self.sessions.close(&session_id).await;

        let session_file = state
            .and_then(|s| s.session_file.as_deref())
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .or_else(|| self.store.get(&session_id.0).map(|e| e.session_file));

        if let Some(file) = session_file {
            let _ = std::fs::remove_file(file);
        }
        self.store.delete(&session_id.0);
    }

    /// Persist a new session's map entry after pi has had a chance to flush
    /// its first assistant response. pi intentionally keeps an empty new
    /// session in memory, so `get_state.sessionFile` alone is not proof that
    /// the path can be restored after this process exits.
    async fn persist_session_if_ready(&self, session: &Arc<PiAcpSession>) {
        let state = match session.get_state().await {
            Ok(state) => state,
            Err(err) => {
                // The prompt already settled; a late process exit should not
                // turn a successful turn into a persistence error.
                tracing::debug!(
                    session = %session.session_id(),
                    error = %err,
                    "could not refresh session state for persistence"
                );
                return;
            }
        };
        let Some(session_file) = state
            .session_file
            .as_deref()
            .filter(|path| !path.trim().is_empty())
        else {
            return;
        };

        let path = resolve_session_file_path(session.cwd(), session_file);
        if state.session_id != session.session_id().0.as_ref() {
            tracing::warn!(
                expected = %session.session_id(),
                actual = %state.session_id,
                "pi session id changed unexpectedly; refusing to persist session map entry"
            );
            return;
        }
        if session_file_matches_id(&path, &state.session_id) {
            self.store.upsert(
                session.session_id().0.as_ref(),
                &session.cwd().to_string_lossy(),
                session_file,
            );
        } else {
            tracing::debug!(
                session = %session.session_id(),
                ?path,
                "session file is still not persisted; leaving session-map unchanged"
            );
        }
    }

    // -----------------------------------------------------------------------
    // session/prompt
    // -----------------------------------------------------------------------

    /// Run one prompt turn. Returns the response plus the session to persist
    /// (`None` for headless built-in commands, which never persist — same as
    /// before); the caller persists *after* queueing the response so the
    /// map write stays off prompt latency (W-479 P1).
    async fn handle_prompt(
        &self,
        req: &PromptRequest,
        cx: &ConnectionTo<Client>,
    ) -> std::result::Result<(PromptResponse, Option<Arc<PiAcpSession>>), AcpError> {
        let session = self.restore_session(&req.session_id, None, cx).await?;
        let session_id = session.session_id().clone();

        let pi_prompt = prompt_to_pi_message(&req.prompt);

        // Built-in ACP slash command handling (headless-friendly subset).
        // File-based slash commands are expanded inside session.prompt().
        if pi_prompt.images.is_empty() && pi_prompt.message.trim_start().starts_with('/') {
            if let Some(resp) = self
                .handle_builtin_command(cx, &session, &session_id, &pi_prompt.message)
                .await?
            {
                return Ok((resp, None));
            }
        }

        // The first real prompt names the thread (fixes #102/#24: without a
        // title, Zed's sidebar keeps "New Agent Thread"). Slash commands don't
        // title the session; the title is provisional — `/name` and pi's own
        // `session_info` entries override it later.
        if !pi_prompt.message.trim_start().starts_with('/') && session.mark_first_prompt() {
            if let Some(title) = provisional_title_from_prompt(&pi_prompt.message) {
                let update = SessionInfoUpdate::new()
                    .title(title)
                    .updated_at(utc_now_iso8601());
                let _ = cx.send_notification(SessionNotification::new(
                    session_id.clone(),
                    SessionUpdate::SessionInfoUpdate(update),
                ));
            }
        }

        let reason = session
            .prompt(pi_prompt.message, to_pi_images(pi_prompt.images))
            .await
            .map_err(acp_error_from_pi)?;
        let stop_reason = acp_stop_reason(reason);
        tracing::info!(session = %session_id, ?stop_reason, "prompt turn settled");
        Ok((PromptResponse::new(stop_reason), Some(session)))
    }

    async fn handle_cancel(&self, notif: &CancelNotification) {
        tracing::info!(session = %notif.session_id, "ACP session/cancel");
        let Some(session) = self.sessions.maybe_get(&notif.session_id).await else {
            return;
        };
        if let Err(e) = session.cancel().await {
            // A notification has no error response; log the failure so it is
            // never silently swallowed (design D5).
            tracing::warn!(session = %notif.session_id, error = %e, "session/cancel failed");
        }
    }

    // -----------------------------------------------------------------------
    // session/load
    // -----------------------------------------------------------------------

    async fn handle_load_session(
        &self,
        req: &LoadSessionRequest,
        cx: &ConnectionTo<Client>,
    ) -> std::result::Result<(LoadSessionResponse, LoadSessionPostResponse), AcpError> {
        if !req.cwd.is_absolute() {
            return Err(invalid_params(&format!(
                "cwd must be an absolute path: {}",
                req.cwd.display()
            )));
        }

        let _lifecycle = self.session_lifecycle.lock().await;

        // Tear down every live pi before spawning the replacement. Besides
        // making the one-live-session policy explicit, this keeps session
        // replacement below the runner's process limit when nested ACP
        // fixtures are used.
        self.sessions.close(&req.session_id).await;
        self.sessions.close_all_except(&req.session_id).await;

        *self.last_session_cwd.lock().await = Some(req.cwd.clone());

        let stored = self
            .find_stored_session(&req.session_id.0)
            .ok_or_else(|| invalid_params(&format!("Unknown sessionId: {}", req.session_id.0)))?;
        let (stored_cwd, stored_file) = stored;

        let enable_skill_commands = get_enable_skill_commands(&req.cwd);
        let file_commands = commands::load_slash_commands(&req.cwd);

        let session = self
            .restore_session(&req.session_id, Some(&req.cwd), cx)
            .await?;

        self.store
            .upsert(&req.session_id.0, &stored_cwd, &stored_file);

        // Fetch full conversation history. It is replayed after the response so
        // the client has registered the restored session first. Failures still
        // surface as an explicit error (TS parity: `loadSession` throws; S8
        // "never swallow errors").
        let data = session.get_messages().await.map_err(acp_error_from_pi)?;
        let title = title_from_session_file(std::path::Path::new(&stored_file));

        let (config_options, _models, modes) =
            get_session_configuration(&session, None, None).await;

        let response = LoadSessionResponse::new()
            .modes(mode_state_to_acp(&modes))
            .config_options(config_options);

        tracing::info!(session = %req.session_id, "session/load complete");
        Ok((
            response,
            LoadSessionPostResponse {
                session,
                history: data,
                title,
                enable_skill_commands,
                file_commands,
            },
        ))
    }

    // -----------------------------------------------------------------------
    // session/list / session/delete
    // -----------------------------------------------------------------------

    async fn handle_list_sessions(
        &self,
        req: &ListSessionsRequest,
    ) -> std::result::Result<ListSessionsResponse, AcpError> {
        let all = list_pi_sessions();

        // ACP: filter by cwd if provided. Zed sends `{}`, so default to the
        // last session cwd to emulate pi's project-scoped `/resume` picker.
        let effective_cwd = req.cwd.clone().or_else(|| {
            self.last_session_cwd
                .try_lock()
                .ok()
                .and_then(|l| l.clone())
        });
        let filtered: Vec<_> = match &effective_cwd {
            Some(cwd) => all
                .into_iter()
                .filter(|s| s.cwd == cwd.to_string_lossy())
                .collect(),
            None => all,
        };

        let offset = req
            .cursor
            .as_deref()
            .and_then(|c| c.parse::<usize>().ok())
            .unwrap_or(0);
        let page: Vec<SessionInfo> = filtered
            .iter()
            .skip(offset)
            .take(LIST_PAGE_SIZE)
            .map(|s| {
                SessionInfo::new(s.session_id.clone(), PathBuf::from(&s.cwd))
                    .title(s.title.clone())
                    .updated_at(s.updated_at.clone())
            })
            .collect();

        let next_cursor = if offset + LIST_PAGE_SIZE < filtered.len() {
            Some((offset + LIST_PAGE_SIZE).to_string())
        } else {
            None
        };

        Ok(ListSessionsResponse::new(page).next_cursor(next_cursor))
    }

    async fn handle_delete_session(
        &self,
        req: &DeleteSessionRequest,
    ) -> std::result::Result<DeleteSessionResponse, AcpError> {
        let stored = self.store.get(&req.session_id.0);
        let pi_session = find_pi_session(&req.session_id.0);

        // Deleting a session that does not exist succeeds idempotently (ACP
        // `session/delete` semantics).
        if stored.is_none() && pi_session.is_none() {
            return Ok(DeleteSessionResponse::new());
        }

        let session_file = stored
            .as_ref()
            .map(|s| s.session_file.clone())
            .or_else(|| pi_session.as_ref().map(|s| s.session_file.clone()));
        if let Some(file) = session_file {
            let _ = std::fs::remove_file(file); // best-effort cleanup
        }
        self.sessions.close(&req.session_id).await;
        self.store.delete(&req.session_id.0);
        Ok(DeleteSessionResponse::new())
    }

    // -----------------------------------------------------------------------
    // session/set_mode / session/set_config_option / session/set_model
    // -----------------------------------------------------------------------

    async fn handle_set_mode(
        &self,
        req: &SetSessionModeRequest,
        cx: &ConnectionTo<Client>,
    ) -> std::result::Result<SetSessionModeResponse, AcpError> {
        let session = self.restore_session(&req.session_id, None, cx).await?;
        let mode = req.mode_id.0.as_ref();
        let level = ThinkingLevel::parse(mode)
            .ok_or_else(|| invalid_params(&format!("Unknown modeId: {mode}")))?;

        session
            .set_thinking_level(level)
            .await
            .map_err(acp_error_from_pi)?;

        // Refreshes both selectors; the mode update carries pi's effective
        // (possibly clamped) level.
        let _ = emit_config_options_update(cx, &req.session_id, &session).await;
        Ok(SetSessionModeResponse::new())
    }

    async fn handle_set_config_option(
        &self,
        req: &SetSessionConfigOptionRequest,
        cx: &ConnectionTo<Client>,
    ) -> std::result::Result<SetSessionConfigOptionResponse, AcpError> {
        let session = self.restore_session(&req.session_id, None, cx).await?;
        let config_id = req.config_id.0.as_ref();

        let value = req
            .value
            .as_value_id()
            .map(|v| v.0.as_ref().to_string())
            .ok_or_else(|| {
                invalid_params(&format!(
                    "Expected string value for config option: {config_id}"
                ))
            })?;

        match config_id {
            MODEL_CONFIG_ID => {
                set_session_model(&session, &value)
                    .await
                    .map_err(acp_error_from_pi)?;
            }
            THOUGHT_LEVEL_CONFIG_ID => {
                let level = ThinkingLevel::parse(&value)
                    .ok_or_else(|| invalid_params(&format!("Unknown thinking level: {value}")))?;
                session
                    .set_thinking_level(level)
                    .await
                    .map_err(acp_error_from_pi)?;
            }
            other => {
                return Err(invalid_params(&format!("Unknown config option: {other}")));
            }
        }

        let config_options = emit_config_options_update(cx, &req.session_id, &session)
            .await
            .map_err(acp_error_from_pi)?;
        Ok(SetSessionConfigOptionResponse::new(config_options))
    }

    /// Handle the unstable `session/set_model` request (raw params:
    /// `{ sessionId, modelId }`).
    async fn handle_set_session_model(
        &self,
        params: &Value,
        cx: &ConnectionTo<Client>,
    ) -> std::result::Result<(), AcpError> {
        let session_id: SessionId = params
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| invalid_params("session/set_model missing sessionId"))?
            .into();
        let model_id = params
            .get("modelId")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_params("session/set_model missing modelId"))?;

        let session = self.restore_session(&session_id, None, cx).await?;
        set_session_model(&session, model_id)
            .await
            .map_err(acp_error_from_pi)?;
        let _ = emit_config_options_update(cx, &session_id, &session).await;
        tracing::info!(session = %session_id.0, model = model_id, "session/set_model applied");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // session restoration & spawning
    // -----------------------------------------------------------------------

    /// Locate a session's `{cwd, sessionFile}`: the session map first, then a
    /// fresh scan of pi's session files (updating the map).
    fn find_stored_session(&self, session_id: &str) -> Option<(String, String)> {
        if let Some(stored) = self.store.get(session_id) {
            return Some((stored.cwd, stored.session_file));
        }
        let pi_session = find_pi_session(session_id)?;
        self.store
            .upsert(session_id, &pi_session.cwd, &pi_session.session_file);
        Some((pi_session.cwd, pi_session.session_file))
    }

    /// Get the live session for `session_id`, restoring it (spawning a pi
    /// against its session file) when necessary. Mirrors TS `restoreSession`.
    async fn restore_session(
        &self,
        session_id: &SessionId,
        cwd: Option<&Path>,
        cx: &ConnectionTo<Client>,
    ) -> std::result::Result<Arc<PiAcpSession>, AcpError> {
        if let Some(existing) = self.sessions.maybe_get(session_id).await {
            return Ok(existing);
        }

        let (stored_cwd, session_file) = self
            .find_stored_session(&session_id.0)
            .ok_or_else(|| invalid_params(&format!("Unknown sessionId: {}", session_id.0)))?;

        let effective_cwd = cwd
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(&stored_cwd));
        let file_commands = commands::load_slash_commands(&effective_cwd);

        let session = self
            .spawn_session(
                Some(&effective_cwd),
                Some(PathBuf::from(&session_file)),
                Some(session_id.clone()),
                cx,
                file_commands,
            )
            .await?;

        *self.last_session_cwd.lock().await = Some(effective_cwd.clone());
        self.store.upsert(
            &session_id.0,
            &effective_cwd.to_string_lossy(),
            &session_file,
        );
        Ok(session)
    }

    /// Spawn a session (new or restored) and register it, wiring its outbound
    /// channel to the SDK connection.
    async fn spawn_session(
        &self,
        cwd: Option<&Path>,
        session_path: Option<PathBuf>,
        session_id_override: Option<SessionId>,
        cx: &ConnectionTo<Client>,
        file_commands: Vec<FileSlashCommand>,
    ) -> std::result::Result<Arc<PiAcpSession>, AcpError> {
        let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(512);
        let conn = cx.clone();
        spawn_outbound_connector(conn, outbound_rx)?;

        let session = PiAcpSession::spawn(SessionParams {
            pi_command: self.cfg.pi_command.clone(),
            extra_args: vec![],
            timeout: std::time::Duration::from_secs(self.cfg.rpc_timeout_secs),
            settle_timeout: std::time::Duration::from_secs(self.cfg.settle_timeout_secs),
            cwd: cwd.unwrap_or_else(|| Path::new(".")).to_path_buf(),
            outbound: outbound_tx,
            session_path,
            session_id_override,
            file_commands,
        })
        .await?;
        self.sessions.insert(session.clone()).await;
        Ok(session)
    }

    // -----------------------------------------------------------------------
    // built-in slash commands (headless subset, TS `agent.ts`)
    // -----------------------------------------------------------------------

    /// Handle one built-in slash command. Returns `Some(response)` when the
    /// text names a built-in (the turn is complete); `None` to fall through to
    /// a real pi turn.
    async fn handle_builtin_command(
        &self,
        cx: &ConnectionTo<Client>,
        session: &Arc<PiAcpSession>,
        session_id: &SessionId,
        message: &str,
    ) -> std::result::Result<Option<PromptResponse>, AcpError> {
        let trimmed = message.trim();
        let space = trimmed.find(' ');
        let cmd = match space {
            Some(i) => &trimmed[1..i],
            None => &trimmed[1..],
        };
        let args_string = match space {
            Some(i) => &trimmed[i + 1..],
            None => "",
        };
        let args = commands::parse_command_args(args_string);

        match cmd {
            "compact" => {
                // TS: `args.join(' ').trim() || undefined` — whitespace-only
                // args mean no custom instructions.
                let joined = args.join(" ");
                let custom = if joined.trim().is_empty() {
                    None
                } else {
                    Some(joined.trim().to_string())
                };
                let res = session
                    .compact(custom.as_deref())
                    .await
                    .map_err(acp_error_from_pi)?;
                let tokens_before = res.get("tokensBefore").and_then(Value::as_u64);
                let summary = res
                    .get("summary")
                    .and_then(Value::as_str)
                    .map(str::to_string);

                let mut first = "Compaction completed.".to_string();
                if custom.is_some() {
                    first.push_str(" (custom instructions applied)");
                }
                let mut lines = vec![first];
                if let Some(tb) = tokens_before {
                    lines.push(format!("Tokens before: {tb}"));
                }
                let mut text = lines.join("\n");
                if let Some(s) = summary {
                    text.push_str(&format!("\n\n{s}"));
                }
                let _ = send_text_chunk(cx, session_id, &text).await;
                Ok(Some(PromptResponse::new(StopReason::EndTurn)))
            }
            "session" => {
                let stats = session
                    .get_session_stats()
                    .await
                    .map_err(acp_error_from_pi)?;
                let mut lines: Vec<String> = Vec::new();
                if let Some(sid) = stats.get("sessionId").and_then(Value::as_str) {
                    lines.push(format!("Session: {sid}"));
                }
                if let Some(sf) = stats.get("sessionFile").and_then(Value::as_str) {
                    lines.push(format!("Session file: {sf}"));
                }
                if let Some(n) = stats.get("totalMessages").and_then(Value::as_u64) {
                    lines.push(format!("Messages: {n}"));
                }
                if let Some(c) = stats.get("cost").and_then(Value::as_f64) {
                    lines.push(format!("Cost: {c}"));
                }
                if let Some(t) = stats.get("tokens") {
                    let mut parts: Vec<String> = Vec::new();
                    for (key, label) in [
                        ("input", "in"),
                        ("output", "out"),
                        ("cacheRead", "cache read"),
                        ("cacheWrite", "cache write"),
                        ("total", "total"),
                    ] {
                        if let Some(v) = t.get(key).and_then(Value::as_u64) {
                            parts.push(format!("{label} {v}"));
                        }
                    }
                    if !parts.is_empty() {
                        lines.push(format!("Tokens: {}", parts.join(", ")));
                    }
                }
                let text = if lines.is_empty() {
                    format!(
                        "Session stats:\n{}",
                        serde_json::to_string_pretty(&stats).unwrap_or_else(|_| "{}".to_string())
                    )
                } else {
                    lines.join("\n")
                };
                let _ = send_text_chunk(cx, session_id, &text).await;
                Ok(Some(PromptResponse::new(StopReason::EndTurn)))
            }
            "name" => {
                let name = args.join(" ").trim().to_string();
                if name.is_empty() {
                    let _ = send_text_chunk(cx, session_id, "Usage: /name <name>").await;
                    return Ok(Some(PromptResponse::new(StopReason::EndTurn)));
                }
                match session.set_session_name(&name).await {
                    Ok(()) => {
                        let update = SessionInfoUpdate::new()
                            .title(name.clone())
                            .updated_at(utc_now_iso8601());
                        let _ = cx.send_notification(SessionNotification::new(
                            session_id.clone(),
                            SessionUpdate::SessionInfoUpdate(update),
                        ));
                        let _ =
                            send_text_chunk(cx, session_id, &format!("Session name set: {name}"))
                                .await;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        let hint = if msg.contains("set_session_name") {
                            " This requires a newer pi version that supports `set_session_name` in RPC mode."
                        } else {
                            ""
                        };
                        let _ = send_text_chunk(
                            cx,
                            session_id,
                            &format!("Failed to set session name: {msg}{hint}"),
                        )
                        .await;
                    }
                }
                Ok(Some(PromptResponse::new(StopReason::EndTurn)))
            }
            "steering" | "follow-up" => {
                let state = session.get_state().await.map_err(acp_error_from_pi)?;
                let (current, mode_name, action) = if cmd == "steering" {
                    (
                        queue_mode_str(state.steering_mode).to_string(),
                        "Steering",
                        "steering",
                    )
                } else {
                    (
                        queue_mode_str(state.follow_up_mode).to_string(),
                        "Follow-up",
                        "follow-up",
                    )
                };
                let mode_raw = args.first().map(|s| s.to_lowercase()).unwrap_or_default();
                if mode_raw.is_empty() {
                    let _ = send_text_chunk(
                        cx,
                        session_id,
                        &format!(
                            "{mode_name} mode: {}",
                            if current.is_empty() {
                                "unknown"
                            } else {
                                &current
                            }
                        ),
                    )
                    .await;
                    return Ok(Some(PromptResponse::new(StopReason::EndTurn)));
                }
                if mode_raw != "all" && mode_raw != "one-at-a-time" {
                    let _ = send_text_chunk(
                        cx,
                        session_id,
                        &format!("Usage: /{action} all | /{action} one-at-a-time"),
                    )
                    .await;
                    return Ok(Some(PromptResponse::new(StopReason::EndTurn)));
                }
                let queue_mode = if mode_raw == "all" {
                    QueueMode::All
                } else {
                    QueueMode::OneAtATime
                };
                if cmd == "steering" {
                    session
                        .set_steering_mode(queue_mode)
                        .await
                        .map_err(acp_error_from_pi)?;
                } else {
                    session
                        .set_follow_up_mode(queue_mode)
                        .await
                        .map_err(acp_error_from_pi)?;
                }
                let _ = send_text_chunk(
                    cx,
                    session_id,
                    &format!("{mode_name} mode set to: {mode_raw}"),
                )
                .await;
                Ok(Some(PromptResponse::new(StopReason::EndTurn)))
            }
            "changelog" => {
                let text = match find_changelog(&self.cfg.pi_command).await {
                    Some(path) => match std::fs::read_to_string(&path) {
                        Ok(mut text) => {
                            const MAX_CHARS: usize = 20_000;
                            if text.chars().count() > MAX_CHARS {
                                text = text.chars().take(MAX_CHARS).collect();
                                text.push_str("\n\n...(truncated)...");
                            }
                            text
                        }
                        Err(e) => format!("Failed to read changelog: {e}"),
                    },
                    None => "Changelog not found (couldn't locate pi installation).".to_string(),
                };
                let _ = send_text_chunk(cx, session_id, &text).await;
                Ok(Some(PromptResponse::new(StopReason::EndTurn)))
            }
            "export" => {
                // Guard: pi's export_html reads the session JSONL file; an
                // empty/missing file makes pi throw an uncorrelated parse
                // error (no id) that would hang the request.
                let state = session.get_state().await.map_err(acp_error_from_pi)?;
                let session_file = state.session_file.clone().unwrap_or_default();
                let message_count = state.message_count;
                let file_ok = if session_file.is_empty() || message_count == 0 {
                    false
                } else {
                    let raw = std::fs::read_to_string(&session_file);
                    match raw {
                        Ok(raw) => !raw.trim().is_empty(),
                        Err(_) => false,
                    }
                };
                if !file_ok {
                    let _ = send_text_chunk(
                        cx,
                        session_id,
                        "Nothing to export yet (no session messages). Send a prompt first.",
                    )
                    .await;
                    return Ok(Some(PromptResponse::new(StopReason::EndTurn)));
                }

                let safe_session_id: String = session_id
                    .0
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();
                let output_path = session
                    .cwd()
                    .join(format!("pi-session-{safe_session_id}.html"));

                match session.export_html(&output_path.to_string_lossy()).await {
                    Ok(result_path) => {
                        if result_path.is_empty() {
                            let _ = send_text_chunk(
                                cx,
                                session_id,
                                "Export failed: no output path returned by pi.",
                            )
                            .await;
                        } else {
                            let _ = send_text_chunk(cx, session_id, "Session exported: ").await;
                            let link = ContentBlock::ResourceLink(
                                ResourceLink::new(
                                    format!("pi-session-{safe_session_id}.html"),
                                    format!("file://{result_path}"),
                                )
                                .mime_type("text/html")
                                .title("Session exported"),
                            );
                            let chunk = ContentChunk::new(link);
                            let _ = cx.send_notification(SessionNotification::new(
                                session_id.clone(),
                                SessionUpdate::AgentMessageChunk(chunk),
                            ));
                        }
                    }
                    Err(e) => {
                        let _ =
                            send_text_chunk(cx, session_id, &format!("Export failed: {e}")).await;
                    }
                }
                Ok(Some(PromptResponse::new(StopReason::EndTurn)))
            }
            "autocompact" => {
                let mode = args
                    .first()
                    .map(|s| s.to_lowercase())
                    .unwrap_or_else(|| "toggle".to_string());
                let mut enabled: Option<bool> = None;
                if matches!(mode.as_str(), "on" | "true" | "enable" | "enabled") {
                    enabled = Some(true);
                } else if matches!(mode.as_str(), "off" | "false" | "disable" | "disabled") {
                    enabled = Some(false);
                }
                let enabled = match enabled {
                    Some(v) => v,
                    None => {
                        let state = session.get_state().await.map_err(acp_error_from_pi)?;
                        !state.auto_compaction_enabled
                    }
                };
                session
                    .set_auto_compaction(enabled)
                    .await
                    .map_err(acp_error_from_pi)?;
                let _ = send_text_chunk(
                    cx,
                    session_id,
                    &format!(
                        "Auto-compaction {}.",
                        if enabled { "enabled" } else { "disabled" }
                    ),
                )
                .await;
                Ok(Some(PromptResponse::new(StopReason::EndTurn)))
            }
            _ => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// `PiImage` (translate) → pi-RPC `ImageContent` (same wire shape).
fn to_pi_images(images: Vec<crate::translate::prompt::PiImage>) -> Vec<ImageContent> {
    images
        .into_iter()
        .map(|i| ImageContent {
            data: i.data,
            mime_type: i.mime_type,
        })
        .collect()
}

/// Convert the session's mode state to the ACP `modes` field shape.
fn mode_state_to_acp(modes: &ModeState) -> SessionModeState {
    SessionModeState::new(modes.current_mode_id.clone(), modes.available_modes.clone())
}

/// The wire form of a pi queue mode (`all` / `one-at-a-time` / legacy `queue`).
fn queue_mode_str(mode: QueueMode) -> &'static str {
    match mode {
        QueueMode::All => "all",
        QueueMode::OneAtATime => "one-at-a-time",
        QueueMode::Queue => "queue",
        QueueMode::Other => "unknown",
    }
}

fn acp_stop_reason(reason: SessionStopReason) -> StopReason {
    match reason {
        SessionStopReason::EndTurn => StopReason::EndTurn,
        SessionStopReason::Cancelled => StopReason::Cancelled,
    }
}

/// Resolve a session file the same way pi does when it receives a relative
/// `--session` path while running in the session cwd.
fn resolve_session_file_path(cwd: &Path, session_file: &str) -> PathBuf {
    let path = Path::new(session_file);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn invalid_params(msg: &str) -> AcpError {
    AcpError::new(ACP_INVALID_PARAMS, msg.to_string())
}

fn auth_required() -> AcpError {
    AcpError::new(
        ACP_AUTH_REQUIRED,
        "Configure an API key or log in with an OAuth provider.",
    )
    .data(crate::error::auth_required_data())
}

/// Map an adapter/pi error onto the ACP error the client receives, promoting
/// auth-looking pi failures to `authRequired` (with the auth methods attached
/// so clients can offer terminal login).
///
/// Promotion is restricted to [`AcpxError::RpcFailed`] — the one variant that
/// carries pi/provider error text — so spawn failures (`EACCES` renders as
/// "Permission denied") and process exits are never misclassified as auth
/// problems (S8 / auth.rs keyword matching).
fn acp_error_from_pi(e: AcpxError) -> AcpError {
    if let AcpxError::RpcFailed { message, .. } = &e {
        if maybe_auth_required_error(message).is_some() {
            return auth_required();
        }
    }
    AcpError::from(e)
}

/// Build the startup prelude text: full startup info unless `quietStartup`,
/// which keeps only the update notice (TS `buildStartupInfo` + quietStartup).
///
/// The caller passes the pi version, which is fetched **asynchronously** on a
/// spawned task (design D6 — no subprocess probe adds to the session/new
/// critical path).
fn build_startup_prelude(
    cwd: &Path,
    pi_version: Option<&str>,
    quiet_startup: bool,
    update_notice: Option<&str>,
) -> String {
    if quiet_startup {
        return update_notice.map(|n| format!("{n}\n")).unwrap_or_default();
    }
    let out = build_startup_info(cwd, pi_version, update_notice);
    tracing::debug!(chars = out.len(), "startup prelude built");
    out
}

/// Advertise slash commands: pi `get_commands` first, file-based prompts as
/// the legacy fallback (TS `newSession`/`loadSession`).
async fn advertise_commands(
    cx: &ConnectionTo<Client>,
    session: &Arc<PiAcpSession>,
    enable_skill_commands: bool,
    file_commands: &[FileSlashCommand],
) {
    let session_id = session.session_id().clone();
    let from_pi = match session.get_commands().await {
        Ok(data) => commands::to_available_commands_from_pi_get_commands(
            &data,
            enable_skill_commands,
            false,
        ),
        Err(_) => Vec::new(),
    };
    let available = if from_pi.is_empty() {
        commands::merge_commands(
            &commands::to_available_commands(file_commands),
            &commands::builtin_available_commands(),
        )
    } else {
        commands::merge_commands(&from_pi, &commands::builtin_available_commands())
    };
    let _ = cx.send_notification(SessionNotification::new(
        session_id,
        SessionUpdate::AvailableCommandsUpdate(AvailableCommandsUpdate::new(available)),
    ));
}

/// Rebuild and emit the `config_option_update` notification; returns the new
/// options (also used in the `session/set_config_option` response).
/// Refresh both selectors after a model/thinking change: the config options
/// (dynamic per-model thought levels, W-478) plus the effective current mode
/// (pi clamps thinking to the model's capabilities, so Zed must follow the
/// read-back level, not the requested one).
async fn emit_config_options_update(
    cx: &ConnectionTo<Client>,
    session_id: &SessionId,
    session: &Arc<PiAcpSession>,
) -> std::result::Result<Vec<SessionConfigOption>, AcpxError> {
    let (config_options, _models, modes) = get_session_configuration(session, None, None).await;
    send_current_mode_update(cx, session_id, &modes.current_mode_id).await;
    let update = ConfigOptionUpdate::new(config_options.clone());
    cx.send_notification(SessionNotification::new(
        session_id.clone(),
        SessionUpdate::ConfigOptionUpdate(update),
    ))
    .map_err(|e| AcpxError::RpcFailed {
        command: "config_option_update".into(),
        message: e.to_string(),
    })?;
    Ok(config_options)
}

async fn send_current_mode_update(cx: &ConnectionTo<Client>, session_id: &SessionId, mode: &str) {
    let _ = cx.send_notification(SessionNotification::new(
        session_id.clone(),
        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(mode.to_string())),
    ));
}

async fn send_text_chunk(cx: &ConnectionTo<Client>, session_id: &SessionId, text: &str) {
    let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text.to_string())));
    let _ = cx.send_notification(SessionNotification::new(
        session_id.clone(),
        SessionUpdate::AgentMessageChunk(chunk),
    ));
}

/// Fetch `session_configuration`: configOptions + model/mode states.
/// Pre-fetched state/models are reused when provided (session/new).
async fn get_session_configuration(
    session: &Arc<PiAcpSession>,
    pre_state: Option<&RpcSessionState>,
    pre_models: Option<&Vec<Model>>,
) -> (Vec<SessionConfigOption>, Option<ModelState>, ModeState) {
    // Fetch all three inputs concurrently in one join (W-479): the refresh
    // path used to fetch `get_state` twice (once for the model state, once
    // for the mode state). pi has no combined state+levels endpoint, so the
    // native levels always cost one RPC.
    let (available, state, levels) = tokio::join!(
        async {
            if let Some(models) = pre_models {
                models.clone()
            } else {
                session.get_available_models().await.unwrap_or_default()
            }
        },
        async {
            if let Some(state) = pre_state {
                Some(state.clone())
            } else {
                session.get_state().await.ok()
            }
        },
        session.available_thinking_levels(),
    );
    let models = model_state_from_parts(&available, state.as_ref());
    let current = state
        .as_ref()
        .map(|s| s.thinking_level.id())
        .unwrap_or("medium");
    let modes = mode_state_from_levels(current, levels);
    let config_options = build_config_options(models.as_ref(), &modes);
    (config_options, models, modes)
}

fn model_state_from_parts(
    available: &[Model],
    state: Option<&RpcSessionState>,
) -> Option<ModelState> {
    let available_models: Vec<AdvertisedModel> = available
        .iter()
        .filter_map(|m| {
            let provider = m.provider.trim();
            let id = m.id.trim();
            if provider.is_empty() || id.is_empty() {
                return None;
            }
            Some(AdvertisedModel {
                model_id: format!("{provider}/{id}"),
                name: format!("{provider}/{}", m.name),
            })
        })
        .collect();

    let current_model_id = state.as_ref().and_then(|s| s.model.as_ref()).and_then(|m| {
        let provider = m.provider.trim();
        let id = m.id.trim();
        if provider.is_empty() || id.is_empty() {
            None
        } else {
            Some(format!("{provider}/{id}"))
        }
    });

    if available_models.is_empty() && current_model_id.is_none() {
        return None;
    }

    let current_model_id = current_model_id
        .or_else(|| available_models.first().map(|a| a.model_id.clone()))
        .unwrap_or_else(|| "default".to_string());

    Some(ModelState {
        available_models,
        current_model_id,
    })
}

/// Build a [`ModeState`] from the current level id + native level list.
/// A current level outside the list (stale pi state) is kept as the current
/// mode so the picker still reflects reality instead of silently flipping.
fn mode_state_from_levels(current: &str, levels: Vec<ThinkingLevel>) -> ModeState {
    let available_modes = levels
        .iter()
        .map(|level| {
            SessionMode::new(
                SessionModeId::new(level.id()),
                format!("Thinking: {}", level.label()),
            )
            .description(level.description().to_string())
        })
        .collect();
    ModeState {
        current_mode_id: current.to_string(),
        available_modes,
        levels,
    }
}

fn build_config_options(
    models: Option<&ModelState>,
    modes: &ModeState,
) -> Vec<SessionConfigOption> {
    let mut options = vec![thought_level_config_option(
        &modes.current_mode_id,
        &modes.levels,
    )];

    if let Some(models) = models {
        let available: Vec<(String, String)> = models
            .available_models
            .iter()
            .map(|m| (m.model_id.clone(), m.name.clone()))
            .collect();
        if let Some(model_option) = model_config_option(&models.current_model_id, &available) {
            options.insert(0, model_option);
        }
    }
    options
}

/// Build the `thought_level` config option for `current_level_id` over the
/// model's native `available` levels (shared with the session pump's
/// `thinking_level_changed` handler so the ACP thinking dropdown and the
/// mode picker always describe the same ladder).
pub(crate) fn thought_level_config_option(
    current_level_id: &str,
    available: &[ThinkingLevel],
) -> SessionConfigOption {
    let options: Vec<SessionConfigSelectOption> = available
        .iter()
        .map(|level| {
            SessionConfigSelectOption::new(level.id().to_string(), level.label().to_string())
                .description(level.description().to_string())
        })
        .collect();
    SessionConfigOption::select(
        THOUGHT_LEVEL_CONFIG_ID,
        "Thinking",
        current_level_id.to_string(),
        options,
    )
    .description("Set the reasoning effort for this session")
    .category(SessionConfigOptionCategory::ThoughtLevel)
}

/// Build the `model` config option (`None` when no models are advertised).
pub(crate) fn model_config_option(
    current_model_id: &str,
    available: &[(String, String)],
) -> Option<SessionConfigOption> {
    if available.is_empty() {
        return None;
    }
    let options: Vec<SessionConfigSelectOption> = available
        .iter()
        .map(|(id, name)| SessionConfigSelectOption::new(id.clone(), name.clone()))
        .collect();
    Some(
        SessionConfigOption::select(
            MODEL_CONFIG_ID,
            "Model",
            current_model_id.to_string(),
            options,
        )
        .description("Select the model for this session")
        .category(SessionConfigOptionCategory::Model),
    )
}

/// Resolve `provider/model` (or bare `model` via the available-model list) and
/// apply it (TS `setSessionModel`).
async fn set_session_model(
    session: &Arc<PiAcpSession>,
    requested_model_id: &str,
) -> std::result::Result<(), AcpxError> {
    let (mut provider, mut model_id) = match requested_model_id.split_once('/') {
        Some((p, rest)) => (Some(p.to_string()), rest.to_string()),
        None => (None, requested_model_id.to_string()),
    };

    if provider.is_none() {
        let models = session.get_available_models().await?;
        if let Some(found) = models.iter().find(|m| m.id == model_id) {
            provider = Some(found.provider.clone());
            model_id = found.id.clone();
        }
    }

    match (provider, model_id.is_empty()) {
        (Some(p), false) => {
            session.set_model(&p, &model_id).await?;
            Ok(())
        }
        _ => Err(AcpxError::RpcFailed {
            command: "set_model".into(),
            message: format!("Unknown modelId: {requested_model_id}"),
        }),
    }
}

/// Replay a session's message history as ACP notifications (TS `loadSession`).
async fn replay_history(cx: &ConnectionTo<Client>, session: &Arc<PiAcpSession>, data: &Value) {
    let session_id = session.session_id().clone();
    let cwd = session.cwd().to_string_lossy().to_string();
    let messages = data
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut synthetic_id: usize = 0;
    for m in &messages {
        let Some(replay) = replay_message(m) else {
            continue;
        };
        match replay {
            ReplayMessage::UserText(text) => {
                let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text)));
                let _ = cx.send_notification(SessionNotification::new(
                    session_id.clone(),
                    SessionUpdate::UserMessageChunk(chunk),
                ));
            }
            ReplayMessage::AssistantText(text) => {
                let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text)));
                let _ = cx.send_notification(SessionNotification::new(
                    session_id.clone(),
                    SessionUpdate::AgentMessageChunk(chunk),
                ));
            }
            ReplayMessage::ToolResult(t) => {
                let tool_call_id = t.tool_call_id.unwrap_or_else(|| {
                    synthetic_id += 1;
                    format!("pi-replay-{synthetic_id}")
                });
                if t.is_bash {
                    let call = ToolCall::new(tool_call_id.clone(), t.title.clone())
                        .kind(ToolKind::Execute)
                        .status(ToolCallStatus::Completed)
                        .content(bash_terminal_content(&tool_call_id))
                        .meta(bash_terminal_info_meta(&tool_call_id, &cwd));
                    let _ = cx.send_notification(SessionNotification::new(
                        session_id.clone(),
                        SessionUpdate::ToolCall(call),
                    ));
                    let mut meta = serde_json::Map::new();
                    if !t.text.is_empty() {
                        meta.extend(bash_terminal_output_meta(&tool_call_id, &t.text));
                    }
                    meta.extend(bash_terminal_exit_meta(
                        &tool_call_id,
                        bash_exit_code(&t.raw, t.is_error),
                    ));
                    let fields = ToolCallUpdateFields::new().status(Some(if t.is_error {
                        ToolCallStatus::Failed
                    } else {
                        ToolCallStatus::Completed
                    }));
                    let update = ToolCallUpdate::new(tool_call_id.clone(), fields).meta(meta);
                    let _ = cx.send_notification(SessionNotification::new(
                        session_id.clone(),
                        SessionUpdate::ToolCallUpdate(update),
                    ));
                } else {
                    let call = ToolCall::new(tool_call_id.clone(), t.title.clone())
                        .kind(to_tool_kind(&t.tool_name))
                        .status(ToolCallStatus::Completed)
                        .raw_input(Value::Null)
                        .raw_output(t.raw.clone());
                    let _ = cx.send_notification(SessionNotification::new(
                        session_id.clone(),
                        SessionUpdate::ToolCall(call),
                    ));
                    let text = tool_result_to_text(&t.raw);
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
                        .status(Some(if t.is_error {
                            ToolCallStatus::Failed
                        } else {
                            ToolCallStatus::Completed
                        }))
                        .content(content)
                        .raw_output(t.raw.clone());
                    let _ = cx.send_notification(SessionNotification::new(
                        session_id.clone(),
                        SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(tool_call_id, fields)),
                    ));
                }
            }
        }
    }
}

/// Locate pi's installed `CHANGELOG.md` (TS `findChangelog`): resolve the
/// configured executable and walk its ancestors, else query the npm global
/// root when the configuration uses the default bare `pi` command.
async fn find_changelog(pi_command: &str) -> Option<PathBuf> {
    let resolved = crate::pi::resolve::resolve_current_env(pi_command);
    if let Some(path) = changelog_near_executable(&changelog_executable(&resolved)) {
        return Some(path);
    }

    // An explicit command path is authoritative. Searching npm in that case
    // can find an unrelated global installation and needlessly spawns a child
    // process (which is especially costly for nested ACP fixtures).
    if !is_bare_pi_command(pi_command) {
        return None;
    }

    // Fallback: npm global root. Bound the probe and make cancellation kill the
    // child so a slow/broken npm cannot leak into the next ACP operation.
    let npm_root = tokio::time::timeout(
        std::time::Duration::from_millis(1500),
        tokio::process::Command::new("npm")
            .args(["root", "-g"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    let root = String::from_utf8_lossy(&npm_root.stdout).trim().to_string();
    if root.is_empty() {
        return None;
    }
    let p = PathBuf::from(root)
        .join("@earendil-works")
        .join("pi-coding-agent")
        .join("CHANGELOG.md");
    p.exists().then_some(p)
}

/// Return whether `pi_command` is the default-style bare command name rather
/// than an explicit path or wrapper filename.
fn is_bare_pi_command(pi_command: &str) -> bool {
    let command = pi_command.trim();
    !command.is_empty()
        && !command.contains('/')
        && !command.contains('\\')
        && Path::new(command).extension().is_none()
}

/// Extract the actual pi executable from a resolved launch command. On Windows
/// a `.cmd`/`.bat` wrapper is launched through `cmd.exe`, so the fourth command
/// argument carries the path that should be searched for its package root.
fn changelog_executable(resolved: &crate::pi::resolve::ResolvedPi) -> PathBuf {
    if cfg!(windows) && resolved.program.eq_ignore_ascii_case("cmd.exe") {
        if let Some(path) = resolved.cmd_args.get(3) {
            return PathBuf::from(path.trim_matches('"'));
        }
    }
    PathBuf::from(&resolved.program)
}

/// Find the nearest changelog above an executable, resolving symlinks first so
/// npm's `bin/pi` link lands in the installed package directory.
fn changelog_near_executable(executable: &Path) -> Option<PathBuf> {
    let resolved = std::fs::canonicalize(executable).ok()?;
    resolved
        .ancestors()
        .map(|ancestor| ancestor.join("CHANGELOG.md"))
        .find(|path| path.is_file())
}

/// Derive a provisional thread title from the first user prompt (fixes
/// #102/#24: without a title, Zed's sidebar keeps "New Agent Thread").
/// Whitespace runs collapse to a single space and the result is truncated to
/// 80 chars (the TS `pickFallbackTitleFromHead` limit). `None` for
/// empty/whitespace-only prompts.
fn provisional_title_from_prompt(message: &str) -> Option<String> {
    let mut out = String::with_capacity(message.len());
    let mut pending_space = false;
    for ch in message.chars() {
        if ch.is_whitespace() {
            pending_space = !out.is_empty();
        } else if pending_space {
            out.push(' ');
            out.push(ch);
            pending_space = false;
        } else {
            out.push(ch);
        }
    }
    if out.is_empty() {
        return None;
    }
    if out.chars().count() > 80 {
        out = out.chars().take(80).collect();
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::provisional_title_from_prompt;
    use super::*;
    use tempfile::TempDir;

    /// ACP `internalError` code (mapping asserted in the S8 tests).
    const ACP_INTERNAL_ERROR: i32 = -32603;

    #[test]
    fn acp_error_from_pi_promotes_auth_looking_rpc_failures() {
        let err = acp_error_from_pi(AcpxError::RpcFailed {
            command: "prompt".into(),
            message: "unauthorized: 401 missing api key".into(),
        });
        assert_eq!(err.code, ACP_AUTH_REQUIRED.into());
        let data = err.data.as_ref().expect("authRequired data");
        let methods = data["authMethods"].as_array().expect("authMethods");
        assert_eq!(methods.len(), 1);
        assert_eq!(methods[0]["id"], "pi_terminal_login");
    }

    #[test]
    fn acp_error_from_pi_does_not_promote_spawn_or_exit_errors() {
        // EACCES renders as "Permission denied" — must stay internal, never
        // misclassified as an auth problem (S8).
        let spawn = acp_error_from_pi(AcpxError::PiSpawn(
            "pi: Permission denied (os error 13)".into(),
        ));
        assert_eq!(spawn.code, ACP_INTERNAL_ERROR.into());
        assert!(
            !spawn.message.contains("Configure an API key"),
            "{}",
            spawn.message
        );

        let exited = acp_error_from_pi(AcpxError::PiExited {
            code: Some(42),
            signal: None,
        });
        assert_eq!(exited.code, ACP_INTERNAL_ERROR.into());
        let data = exited.data.as_ref().expect("error data");
        assert_eq!(data["errorType"], "piExited");
        assert_eq!(data["code"], 42);
    }

    #[test]
    fn acp_error_from_pi_leaves_non_auth_failures_internal() {
        let err = acp_error_from_pi(AcpxError::RpcFailed {
            command: "get_state".into(),
            message: "something else entirely".into(),
        });
        assert_eq!(err.code, ACP_INTERNAL_ERROR.into());
        assert!(
            err.message.contains("something else entirely"),
            "{}",
            err.message
        );
    }

    #[test]
    fn provisional_title_plain_message() {
        assert_eq!(
            provisional_title_from_prompt("fix the login bug"),
            Some("fix the login bug".to_string())
        );
    }

    #[test]
    fn provisional_title_collapses_whitespace() {
        assert_eq!(
            provisional_title_from_prompt("  fix  the\nlogin\tbug  "),
            Some("fix the login bug".to_string())
        );
    }

    #[test]
    fn provisional_title_truncates_at_80_chars() {
        let long = "x".repeat(200);
        let title = provisional_title_from_prompt(&long).unwrap();
        assert_eq!(title.chars().count(), 80);
        assert!(title.chars().all(|c| c == 'x'));
    }

    #[test]
    fn provisional_title_empty_and_whitespace_only() {
        assert_eq!(provisional_title_from_prompt(""), None);
        assert_eq!(provisional_title_from_prompt("   \n\t "), None);
    }

    #[test]
    fn changelog_lookup_walks_up_from_resolved_executable() {
        let tmp = TempDir::new().unwrap();
        let package = tmp.path().join("node_modules/pi-coding-agent");
        let bin = package.join("bin/pi");
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, "mock executable").unwrap();
        let changelog = package.join("CHANGELOG.md");
        std::fs::write(&changelog, "changes").unwrap();

        assert_eq!(
            changelog_near_executable(&bin),
            Some(std::fs::canonicalize(changelog).unwrap())
        );
    }

    #[test]
    fn explicit_pi_paths_do_not_use_global_fallback() {
        assert!(!is_bare_pi_command("/opt/pi/bin/pi"));
        assert!(!is_bare_pi_command("C:\\tools\\pi.cmd"));
        assert!(is_bare_pi_command("pi"));
    }
}
