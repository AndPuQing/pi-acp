//! pi RPC protocol types (serde) — built in S3 (W-450).
//!
//! pi `--mode rpc` speaks strict JSONL over stdio:
//!
//! - **Commands** (client → pi): one JSON object per line, discriminated by
//!   `type`. Every command carries an `id` that pi echoes back on its response.
//! - **Responses** (pi → client): `{ "type": "response", "id", "command",
//!   "success", "data"?, "error"? }` — matched to the pending command by `id`.
//! - **Events** (pi → client): everything else. Modeled here as an `enum` so
//!   the session event pump (S5) is forced to handle every event type at
//!   compile time. Unknown/unparseable lines are captured in [`RpcEvent::Unknown`]
//!   so the stream survives pi protocol evolution.
//!
//! Command set matches the TS reference (`pi-rpc/process.ts`) and design §6.1:
//! `prompt` / `abort` / `get_state` / `get_available_models` / `set_model` /
//! `set_thinking_level` / `set_{follow_up,steering}_mode` / `compact` /
//! `set_auto_compaction` / `get_session_stats` / `set_session_name` /
//! `export_html` / `switch_session` / `get_messages` / `get_commands`, plus the
//! id-less `extension_ui_response` (matched by pi against its pending extension
//! UI request, not by command id).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::AcpxError;

/// Streaming behavior for `prompt` in newer pi versions. Not sent by pi-acp yet
/// (the TS reference doesn't either); kept off the wire via `skip_serializing_if`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StreamingBehavior {
    Steer,
    FollowUp,
}

/// A single image attachment for `prompt` (pi-ai `ImageContent`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageContent {
    /// Base64-encoded image bytes.
    pub data: String,
    /// MIME type, e.g. `image/png`.
    pub mime_type: String,
}

/// pi thinking levels (`ThinkingLevel` from pi-agent-core).
///
/// Single source of truth for the thinking selector: wire ids, display
/// labels and descriptions all live here so the ACP `modes` list, the
/// `thought_level` config option and the session event pump cannot drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingLevel {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    #[serde(rename = "xhigh")]
    XHigh,
    Max,
}

impl ThinkingLevel {
    /// All levels in ascending reasoning order (the `pi --thinking` ladder).
    pub fn all() -> [ThinkingLevel; 7] {
        [
            ThinkingLevel::Off,
            ThinkingLevel::Minimal,
            ThinkingLevel::Low,
            ThinkingLevel::Medium,
            ThinkingLevel::High,
            ThinkingLevel::XHigh,
            ThinkingLevel::Max,
        ]
    }

    /// Wire / mode id (`off` .. `max`).
    pub fn id(self) -> &'static str {
        match self {
            ThinkingLevel::Off => "off",
            ThinkingLevel::Minimal => "minimal",
            ThinkingLevel::Low => "low",
            ThinkingLevel::Medium => "medium",
            ThinkingLevel::High => "high",
            ThinkingLevel::XHigh => "xhigh",
            ThinkingLevel::Max => "max",
        }
    }

    /// Short display label for selectors (Zed renders the raw id otherwise).
    pub fn label(self) -> &'static str {
        match self {
            ThinkingLevel::Off => "Off",
            ThinkingLevel::Minimal => "Minimal",
            ThinkingLevel::Low => "Low",
            ThinkingLevel::Medium => "Medium",
            ThinkingLevel::High => "High",
            ThinkingLevel::XHigh => "Extra high",
            ThinkingLevel::Max => "Max",
        }
    }

    /// One-line description for selectors, using pi's native thinking token
    /// budgets (`DEFAULT_THINKING_BUDGETS` in pi-ai: minimal 1k / low 2k /
    /// medium 8k / high 16k; `xhigh`/`max` are model-specific and only
    /// honored by selected model families) so Zed shows the same ladder pi
    /// itself documents instead of invented static strings.
    pub fn description(self) -> &'static str {
        match self {
            ThinkingLevel::Off => "No reasoning",
            ThinkingLevel::Minimal => "Minimal reasoning (~1k tokens)",
            ThinkingLevel::Low => "Light reasoning (~2k tokens)",
            ThinkingLevel::Medium => "Moderate reasoning (~8k tokens)",
            ThinkingLevel::High => "Deep reasoning (~16k tokens)",
            ThinkingLevel::XHigh => "Extra-high reasoning (~32k tokens where supported)",
            ThinkingLevel::Max => "Maximum reasoning (model-specific maximum)",
        }
    }

    /// Parse a wire / mode / config value id. `None` for unknown levels.
    pub fn parse(s: &str) -> Option<ThinkingLevel> {
        match s {
            "off" => Some(ThinkingLevel::Off),
            "minimal" => Some(ThinkingLevel::Minimal),
            "low" => Some(ThinkingLevel::Low),
            "medium" => Some(ThinkingLevel::Medium),
            "high" => Some(ThinkingLevel::High),
            "xhigh" => Some(ThinkingLevel::XHigh),
            "max" => Some(ThinkingLevel::Max),
            _ => None,
        }
    }
}

/// Queue drain mode for follow-up / steering messages.
///
/// pi's own `.d.ts` declares only `all | one-at-a-time`, but real pi also emits
/// `queue` (a legacy `queueMode` setting migrated into `steeringMode` — see
/// pi's settings-manager). `Other` keeps the state parseable if pi adds more
/// values in the future.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueMode {
    All,
    #[serde(rename = "one-at-a-time")]
    OneAtATime,
    /// Legacy/settings-driven value real pi emits (migrated `queueMode`).
    Queue,
    /// Any future pi value; the state still parses.
    #[serde(other)]
    Other,
}

/// Reason a compaction was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}

/// A pi RPC command (client → pi). Serialized as `{ "type": <variant>, ... }`;
/// the transport (`pi::process`) injects the per-request `id` before writing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RpcCommand {
    /// Start a turn. pi replies with `success: true` **early** (right after
    /// preflight) — the turn really ends at the `agent_settled` event (S2
    /// constraint 2), so consumers must pump events, not just await this.
    Prompt {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        images: Option<Vec<ImageContent>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        streaming_behavior: Option<StreamingBehavior>,
    },
    Abort,
    GetState,
    GetAvailableModels,
    SetModel {
        provider: String,
        #[serde(rename = "modelId")]
        model_id: String,
    },
    /// Native per-model thinking levels (`AgentSession.getAvailableThinkingLevels`).
    /// Answers `{ "levels": ["off", "low", ...] }` — the single source of
    /// truth for the ACP thinking selector (W-478).
    GetAvailableThinkingLevels,
    SetThinkingLevel {
        level: ThinkingLevel,
    },
    SetFollowUpMode {
        mode: QueueMode,
    },
    SetSteeringMode {
        mode: QueueMode,
    },
    Compact {
        #[serde(rename = "customInstructions", skip_serializing_if = "Option::is_none")]
        custom_instructions: Option<String>,
    },
    SetAutoCompaction {
        enabled: bool,
    },
    GetSessionStats,
    SetSessionName {
        name: String,
    },
    ExportHtml {
        #[serde(rename = "outputPath", skip_serializing_if = "Option::is_none")]
        output_path: Option<String>,
    },
    SwitchSession {
        #[serde(rename = "sessionPath")]
        session_path: String,
    },
    GetMessages,
    GetCommands,
}

impl RpcCommand {
    /// The wire `type` string for this command (used in `AcpxError::RpcTimeout`
    /// diagnostics and in the mock server).
    pub fn name(&self) -> &'static str {
        match self {
            RpcCommand::Prompt { .. } => "prompt",
            RpcCommand::Abort => "abort",
            RpcCommand::GetState => "get_state",
            RpcCommand::GetAvailableModels => "get_available_models",
            RpcCommand::GetAvailableThinkingLevels => "get_available_thinking_levels",
            RpcCommand::SetModel { .. } => "set_model",
            RpcCommand::SetThinkingLevel { .. } => "set_thinking_level",
            RpcCommand::SetFollowUpMode { .. } => "set_follow_up_mode",
            RpcCommand::SetSteeringMode { .. } => "set_steering_mode",
            RpcCommand::Compact { .. } => "compact",
            RpcCommand::SetAutoCompaction { .. } => "set_auto_compaction",
            RpcCommand::GetSessionStats => "get_session_stats",
            RpcCommand::SetSessionName { .. } => "set_session_name",
            RpcCommand::ExportHtml { .. } => "export_html",
            RpcCommand::SwitchSession { .. } => "switch_session",
            RpcCommand::GetMessages => "get_messages",
            RpcCommand::GetCommands => "get_commands",
        }
    }
}

/// A pi RPC response line (pi → client).
///
/// Modeled as a struct rather than an enum because `success: false` responses
/// replace `data` with `error` regardless of command; typed per-command data
/// extraction happens in `pi::process` convenience methods (e.g. [`RpcSessionState`]).
#[derive(Debug, Clone, Deserialize)]
pub struct RpcResponse {
    /// Request id echoed from the command; absent on pi's own parse errors.
    #[serde(default)]
    pub id: Option<String>,
    /// Always `"response"` on the wire.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// The command this answers (matches the request `type`).
    pub command: String,
    pub success: bool,
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
}

impl RpcResponse {
    /// Require `success: true` and return `data` (or `Null` when absent).
    /// Maps pi's `success: false` onto [`AcpxError::RpcFailed`] so failures
    /// reach the ACP client instead of being swallowed (#98/#92/#43).
    pub fn ok(self) -> Result<Value, AcpxError> {
        if self.success {
            Ok(self.data.unwrap_or(Value::Null))
        } else {
            Err(AcpxError::RpcFailed {
                command: self.command,
                message: self.error.unwrap_or_else(|| "no error message".to_string()),
            })
        }
    }
}

/// pi's `get_state` payload (`RpcSessionState`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSessionState {
    #[serde(default)]
    pub model: Option<Model>,
    pub thinking_level: ThinkingLevel,
    pub is_streaming: bool,
    pub is_compacting: bool,
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
    #[serde(default)]
    pub session_file: Option<String>,
    pub session_id: String,
    #[serde(default)]
    pub session_name: Option<String>,
    pub auto_compaction_enabled: bool,
    pub message_count: u64,
    pub pending_message_count: u64,
}

/// A pi model entry (`Model` from pi-ai). Only the stable fields pi-acp consumes
/// are typed; the rest (cost tables, compat overrides) stay untyped for now.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    pub id: String,
    pub name: String,
    pub provider: String,
    #[serde(default)]
    pub reasoning: bool,
    /// Per-level provider mapping (`thinkingLevelMap` in pi-ai): a `None`
    /// value disables the level (`mapped === null`), a missing key means the
    /// model has no opinion (default ladder applies, except `xhigh`/`max`
    /// which need an explicit entry). Drives the per-model thinking selector.
    #[serde(default)]
    pub thinking_level_map: Option<HashMap<String, Option<String>>>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub input: Option<Vec<String>>,
}

/// Levels the given model supports, in ascending reasoning order.
///
/// Ports pi-ai's `getSupportedThinkingLevels` (+ the `AgentSession` fallback
/// of the full ladder when no model is known) so the ACP thinking selector
/// shows pi-native per-model levels instead of a static list:
/// - a model without reasoning supports `off` only;
/// - a `null` map entry disables its level;
/// - `xhigh`/`max` additionally require an explicit (non-`null`) entry.
///
/// `None` (no model known yet) yields the full ladder, matching pi's
/// `getAvailableThinkingLevels` fallback.
pub fn supported_thinking_levels(model: Option<&Model>) -> Vec<ThinkingLevel> {
    let Some(model) = model else {
        return ThinkingLevel::all().to_vec();
    };
    if !model.reasoning {
        return vec![ThinkingLevel::Off];
    }
    ThinkingLevel::all()
        .into_iter()
        .filter(|level| {
            match model
                .thinking_level_map
                .as_ref()
                .and_then(|m| m.get(level.id()))
            {
                // Explicit `null`: the model disables this level.
                Some(None) => false,
                // `xhigh`/`max` need an explicit entry; without one the model
                // has no mapping for them.
                None if matches!(level, ThinkingLevel::XHigh | ThinkingLevel::Max) => false,
                _ => true,
            }
        })
        .collect()
}

/// Clamp a requested level to what the model supports (ports pi-ai's
/// `clampThinkingLevel`: exact hit wins, otherwise the nearest *higher*
/// supported level, else the nearest lower one). `None` model clamps to
/// `off`, matching pi's session fallback when no model is known.
pub fn clamp_thinking_level(model: Option<&Model>, level: ThinkingLevel) -> ThinkingLevel {
    let available = supported_thinking_levels(model);
    if available.contains(&level) {
        return level;
    }
    let order = ThinkingLevel::all();
    let requested = order.iter().position(|l| *l == level).unwrap_or(0);
    // Nearest higher first (pi tries upward before falling back down).
    if let Some(up) = order
        .iter()
        .skip(requested + 1)
        .find(|l| available.contains(l))
    {
        return *up;
    }
    if let Some(down) = order[..requested]
        .iter()
        .rev()
        .find(|l| available.contains(l))
    {
        return *down;
    }
    available.into_iter().next().unwrap_or(ThinkingLevel::Off)
}

/// Token/cost accounting carried by pi assistant messages (`Usage` from
/// pi-ai). Streaming `message_update` snapshots may be empty; the final
/// `message_end` message carries the authoritative values.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    #[serde(default)]
    pub input: u64,
    #[serde(default)]
    pub output: u64,
    #[serde(default)]
    pub cache_read: u64,
    #[serde(default)]
    pub cache_write: u64,
    /// Subset of `cacheWrite` with 1h retention (Anthropic only).
    #[serde(default)]
    pub cache_write_1h: Option<u64>,
    /// Reasoning/thinking tokens when the provider reports them (subset of output).
    #[serde(default)]
    pub reasoning: Option<u64>,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cost: Option<Cost>,
}

/// USD cost breakdown inside [`Usage`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cost {
    #[serde(default)]
    pub input: f64,
    #[serde(default)]
    pub output: f64,
    #[serde(default)]
    pub cache_read: f64,
    #[serde(default)]
    pub cache_write: f64,
    #[serde(default)]
    pub total: f64,
}

/// One entry of `get_commands` (`RpcSlashCommand`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcSlashCommand {
    /// Command name without the leading slash.
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// `extension` | `prompt` | `skill`.
    pub source: String,
    #[serde(default)]
    pub source_info: Option<Value>,
}

/// A pi session event (pi → client), discriminated by `type`.
///
/// The event pump in S5 matches this enum exhaustively; adding a new pi event
/// type is a compile error until a variant (and its handling) exists.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RpcEvent {
    // --- core agent loop events (pi-agent-core `AgentEvent`) ---
    AgentStart,
    AgentEnd {
        messages: Vec<Value>,
        #[serde(rename = "willRetry", default)]
        will_retry: bool,
    },
    TurnStart,
    TurnEnd {
        message: Value,
        #[serde(rename = "toolResults", default)]
        tool_results: Vec<Value>,
    },
    MessageStart {
        message: Value,
    },
    MessageUpdate {
        // Older pi-compatible producers and the original adapter's test
        // doubles omit usage. It is accounting-only and must not prevent the
        // assistant delta from reaching the session pump.
        #[serde(default)]
        usage: Usage,
        #[serde(rename = "assistantMessageEvent")]
        assistant_message_event: AssistantMessageEvent,
    },
    MessageEnd {
        message: Value,
    },
    ToolExecutionStart {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: Value,
    },
    ToolExecutionUpdate {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: Value,
        #[serde(rename = "partialResult")]
        partial_result: Value,
    },
    ToolExecutionEnd {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        result: Value,
        #[serde(rename = "isError")]
        is_error: bool,
    },
    // --- session-level events (pi-agent-core `AgentSessionEvent`) ---
    /// The turn is truly over. This is the completion signal pi-acp waits for —
    /// **not** the (early) `prompt` response (S2 constraint 2).
    AgentSettled,
    QueueUpdate {
        steering: Vec<String>,
        #[serde(rename = "followUp")]
        follow_up: Vec<String>,
    },
    CompactionStart {
        reason: CompactionReason,
    },
    EntryAppended {
        entry: Value,
    },
    SessionInfoChanged {
        #[serde(default)]
        name: Option<String>,
    },
    ThinkingLevelChanged {
        level: ThinkingLevel,
    },
    CompactionEnd {
        reason: CompactionReason,
        #[serde(default)]
        result: Option<Value>,
        aborted: bool,
        #[serde(rename = "willRetry")]
        will_retry: bool,
        #[serde(rename = "errorMessage", default)]
        error_message: Option<String>,
    },
    AutoRetryStart {
        attempt: u32,
        #[serde(rename = "maxAttempts")]
        max_attempts: u32,
        #[serde(rename = "delayMs")]
        delay_ms: u64,
        #[serde(rename = "errorMessage")]
        error_message: String,
    },
    AutoRetryEnd {
        success: bool,
        attempt: u32,
        #[serde(rename = "finalError", default)]
        final_error: Option<String>,
    },
    SummarizationRetryScheduled {
        attempt: u32,
        #[serde(rename = "maxAttempts")]
        max_attempts: u32,
        #[serde(rename = "delayMs")]
        delay_ms: u64,
        #[serde(rename = "errorMessage")]
        error_message: String,
    },
    SummarizationRetryAttemptStart {
        source: String,
        #[serde(default)]
        reason: Option<CompactionReason>,
    },
    SummarizationRetryFinished,
    BashExecutionUpdate {
        #[serde(default)]
        id: Option<String>,
        delta: String,
    },
    // --- extension UI bridge ---
    /// A pi extension requests user interaction (select / confirm / input /
    /// editor / notify / status / widget / title). S5 bridges `select`/`confirm`
    /// to ACP `session/request_permission`; answers go back via
    /// `extension_ui_response`.
    #[serde(rename = "extension_ui_request")]
    ExtensionUiRequest {
        #[serde(flatten)]
        inner: ExtensionUiRequest,
    },
    ExtensionError {
        #[serde(rename = "extensionPath")]
        extension_path: String,
        event: String,
        error: String,
    },
    // --- robustness escapes ---
    /// A `response` line whose id matched no pending request (a late response
    /// after a timeout, or an unsolicited one). Per TS parity these are
    /// dispatched to the event stream rather than dropped.
    UnmatchedResponse {
        raw: Value,
    },
    /// A JSON line that parsed but is not a known event shape (new pi version).
    /// Kept so the stream survives protocol evolution; consumers should log and
    /// ignore.
    Unknown {
        raw: Value,
    },
}

/// Streaming assistant-message sub-event inside `message_update`
/// (`AssistantMessageEvent` from pi-ai, wire form: `partial` stripped,
/// `toolcall_start` gains `id` + `toolName` — see pi's `toJsonEvent`).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantMessageEvent {
    Start {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
    },
    TextStart {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
    },
    /// Streaming assistant text arrives here, chunk by chunk (S2 constraint 3).
    TextDelta {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
        delta: String,
    },
    TextEnd {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
        content: String,
    },
    ThinkingStart {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
    },
    ThinkingDelta {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
        delta: String,
    },
    ThinkingEnd {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
        content: String,
    },
    ToolcallStart {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
        id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
    },
    ToolcallDelta {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
        delta: String,
    },
    ToolcallEnd {
        #[serde(rename = "contentIndex", default)]
        content_index: u32,
        #[serde(rename = "toolCall")]
        tool_call: Value,
    },
    Done {
        reason: String,
        message: Value,
    },
    Error {
        reason: String,
        error: Value,
    },
}

impl AssistantMessageEvent {
    /// The streaming assistant text delta, if this is a `text_delta` event.
    pub fn text_delta(&self) -> Option<&str> {
        match self {
            AssistantMessageEvent::TextDelta { delta, .. } => Some(delta),
            _ => None,
        }
    }

    /// The message content index this sub-event targets.
    pub fn content_index(&self) -> u32 {
        match self {
            AssistantMessageEvent::Start { content_index }
            | AssistantMessageEvent::TextStart { content_index }
            | AssistantMessageEvent::TextDelta { content_index, .. }
            | AssistantMessageEvent::TextEnd { content_index, .. }
            | AssistantMessageEvent::ThinkingStart { content_index }
            | AssistantMessageEvent::ThinkingDelta { content_index, .. }
            | AssistantMessageEvent::ThinkingEnd { content_index, .. }
            | AssistantMessageEvent::ToolcallStart { content_index, .. }
            | AssistantMessageEvent::ToolcallDelta { content_index, .. }
            | AssistantMessageEvent::ToolcallEnd { content_index, .. } => *content_index,
            AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. } => 0,
        }
    }
}

/// An extension UI request (`RpcExtensionUIRequest`), discriminated by `method`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum ExtensionUiRequest {
    Select {
        id: String,
        title: String,
        options: Vec<String>,
        #[serde(default)]
        timeout: Option<u64>,
    },
    Confirm {
        id: String,
        title: String,
        message: String,
        #[serde(default)]
        timeout: Option<u64>,
    },
    Input {
        id: String,
        title: String,
        #[serde(default)]
        placeholder: Option<String>,
        #[serde(default)]
        timeout: Option<u64>,
    },
    Editor {
        id: String,
        title: String,
        #[serde(default)]
        prefill: Option<String>,
    },
    Notify {
        id: String,
        message: String,
        #[serde(rename = "notifyType", default)]
        notify_type: Option<String>,
    },
    #[serde(rename = "setStatus")]
    SetStatus {
        id: String,
        #[serde(rename = "statusKey")]
        status_key: String,
        #[serde(rename = "statusText", default)]
        status_text: Option<String>,
    },
    #[serde(rename = "setWidget")]
    SetWidget {
        id: String,
        #[serde(rename = "widgetKey")]
        widget_key: String,
        #[serde(rename = "widgetLines", default)]
        widget_lines: Option<Vec<String>>,
        #[serde(rename = "widgetPlacement", default)]
        widget_placement: Option<String>,
    },
    #[serde(rename = "setTitle")]
    SetTitle { id: String, title: String },
    #[serde(rename = "set_editor_text")]
    SetEditorText { id: String, text: String },
}

/// Answer to an [`ExtensionUiRequest`], sent back to pi via
/// `extension_ui_response` (matched by pi against its pending request by `id`).
/// The transport injects `type: "extension_ui_response"`.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ExtensionUiResponse {
    Value { id: String, value: String },
    Confirmed { id: String, confirmed: bool },
    Cancelled { id: String, cancelled: bool },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_event(s: &str) -> RpcEvent {
        serde_json::from_str::<RpcEvent>(s).unwrap()
    }

    #[test]
    fn parses_core_agent_events() {
        let settled = parse_event(r#"{"type":"agent_settled"}"#);
        assert!(matches!(settled, RpcEvent::AgentSettled));

        let start = parse_event(r#"{"type":"turn_start"}"#);
        assert!(matches!(start, RpcEvent::TurnStart));

        let end = parse_event(
            r#"{"type":"agent_end","messages":[{"role":"assistant"}],"willRetry":false}"#,
        );
        match end {
            RpcEvent::AgentEnd {
                messages,
                will_retry,
            } => {
                assert_eq!(messages.len(), 1);
                assert!(!will_retry);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_message_update_with_text_delta() {
        let ev = parse_event(
            r#"{"type":"message_update","usage":{},"assistantMessageEvent":{"type":"text_delta","contentIndex":1,"delta":"pong"}}"#,
        );
        match ev {
            RpcEvent::MessageUpdate {
                usage,
                assistant_message_event,
            } => {
                assert_eq!(usage.total_tokens, 0);
                assert_eq!(assistant_message_event.text_delta(), Some("pong"));
                assert_eq!(assistant_message_event.content_index(), 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_deltas_without_usage_or_content_index() {
        let ev = parse_event(
            r#"{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","delta":"thinking..."}}"#,
        );
        match ev {
            RpcEvent::MessageUpdate {
                usage,
                assistant_message_event,
            } => {
                assert_eq!(usage.total_tokens, 0);
                assert_eq!(assistant_message_event.content_index(), 0);
                assert!(matches!(
                    assistant_message_event,
                    AssistantMessageEvent::ThinkingDelta { delta, .. } if delta == "thinking..."
                ));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn thinking_levels_cover_pi_cli_ladder() {
        // pi `--thinking` accepts off/minimal/low/medium/high/xhigh/max;
        // every id must parse, round-trip on the wire, and carry selector
        // metadata (labels/descriptions Zed renders instead of raw ids).
        let ids: Vec<&str> = ThinkingLevel::all().iter().map(|l| l.id()).collect();
        assert_eq!(
            ids,
            vec!["off", "minimal", "low", "medium", "high", "xhigh", "max"]
        );
        for level in ThinkingLevel::all() {
            assert_eq!(ThinkingLevel::parse(level.id()), Some(level));
            let wire = serde_json::to_value(level).unwrap();
            assert_eq!(wire, serde_json::Value::String(level.id().to_string()));
            let back: ThinkingLevel = serde_json::from_value(wire).unwrap();
            assert_eq!(back, level);
            assert!(!level.label().is_empty());
            assert!(!level.description().is_empty());
        }
        assert_eq!(ThinkingLevel::parse("turbo"), None);
        // A pi running at `max` must deserialize in `get_state` (previously
        // the missing variant failed the whole state parse).
        let state: RpcSessionState = serde_json::from_str(
            r#"{
                "thinkingLevel":"max",
                "isStreaming":false,
                "isCompacting":false,
                "steeringMode":"all",
                "followUpMode":"all",
                "sessionId":"abc",
                "autoCompactionEnabled":false,
                "messageCount":0,
                "pendingMessageCount":0
            }"#,
        )
        .unwrap();
        assert_eq!(state.thinking_level, ThinkingLevel::Max);
    }

    fn model_with_map(reasoning: bool, map: &[(&str, Option<&str>)]) -> Model {
        let thinking_level_map = Some(
            map.iter()
                .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
                .collect(),
        );
        Model {
            id: "m".into(),
            name: "M".into(),
            provider: "mock".into(),
            reasoning,
            thinking_level_map,
            base_url: None,
            context_window: None,
            max_tokens: None,
            input: None,
        }
    }

    fn level_ids(levels: &[ThinkingLevel]) -> Vec<&str> {
        levels.iter().map(|l| l.id()).collect()
    }

    #[test]
    fn supported_levels_mirror_pi_get_supported_thinking_levels() {
        use ThinkingLevel::*;
        // No model known: full ladder (pi's `getAvailableThinkingLevels` fallback).
        assert_eq!(
            level_ids(&supported_thinking_levels(None)),
            level_ids(&ThinkingLevel::all())
        );
        // No reasoning: `off` only.
        assert_eq!(
            supported_thinking_levels(Some(&model_with_map(false, &[]))),
            vec![Off]
        );
        // Reasoning without a map: classic ladder (xhigh/max need an entry).
        assert_eq!(
            level_ids(&supported_thinking_levels(Some(&model_with_map(true, &[])))),
            vec!["off", "minimal", "low", "medium", "high"]
        );
        // `null` entries disable their level (real gpt-5.6 shape: no minimal).
        let gptish = model_with_map(
            true,
            &[
                ("off", Some("none")),
                ("minimal", None),
                ("low", Some("low")),
                ("medium", Some("medium")),
                ("high", Some("high")),
                ("xhigh", Some("xhigh")),
                ("max", Some("max")),
            ],
        );
        assert_eq!(
            level_ids(&supported_thinking_levels(Some(&gptish))),
            vec!["off", "low", "medium", "high", "xhigh", "max"]
        );
        // Missing keys behave like "no opinion" except xhigh/max (real
        // DeepSeek shape: only high/max mapped).
        let deepseekish = model_with_map(true, &[("high", Some("high")), ("max", Some("max"))]);
        assert_eq!(
            level_ids(&supported_thinking_levels(Some(&deepseekish))),
            vec!["off", "minimal", "low", "medium", "high", "max"]
        );
        // `thinkingLevelMap` deserializes from real pi payloads (null stays null).
        let parsed: Model = serde_json::from_value(serde_json::json!({
            "id": "m", "name": "M", "provider": "mock", "reasoning": true,
            "thinkingLevelMap": {"off": "none", "minimal": null}
        }))
        .unwrap();
        let map = parsed.thinking_level_map.unwrap();
        assert_eq!(map["off"], Some("none".to_string()));
        assert_eq!(map["minimal"], None);
    }

    #[test]
    fn clamp_thinking_level_prefers_upward_then_downward() {
        use ThinkingLevel::*;
        // Exact hit always wins.
        let limited = model_with_map(
            true,
            &[
                ("off", Some("none")),
                ("minimal", None),
                ("low", Some("low")),
                ("medium", Some("medium")),
                ("high", Some("high")),
            ],
        );
        assert_eq!(clamp_thinking_level(Some(&limited), Medium), Medium);
        // `minimal` unsupported → nearest higher (`low`), pi parity.
        assert_eq!(clamp_thinking_level(Some(&limited), Minimal), Low);
        // `max` unsupported → nearest lower (`high`).
        assert_eq!(clamp_thinking_level(Some(&limited), Max), High);
        // Non-reasoning model: everything clamps to `off`.
        let plain = model_with_map(false, &[]);
        assert_eq!(clamp_thinking_level(Some(&plain), High), Off);
        // No model: full ladder, exact hit.
        assert_eq!(clamp_thinking_level(None, XHigh), XHigh);
    }

    #[test]
    fn thinking_descriptions_use_native_token_budgets() {
        // pi-ai `DEFAULT_THINKING_BUDGETS`: minimal 1k / low 2k / medium 8k /
        // high 16k — Zed must show these, not invented static strings.
        assert_eq!(ThinkingLevel::Off.description(), "No reasoning");
        assert!(ThinkingLevel::Minimal.description().contains("~1k"));
        assert!(ThinkingLevel::Low.description().contains("~2k"));
        assert!(ThinkingLevel::Medium.description().contains("~8k"));
        assert!(ThinkingLevel::High.description().contains("~16k"));
        assert!(ThinkingLevel::XHigh.description().contains("~32k"));
    }

    #[test]
    fn parses_usage_and_thinking_events() {
        let ev = parse_event(
            r#"{"type":"message_update","usage":{"input":5,"output":3,"totalTokens":8,"cost":{"input":1.0,"output":2.0,"total":3.0}},"assistantMessageEvent":{"type":"thinking_delta","contentIndex":0,"delta":"hmm"}}"#,
        );
        match ev {
            RpcEvent::MessageUpdate { usage, .. } => {
                assert_eq!(usage.input, 5);
                assert_eq!(usage.output, 3);
                assert_eq!(usage.total_tokens, 8);
                let cost = usage.cost.as_ref().unwrap();
                assert_eq!(cost.total, 3.0);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let level = parse_event(r#"{"type":"thinking_level_changed","level":"xhigh"}"#);
        match level {
            RpcEvent::ThinkingLevelChanged { level } => {
                assert_eq!(level, ThinkingLevel::XHigh);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_tool_events() {
        let ts = parse_event(
            r#"{"type":"message_update","usage":{},"assistantMessageEvent":{"type":"toolcall_start","contentIndex":0,"id":"tool-1","toolName":"bash"}}"#,
        );
        match ts {
            RpcEvent::MessageUpdate {
                assistant_message_event,
                ..
            } => match assistant_message_event {
                AssistantMessageEvent::ToolcallStart {
                    content_index,
                    id,
                    tool_name,
                } => {
                    assert_eq!(content_index, 0);
                    assert_eq!(id, "tool-1");
                    assert_eq!(tool_name, "bash");
                }
                other => panic!("unexpected sub-event: {other:?}"),
            },
            other => panic!("unexpected event: {other:?}"),
        }

        let exec = parse_event(
            r#"{"type":"tool_execution_end","toolCallId":"tool-1","toolName":"bash","result":{"exitCode":0},"isError":false}"#,
        );
        match exec {
            RpcEvent::ToolExecutionEnd {
                tool_call_id,
                tool_name,
                result,
                is_error,
            } => {
                assert_eq!(tool_call_id, "tool-1");
                assert_eq!(tool_name, "bash");
                assert_eq!(result["exitCode"], 0);
                assert!(!is_error);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_session_events() {
        let queue =
            parse_event(r#"{"type":"queue_update","steering":["s"],"followUp":["f1","f2"]}"#);
        match queue {
            RpcEvent::QueueUpdate {
                steering,
                follow_up,
            } => {
                assert_eq!(steering, vec!["s"]);
                assert_eq!(follow_up, vec!["f1", "f2"]);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let info = parse_event(r#"{"type":"session_info_changed","name":"My Session"}"#);
        assert!(matches!(
            info,
            RpcEvent::SessionInfoChanged { name: Some(_) }
        ));

        let compact = parse_event(
            r#"{"type":"compaction_end","reason":"threshold","aborted":false,"willRetry":true,"errorMessage":"boom"}"#,
        );
        match compact {
            RpcEvent::CompactionEnd {
                reason,
                aborted,
                will_retry,
                error_message,
                ..
            } => {
                assert_eq!(reason, CompactionReason::Threshold);
                assert!(!aborted);
                assert!(will_retry);
                assert_eq!(error_message.as_deref(), Some("boom"));
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let retry = parse_event(
            r#"{"type":"summarization_retry_attempt_start","source":"compaction","reason":"overflow"}"#,
        );
        match retry {
            RpcEvent::SummarizationRetryAttemptStart { source, reason } => {
                assert_eq!(source, "compaction");
                assert_eq!(reason, Some(CompactionReason::Overflow));
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let bash = parse_event(r#"{"type":"bash_execution_update","id":"b1","delta":"out"}"#);
        assert!(matches!(
            bash,
            RpcEvent::BashExecutionUpdate { id: Some(_), .. }
        ));
    }

    #[test]
    fn parses_extension_ui_requests() {
        let select = parse_event(
            r#"{"type":"extension_ui_request","id":"ui-1","method":"select","title":"Pick","options":["a","b"]}"#,
        );
        match select {
            RpcEvent::ExtensionUiRequest { inner } => match inner {
                ExtensionUiRequest::Select {
                    id, title, options, ..
                } => {
                    assert_eq!(id, "ui-1");
                    assert_eq!(title, "Pick");
                    assert_eq!(options, vec!["a", "b"]);
                }
                other => panic!("unexpected ui request: {other:?}"),
            },
            other => panic!("unexpected event: {other:?}"),
        }

        let confirm = parse_event(
            r#"{"type":"extension_ui_request","id":"ui-2","method":"confirm","title":"Run?","message":"sure?"}"#,
        );
        assert!(matches!(
            confirm,
            RpcEvent::ExtensionUiRequest {
                inner: ExtensionUiRequest::Confirm { .. }
            }
        ));

        let widget = parse_event(
            r#"{"type":"extension_ui_request","id":"ui-3","method":"setWidget","widgetKey":"k","widgetLines":["l1"],"widgetPlacement":"aboveEditor"}"#,
        );
        match widget {
            RpcEvent::ExtensionUiRequest { inner } => match inner {
                ExtensionUiRequest::SetWidget {
                    widget_key,
                    widget_lines,
                    widget_placement,
                    ..
                } => {
                    assert_eq!(widget_key, "k");
                    assert_eq!(widget_lines, Some(vec!["l1".to_string()]));
                    assert_eq!(widget_placement.as_deref(), Some("aboveEditor"));
                }
                other => panic!("unexpected ui request: {other:?}"),
            },
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn catches_unknown_event_types_as_unknown() {
        let unknown =
            serde_json::from_str::<RpcEvent>(r#"{"type":"brand_new_event","some":"field"}"#);
        // A strict enum must NOT silently swallow unknown tags — deserialization
        // fails, and the reader wraps the raw line into RpcEvent::Unknown.
        assert!(unknown.is_err());

        // The reader-side wrapper is constructed manually:
        let raw = json!({"type":"brand_new_event","some":"field"});
        let wrapped = RpcEvent::Unknown { raw: raw.clone() };
        assert!(matches!(wrapped, RpcEvent::Unknown { .. }));
    }

    #[test]
    fn parses_response_lines() {
        let ok: RpcResponse = serde_json::from_str(
            r#"{"id":"3","type":"response","command":"get_state","success":true,"data":{"sessionId":"s1"}}"#,
        )
        .unwrap();
        assert_eq!(ok.id.as_deref(), Some("3"));
        assert_eq!(ok.command, "get_state");
        assert!(ok.success);
        assert_eq!(ok.clone().ok().unwrap()["sessionId"], "s1");

        let err: RpcResponse = serde_json::from_str(
            r#"{"id":"4","type":"response","command":"prompt","success":false,"error":"no provider"}"#,
        )
        .unwrap();
        match err.ok() {
            Err(AcpxError::RpcFailed { command, message }) => {
                assert_eq!(command, "prompt");
                assert_eq!(message, "no provider");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn serializes_commands_with_type_tag_and_id_friendly() {
        let cmd = RpcCommand::Prompt {
            message: "hi".into(),
            images: None,
            streaming_behavior: None,
        };
        let v = serde_json::to_value(cmd).unwrap();
        assert_eq!(v["type"], "prompt");
        assert_eq!(v["message"], "hi");
        // optional fields stay off the wire
        assert!(v.get("images").is_none());

        let cmd = RpcCommand::SetModel {
            provider: "anthropic".into(),
            model_id: "claude-sonnet".into(),
        };
        let v = serde_json::to_value(cmd).unwrap();
        assert_eq!(v["type"], "set_model");
        assert_eq!(v["modelId"], "claude-sonnet");

        let cmd = RpcCommand::SetThinkingLevel {
            level: ThinkingLevel::Medium,
        };
        let v = serde_json::to_value(cmd).unwrap();
        assert_eq!(v["type"], "set_thinking_level");
        assert_eq!(v["level"], "medium");

        let cmd = RpcCommand::SetSteeringMode {
            mode: QueueMode::OneAtATime,
        };
        let v = serde_json::to_value(cmd).unwrap();
        assert_eq!(v["type"], "set_steering_mode");
        assert_eq!(v["mode"], "one-at-a-time");

        assert_eq!(RpcCommand::GetState.name(), "get_state");
    }

    #[test]
    fn parses_session_state() {
        let state: RpcSessionState = serde_json::from_str(
            r#"{
                "model":{"id":"m1","name":"Model","provider":"mock","reasoning":false,"contextWindow":1000},
                "thinkingLevel":"medium",
                "isStreaming":false,
                "isCompacting":false,
                "steeringMode":"one-at-a-time",
                "followUpMode":"all",
                "sessionFile":"/tmp/s.jsonl",
                "sessionId":"abc",
                "sessionName":"T",
                "autoCompactionEnabled":true,
                "messageCount":3,
                "pendingMessageCount":0
            }"#,
        )
        .unwrap();
        assert_eq!(state.session_id, "abc");
        assert_eq!(state.session_file.as_deref(), Some("/tmp/s.jsonl"));
        assert_eq!(state.thinking_level, ThinkingLevel::Medium);
        assert_eq!(state.steering_mode, QueueMode::OneAtATime);
        assert_eq!(state.follow_up_mode, QueueMode::All);
        assert!(state.auto_compaction_enabled);
        let model = state.model.unwrap();
        assert_eq!(model.id, "m1");
        assert_eq!(model.context_window, Some(1000));

        // Real pi emits "queue" for a migrated legacy queueMode setting;
        // unknown future values fall through to `Other` without breaking parse.
        let state: RpcSessionState = serde_json::from_str(
            r#"{
                "thinkingLevel":"medium",
                "isStreaming":false,
                "isCompacting":false,
                "steeringMode":"queue",
                "followUpMode":"one-at-a-time",
                "sessionId":"abc",
                "autoCompactionEnabled":false,
                "messageCount":0,
                "pendingMessageCount":0
            }"#,
        )
        .unwrap();
        assert_eq!(state.steering_mode, QueueMode::Queue);
        let state: RpcSessionState = serde_json::from_str(
            r#"{
                "thinkingLevel":"medium",
                "isStreaming":false,
                "isCompacting":false,
                "steeringMode":"future-mode",
                "followUpMode":"all",
                "sessionId":"abc",
                "autoCompactionEnabled":false,
                "messageCount":0,
                "pendingMessageCount":0
            }"#,
        )
        .unwrap();
        assert_eq!(state.steering_mode, QueueMode::Other);
    }

    #[test]
    fn serializes_extension_ui_responses() {
        let v = serde_json::to_value(ExtensionUiResponse::Value {
            id: "ui-1".into(),
            value: "pick-a".into(),
        })
        .unwrap();
        assert_eq!(v["id"], "ui-1");
        assert_eq!(v["value"], "pick-a");

        let v = serde_json::to_value(ExtensionUiResponse::Cancelled {
            id: "ui-2".into(),
            cancelled: true,
        })
        .unwrap();
        assert_eq!(v["cancelled"], true);
    }

    /// Exhaustive-match smoke test: any new `RpcEvent` variant is a compile
    /// error here until deliberately classified (the "编译期穷尽" requirement).
    #[test]
    fn event_enum_is_exhaustively_classified() {
        fn classify(e: &RpcEvent) -> &'static str {
            match e {
                RpcEvent::AgentStart | RpcEvent::AgentSettled => "agent",
                RpcEvent::TurnStart
                | RpcEvent::TurnEnd { .. }
                | RpcEvent::MessageStart { .. }
                | RpcEvent::MessageUpdate { .. }
                | RpcEvent::MessageEnd { .. } => "message",
                RpcEvent::AgentEnd { .. } => "agent-end",
                RpcEvent::ToolExecutionStart { .. }
                | RpcEvent::ToolExecutionUpdate { .. }
                | RpcEvent::ToolExecutionEnd { .. } => "tool",
                RpcEvent::QueueUpdate { .. }
                | RpcEvent::SessionInfoChanged { .. }
                | RpcEvent::ThinkingLevelChanged { .. }
                | RpcEvent::BashExecutionUpdate { .. } => "session",
                RpcEvent::CompactionStart { .. } | RpcEvent::CompactionEnd { .. } => "compaction",
                RpcEvent::EntryAppended { .. } => "entry",
                RpcEvent::AutoRetryStart { .. } | RpcEvent::AutoRetryEnd { .. } => "retry",
                RpcEvent::SummarizationRetryScheduled { .. }
                | RpcEvent::SummarizationRetryAttemptStart { .. }
                | RpcEvent::SummarizationRetryFinished => "summarization",
                RpcEvent::ExtensionUiRequest { .. } => "extension-ui",
                RpcEvent::ExtensionError { .. } => "extension-error",
                RpcEvent::UnmatchedResponse { .. } => "unmatched-response",
                RpcEvent::Unknown { .. } => "unknown",
            }
        }
        assert_eq!(classify(&RpcEvent::AgentSettled), "agent");
    }
}
