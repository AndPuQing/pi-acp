//! Native outbound frame construction, one renderer per protocol version.
//!
//! ACP v1 and v2 are separate protocols, not two spellings of one: several
//! concepts changed shape between them, and a bash tool call is the sharpest
//! example. v1 embeds a **client-owned** terminal through
//! `ToolCallContent::Terminal` plus `terminal_*` `_meta`; v2 has no such tool
//! content and no client-owned terminal at all. There is therefore no v1 frame
//! that can be *converted* into the v2 frame the client needs — the schema
//! refuses v1 `Terminal` content on the v2 path — and the earlier design, which
//! built a v1 frame and converted it at the edge, silently dropped every v2
//! bash call.
//!
//! This module is the fix: the pump states the facts of a bash call once
//! ([`crate::session::BashToolCall`]) and each protocol's renderer builds its
//! **own native frames** from them. Nothing is converted; the shared thing is
//! the fact, not the wire shape.
//!
//! This module is the fix: the pump states the facts of an update once (in
//! [`crate::session`]) and each protocol's renderer builds its **own native
//! frames** from them. Nothing is converted; the shared thing is the fact, not
//! the wire shape.
//!
//! Every outbound update is rendered here. The differences are real, not
//! cosmetic: v1's `tool_call` vs v2's `tool_call_update`, v1's `Diff` old/new
//! text vs v2's structured changes plus a Git patch, v1's `id` on a config
//! option vs v2's `configId`, v1's `Unstructured` command input vs v2's `Text`,
//! v1's `current_mode_update` (gone in v2), and v2's required `messageId`.

use crate::error::AcpxError;
use crate::protocol::Protocol;
use crate::session::{
    AvailableCommandsFact, BashToolCall, BashToolStatus, ConfigCategoryFact, ConfigOptionsFact,
    LinkChunkFact, MessagePatch, ModeFact, PermissionOptionKindFact, PermissionOutcomeFact,
    PermissionRequestFact, SessionInfoFact, TextChunk, TextChunkKind, ToolCallFact,
    ToolContentFact, ToolKindFact, ToolLocationFact, ToolStatusFact, UsageFact,
};
use crate::translate::bash::{
    bash_terminal_content, bash_terminal_exit_meta, bash_terminal_info_meta,
    bash_terminal_output_meta,
};
use agent_client_protocol::schema::v1;
use agent_client_protocol::{Client, ConnectionTo};
use agent_client_protocol_schema::v2;
use serde_json::json;

/// v1's status for a bash call.
fn v1_status(call: &BashToolCall) -> v1::ToolCallStatus {
    use crate::session::BashToolStatus as S;
    match call.status {
        S::Pending => v1::ToolCallStatus::Pending,
        S::InProgress => v1::ToolCallStatus::InProgress,
        S::Completed => v1::ToolCallStatus::Completed,
        S::Failed => v1::ToolCallStatus::Failed,
    }
}

/// The `terminal_output` / `terminal_exit` `_meta` a bash frame carries.
///
/// Both protocols name the terminal after the tool call and read the same two
/// keys, so the payload is built once and attached to whichever frame the
/// protocol sends. A call with no new output and no exit carries none.
fn terminal_meta(call: &BashToolCall) -> Option<v1::Meta> {
    use crate::session::BashToolStatus as S;
    let mut meta = serde_json::Map::new();
    if !call.output_delta.is_empty() {
        meta.extend(bash_terminal_output_meta(
            &call.tool_call_id,
            &call.output_delta,
        ));
    }
    if let Some(exit_code) = call.exit_code {
        if matches!(call.status, S::Completed | S::Failed) {
            meta.extend(bash_terminal_exit_meta(&call.tool_call_id, exit_code));
        }
    }
    (!meta.is_empty()).then_some(meta)
}

/// The native v1 frames for a bash call.
///
/// The first frame opens the call with `ToolCallContent::Terminal` and the
/// `terminal_info` `_meta`; every later frame is a status + output/exit update.
/// This is the v1 shape clients (Zed) already consume, byte for byte.
pub fn bash_v1_frames(call: &BashToolCall) -> Vec<v1::SessionNotification> {
    let status = v1_status(call);
    if call.first {
        let tool_call = v1::ToolCall::new(call.tool_call_id.clone(), call.command.clone())
            .kind(v1::ToolKind::Execute)
            .status(status)
            .content(bash_terminal_content(&call.tool_call_id))
            .meta(bash_terminal_info_meta(
                &call.tool_call_id,
                call.cwd.as_deref().unwrap_or_default(),
            ));
        return vec![v1::SessionNotification::new(
            call.session_id.clone(),
            v1::SessionUpdate::ToolCall(tool_call),
        )];
    }

    let fields = v1::ToolCallUpdateFields::new().status(Some(status));
    let update = match terminal_meta(call) {
        Some(meta) => v1::ToolCallUpdate::new(call.tool_call_id.clone(), fields).meta(meta),
        None => v1::ToolCallUpdate::new(call.tool_call_id.clone(), fields),
    };
    vec![v1::SessionNotification::new(
        call.session_id.clone(),
        v1::SessionUpdate::ToolCallUpdate(update),
    )]
}

/// The native v1 frame for a replayed bash call.
///
/// A replay is already finished: one `tool_call` carrying the terminal, the
/// whole output and the exit code. It is a single frame because v1 has no
/// separate terminal stream to feed.
pub fn bash_replay_v1(
    session_id: v1::SessionId,
    tool_call_id: &str,
    command: &str,
    cwd: &str,
    output: &str,
    exit_code: i32,
    failed: bool,
) -> v1::SessionNotification {
    let mut meta = bash_terminal_info_meta(tool_call_id, cwd);
    if !output.is_empty() {
        meta.extend(bash_terminal_output_meta(tool_call_id, output));
    }
    meta.extend(bash_terminal_exit_meta(tool_call_id, exit_code));
    let call = v1::ToolCall::new(tool_call_id.to_owned(), command.to_owned())
        .kind(v1::ToolKind::Execute)
        .status(if failed {
            v1::ToolCallStatus::Failed
        } else {
            v1::ToolCallStatus::Completed
        })
        .content(bash_terminal_content(tool_call_id))
        .meta(meta);
    v1::SessionNotification::new(session_id, v1::SessionUpdate::ToolCall(call))
}

/// v2's status for a bash call.
fn v2_status(status: BashToolStatus) -> v2::ToolCallStatus {
    match status {
        BashToolStatus::Pending => v2::ToolCallStatus::Pending,
        BashToolStatus::InProgress => v2::ToolCallStatus::InProgress,
        BashToolStatus::Completed => v2::ToolCallStatus::Completed,
        BashToolStatus::Failed => v2::ToolCallStatus::Failed,
    }
}

/// The native v2 frames for a bash call.
///
/// v2 dropped v1's client-owned terminal, so the opening frame is a
/// `tool_call_update` that names the call (`kind: execute`, `title`, and the
/// command in `rawInput`) and every frame streams the terminal through the
/// `terminal_output` / `terminal_exit` `_meta`. The frames are built in v2 types
/// directly — nothing is converted from v1.
pub fn bash_v2_frames(call: &BashToolCall) -> Vec<v2::UpdateSessionNotification> {
    let mut update =
        v2::ToolCallUpdate::new(call.tool_call_id.clone()).status(v2_status(call.status));
    if call.first {
        update = update
            .kind(v2::ToolKind::Execute)
            .title(call.command.clone())
            .raw_input(match &call.cwd {
                Some(cwd) => json!({ "command": call.command, "cwd": cwd }),
                None => json!({ "command": call.command }),
            });
    }
    if let Some(meta) = terminal_meta(call) {
        update = update.meta(meta);
    }
    vec![v2::UpdateSessionNotification::new(
        v2::SessionId::new(call.session_id.0.to_string()),
        v2::SessionUpdate::ToolCallUpdate(update),
    )]
}

/// The native v2 frame for a replayed bash call.
///
/// v2 has no embedded terminal: the call is named by the `tool_call_update`
/// itself and its output and exit travel as `_meta`, the same stream a live
/// conversation uses.
pub fn bash_replay_v2(
    session_id: v1::SessionId,
    tool_call_id: &str,
    command: &str,
    cwd: &str,
    output: &str,
    exit_code: i32,
    failed: bool,
) -> v2::UpdateSessionNotification {
    let mut meta = serde_json::Map::new();
    if !output.is_empty() {
        meta.extend(bash_terminal_output_meta(tool_call_id, output));
    }
    meta.extend(bash_terminal_exit_meta(tool_call_id, exit_code));
    let update = v2::ToolCallUpdate::new(tool_call_id.to_owned())
        .kind(v2::ToolKind::Execute)
        .title(command.to_owned())
        .raw_input(json!({ "command": command, "cwd": cwd }))
        .raw_output(json!(output))
        .status(if failed {
            v2::ToolCallStatus::Failed
        } else {
            v2::ToolCallStatus::Completed
        })
        .meta(meta);
    v2::UpdateSessionNotification::new(
        v2::SessionId::new(session_id.0.to_string()),
        v2::SessionUpdate::ToolCallUpdate(update),
    )
}

/// The native v1 frame for a streamed text chunk.
///
/// v1's `messageId` is optional, so a chunk that has no message of its own
/// (an extension notice) simply carries none. The id, when present, is spelled
/// in v1's own type here — it never traveled as a v1 type through the pump.
pub fn text_chunk_v1(chunk: &TextChunk) -> v1::SessionNotification {
    let mut content = v1::ContentChunk::new(v1::ContentBlock::Text(v1::TextContent::new(
        chunk.text.clone(),
    )));
    if let Some(message_id) = &chunk.message_id {
        content = content.message_id(v1::MessageId::new(message_id.clone()));
    }
    if let Some(meta) = chunk.meta.clone() {
        content = content.meta(meta);
    }
    let update = match chunk.kind {
        TextChunkKind::Agent => v1::SessionUpdate::AgentMessageChunk(content),
        TextChunkKind::Thought => v1::SessionUpdate::AgentThoughtChunk(content),
        TextChunkKind::User => v1::SessionUpdate::UserMessageChunk(content),
    };
    v1::SessionNotification::new(chunk.session_id.clone(), update)
}

/// The native v2 frame for a streamed text chunk.
///
/// v2 made `messageId` **required**, so a chunk the pump left anonymous (an
/// extension notice) is given one here. That is the only v2-specific fact about
/// a chunk; the text and the kind are the same. The id is built as a v2 type
/// directly, so the frame never depends on v1→v2 conversion succeeding.
pub fn text_chunk_v2(chunk: &TextChunk) -> v2::UpdateSessionNotification {
    let message_id = match &chunk.message_id {
        Some(id) => id.clone(),
        None => mint_synthetic_message_id(),
    };
    let mut content = v2::ContentChunk::new(
        v2::ContentBlock::Text(v2::TextContent::new(chunk.text.clone())),
        v2::MessageId::new(message_id),
    );
    if let Some(meta) = chunk.meta.clone() {
        content = content.meta(meta);
    }
    let update = match chunk.kind {
        TextChunkKind::Agent => v2::SessionUpdate::AgentMessageChunk(content),
        TextChunkKind::Thought => v2::SessionUpdate::AgentThoughtChunk(content),
        TextChunkKind::User => v2::SessionUpdate::UserMessageChunk(content),
    };
    v2::UpdateSessionNotification::new(v2::SessionId::new(chunk.session_id.0.to_string()), update)
}

/// A process-unique id for a chunk that has no message of its own.
///
/// v2 requires a `messageId` on every chunk. A producer without a message (an
/// extension notice, a replayed history frame) is genuinely its own one-chunk
/// message, so a fresh id per chunk is the honest answer rather than a shared
/// sentinel.
fn mint_synthetic_message_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let next = SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    format!("pi-synthetic-{next}")
}

/// The native v1 frames for a complete-message patch: none.
///
/// v1 has no full-object message update. Fabricating one would be wrong — v1
/// clients reassemble messages from chunks, which the pump already sent.
pub fn message_patch_v1(_patch: &MessagePatch) -> Vec<v1::SessionNotification> {
    Vec::new()
}

/// The native v2 frame for a complete-message patch.
///
/// v2 `agent_message.content` replaces everything accumulated for the id, which
/// is how the streamed chunks and the final object converge. The content is a
/// v2 `ContentBlock` built here, not converted from a v1 one.
pub fn message_patch_v2(patch: &MessagePatch) -> v2::UpdateSessionNotification {
    let content = vec![v2::ContentBlock::Text(v2::TextContent::new(
        patch.text.clone(),
    ))];
    let message =
        v2::AgentMessage::new(v2::MessageId::new(patch.message_id.clone())).content(content);
    v2::UpdateSessionNotification::new(
        v2::SessionId::new(patch.session_id.0.to_string()),
        v2::SessionUpdate::AgentMessage(message),
    )
}

/// The native v2 `state_update` for a foreground transition.
///
/// v2 reports turn completion through this update rather than the prompt
/// response's `stopReason`, so pi-acp publishes it. Everything is built in v2
/// types — the stop reason is chosen directly, not converted from a v1 one.
pub fn foreground_v2(
    session_id: &v1::SessionId,
    state: crate::session::ForegroundState,
) -> v2::UpdateSessionNotification {
    use crate::session::ForegroundState;
    let state = match state {
        ForegroundState::Running => v2::StateUpdate::Running(v2::RunningStateUpdate::new()),
        ForegroundState::Idle(reason) => {
            let stop_reason = match reason {
                crate::session::StopReason::EndTurn => v2::StopReason::EndTurn,
                crate::session::StopReason::Cancelled => v2::StopReason::Cancelled,
            };
            v2::StateUpdate::Idle(v2::IdleStateUpdate::new().stop_reason(stop_reason))
        }
    };
    v2::UpdateSessionNotification::new(
        v2::SessionId::new(session_id.0.to_string()),
        v2::SessionUpdate::StateUpdate(state),
    )
}

// ---------------------------------------------------------------------------
// Remaining outbound updates
// ---------------------------------------------------------------------------

/// Translate a transport failure into the adapter's error type.
fn transport_error(e: impl std::fmt::Display) -> AcpxError {
    AcpxError::RpcFailed {
        command: "session/update".into(),
        message: e.to_string(),
    }
}

/// Send one native v1 frame.
pub fn send_v1(cx: &ConnectionTo<Client>, notif: v1::SessionNotification) -> Result<(), AcpxError> {
    cx.send_notification(notif).map_err(transport_error)
}

/// Send one native v2 frame.
pub fn send_v2(
    cx: &ConnectionTo<Client>,
    notif: v2::UpdateSessionNotification,
) -> Result<(), AcpxError> {
    cx.send_notification(notif).map_err(transport_error)
}

fn v1_kind(kind: &ToolKindFact) -> v1::ToolKind {
    match kind {
        ToolKindFact::Read => v1::ToolKind::Read,
        ToolKindFact::Edit => v1::ToolKind::Edit,
        ToolKindFact::Delete => v1::ToolKind::Delete,
        ToolKindFact::Move => v1::ToolKind::Move,
        ToolKindFact::Search => v1::ToolKind::Search,
        ToolKindFact::Execute => v1::ToolKind::Execute,
        ToolKindFact::Think => v1::ToolKind::Think,
        ToolKindFact::Fetch => v1::ToolKind::Fetch,
        ToolKindFact::SwitchMode => v1::ToolKind::SwitchMode,
        // v1 collapses every unknown category into `other`.
        ToolKindFact::Other | ToolKindFact::Unknown(_) => v1::ToolKind::Other,
    }
}

fn v1_tool_status(status: &ToolStatusFact) -> v1::ToolCallStatus {
    match status {
        ToolStatusFact::Pending => v1::ToolCallStatus::Pending,
        ToolStatusFact::InProgress => v1::ToolCallStatus::InProgress,
        ToolStatusFact::Completed => v1::ToolCallStatus::Completed,
        ToolStatusFact::Failed => v1::ToolCallStatus::Failed,
        ToolStatusFact::Unknown(_) => v1::ToolCallStatus::Pending,
    }
}

fn v1_tool_locations(locations: &[ToolLocationFact]) -> Vec<v1::ToolCallLocation> {
    locations
        .iter()
        .map(|l| v1::ToolCallLocation::new(l.path.clone()).line(l.line))
        .collect()
}

fn v1_tool_content(content: &ToolContentFact) -> v1::ToolCallContent {
    match content {
        ToolContentFact::Text(text) => {
            v1::ToolCallContent::Content(agent_client_protocol::schema::v1::Content::new(
                v1::ContentBlock::Text(v1::TextContent::new(text.clone())),
            ))
        }
        ToolContentFact::Diff {
            path,
            new_text,
            old_text,
        } => v1::ToolCallContent::Diff(
            v1::Diff::new(path.clone(), new_text.clone()).old_text(old_text.clone()),
        ),
    }
}

/// v1's frames for a non-bash tool call.
///
/// The first frame is a full `tool_call`; every later frame patches it. This is
/// the shape v1 clients already consume, byte for byte.
pub fn tool_call_v1(fact: &ToolCallFact) -> v1::SessionNotification {
    let update = if fact.first {
        let mut call = v1::ToolCall::new(
            fact.tool_call_id.clone(),
            fact.title.clone().unwrap_or_default(),
        );
        if let Some(kind) = &fact.kind {
            call = call.kind(v1_kind(kind));
        }
        if let Some(status) = &fact.status {
            call = call.status(v1_tool_status(status));
        }
        if let Some(content) = &fact.content {
            call = call.content(content.iter().map(v1_tool_content).collect());
        }
        if !fact.locations.is_empty() {
            call = call.locations(v1_tool_locations(&fact.locations));
        }
        if let Some(input) = &fact.raw_input {
            call = call.raw_input(input.clone());
        }
        if let Some(output) = &fact.raw_output {
            call = call.raw_output(output.clone());
        }
        if let Some(meta) = &fact.meta {
            call = call.meta(meta.clone());
        }
        v1::SessionUpdate::ToolCall(call)
    } else {
        let mut fields = v1::ToolCallUpdateFields::new();
        if let Some(title) = &fact.title {
            fields = fields.title(Some(title.clone()));
        }
        if let Some(kind) = &fact.kind {
            fields = fields.kind(Some(v1_kind(kind)));
        }
        if let Some(status) = &fact.status {
            fields = fields.status(Some(v1_tool_status(status)));
        }
        if let Some(content) = &fact.content {
            fields = fields.content(Some(content.iter().map(v1_tool_content).collect()));
        }
        if !fact.locations.is_empty() {
            fields = fields.locations(Some(v1_tool_locations(&fact.locations)));
        }
        if let Some(input) = &fact.raw_input {
            fields = fields.raw_input(Some(input.clone()));
        }
        if let Some(output) = &fact.raw_output {
            fields = fields.raw_output(Some(output.clone()));
        }
        let mut update = v1::ToolCallUpdate::new(fact.tool_call_id.clone(), fields);
        if let Some(meta) = &fact.meta {
            update = update.meta(meta.clone());
        }
        v1::SessionUpdate::ToolCallUpdate(update)
    };
    v1::SessionNotification::new(fact.session_id.clone(), update)
}

fn v2_kind(kind: &ToolKindFact) -> v2::ToolKind {
    match kind {
        ToolKindFact::Read => v2::ToolKind::Read,
        ToolKindFact::Edit => v2::ToolKind::Edit,
        ToolKindFact::Delete => v2::ToolKind::Delete,
        ToolKindFact::Move => v2::ToolKind::Move,
        ToolKindFact::Search => v2::ToolKind::Search,
        ToolKindFact::Execute => v2::ToolKind::Execute,
        ToolKindFact::Think => v2::ToolKind::Think,
        ToolKindFact::Fetch => v2::ToolKind::Fetch,
        ToolKindFact::SwitchMode => v2::ToolKind::SwitchMode,
        ToolKindFact::Other => v2::ToolKind::Other,
        ToolKindFact::Unknown(other) => v2::ToolKind::Unknown(other.clone()),
    }
}

fn v2_tool_status(status: &ToolStatusFact) -> v2::ToolCallStatus {
    match status {
        ToolStatusFact::Pending => v2::ToolCallStatus::Pending,
        ToolStatusFact::InProgress => v2::ToolCallStatus::InProgress,
        ToolStatusFact::Completed => v2::ToolCallStatus::Completed,
        ToolStatusFact::Failed => v2::ToolCallStatus::Failed,
        ToolStatusFact::Unknown(other) => v2::ToolCallStatus::Other(other.clone()),
    }
}

fn v2_tool_locations(locations: &[ToolLocationFact]) -> Vec<v2::ToolCallLocation> {
    locations
        .iter()
        .map(|l| v2::ToolCallLocation::new(l.path.clone()).line(l.line))
        .collect()
}

/// The native v2 form of a file edit.
///
/// v2 replaced v1's `oldText`/`newText` pair with structured changes plus an
/// optional renderable patch, so the same facts produce a `modify`/`add` change
/// and a Git `--patch` of the whole file.
fn v2_diff(path: &str, new_text: &str, old_text: Option<&str>) -> v2::Diff {
    let path = v2::AbsolutePath::new(path);
    let change = if old_text.is_some() {
        v2::DiffChange::modify(path.clone()).file_type(v2::DiffFileType::Text)
    } else {
        v2::DiffChange::add(path.clone()).file_type(v2::DiffFileType::Text)
    };
    let patch = full_file_git_patch(&path, old_text, new_text);
    v2::Diff::patch(patch, vec![change])
}

/// A Git `--patch` of a whole file, in the format v2's `DiffPatch` requires.
fn full_file_git_patch(path: &v2::AbsolutePath, old_text: Option<&str>, new_text: &str) -> String {
    let path = path.0.to_string_lossy();
    let old = old_text.unwrap_or_default();
    let original_filename = if old_text.is_some() {
        path.to_string()
    } else {
        "/dev/null".to_string()
    };

    let mut options = diffy::DiffOptions::new();
    options
        .set_original_filename(original_filename)
        .set_modified_filename(path.to_string());

    let mut patch_text = format!("diff --git {path} {path}\n");
    if old_text.is_none() {
        patch_text.push_str("new file mode 100644\n");
    }
    patch_text.push_str(&options.create_patch(old, new_text).to_string());
    patch_text
}

fn v2_tool_content(content: &ToolContentFact) -> v2::ToolCallContent {
    match content {
        ToolContentFact::Text(text) => v2::ToolCallContent::Content(Box::new(v2::Content::new(
            v2::ContentBlock::Text(v2::TextContent::new(text.clone())),
        ))),
        ToolContentFact::Diff {
            path,
            new_text,
            old_text,
        } => v2::ToolCallContent::Diff(v2_diff(path, new_text, old_text.as_deref())),
    }
}

fn v2_session_id(session_id: &v1::SessionId) -> v2::SessionId {
    v2::SessionId::new(session_id.0.to_string())
}

/// The native v2 frame for a non-bash tool call.
///
/// v2 has no first-class `tool_call`: every frame is a patch-style
/// `tool_call_update`, so the opening frame simply carries the naming fields.
pub fn tool_call_v2(fact: &ToolCallFact) -> v2::UpdateSessionNotification {
    let mut update = v2::ToolCallUpdate::new(fact.tool_call_id.clone());
    if let Some(title) = &fact.title {
        update = update.title(title.clone());
    }
    if let Some(kind) = &fact.kind {
        update = update.kind(v2_kind(kind));
    }
    if let Some(status) = &fact.status {
        update = update.status(v2_tool_status(status));
    }
    if let Some(content) = &fact.content {
        update = update.content(content.iter().map(v2_tool_content).collect::<Vec<_>>());
    }
    if !fact.locations.is_empty() {
        update = update.locations(v2_tool_locations(&fact.locations));
    }
    if let Some(input) = &fact.raw_input {
        update = update.raw_input(input.clone());
    }
    if let Some(output) = &fact.raw_output {
        update = update.raw_output(output.clone());
    }
    if let Some(meta) = &fact.meta {
        update = update.meta(meta.clone());
    }
    v2::UpdateSessionNotification::new(
        v2_session_id(&fact.session_id),
        v2::SessionUpdate::ToolCallUpdate(update),
    )
}

/// The native v1 frame for a session metadata update.
pub fn session_info_v1(fact: &SessionInfoFact) -> v1::SessionNotification {
    let mut update = v1::SessionInfoUpdate::new();
    if let Some(title) = &fact.title {
        update = update.title(title.clone());
    }
    if let Some(updated_at) = &fact.updated_at {
        update = update.updated_at(updated_at.clone());
    }
    if let Some(meta) = &fact.meta {
        update = update.meta(meta.clone());
    }
    v1::SessionNotification::new(
        fact.session_id.clone(),
        v1::SessionUpdate::SessionInfoUpdate(update),
    )
}

/// The native v2 frame for a session metadata update.
pub fn session_info_v2(fact: &SessionInfoFact) -> v2::UpdateSessionNotification {
    let mut update = v2::SessionInfoUpdate::new();
    if let Some(title) = &fact.title {
        update = update.title(title.clone());
    }
    if let Some(updated_at) = &fact.updated_at {
        update = update.updated_at(updated_at.clone());
    }
    if let Some(meta) = &fact.meta {
        update = update.meta(meta.clone());
    }
    v2::UpdateSessionNotification::new(
        v2_session_id(&fact.session_id),
        v2::SessionUpdate::SessionInfoUpdate(update),
    )
}

/// The native v1 frame for a context / cost update.
pub fn usage_v1(fact: &UsageFact) -> v1::SessionNotification {
    let mut update = v1::UsageUpdate::new(fact.used, fact.size);
    if let Some(cost) = &fact.cost {
        update = update.cost(v1::Cost::new(cost.amount, cost.currency.clone()));
    }
    v1::SessionNotification::new(
        fact.session_id.clone(),
        v1::SessionUpdate::UsageUpdate(update),
    )
}

/// The native v2 frame for a context / cost update.
pub fn usage_v2(fact: &UsageFact) -> v2::UpdateSessionNotification {
    let mut update = v2::UsageUpdate::new(fact.used, fact.size);
    if let Some(cost) = &fact.cost {
        update = update.cost(v2::Cost::new(cost.amount, cost.currency.clone()));
    }
    v2::UpdateSessionNotification::new(
        v2_session_id(&fact.session_id),
        v2::SessionUpdate::UsageUpdate(update),
    )
}

/// The native v1 frame for the session's current mode.
///
/// v2 has no session modes (it expresses them as config options), so there is
/// deliberately no `mode_v2`: the connector sends nothing on a v2 connection.
pub fn mode_v1(fact: &ModeFact) -> v1::SessionNotification {
    v1::SessionNotification::new(
        fact.session_id.clone(),
        v1::SessionUpdate::CurrentModeUpdate(v1::CurrentModeUpdate::new(fact.mode_id.clone())),
    )
}

fn v1_category(category: &ConfigCategoryFact) -> v1::SessionConfigOptionCategory {
    match category {
        ConfigCategoryFact::Mode => v1::SessionConfigOptionCategory::Mode,
        ConfigCategoryFact::Model => v1::SessionConfigOptionCategory::Model,
        ConfigCategoryFact::ModelConfig => v1::SessionConfigOptionCategory::ModelConfig,
        ConfigCategoryFact::ThoughtLevel => v1::SessionConfigOptionCategory::ThoughtLevel,
        ConfigCategoryFact::Other(other) => v1::SessionConfigOptionCategory::Other(other.clone()),
    }
}

fn v2_category(category: &ConfigCategoryFact) -> v2::SessionConfigOptionCategory {
    match category {
        ConfigCategoryFact::Mode => v2::SessionConfigOptionCategory::Mode,
        ConfigCategoryFact::Model => v2::SessionConfigOptionCategory::Model,
        ConfigCategoryFact::ModelConfig => v2::SessionConfigOptionCategory::ModelConfig,
        ConfigCategoryFact::ThoughtLevel => v2::SessionConfigOptionCategory::ThoughtLevel,
        ConfigCategoryFact::Other(other) => v2::SessionConfigOptionCategory::Other(other.clone()),
    }
}

/// The native v1 config options.
pub fn config_options_v1(fact: &ConfigOptionsFact) -> Vec<v1::SessionConfigOption> {
    fact.options
        .iter()
        .map(|option| {
            let choices: Vec<v1::SessionConfigSelectOption> = option
                .choices
                .iter()
                .map(|choice| {
                    let mut built = v1::SessionConfigSelectOption::new(
                        choice.value.clone(),
                        choice.name.clone(),
                    );
                    if let Some(description) = &choice.description {
                        built = built.description(description.clone());
                    }
                    if let Some(meta) = &choice.meta {
                        built = built.meta(meta.clone());
                    }
                    built
                })
                .collect();
            let mut built = v1::SessionConfigOption::select(
                option.id.clone(),
                option.name.clone(),
                option.current_value.clone(),
                choices,
            );
            if let Some(description) = &option.description {
                built = built.description(description.clone());
            }
            if let Some(category) = &option.category {
                built = built.category(v1_category(category));
            }
            if let Some(meta) = &option.meta {
                built = built.meta(meta.clone());
            }
            built
        })
        .collect()
}

/// The native v2 config options.
///
/// v2 renamed the option key from `id` to `configId`; the rest of the shape is
/// the same, so it is built directly rather than converted.
pub fn config_options_v2(fact: &ConfigOptionsFact) -> Vec<v2::SessionConfigOption> {
    fact.options
        .iter()
        .map(|option| {
            let choices: Vec<v2::SessionConfigSelectOption> = option
                .choices
                .iter()
                .map(|choice| {
                    let mut built = v2::SessionConfigSelectOption::new(
                        choice.value.clone(),
                        choice.name.clone(),
                    );
                    if let Some(description) = &choice.description {
                        built = built.description(description.clone());
                    }
                    if let Some(meta) = &choice.meta {
                        built = built.meta(meta.clone());
                    }
                    built
                })
                .collect();
            let mut built = v2::SessionConfigOption::select(
                option.id.clone(),
                option.name.clone(),
                option.current_value.clone(),
                choices,
            );
            if let Some(description) = &option.description {
                built = built.description(description.clone());
            }
            if let Some(category) = &option.category {
                built = built.category(v2_category(category));
            }
            if let Some(meta) = &option.meta {
                built = built.meta(meta.clone());
            }
            built
        })
        .collect()
}

/// The native v1 frame for the session's config options.
pub fn config_options_update_v1(fact: &ConfigOptionsFact) -> v1::SessionNotification {
    v1::SessionNotification::new(
        fact.session_id.clone(),
        v1::SessionUpdate::ConfigOptionUpdate(v1::ConfigOptionUpdate::new(config_options_v1(fact))),
    )
}

/// The native v2 frame for the session's config options.
pub fn config_options_update_v2(fact: &ConfigOptionsFact) -> v2::UpdateSessionNotification {
    v2::UpdateSessionNotification::new(
        v2_session_id(&fact.session_id),
        v2::SessionUpdate::ConfigOptionUpdate(v2::ConfigOptionUpdate::new(config_options_v2(fact))),
    )
}

/// The native v1 frame for the session's available commands.
pub fn available_commands_v1(fact: &AvailableCommandsFact) -> v1::SessionNotification {
    let commands = fact
        .commands
        .iter()
        .map(|command| {
            let mut built =
                v1::AvailableCommand::new(command.name.clone(), command.description.clone());
            if let Some(hint) = &command.input_hint {
                built = built.input(v1::AvailableCommandInput::Unstructured(
                    v1::UnstructuredCommandInput::new(hint.clone()),
                ));
            }
            if let Some(meta) = &command.meta {
                built = built.meta(meta.clone());
            }
            built
        })
        .collect();
    v1::SessionNotification::new(
        fact.session_id.clone(),
        v1::SessionUpdate::AvailableCommandsUpdate(v1::AvailableCommandsUpdate::new(commands)),
    )
}

/// The native v2 frame for the session's available commands.
///
/// v2 renamed the unstructured input kind to `text`, and made the hint a typed
/// `TextCommandInput`; the command itself is built directly.
pub fn available_commands_v2(fact: &AvailableCommandsFact) -> v2::UpdateSessionNotification {
    let commands = fact
        .commands
        .iter()
        .map(|command| {
            let mut built =
                v2::AvailableCommand::new(command.name.clone(), command.description.clone());
            if let Some(hint) = &command.input_hint {
                built = built.input(v2::AvailableCommandInput::Text(v2::TextCommandInput::new(
                    hint.clone(),
                )));
            }
            if let Some(meta) = &command.meta {
                built = built.meta(meta.clone());
            }
            built
        })
        .collect();
    v2::UpdateSessionNotification::new(
        v2_session_id(&fact.session_id),
        v2::SessionUpdate::AvailableCommandsUpdate(v2::AvailableCommandsUpdate::new(commands)),
    )
}

/// The native v1 frame for a resource-link chunk.
pub fn link_chunk_v1(fact: &LinkChunkFact) -> v1::SessionNotification {
    let mut link = v1::ResourceLink::new(fact.name.clone(), fact.uri.clone());
    if let Some(mime_type) = &fact.mime_type {
        link = link.mime_type(mime_type.clone());
    }
    if let Some(title) = &fact.title {
        link = link.title(title.clone());
    }
    let mut chunk = v1::ContentChunk::new(v1::ContentBlock::ResourceLink(link));
    if let Some(message_id) = &fact.message_id {
        chunk = chunk.message_id(v1::MessageId::new(message_id.clone()));
    }
    v1::SessionNotification::new(
        fact.session_id.clone(),
        v1::SessionUpdate::AgentMessageChunk(chunk),
    )
}

/// The native v2 frame for a resource-link chunk.
///
/// v2 requires a `messageId`; a producer without one mints a fresh id, exactly
/// as the streamed-chunk renderer does.
pub fn link_chunk_v2(fact: &LinkChunkFact) -> v2::UpdateSessionNotification {
    let message_id = match &fact.message_id {
        Some(id) => id.clone(),
        None => mint_synthetic_message_id(),
    };
    let mut link = v2::ResourceLink::new(fact.name.clone(), fact.uri.clone());
    if let Some(mime_type) = &fact.mime_type {
        link = link.mime_type(mime_type.clone());
    }
    if let Some(title) = &fact.title {
        link = link.title(title.clone());
    }
    let chunk = v2::ContentChunk::new(
        v2::ContentBlock::ResourceLink(link),
        v2::MessageId::new(message_id),
    );
    v2::UpdateSessionNotification::new(
        v2_session_id(&fact.session_id),
        v2::SessionUpdate::AgentMessageChunk(chunk),
    )
}

// ---------------------------------------------------------------------------
// Permission requests
// ---------------------------------------------------------------------------

fn v1_permission_kind(kind: &PermissionOptionKindFact) -> v1::PermissionOptionKind {
    match kind {
        PermissionOptionKindFact::AllowOnce => v1::PermissionOptionKind::AllowOnce,
        PermissionOptionKindFact::AllowAlways => v1::PermissionOptionKind::AllowAlways,
        PermissionOptionKindFact::RejectOnce => v1::PermissionOptionKind::RejectOnce,
        PermissionOptionKindFact::RejectAlways => v1::PermissionOptionKind::RejectAlways,
        // v1 has no extension variant; rejecting is the safe reading of a kind
        // this adapter does not understand.
        PermissionOptionKindFact::Unknown(_) => v1::PermissionOptionKind::RejectOnce,
    }
}

fn v2_permission_kind(kind: &PermissionOptionKindFact) -> v2::PermissionOptionKind {
    match kind {
        PermissionOptionKindFact::AllowOnce => v2::PermissionOptionKind::AllowOnce,
        PermissionOptionKindFact::AllowAlways => v2::PermissionOptionKind::AllowAlways,
        PermissionOptionKindFact::RejectOnce => v2::PermissionOptionKind::RejectOnce,
        PermissionOptionKindFact::RejectAlways => v2::PermissionOptionKind::RejectAlways,
        PermissionOptionKindFact::Unknown(other) => v2::PermissionOptionKind::Other(other.clone()),
    }
}

/// The native v1 `session/request_permission` request.
///
/// v1 names the subject through a full `ToolCallUpdate`.
pub fn permission_request_v1(fact: &PermissionRequestFact) -> v1::RequestPermissionRequest {
    let tool_call = v1::ToolCallUpdate::new(
        fact.tool_call_id.clone(),
        v1::ToolCallUpdateFields::new()
            .kind(Some(v1::ToolKind::Other))
            .status(Some(v1::ToolCallStatus::Pending))
            .title(Some(fact.title.clone()))
            .raw_input(Some(fact.raw_input.clone())),
    );
    let options = fact
        .options
        .iter()
        .map(|option| {
            v1::PermissionOption::new(
                option.id.clone(),
                option.name.clone(),
                v1_permission_kind(&option.kind),
            )
        })
        .collect();
    let mut request =
        v1::RequestPermissionRequest::new(fact.session_id.clone(), tool_call, options);
    if let Some(meta) = &fact.meta {
        request = request.meta(meta.clone());
    }
    request
}

/// The native v2 `session/request_permission` request.
///
/// v2 lifted the title to the request itself and replaced the bare `toolCall`
/// with an optional typed `subject`; the prompt pi-acp bridges is a tool call.
pub fn permission_request_v2(fact: &PermissionRequestFact) -> v2::RequestPermissionRequest {
    let tool_call = v2::ToolCallUpdate::new(fact.tool_call_id.clone())
        .kind(v2::ToolKind::Other)
        .status(v2::ToolCallStatus::Pending)
        .title(fact.title.clone())
        .raw_input(fact.raw_input.clone());
    let options = fact
        .options
        .iter()
        .map(|option| {
            v2::PermissionOption::new(
                option.id.clone(),
                option.name.clone(),
                v2_permission_kind(&option.kind),
            )
        })
        .collect();
    let mut request = v2::RequestPermissionRequest::new(
        fact.session_id.0.to_string(),
        fact.title.clone(),
        options,
    )
    .subject(v2::RequestPermissionSubject::ToolCall(Box::new(
        v2::ToolCallPermissionSubject::new(tool_call),
    )));
    if let Some(meta) = &fact.meta {
        request = request.meta(meta.clone());
    }
    request
}

fn permission_outcome_from_v1(response: v1::RequestPermissionResponse) -> PermissionOutcomeFact {
    match response.outcome {
        v1::RequestPermissionOutcome::Selected(selected) => {
            PermissionOutcomeFact::Selected(selected.option_id.0.to_string())
        }
        _ => PermissionOutcomeFact::Cancelled,
    }
}

fn permission_outcome_from_v2(response: v2::RequestPermissionResponse) -> PermissionOutcomeFact {
    match response.outcome {
        v2::RequestPermissionOutcome::Selected(selected) => {
            PermissionOutcomeFact::Selected(selected.option_id.0.to_string())
        }
        // `Other` is a future outcome this version cannot act on; treating it
        // as a dismissal is the safe default.
        _ => PermissionOutcomeFact::Cancelled,
    }
}

/// Send one `session/request_permission` and await its answer.
///
/// Must run on a spawned task: `block_task` blocks the calling task on the
/// client's answer.
pub async fn send_permission_request(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &PermissionRequestFact,
) -> Result<PermissionOutcomeFact, AcpxError> {
    if protocol.is_v2() {
        let response: v2::RequestPermissionResponse = cx
            .send_request(permission_request_v2(fact))
            .block_task()
            .await
            .map_err(transport_error)?;
        Ok(permission_outcome_from_v2(response))
    } else {
        let response: v1::RequestPermissionResponse = cx
            .send_request(permission_request_v1(fact))
            .block_task()
            .await
            .map_err(transport_error)?;
        Ok(permission_outcome_from_v1(response))
    }
}

// ---------------------------------------------------------------------------
// Per-protocol dispatch (for callers outside the outbound connector)
// ---------------------------------------------------------------------------

/// Send a session metadata update on the connection's protocol.
pub fn send_session_info(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &SessionInfoFact,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        send_v2(cx, session_info_v2(fact))
    } else {
        send_v1(cx, session_info_v1(fact))
    }
}

/// Send a context / cost update on the connection's protocol.
pub fn send_usage(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &UsageFact,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        send_v2(cx, usage_v2(fact))
    } else {
        send_v1(cx, usage_v1(fact))
    }
}

/// Send the session's current mode. v2 has none, so this is a no-op there.
pub fn send_mode(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &ModeFact,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        Ok(())
    } else {
        send_v1(cx, mode_v1(fact))
    }
}

/// Send the session's config options on the connection's protocol.
pub fn send_config_options(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &ConfigOptionsFact,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        send_v2(cx, config_options_update_v2(fact))
    } else {
        send_v1(cx, config_options_update_v1(fact))
    }
}

/// Send the session's available commands on the connection's protocol.
pub fn send_available_commands(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &AvailableCommandsFact,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        send_v2(cx, available_commands_v2(fact))
    } else {
        send_v1(cx, available_commands_v1(fact))
    }
}

/// Send a non-bash tool call on the connection's protocol.
pub fn send_tool_call(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &ToolCallFact,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        send_v2(cx, tool_call_v2(fact))
    } else {
        send_v1(cx, tool_call_v1(fact))
    }
}

/// Send a streamed text chunk on the connection's protocol.
pub fn send_text_chunk(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    chunk: &TextChunk,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        send_v2(cx, text_chunk_v2(chunk))
    } else {
        send_v1(cx, text_chunk_v1(chunk))
    }
}

/// Send a resource-link chunk on the connection's protocol.
pub fn send_link_chunk(
    cx: &ConnectionTo<Client>,
    protocol: Protocol,
    fact: &LinkChunkFact,
) -> Result<(), AcpxError> {
    if protocol.is_v2() {
        send_v2(cx, link_chunk_v2(fact))
    } else {
        send_v1(cx, link_chunk_v1(fact))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::BashToolStatus as S;

    fn call(first: bool, status: S) -> BashToolCall {
        BashToolCall {
            session_id: v1::SessionId::new("s1"),
            tool_call_id: "t1".to_owned(),
            command: "ls -la".to_owned(),
            cwd: Some("/work".to_owned()),
            status,
            output: String::new(),
            output_delta: String::new(),
            exit_code: None,
            first,
        }
    }

    /// v1 keeps its embedded client terminal and `terminal_info` meta exactly.
    #[test]
    fn v1_open_embeds_the_client_terminal() {
        let frames = bash_v1_frames(&call(true, S::Pending));
        let v1::SessionUpdate::ToolCall(tool_call) = &frames[0].update else {
            panic!("a bash call opens as a tool_call on v1");
        };
        assert_eq!(tool_call.kind, v1::ToolKind::Execute);
        assert!(matches!(
            tool_call.content.first(),
            Some(v1::ToolCallContent::Terminal(_))
        ));
        assert_eq!(
            tool_call.meta.as_ref().unwrap()["terminal_info"]["terminal_id"],
            "t1"
        );
    }

    /// Later v1 frames are status + output/exit patches.
    #[test]
    fn v1_output_and_exit_are_meta_updates() {
        let mut c = call(false, S::Completed);
        c.output_delta = "done\n".to_owned();
        c.output = "done\n".to_owned();
        c.exit_code = Some(0);
        let frames = bash_v1_frames(&c);
        let v1::SessionUpdate::ToolCallUpdate(update) = &frames[0].update else {
            panic!("a bash continuation is a tool_call_update on v1");
        };
        let meta = update.meta.as_ref().expect("terminal meta");
        assert_eq!(meta["terminal_output"]["data"], "done\n");
        assert_eq!(meta["terminal_exit"]["exit_code"], 0);
    }

    /// The v2 opening frame names the call itself, because v2 dropped the
    /// client-owned terminal v1 embeds.
    #[test]
    fn v2_open_names_the_call_and_never_embeds_a_v1_terminal() {
        let frames = bash_v2_frames(&call(true, S::InProgress));
        let v2::SessionUpdate::ToolCallUpdate(update) = &frames[0].update else {
            panic!("a bash call is a tool_call_update on v2");
        };
        assert_eq!(update.kind.value(), Some(&v2::ToolKind::Execute));
        assert_eq!(update.title.value().map(String::as_str), Some("ls -la"));
        assert_eq!(
            update.raw_input.value().and_then(|v| v.get("command")),
            Some(&json!("ls -la"))
        );
    }

    /// v2 streams the terminal through `_meta`, which is what the client reads.
    #[test]
    fn v2_output_and_exit_travel_as_meta() {
        let mut c = call(true, S::Completed);
        c.output_delta = "done\n".to_owned();
        c.output = "done\n".to_owned();
        c.exit_code = Some(0);
        let frames = bash_v2_frames(&c);
        let v2::SessionUpdate::ToolCallUpdate(update) = &frames[0].update else {
            panic!("a bash call is a tool_call_update on v2");
        };
        let meta = update.meta.value().expect("terminal meta");
        assert_eq!(meta["terminal_output"]["data"], "done\n");
        assert_eq!(meta["terminal_exit"]["exit_code"], 0);
    }

    /// The bug this module exists to fix, pinned as a regression.
    ///
    /// The old path built a v1 `tool_call` with `ToolCallContent::Terminal` and
    /// handed it to the v1→v2 converter, which **refuses** that content — v2
    /// deleted the client-owned terminal — so the whole frame was dropped and
    /// every replayed bash call reached the client as `other` with no result.
    /// The native v2 frame never takes that path.
    #[test]
    fn the_old_v1_bash_frame_could_not_be_converted_but_the_native_one_needs_no_conversion() {
        use agent_client_protocol_schema::v2::conversion::try_v1_to_v2;

        // What the old code produced, and what the converter did to it.
        let legacy = bash_v1_frames(&call(true, S::InProgress)).remove(0);
        let converted: Result<v2::UpdateSessionNotification, _> = try_v1_to_v2(legacy);
        assert!(
            converted.is_err(),
            "v1 terminal content has no v2 form — this refusal is the bug"
        );

        // What the module produces now: already v2, so nothing can drop it.
        let native = bash_v2_frames(&call(true, S::InProgress));
        assert_eq!(native.len(), 1);
        let serialized = serde_json::to_value(&native[0]).expect("native v2 frame serializes");
        assert_eq!(serialized["update"]["sessionUpdate"], "tool_call_update");
        assert_eq!(serialized["update"]["kind"], "execute");
        assert_eq!(serialized["update"]["title"], "ls -la");
    }

    // --- the remaining renderers ---

    use crate::session::{
        AvailableCommandFact, ConfigCategoryFact, ConfigChoiceFact, ConfigOptionFact,
        PermissionOptionFact,
    };

    fn tool_fact() -> ToolCallFact {
        ToolCallFact {
            session_id: v1::SessionId::new("s1"),
            tool_call_id: "t9".to_owned(),
            first: true,
            title: Some("read file".to_owned()),
            kind: Some(ToolKindFact::Read),
            status: Some(ToolStatusFact::InProgress),
            content: None,
            locations: Vec::new(),
            raw_input: Some(json!({"path": "/tmp/a"})),
            raw_output: None,
            meta: None,
        }
    }

    /// v1 opens a non-bash call with a full `tool_call`; v2 has only the
    /// patch-style `tool_call_update`, so one fact renders two shapes.
    #[test]
    fn a_non_bash_tool_call_is_tool_call_on_v1_and_tool_call_update_on_v2() {
        let fact = tool_fact();
        let v1_frame = serde_json::to_value(tool_call_v1(&fact)).unwrap();
        assert_eq!(v1_frame["update"]["sessionUpdate"], "tool_call");
        assert_eq!(v1_frame["update"]["kind"], "read");
        assert_eq!(v1_frame["update"]["title"], "read file");

        let v2_frame = serde_json::to_value(tool_call_v2(&fact)).unwrap();
        assert_eq!(v2_frame["update"]["sessionUpdate"], "tool_call_update");
        assert_eq!(v2_frame["update"]["kind"], "read");
        assert_eq!(v2_frame["update"]["rawInput"]["path"], "/tmp/a");
    }

    /// v2's structured diff carries a Git patch; v1's carries old/new text.
    #[test]
    fn a_file_edit_diff_takes_each_protocols_own_shape() {
        let mut fact = tool_fact();
        fact.content = Some(vec![ToolContentFact::Diff {
            path: "/tmp/a.txt".to_owned(),
            new_text: "new\n".to_owned(),
            old_text: Some("old\n".to_owned()),
        }]);

        let v1_frame = serde_json::to_value(tool_call_v1(&fact)).unwrap();
        let v1_diff = &v1_frame["update"]["content"][0];
        assert_eq!(v1_diff["type"], "diff");
        assert_eq!(v1_diff["newText"], "new\n");
        assert_eq!(v1_diff["oldText"], "old\n");

        let v2_frame = serde_json::to_value(tool_call_v2(&fact)).unwrap();
        let v2_diff = &v2_frame["update"]["content"][0];
        assert_eq!(v2_diff["type"], "diff");
        assert_eq!(v2_diff["changes"][0]["operation"], "modify");
        assert_eq!(v2_diff["changes"][0]["path"], "/tmp/a.txt");
        assert!(v2_diff["patch"]["text"]
            .as_str()
            .unwrap()
            .contains("diff --git"));
    }

    /// v1's config option key is `id`; v2 renamed it to `configId`.
    #[test]
    fn config_options_use_each_protocols_key() {
        let fact = ConfigOptionsFact {
            session_id: v1::SessionId::new("s1"),
            options: vec![ConfigOptionFact {
                id: "model".to_owned(),
                name: "Model".to_owned(),
                description: None,
                category: Some(ConfigCategoryFact::Model),
                current_value: "p/m".to_owned(),
                choices: vec![ConfigChoiceFact {
                    value: "p/m".to_owned(),
                    name: "m".to_owned(),
                    description: None,
                    meta: None,
                }],
                meta: None,
            }],
        };

        let v1_frame = serde_json::to_value(config_options_update_v1(&fact)).unwrap();
        assert_eq!(v1_frame["update"]["configOptions"][0]["id"], "model");

        let v2_frame = serde_json::to_value(config_options_update_v2(&fact)).unwrap();
        assert_eq!(v2_frame["update"]["configOptions"][0]["configId"], "model");
        assert!(v2_frame["update"]["configOptions"][0].get("id").is_none());
    }

    /// v2 tagged the unstructured command input `text`; v1 left it untagged.
    #[test]
    fn command_input_hint_takes_each_protocols_tag() {
        let fact = AvailableCommandsFact {
            session_id: v1::SessionId::new("s1"),
            commands: vec![AvailableCommandFact {
                name: "compact".to_owned(),
                description: "d".to_owned(),
                input_hint: Some("instructions".to_owned()),
                meta: None,
            }],
        };

        let v1_frame = serde_json::to_value(available_commands_v1(&fact)).unwrap();
        let v1_input = &v1_frame["update"]["availableCommands"][0]["input"];
        assert_eq!(v1_input["hint"], "instructions");
        assert!(v1_input.get("type").is_none());

        let v2_frame = serde_json::to_value(available_commands_v2(&fact)).unwrap();
        assert_eq!(
            v2_frame["update"]["availableCommands"][0]["input"]["type"],
            "text"
        );
    }

    /// v1 has `current_mode_update`; v2 removed session modes entirely.
    #[test]
    fn current_mode_is_v1_only() {
        let fact = ModeFact {
            session_id: v1::SessionId::new("s1"),
            mode_id: "medium".to_owned(),
        };
        let frame = serde_json::to_value(mode_v1(&fact)).unwrap();
        assert_eq!(frame["update"]["sessionUpdate"], "current_mode_update");
        assert_eq!(frame["update"]["currentModeId"], "medium");
    }

    /// Permission requests: v1 names the subject through `toolCall`, v2 lifted
    /// the title and uses a typed `subject`.
    #[test]
    fn permission_requests_take_each_protocols_shape() {
        let fact = PermissionRequestFact {
            session_id: v1::SessionId::new("s1"),
            tool_call_id: "pi-ui-1".to_owned(),
            title: "Pick".to_owned(),
            raw_input: json!({"options": ["a"]}),
            options: vec![PermissionOptionFact {
                id: "choice-0".to_owned(),
                name: "a".to_owned(),
                kind: PermissionOptionKindFact::AllowOnce,
            }],
            meta: None,
        };

        let v1_request = serde_json::to_value(permission_request_v1(&fact)).unwrap();
        assert_eq!(v1_request["toolCall"]["title"], "Pick");
        assert_eq!(v1_request["options"][0]["optionId"], "choice-0");

        let v2_request = serde_json::to_value(permission_request_v2(&fact)).unwrap();
        assert_eq!(v2_request["title"], "Pick");
        assert_eq!(v2_request["subject"]["type"], "tool_call");
        assert_eq!(v2_request["subject"]["toolCall"]["toolCallId"], "pi-ui-1");
        assert!(v2_request.get("toolCall").is_none());
    }

    /// A resource-link chunk always carries a message id on v2.
    #[test]
    fn a_link_chunk_is_tagged_with_a_message_id_on_v2() {
        let fact = LinkChunkFact {
            session_id: v1::SessionId::new("s1"),
            message_id: None,
            name: "f.html".to_owned(),
            uri: "file:///tmp/f.html".to_owned(),
            mime_type: Some("text/html".to_owned()),
            title: Some("Session exported".to_owned()),
        };

        let v2_frame = serde_json::to_value(link_chunk_v2(&fact)).unwrap();
        assert_eq!(v2_frame["update"]["sessionUpdate"], "agent_message_chunk");
        assert!(v2_frame["update"]["messageId"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));
        assert_eq!(v2_frame["update"]["content"]["type"], "resource_link");

        let v1_frame = serde_json::to_value(link_chunk_v1(&fact)).unwrap();
        assert!(v1_frame["update"].get("messageId").is_none());
    }
}
