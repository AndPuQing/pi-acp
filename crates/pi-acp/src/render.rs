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
//! Only bash is rendered here so far. The remaining updates still travel the v1
//! path, and are moved here one at a time — each time deleting the v1↔v2
//! conversion it replaces — until the conversion layer is gone and the
//! `protocol-v2` feature can be dropped for good.

use crate::session::BashToolCall;
use crate::session::{MessagePatch, TextChunk, TextChunkKind};
use crate::translate::bash::{
    bash_terminal_content, bash_terminal_exit_meta, bash_terminal_info_meta,
    bash_terminal_output_meta,
};
use agent_client_protocol::schema::v1;

#[cfg(feature = "protocol-v2")]
use crate::session::BashToolStatus;
#[cfg(feature = "protocol-v2")]
use agent_client_protocol_schema::v2;
#[cfg(feature = "protocol-v2")]
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
#[cfg(feature = "protocol-v2")]
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
#[cfg(feature = "protocol-v2")]
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
#[cfg(feature = "protocol-v2")]
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

#[cfg(feature = "protocol-v2")]
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
#[cfg(feature = "protocol-v2")]
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
#[cfg(feature = "protocol-v2")]
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
#[cfg(feature = "protocol-v2")]
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

/// The neutral facts of a complete-message patch are [`MessagePatch`] itself.

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
    #[cfg(feature = "protocol-v2")]
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
    #[cfg(feature = "protocol-v2")]
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
    #[cfg(feature = "protocol-v2")]
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
}
