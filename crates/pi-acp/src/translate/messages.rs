//! pi message -> ACP content (history replay).
//!
//! Ports `acp/translate/pi-messages.ts` (the two text normalizers) and the
//! pure part of the `session/load` history replay loop in `acp/agent.ts`
//! (user / assistant / toolResult message classification). S6 (W-453) turns
//! [`ReplayMessage`] into ACP `session/update` notifications.

use serde_json::Value;

use super::bash::{bash_command, bash_result_text, is_bash_tool};
use super::tools::tool_result_to_text;

/// Normalize a pi user-message `content` to plain text.
///
/// pi user content is usually a string; when it is an array of content blocks,
/// only `{type:"text", text}` blocks are joined (mirrors
/// `normalizePiMessageText`). Anything else yields `""`.
pub fn normalize_pi_message_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(blocks) => join_text_blocks(blocks),
        _ => String::new(),
    }
}

/// Normalize a pi assistant-message `content` to plain text.
///
/// Assistant content is an array of blocks (text / thinking / tool calls);
/// only text blocks are replayed for the MVP (mirrors
/// `normalizePiAssistantText`).
pub fn normalize_pi_assistant_text(content: &Value) -> String {
    match content {
        Value::Array(blocks) => join_text_blocks(blocks),
        _ => String::new(),
    }
}

/// A single pi history message mapped to its ACP replay shape.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplayMessage {
    /// `role: "user"` — replayed as a `user_message_chunk`.
    UserText(String),
    /// `role: "assistant"` — replayed as an `agent_message_chunk`.
    AssistantText(String),
    /// `role: "toolResult"` — replayed as a synthetic tool call + update.
    ToolResult(ToolResultReplay),
}

/// Replay payload for a pi `toolResult` history message (mirrors the TS
/// `session/load` loop in `acp/agent.ts`).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResultReplay {
    /// pi tool name (`read` / `edit` / `bash` / ...).
    pub tool_name: String,
    /// pi's own tool-call id when the session file records one; the caller
    /// supplies a fresh id when absent (TS uses `crypto.randomUUID()`).
    pub tool_call_id: Option<String>,
    pub is_error: bool,
    /// Bash results are rendered as ACP terminals (content + `_meta`); other
    /// tools as a synthetic tool call with text content.
    pub is_bash: bool,
    /// Human-readable title (`bashCommand(...) ?? toolName` for bash).
    pub title: String,
    /// Text to surface (`bashResultText` for bash, `toolResultToText` otherwise).
    pub text: String,
    /// The raw pi message (kept for `rawOutput` parity).
    pub raw: Value,
}

/// Classify and normalize one pi history message into its ACP replay shape.
///
/// Mirrors the `session/load` replay loop in TS `acp/agent.ts`:
/// - `user` / `assistant` with no replayable text yield `None` (skipped);
/// - `toolResult` always yields a replay (bash vs. other shapes differ).
pub fn replay_message(message: &Value) -> Option<ReplayMessage> {
    let role = message.get("role").and_then(Value::as_str)?;
    let content = message.get("content").unwrap_or(&Value::Null);

    match role {
        "user" => {
            let text = normalize_pi_message_text(content);
            if text.is_empty() {
                None
            } else {
                Some(ReplayMessage::UserText(text))
            }
        }
        "assistant" => {
            let text = normalize_pi_assistant_text(content);
            if text.is_empty() {
                None
            } else {
                Some(ReplayMessage::AssistantText(text))
            }
        }
        "toolResult" => {
            let tool_name = message
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_string();
            let tool_call_id = message
                .get("toolCallId")
                .and_then(Value::as_str)
                .map(str::to_string);
            let is_error = message
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            // In pi session files the tool result record sits directly on the
            // message (`content`/`details` are top-level fields), so the whole
            // message is what the TS reference passes to the result readers.
            let is_bash = is_bash_tool(&tool_name);

            let (title, text) = if is_bash {
                (
                    bash_command(message).unwrap_or_else(|| tool_name.clone()),
                    bash_result_text(message),
                )
            } else {
                (tool_name.clone(), tool_result_to_text(message))
            };

            Some(ReplayMessage::ToolResult(ToolResultReplay {
                tool_name,
                tool_call_id,
                is_error,
                is_bash,
                title,
                text,
                raw: message.clone(),
            }))
        }
        _ => None,
    }
}

fn join_text_blocks(blocks: &[Value]) -> String {
    blocks
        .iter()
        .filter_map(|c| {
            if c.get("type").and_then(Value::as_str) == Some("text") {
                c.get("text").and_then(Value::as_str)
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalize_message_text_supports_string() {
        assert_eq!(normalize_pi_message_text(&json!("hello")), "hello");
    }

    #[test]
    fn normalize_message_text_joins_text_blocks() {
        let content = json!([
            { "type": "text", "text": "a" },
            { "type": "text", "text": "b" },
            { "type": "not_text", "x": 1 }
        ]);
        assert_eq!(normalize_pi_message_text(&content), "ab");
    }

    #[test]
    fn normalize_message_text_edge_cases() {
        assert_eq!(normalize_pi_message_text(&Value::Null), "");
        assert_eq!(normalize_pi_message_text(&json!(42)), "");
        assert_eq!(normalize_pi_message_text(&json!([])), "");
        // non-string text blocks are skipped
        assert_eq!(
            normalize_pi_message_text(&json!([{ "type": "text", "text": 7 }])),
            ""
        );
    }

    #[test]
    fn normalize_assistant_text_joins_only_text_blocks() {
        let content = json!([
            { "type": "text", "text": "hi" },
            { "type": "thinking", "text": "..." },
            { "type": "text", "text": "!" }
        ]);
        assert_eq!(normalize_pi_assistant_text(&content), "hi!");
    }

    #[test]
    fn normalize_assistant_text_non_array_is_empty() {
        assert_eq!(normalize_pi_assistant_text(&json!("str")), "");
        assert_eq!(normalize_pi_assistant_text(&Value::Null), "");
    }

    #[test]
    fn replay_user_message() {
        let msg = json!({ "role": "user", "content": "hi there" });
        assert_eq!(
            replay_message(&msg),
            Some(ReplayMessage::UserText("hi there".into()))
        );
    }

    #[test]
    fn replay_user_message_skips_empty() {
        let msg = json!({ "role": "user", "content": "" });
        assert_eq!(replay_message(&msg), None);
        let msg = json!({ "role": "user", "content": [] });
        assert_eq!(replay_message(&msg), None);
    }

    #[test]
    fn replay_assistant_message_joins_text() {
        let msg = json!({
            "role": "assistant",
            "content": [
                { "type": "thinking", "text": "hmm" },
                { "type": "text", "text": "answer" }
            ]
        });
        assert_eq!(
            replay_message(&msg),
            Some(ReplayMessage::AssistantText("answer".into()))
        );
    }

    #[test]
    fn replay_tool_result_non_bash() {
        let msg = json!({
            "role": "toolResult",
            "toolName": "edit",
            "toolCallId": "tc-1",
            "isError": false,
            "content": [{ "type": "text", "text": "ok" }]
        });
        match replay_message(&msg) {
            Some(ReplayMessage::ToolResult(r)) => {
                assert_eq!(r.tool_name, "edit");
                assert_eq!(r.tool_call_id.as_deref(), Some("tc-1"));
                assert!(!r.is_error);
                assert!(!r.is_bash);
                assert_eq!(r.title, "edit");
                assert_eq!(r.text, "ok");
            }
            other => panic!("expected tool result replay, got {other:?}"),
        }
    }

    #[test]
    fn replay_tool_result_prefers_details_diff() {
        let msg = json!({
            "role": "toolResult",
            "toolName": "edit",
            "content": [{ "type": "text", "text": "done" }],
            "details": { "diff": "--- a\n+++ b\n" }
        });
        match replay_message(&msg) {
            Some(ReplayMessage::ToolResult(r)) => {
                assert_eq!(r.text, "--- a\n+++ b\n");
            }
            other => panic!("expected tool result replay, got {other:?}"),
        }
    }

    #[test]
    fn replay_tool_result_bash_uses_terminal_text_and_command_title() {
        let msg = json!({
            "role": "toolResult",
            "toolName": "bash",
            "toolCallId": "tc-b",
            "isError": true,
            "content": [{ "type": "text", "text": "boom" }],
            "details": { "exitCode": 1 }
        });
        match replay_message(&msg) {
            Some(ReplayMessage::ToolResult(r)) => {
                assert!(r.is_bash);
                assert!(r.is_error);
                // no command in content => title falls back to tool name
                assert_eq!(r.title, "bash");
                assert_eq!(r.text, "boom");
            }
            other => panic!("expected tool result replay, got {other:?}"),
        }
    }

    #[test]
    fn replay_tool_result_bash_command_title() {
        let msg = json!({
            "role": "toolResult",
            "toolName": "Bash",
            "content": [{ "type": "text", "text": "out" }],
            "details": { "command": "ls -la" }
        });
        match replay_message(&msg) {
            Some(ReplayMessage::ToolResult(r)) => {
                assert!(r.is_bash);
                assert_eq!(r.title, "ls -la");
            }
            other => panic!("expected tool result replay, got {other:?}"),
        }
    }

    #[test]
    fn replay_unknown_role_is_none() {
        assert_eq!(
            replay_message(&json!({ "role": "system", "content": "x" })),
            None
        );
        assert_eq!(replay_message(&json!({ "content": "x" })), None);
        assert_eq!(replay_message(&Value::Null), None);
    }

    #[test]
    fn replay_tool_result_always_yields_even_without_text() {
        let msg = json!({ "role": "toolResult", "toolName": "read" });
        match replay_message(&msg) {
            Some(ReplayMessage::ToolResult(r)) => {
                // No content/diff/stdout: toolResultToText falls back to the
                // pretty-printed message itself (TS parity — the client sees
                // the raw result rather than nothing).
                assert_eq!(r.text, serde_json::to_string_pretty(&msg).unwrap());
                assert_eq!(r.tool_call_id, None);
            }
            other => panic!("expected tool result replay, got {other:?}"),
        }
    }
}
