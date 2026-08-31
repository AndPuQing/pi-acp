//! bash tool -> ACP terminal content + `_meta`.
//!
//! Ports `acp/translate/bash.ts`. Zed renders ACP `execute` tools as
//! display-only terminals when the tool call carries `Terminal` content plus
//! the `terminal_info` / `terminal_output` / `terminal_exit` `_meta` values
//! built here (see the ACP execute tool schema).

use agent_client_protocol::schema::v1::{Meta, Terminal, TerminalId, ToolCallContent};
use serde_json::{json, Value};

/// Whether a pi tool name is the bash tool (case-insensitive).
pub fn is_bash_tool(tool_name: &str) -> bool {
    tool_name.eq_ignore_ascii_case("bash")
}

/// The human-readable command of a bash tool call, from any of the argument
/// container shapes pi uses (`command`/`cmd` at the top level or nested under
/// `args`/`input`/`rawInput`/`toolInput`/`details`).
pub fn bash_command(value: &Value) -> Option<String> {
    for key in ["command", "cmd"] {
        if let Some(cmd) = string_field(value, key) {
            return Some(cmd);
        }
    }
    for container in ["args", "input", "rawInput", "toolInput", "details"] {
        if let Some(nested) = value.get(container) {
            for key in ["command", "cmd"] {
                if let Some(cmd) = string_field(nested, key) {
                    return Some(cmd);
                }
            }
        }
    }
    None
}

/// Text to stream into the bash terminal for a tool result: content text
/// blocks first, then `stdout`/`stderr` from `details` or the top level.
pub fn bash_result_text(result: &Value) -> String {
    let content = result.get("content");
    if let Some(Value::Array(blocks)) = content {
        let texts: Vec<&str> = blocks
            .iter()
            .filter_map(|c| {
                if c.get("type").and_then(Value::as_str) == Some("text") {
                    c.get("text").and_then(Value::as_str)
                } else {
                    None
                }
            })
            .collect();
        if !texts.is_empty() {
            return texts.join("");
        }
    }

    let details = result.get("details");
    let stdout = string_chain(details, result, &["stdout"])
        .or_else(|| string_chain(details, result, &["output"]));
    let stderr = string_chain(details, result, &["stderr"]);

    [stdout, stderr]
        .into_iter()
        .flatten()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Exit code for a bash result; falls back to `isError ? 1 : 0` when pi does
/// not report one.
pub fn bash_exit_code(result: &Value, is_error: bool) -> i32 {
    let details = result.get("details");
    let code = number_chain(details, result, &["exitCode"])
        .or_else(|| number_chain(details, result, &["code"]));
    match code {
        Some(code) => code as i32,
        None => {
            if is_error {
                1
            } else {
                0
            }
        }
    }
}

/// Streaming delta from `previous` accumulated output to `next`; falls back to
/// the full `next` when the output was reset (non-prefix).
pub fn bash_output_delta(previous: &str, next: &str) -> String {
    next.strip_prefix(previous).unwrap_or(next).to_string()
}

/// ACP tool-call content embedding the terminal by the tool-call id.
pub fn bash_terminal_content(tool_call_id: &str) -> Vec<ToolCallContent> {
    vec![ToolCallContent::Terminal(Terminal::new(TerminalId::new(
        tool_call_id,
    )))]
}

/// `_meta` value announcing the terminal for a bash tool call.
pub fn bash_terminal_info_meta(tool_call_id: &str, cwd: &str) -> Meta {
    json!({
        "terminal_info": { "terminal_id": tool_call_id, "cwd": cwd }
    })
    .as_object()
    .expect("static terminal_info meta is an object")
    .clone()
}

/// `_meta` value streaming an output delta into the terminal.
pub fn bash_terminal_output_meta(tool_call_id: &str, data: &str) -> Meta {
    json!({
        "terminal_output": { "terminal_id": tool_call_id, "data": data }
    })
    .as_object()
    .expect("static terminal_output meta is an object")
    .clone()
}

/// `_meta` value closing the terminal with an exit code.
pub fn bash_terminal_exit_meta(tool_call_id: &str, exit_code: i32) -> Meta {
    json!({
        "terminal_exit": {
            "terminal_id": tool_call_id,
            "exit_code": exit_code,
            "signal": null
        }
    })
    .as_object()
    .expect("static terminal_exit meta is an object")
    .clone()
}

fn string_field(record: &Value, key: &str) -> Option<String> {
    record
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
}

fn string_chain(details: Option<&Value>, record: &Value, keys: &[&str]) -> Option<String> {
    for c in [details, Some(record)].into_iter().flatten() {
        for key in keys {
            if let Some(s) = c.get(*key).and_then(Value::as_str) {
                return Some(s.to_string());
            }
        }
    }
    None
}

fn number_chain(details: Option<&Value>, record: &Value, keys: &[&str]) -> Option<i64> {
    for c in [details, Some(record)].into_iter().flatten() {
        for key in keys {
            if let Some(n) = c.get(*key).and_then(Value::as_i64) {
                return Some(n);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_bash_tool_is_case_insensitive() {
        assert!(is_bash_tool("bash"));
        assert!(is_bash_tool("Bash"));
        assert!(is_bash_tool("BASH"));
        assert!(!is_bash_tool("read"));
        assert!(!is_bash_tool("bashish"));
    }

    #[test]
    fn bash_command_extracts_from_all_shapes() {
        assert_eq!(
            bash_command(&json!({ "command": "ls -la" })),
            Some("ls -la".into())
        );
        assert_eq!(bash_command(&json!({ "cmd": "pwd" })), Some("pwd".into()));
        assert_eq!(
            bash_command(&json!({ "args": { "command": "git status" } })),
            Some("git status".into())
        );
        assert_eq!(
            bash_command(&json!({ "input": { "cmd": "npm test" } })),
            Some("npm test".into())
        );
        assert_eq!(
            bash_command(&json!({ "rawInput": { "command": "cargo build" } })),
            Some("cargo build".into())
        );
        assert_eq!(
            bash_command(&json!({ "toolInput": { "command": "make" } })),
            Some("make".into())
        );
        assert_eq!(
            bash_command(&json!({ "details": { "cmd": "ls" } })),
            Some("ls".into())
        );
    }

    #[test]
    fn bash_command_ignores_non_string_or_blank() {
        assert_eq!(bash_command(&json!({ "command": 42 })), None);
        assert_eq!(bash_command(&json!({ "command": "   " })), None);
        assert_eq!(bash_command(&json!({ "cmd": null })), None);
        assert_eq!(bash_command(&json!(null)), None);
        assert_eq!(bash_command(&json!("plain string")), None);
        // top-level wins over nested
        assert_eq!(
            bash_command(&json!({ "command": "top", "args": { "command": "nested" } })),
            Some("top".into())
        );
    }

    #[test]
    fn bash_result_text_joins_content_blocks() {
        let text = bash_result_text(&json!({
            "content": [
                { "type": "text", "text": "out1" },
                { "type": "text", "text": "out2" }
            ]
        }));
        assert_eq!(text, "out1out2");
    }

    #[test]
    fn bash_result_text_prefers_details_over_top_level() {
        let text = bash_result_text(&json!({
            "stdout": "top",
            "details": { "stdout": "nested" }
        }));
        assert_eq!(text, "nested");
    }

    #[test]
    fn bash_result_text_combines_stdout_and_stderr() {
        let text = bash_result_text(&json!({
            "details": { "stdout": "hello", "stderr": "warn" }
        }));
        assert_eq!(text, "hello\nwarn");
    }

    #[test]
    fn bash_result_text_supports_output_alias() {
        let text = bash_result_text(&json!({
            "details": { "output": "alias-out" }
        }));
        assert_eq!(text, "alias-out");
    }

    #[test]
    fn bash_result_text_empty_for_no_text() {
        assert_eq!(bash_result_text(&Value::Null), "");
        assert_eq!(bash_result_text(&json!({})), "");
        assert_eq!(bash_result_text(&json!([])), "");
        // empty strings are dropped
        assert_eq!(
            bash_result_text(&json!({ "details": { "stdout": "", "stderr": "" } })),
            ""
        );
    }

    #[test]
    fn bash_exit_code_priority_and_fallback() {
        assert_eq!(
            bash_exit_code(&json!({ "details": { "exitCode": 0 } }), false),
            0
        );
        assert_eq!(bash_exit_code(&json!({ "exitCode": 2 }), false), 2);
        assert_eq!(
            bash_exit_code(&json!({ "details": { "code": 3 } }), false),
            3
        );
        assert_eq!(bash_exit_code(&json!({ "code": 4 }), false), 4);
        // negative (killed by signal) is preserved
        assert_eq!(
            bash_exit_code(&json!({ "details": { "exitCode": -9 } }), false),
            -9
        );
        // fallback
        assert_eq!(bash_exit_code(&json!({}), false), 0);
        assert_eq!(bash_exit_code(&json!({}), true), 1);
        assert_eq!(bash_exit_code(&json!({ "exitCode": "0" }), true), 1);
    }

    #[test]
    fn bash_output_delta_prefix_and_reset() {
        assert_eq!(bash_output_delta("hello", "hello world"), " world");
        assert_eq!(bash_output_delta("hello", "goodbye"), "goodbye");
        assert_eq!(bash_output_delta("", "fresh"), "fresh");
        assert_eq!(bash_output_delta("same", "same"), "");
    }

    #[test]
    fn bash_terminal_content_embeds_terminal_by_id() {
        let content = bash_terminal_content("tc-9");
        assert_eq!(content.len(), 1);
        match &content[0] {
            ToolCallContent::Terminal(t) => {
                assert_eq!(t.terminal_id.0.as_ref(), "tc-9");
            }
            other => panic!("expected terminal content, got {other:?}"),
        }
    }

    #[test]
    fn bash_terminal_meta_shapes_match_zed_contract() {
        let info = bash_terminal_info_meta("tc-1", "/work");
        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            json!({ "terminal_info": { "terminal_id": "tc-1", "cwd": "/work" } })
        );

        let out = bash_terminal_output_meta("tc-1", "delta");
        assert_eq!(
            serde_json::to_value(&out).unwrap(),
            json!({ "terminal_output": { "terminal_id": "tc-1", "data": "delta" } })
        );

        let exit = bash_terminal_exit_meta("tc-1", 5);
        assert_eq!(
            serde_json::to_value(&exit).unwrap(),
            json!({ "terminal_exit": { "terminal_id": "tc-1", "exit_code": 5, "signal": null } })
        );
    }
}
