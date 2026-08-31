//! pi tool result -> ACP text / structured diff helpers.
//!
//! Ports `acp/translate/pi-tools.ts` (`toolResultToText`) plus the pure
//! edit-diff positioning helpers that the TS reference keeps private in
//! `acp/session.ts` (`findUniqueLineNumber` / `getToolPath` / `getParsedEdits` /
//! `getEditOldTexts` / `toToolKind` / `toToolCallLocations`). S5 (W-452) reuses
//! the diff-positioning helpers when building structured `Diff` content.

use std::path::{Path, PathBuf};

use agent_client_protocol::schema::v1::{ToolCall, ToolCallLocation, ToolKind};
use serde_json::Value;

/// Render a pi tool result as plain text for the ACP client.
///
/// Mirrors TS `toolResultToText` precedence:
/// 1. `details.diff` (the full unified diff pi's `edit` tool returns) when non-empty;
/// 2. text blocks in `content` (`[{type:"text", text}]`);
/// 3. `stdout`/`stderr`/`exitCode` (in `details` or at the top level);
/// 4. pretty-printed JSON fallback so no result is ever silently dropped.
pub fn tool_result_to_text(result: &Value) -> String {
    // TS `if (!result) return ''` — null / false / 0 / "" are empty.
    if !is_truthy(result) {
        return String::new();
    }
    let details = result.get("details");

    // pi's edit tool returns a terse success message in content and the full
    // unified diff in details.diff.
    let diff = details.and_then(|d| d.get("diff")).and_then(Value::as_str);
    if let Some(diff) = diff {
        if !diff.trim().is_empty() {
            return diff.to_string();
        }
    }

    // pi tool results generally look like: { content: [{type:"text", text:"..."}], details: {...} }
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

    // The bash tool frequently returns stdout/stderr in `details` rather than
    // content blocks.
    let stdout = string_chain(details, result, &["stdout"])
        .or_else(|| string_chain(details, result, &["output"]));
    let stderr = string_chain(details, result, &["stderr"]);
    let exit_code = number_chain(details, result, &["exitCode"])
        .or_else(|| number_chain(details, result, &["code"]));

    let stdout_has = stdout.as_ref().is_some_and(|s| !s.trim().is_empty());
    let stderr_has = stderr.as_ref().is_some_and(|s| !s.trim().is_empty());
    if stdout_has || stderr_has {
        let mut parts: Vec<String> = Vec::new();
        if let Some(s) = stdout {
            if !s.trim().is_empty() {
                parts.push(s);
            }
        }
        if let Some(s) = stderr {
            if !s.trim().is_empty() {
                parts.push(format!("stderr:\n{s}"));
            }
        }
        if let Some(code) = exit_code {
            parts.push(format!("exit code: {code}"));
        }
        return parts.join("\n\n").trim_end().to_string();
    }

    // Fall back to JSON so the client always sees something.
    match serde_json::to_string_pretty(result) {
        Ok(json) => json,
        Err(_) => result.to_string(),
    }
}

/// Extract the file path from a tool `args` record (`path` or `file_path`).
/// Mirrors TS `getToolPath`.
pub fn tool_path(args: &Value) -> Option<String> {
    if !args.is_object() {
        return None;
    }
    args.get("path")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            args.get("file_path")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
}

/// A single `{oldText, newText}` edit from pi's edit tool schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    pub old_text: String,
    pub new_text: String,
}

/// Parse the edit operations from a tool `args` record.
///
/// Mirrors TS `getParsedEdits`: accepts the legacy top-level
/// `{oldText, newText}`, the current `{edits: [{oldText,newText}]}` shape, and
/// stringified `edits` (pi normalizes stringified edits on the wire).
pub fn parsed_edits(args: &Value) -> Vec<Edit> {
    let mut parsed: Vec<Edit> = Vec::new();

    if args.is_object() {
        if let (Some(old), Some(new)) = (
            args.get("oldText").and_then(Value::as_str),
            args.get("newText").and_then(Value::as_str),
        ) {
            parsed.push(Edit {
                old_text: old.to_string(),
                new_text: new.to_string(),
            });
        }
    }

    let edits = string_field(args, "edits")
        .and_then(|s| serde_json::from_str::<Value>(s.as_str()).ok())
        .or_else(|| args.get("edits").cloned());

    if let Some(Value::Array(items)) = edits {
        for item in items {
            if let (Some(old), Some(new)) = (
                item.get("oldText").and_then(Value::as_str),
                item.get("newText").and_then(Value::as_str),
            ) {
                parsed.push(Edit {
                    old_text: old.to_string(),
                    new_text: new.to_string(),
                });
            }
        }
    }

    parsed
}

/// All distinct `oldText` needles for an edit tool call, in order.
/// Mirrors TS `getEditOldTexts` (used to pick the line number for ACP
/// `ToolCallLocation.line` from the first uniquely-locatable needle).
pub fn edit_old_texts(args: &Value) -> Vec<String> {
    let mut old_texts: Vec<String> = parsed_edits(args).into_iter().map(|e| e.old_text).collect();

    if let Some(t) = args.get("oldText").and_then(Value::as_str) {
        if !old_texts.iter().any(|s| s == t) {
            old_texts.push(t.to_string());
        }
    }

    if let Some(s) = string_field(args, "edits") {
        if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(s.as_str()) {
            for item in items {
                if let Some(t) = item.get("oldText").and_then(Value::as_str) {
                    if !old_texts.iter().any(|x| x == t) {
                        old_texts.push(t.to_string());
                    }
                }
            }
        }
    }

    old_texts
}

/// 1-based line number of `needle` in `text` when it occurs exactly once.
///
/// Mirrors TS `findUniqueLineNumber`: `None` for an empty needle, a needle that
/// is absent, or one that occurs more than once (ambiguity must not produce a
/// wrong location). Lines are counted on `\n` only, matching the TS reference.
pub fn find_unique_line_number(text: &str, needle: &str) -> Option<u32> {
    if needle.is_empty() {
        return None;
    }

    let first = text.find(needle)?;
    if text[first + needle.len()..].find(needle).is_some() {
        return None;
    }

    let line = text[..first].bytes().filter(|&b| b == b'\n').count() as u32 + 1;
    Some(line)
}

/// ACP `ToolKind` for a pi tool name. Mirrors TS `toToolKind` (session.ts).
pub fn to_tool_kind(tool_name: &str) -> ToolKind {
    match tool_name {
        "read" => ToolKind::Read,
        "write" | "edit" => ToolKind::Edit,
        "bash" => ToolKind::Execute,
        _ => ToolKind::Other,
    }
}

/// ACP `ToolCallLocation`s for a tool `args` record.
///
/// Mirrors TS `toToolCallLocations`: resolves relative paths against `cwd`,
/// optionally carrying a line number (e.g. from an edit diff position). Returns
/// an empty vec when the args carry no path.
pub fn to_tool_call_locations(
    args: &Value,
    cwd: &Path,
    line: Option<u32>,
) -> Vec<ToolCallLocation> {
    let Some(path) = tool_path(args) else {
        return Vec::new();
    };
    let resolved = PathBuf::from(&path);
    let resolved = if resolved.is_absolute() {
        resolved
    } else {
        cwd.join(resolved)
    };
    let loc = ToolCallLocation::new(resolved);
    let loc = match line {
        Some(line) => loc.line(line),
        None => loc,
    };
    vec![loc]
}

/// JS truthiness for the TS `!result` guard in `toolResultToText`.
fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_none_or(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}
/// Build a [`ToolCall`] for a non-bash pi tool from the streaming/execution
/// events (shared shape used by S5; kept here so the pure mapping is testable).
#[allow(dead_code)] // consumed by S5 (session event pump); exercised by tests.
pub fn tool_call_from_parts(
    tool_call_id: &str,
    title: &str,
    kind: ToolKind,
    status: agent_client_protocol::schema::v1::ToolCallStatus,
    locations: Vec<ToolCallLocation>,
    raw_input: Option<Value>,
) -> ToolCall {
    let mut call = ToolCall::new(tool_call_id.to_string(), title.to_string())
        .kind(kind)
        .status(status);
    if !locations.is_empty() {
        call = call.locations(locations);
    }
    if let Some(input) = raw_input {
        call = call.raw_input(input);
    }
    call
}

/// First string field present in `obj[group]` for one of `keys`, in order.
fn string_chain<'a>(a: Option<&'a Value>, b: &'a Value, keys: &[&str]) -> Option<String> {
    for c in [a, Some(b)].into_iter().flatten() {
        for key in keys {
            if let Some(s) = c.get(*key).and_then(Value::as_str) {
                return Some(s.to_string());
            }
        }
    }
    None
}

/// First numeric field present in `obj[group]` for one of `keys`, in order.
fn number_chain(a: Option<&Value>, b: &Value, keys: &[&str]) -> Option<i64> {
    for c in [a, Some(b)].into_iter().flatten() {
        for key in keys {
            if let Some(n) = c.get(*key).and_then(Value::as_i64) {
                return Some(n);
            }
        }
    }
    None
}

/// A string field, either as a JSON string or via `serde_json::Value`.
fn string_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::ToolCallStatus;
    use serde_json::json;

    // --- tool_result_to_text ---

    #[test]
    fn extracts_text_from_content_blocks() {
        let text = tool_result_to_text(&json!({
            "content": [
                { "type": "text", "text": "hello" },
                { "type": "text", "text": " world" }
            ]
        }));
        assert_eq!(text, "hello world");
    }

    #[test]
    fn prefers_details_diff_when_present() {
        let text = tool_result_to_text(&json!({
            "content": [{ "type": "text", "text": "Successfully replaced 2 block(s) in a.txt." }],
            "details": { "diff": "--- a\n+++ b\n" }
        }));
        assert_eq!(text, "--- a\n+++ b\n");
    }

    #[test]
    fn ignores_blank_diff() {
        let text = tool_result_to_text(&json!({
            "content": [{ "type": "text", "text": "ok" }],
            "details": { "diff": "   " }
        }));
        assert_eq!(text, "ok");
    }

    #[test]
    fn falls_back_to_json() {
        let text = tool_result_to_text(&json!({ "a": 1 }));
        assert!(text.contains("\"a\": 1"), "got: {text}");
    }

    #[test]
    fn extracts_bash_stdout_stderr_from_details() {
        let text = tool_result_to_text(&json!({
            "details": { "stdout": "ok\n", "stderr": "warn\n", "exitCode": 0 }
        }));
        assert!(text.contains("ok"));
        assert!(text.contains("stderr:"));
        assert!(text.contains("warn"));
        assert!(text.contains("exit code: 0"));
    }

    #[test]
    fn stdout_stderr_prefer_details_over_top_level() {
        let text = tool_result_to_text(&json!({
            "stdout": "top",
            "details": { "stdout": "nested" }
        }));
        assert_eq!(text, "nested");
    }

    #[test]
    fn empty_results_yield_empty_text() {
        assert_eq!(tool_result_to_text(&Value::Null), "");
        assert_eq!(tool_result_to_text(&json!(false)), "");
        assert_eq!(tool_result_to_text(&json!(0)), "");
        assert_eq!(tool_result_to_text(&json!("")), "");
        assert_eq!(tool_result_to_text(&json!({})), "{}");
        assert_eq!(tool_result_to_text(&json!([])), "[]");
    }

    #[test]
    fn scalar_result_falls_back_to_json() {
        assert_eq!(tool_result_to_text(&json!("boom")), "\"boom\"");
    }

    #[test]
    fn non_string_values_in_content_are_ignored() {
        let text = tool_result_to_text(&json!({
            "content": [
                { "type": "text", "text": "a" },
                { "type": "text", "text": 42 },
                { "type": "image", "text": "ignored" }
            ]
        }));
        assert_eq!(text, "a");
    }

    // --- edit diff positioning ---

    #[test]
    fn find_unique_line_number_locations() {
        let text = "one\ntwo\nthree";
        assert_eq!(find_unique_line_number(text, "two"), Some(2));
        assert_eq!(find_unique_line_number(text, "one"), Some(1));
        assert_eq!(find_unique_line_number(text, "three"), Some(3));
        // absent
        assert_eq!(find_unique_line_number(text, "four"), None);
        // empty needle
        assert_eq!(find_unique_line_number(text, ""), None);
    }

    #[test]
    fn find_unique_line_number_rejects_duplicates() {
        let text = "dup\ndup\nother";
        assert_eq!(find_unique_line_number(text, "dup"), None);
        // substring that appears once is fine
        assert_eq!(find_unique_line_number(text, "other"), Some(3));
    }

    #[test]
    fn find_unique_line_number_uses_newline_only() {
        let text = "a\r\nb\r\nc";
        // \r\n is one line separator; "b" is on line 2
        assert_eq!(find_unique_line_number(text, "b"), Some(2));
    }

    #[test]
    fn tool_path_extracts_path_and_file_path() {
        assert_eq!(
            tool_path(&json!({ "path": "src/main.rs" })),
            Some("src/main.rs".to_string())
        );
        assert_eq!(
            tool_path(&json!({ "file_path": "a.txt" })),
            Some("a.txt".to_string())
        );
        assert_eq!(tool_path(&json!({ "other": 1 })), None);
        assert_eq!(tool_path(&json!(null)), None);
    }

    #[test]
    fn parsed_edits_accepts_all_shapes() {
        // legacy top-level
        assert_eq!(
            parsed_edits(&json!({ "oldText": "a", "newText": "b" })),
            vec![Edit {
                old_text: "a".into(),
                new_text: "b".into()
            }]
        );
        // current edits[] shape
        assert_eq!(
            parsed_edits(&json!({ "edits": [{ "oldText": "x", "newText": "y" }] })),
            vec![Edit {
                old_text: "x".into(),
                new_text: "y".into()
            }]
        );
        // stringified edits (pi normalizes these)
        assert_eq!(
            parsed_edits(&json!({ "edits": "[{\"oldText\":\"p\",\"newText\":\"q\"}]" })),
            vec![Edit {
                old_text: "p".into(),
                new_text: "q".into()
            }]
        );
        // malformed stringified edits are ignored
        assert_eq!(parsed_edits(&json!({ "edits": "not-json" })), vec![]);
        // partial entries skipped
        assert_eq!(
            parsed_edits(&json!({ "edits": [{ "oldText": "a" }, { "newText": "b" }] })),
            vec![]
        );
    }

    #[test]
    fn edit_old_texts_are_distinct() {
        let old = edit_old_texts(&json!({
            "oldText": "dup",
            "edits": [
                { "oldText": "one", "newText": "1" },
                { "oldText": "dup", "newText": "2" }
            ]
        }));
        assert_eq!(old, vec!["one", "dup"]);
    }

    #[test]
    fn edit_old_texts_parse_stringified_edits() {
        let old = edit_old_texts(&json!({
            "edits": "[{\"oldText\":\"s1\",\"newText\":\"t1\"}]"
        }));
        assert_eq!(old, vec!["s1"]);
    }

    // --- tool kind / locations ---

    #[test]
    fn tool_kind_mapping() {
        assert_eq!(to_tool_kind("read"), ToolKind::Read);
        assert_eq!(to_tool_kind("write"), ToolKind::Edit);
        assert_eq!(to_tool_kind("edit"), ToolKind::Edit);
        assert_eq!(to_tool_kind("bash"), ToolKind::Execute);
        assert_eq!(to_tool_kind("weird"), ToolKind::Other);
    }

    #[test]
    fn locations_resolve_relative_to_cwd_and_carry_line() {
        let cwd = Path::new("/work");
        let locs = to_tool_call_locations(&json!({ "path": "src/lib.rs" }), cwd, Some(7));
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].path, PathBuf::from("/work/src/lib.rs"));
        assert_eq!(locs[0].line, Some(7));
    }

    #[test]
    fn locations_keep_absolute_paths_and_omit_line() {
        let locs = to_tool_call_locations(
            &json!({ "path": "/abs/x.rs", "other": 1 }),
            Path::new("/work"),
            None,
        );
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].path, PathBuf::from("/abs/x.rs"));
        assert_eq!(locs[0].line, None);
    }

    #[test]
    fn locations_empty_without_path() {
        assert!(to_tool_call_locations(&json!({ "cmd": "ls" }), Path::new("/w"), None).is_empty());
    }

    #[test]
    fn tool_call_from_parts_builds_acp_shape() {
        let call = tool_call_from_parts(
            "t-1",
            "read",
            ToolKind::Read,
            ToolCallStatus::InProgress,
            vec![ToolCallLocation::new("/w/a.txt")],
            Some(json!({ "path": "a.txt" })),
        );
        assert_eq!(call.tool_call_id.0.as_ref(), "t-1");
        assert_eq!(call.title, "read");
        assert_eq!(call.kind, ToolKind::Read);
        assert_eq!(call.status, ToolCallStatus::InProgress);
        assert_eq!(call.locations.len(), 1);
        assert_eq!(call.raw_input, Some(json!({ "path": "a.txt" })));
    }
}
