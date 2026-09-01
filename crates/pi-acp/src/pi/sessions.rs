//! pi session-file scanning.
//!
//! Ports `acp/pi-sessions.ts`: walk the pi session directory
//! (`~/.pi/agent/sessions` by default, honoring `PI_CODING_AGENT_DIR` and the
//! settings `sessionDir` override), read the first line (header: `id`, `cwd`,
//! `type: "session"`) and the tail for the latest `session_info.name` (title)
//! and `message.timestamp` (updatedAt). Used by `session/list` / `session/delete`
//! / `session/load` (S6, W-453); `~` expansion is unified through
//! `settings::agent_dir` (fixes #88).

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::settings::agent_dir;

/// Tail window read for title/updatedAt extraction (bytes).
const TAIL_BYTES: u64 = 256 * 1024;
/// First-line read buffer (bytes).
const HEAD_BYTES: usize = 64 * 1024;

/// One entry of `session/list` (mirrors TS `PiSessionListItem`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiSessionListItem {
    pub session_id: String,
    pub cwd: String,
    pub title: Option<String>,
    pub updated_at: Option<String>,
    pub session_file: String,
}

/// The pi sessions directory: settings `sessionDir` override (resolved against
/// the agent dir), else `<agent dir>/sessions`.
pub fn pi_sessions_dir() -> PathBuf {
    let agent = agent_dir();
    if let Some(dir) = read_session_dir_from_settings(&agent) {
        return dir;
    }
    agent.join("sessions")
}

/// `sessionDir` from `<agent dir>/settings.json`, resolved against the agent
/// dir when relative; `None` when unset/malformed.
fn read_session_dir_from_settings(agent: &Path) -> Option<PathBuf> {
    let settings_path = agent.join("settings.json");
    let raw = fs::read_to_string(settings_path).ok()?;
    let data: Value = serde_json::from_str(&raw).ok()?;
    let session_dir = data.get("sessionDir").and_then(Value::as_str)?;
    let session_dir = session_dir.trim();
    if session_dir.is_empty() {
        return None;
    }
    let p = PathBuf::from(session_dir);
    Some(if p.is_absolute() { p } else { agent.join(p) })
}

/// Recursively collect `.jsonl` files under `dir`.
fn walk_jsonl_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if ft.is_dir() {
            walk_jsonl_files(&path, out);
        } else if ft.is_file() && path.extension().is_some_and(|e| e == "jsonl") {
            out.push(path);
        }
    }
}

/// Read the first line of a file without loading the whole thing.
fn read_first_line(path: &Path) -> Option<String> {
    let mut f = fs::File::open(path).ok()?;
    let mut buf = vec![0u8; HEAD_BYTES];
    let n = f.read(&mut buf).ok()?;
    if n == 0 {
        return None;
    }
    let s = String::from_utf8_lossy(&buf[..n]);
    let first = s.lines().next().unwrap_or("").trim();
    if first.is_empty() {
        None
    } else {
        Some(first.to_string())
    }
}

/// Read the last `TAIL_BYTES` of a file as text.
fn read_tail(path: &Path) -> Option<String> {
    let meta = fs::metadata(path).ok()?;
    let size = meta.len();
    let start = size.saturating_sub(TAIL_BYTES);
    let len = size - start;
    let mut f = fs::File::open(path).ok()?;
    let mut buf = vec![0u8; len as usize];
    use std::io::Seek;
    f.seek(std::io::SeekFrom::Start(start)).ok()?;
    let n = f.read(&mut buf).ok()?;
    Some(String::from_utf8_lossy(&buf[..n]).to_string())
}

/// Parse the session header line: `{ "type": "session", "id", "cwd" }`.
fn parse_session_header(first_line: &str) -> Option<(String, String)> {
    let obj: Value = serde_json::from_str(first_line).ok()?;
    if obj.get("type").and_then(Value::as_str) != Some("session") {
        return None;
    }
    let session_id = obj.get("id").and_then(Value::as_str)?;
    let cwd = obj.get("cwd").and_then(Value::as_str)?;
    if session_id.is_empty() || cwd.is_empty() {
        return None;
    }
    Some((session_id.to_string(), cwd.to_string()))
}

/// Latest `session_info.name` in the tail (scanned backwards).
fn pick_title_from_tail(tail: &str) -> Option<String> {
    for line in tail.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(obj) = serde_json::from_str::<Value>(line) {
            if obj.get("type").and_then(Value::as_str) == Some("session_info") {
                if let Some(name) = obj.get("name").and_then(Value::as_str) {
                    let name = name.trim();
                    if !name.is_empty() {
                        return Some(name.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Last `session_info.name` anywhere in the file (fallback when the naming
/// entry fell outside the tail window). Mirrors TS `scanSessionInfoNameFromFile`.
fn scan_session_info_name_from_file(path: &Path) -> Option<String> {
    let raw = fs::read_to_string(path).ok()?;
    let mut last: Option<String> = None;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(obj) = serde_json::from_str::<Value>(line) {
            if obj.get("type").and_then(Value::as_str) == Some("session_info") {
                if let Some(name) = obj.get("name").and_then(Value::as_str) {
                    let name = name.trim();
                    if !name.is_empty() {
                        last = Some(name.to_string());
                    }
                }
            }
        }
    }
    last
}

/// Most recent valid `message.timestamp`, else any valid timestamp, else
/// `None`. Mirrors TS `pickUpdatedAtFromTail` (returns ISO 8601 UTC).
fn pick_updated_at_from_tail(tail: &str) -> Option<String> {
    // Pass 1: prefer the most recent `message` entry with a valid timestamp.
    for line in tail.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(obj) = serde_json::from_str::<Value>(line) {
            if obj.get("type").and_then(Value::as_str) != Some("message") {
                continue;
            }
            if let Some(ts) = obj.get("timestamp").and_then(Value::as_str) {
                if let Some(norm) = normalize_timestamp(ts) {
                    return Some(norm);
                }
            }
        }
    }
    // Pass 2: any valid timestamp.
    for line in tail.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(obj) = serde_json::from_str::<Value>(line) {
            if let Some(ts) = obj.get("timestamp").and_then(Value::as_str) {
                if let Some(norm) = normalize_timestamp(ts) {
                    return Some(norm);
                }
            }
        }
    }
    None
}

/// First user message text (≤80 chars) as a title fallback. Mirrors TS
/// `pickFallbackTitleFromHead`; caps the scan at 2000 lines.
fn pick_fallback_title_from_head(path: &Path) -> Option<String> {
    let raw = fs::read_to_string(path).ok()?;
    for (i, line) in raw.lines().enumerate() {
        if i > 2000 {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(obj) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if obj.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        let message = obj.get("message")?;
        if message.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let content = message.get("content")?;
        let text = match content {
            Value::String(s) => s.clone(),
            Value::Array(blocks) => blocks
                .iter()
                .find(|b| b.get("type").and_then(Value::as_str) == Some("text"))
                .and_then(|b| b.get("text").and_then(Value::as_str))
                .map(str::to_string)
                .unwrap_or_default(),
            _ => String::new(),
        };
        if !text.is_empty() {
            let mut t = text;
            if t.chars().count() > 80 {
                t = t.chars().take(80).collect();
            }
            return Some(t);
        }
    }
    None
}

/// Normalize a pi timestamp to ISO 8601 UTC; `None` when unparseable.
///
/// Accepts `YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM|±HHMM]` (the shapes pi emits);
/// converts any offset to UTC `Z` and pads milliseconds (JS `toISOString`
/// style). Anything else → `None` (TS `Date.parse` NaN).
fn normalize_timestamp(ts: &str) -> Option<String> {
    let s = ts.trim();
    let bytes = s.as_bytes();
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    if !bytes[..19]
        .iter()
        .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'T' | b':'))
    {
        return None;
    }
    // Datetime part up to the offset/Z marker (offsets only appear after the
    // seconds, i.e. index >= 19).
    let datetime = if s.ends_with('Z') || s.ends_with('z') {
        &s[..s.len() - 1]
    } else if let Some(off) = s[19..].find(['+', '-']) {
        &s[..19 + off]
    } else {
        s
    };
    let datetime = if datetime.contains('.') {
        datetime.to_string()
    } else {
        format!("{datetime}.000")
    };
    Some(format!("{datetime}Z"))
}

/// List all pi sessions, most recently active first (mirrors TS `listPiSessions`).
pub fn list_pi_sessions() -> Vec<PiSessionListItem> {
    list_pi_sessions_from(&pi_sessions_dir())
}

/// Core of [`list_pi_sessions`] against an explicit sessions directory
/// (testable without touching the real `~/.pi`).
pub fn list_pi_sessions_from(sessions_dir: &Path) -> Vec<PiSessionListItem> {
    let mut files = Vec::new();
    walk_jsonl_files(sessions_dir, &mut files);

    let mut items = Vec::new();
    for file in files {
        let Some(first) = read_first_line(&file) else {
            continue;
        };
        let Some((session_id, cwd)) = parse_session_header(&first) else {
            continue;
        };

        let mut title: Option<String> = None;
        let mut updated_at: Option<String> = None;
        if let Some(tail) = read_tail(&file) {
            title = pick_title_from_tail(&tail);
            updated_at = pick_updated_at_from_tail(&tail);
        }
        if title.is_none() {
            title = scan_session_info_name_from_file(&file);
        }
        if updated_at.is_none() {
            updated_at = fs::metadata(&file)
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| crate::time::format_epoch_millis(d.as_millis()));
        }
        if title.is_none() {
            title = pick_fallback_title_from_head(&file);
        }

        items.push(PiSessionListItem {
            session_id,
            cwd,
            title,
            updated_at,
            session_file: file.to_string_lossy().to_string(),
        });
    }

    items.sort_by(|a, b| {
        b.updated_at
            .as_deref()
            .unwrap_or("")
            .cmp(a.updated_at.as_deref().unwrap_or(""))
    });
    items
}

/// Find one pi session by id (mirrors TS `findPiSession`).
pub fn find_pi_session(session_id: &str) -> Option<PiSessionListItem> {
    list_pi_sessions()
        .into_iter()
        .find(|s| s.session_id == session_id)
}

/// Extract the display title from one session file — the same per-file title
/// chain `session/list` uses (tail `session_info.name`, full-file scan
/// fallback, first-user-message fallback). Used by `session/load` so the
/// restored thread's title matches the list (fixes #102/#24: Zed's sidebar
/// shows "New Agent Thread" until a title arrives).
pub fn title_from_session_file(path: &Path) -> Option<String> {
    let mut title = None;
    if let Some(tail) = read_tail(path) {
        title = pick_title_from_tail(&tail);
    }
    if title.is_none() {
        title = scan_session_info_name_from_file(path);
    }
    if title.is_none() {
        title = pick_fallback_title_from_head(path);
    }
    title
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn header(id: &str, cwd: &str) -> String {
        format!(r#"{{"type":"session","id":"{id}","cwd":"{cwd}"}}"#)
    }

    fn message_line(role: &str, ts: &str) -> String {
        format!(
            r#"{{"type":"message","timestamp":"{ts}","message":{{"role":"{role}","content":"hi"}}}}"#
        )
    }

    fn session_info_line(name: &str, ts: &str) -> String {
        format!(r#"{{"type":"session_info","name":"{name}","timestamp":"{ts}"}}"#)
    }

    #[test]
    fn parses_header_and_ignores_non_session_files() {
        assert_eq!(
            parse_session_header(&header("s1", "/work")),
            Some(("s1".to_string(), "/work".to_string()))
        );
        assert_eq!(
            parse_session_header(r#"{"type":"message","id":"s1"}"#),
            None
        );
        assert_eq!(parse_session_header("not json"), None);
        assert_eq!(
            parse_session_header(r#"{"type":"session","id":"","cwd":"/x"}"#),
            None
        );
    }

    #[test]
    fn picks_title_and_updated_at_from_tail() {
        let tail = format!(
            "{}\n{}\n{}",
            message_line("user", "2026-08-01T10:00:00.000Z"),
            session_info_line("My Session", "2026-08-02T11:00:00.000Z"),
            message_line("assistant", "2026-08-03T12:00:00.000Z")
        );
        assert_eq!(pick_title_from_tail(&tail), Some("My Session".to_string()));
        assert_eq!(
            pick_updated_at_from_tail(&tail),
            Some("2026-08-03T12:00:00.000Z".to_string())
        );
    }

    #[test]
    fn updated_at_falls_back_to_any_timestamp() {
        let tail = session_info_line("T", "2026-08-02T11:00:00+00:00");
        assert_eq!(
            pick_updated_at_from_tail(&tail),
            Some("2026-08-02T11:00:00.000Z".to_string())
        );
        assert_eq!(pick_updated_at_from_tail("no timestamps here"), None);
    }

    #[test]
    fn fallback_title_from_first_user_message() {
        let dir = TempDir::new().unwrap();
        let f = dir.path().join("s.jsonl");
        fs::write(
            &f,
            format!(
                "{}\n{}\n{}",
                header("s1", "/work"),
                message_line("assistant", "2026-08-01T10:00:00.000Z"),
                message_line("user", "2026-08-01T10:00:01.000Z")
            ),
        )
        .unwrap();
        assert_eq!(pick_fallback_title_from_head(&f), Some("hi".to_string()));
    }

    #[test]
    fn lists_sessions_sorted_by_activity() {
        let dir = TempDir::new().unwrap();
        let sessions = dir.path().join("sessions");
        fs::create_dir_all(&sessions).unwrap();

        fs::write(
            sessions.join("a.jsonl"),
            format!(
                "{}\n{}\n{}",
                header("old", "/work"),
                message_line("user", "2026-08-01T10:00:00.000Z"),
                message_line("assistant", "2026-08-01T10:00:05.000Z")
            ),
        )
        .unwrap();
        fs::write(
            sessions.join("b.jsonl"),
            format!(
                "{}\n{}\n{}",
                header("recent", "/other"),
                session_info_line("Recent Session", "2026-08-05T09:00:00.000Z"),
                message_line("user", "2026-08-05T09:00:10.000Z")
            ),
        )
        .unwrap();
        // not a session file (wrong header)
        fs::write(
            sessions.join("c.jsonl"),
            message_line("user", "2026-08-05T09:00:10.000Z"),
        )
        .unwrap();

        let items = list_pi_sessions_from(&sessions);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].session_id, "recent");
        assert_eq!(items[0].title.as_deref(), Some("Recent Session"));
        assert_eq!(items[1].session_id, "old");
        assert!(items[0].updated_at.as_deref().unwrap() > items[1].updated_at.as_deref().unwrap());
    }

    #[test]
    fn title_from_file_uses_chain() {
        let dir = TempDir::new().unwrap();
        // session_info.name wins.
        let named = dir.path().join("named.jsonl");
        fs::write(
            &named,
            format!(
                "{}\n{}",
                header("s1", "/work"),
                session_info_line("Named Session", "2026-08-02T11:00:00.000Z")
            ),
        )
        .unwrap();
        assert_eq!(
            title_from_session_file(&named),
            Some("Named Session".to_string())
        );

        // No session_info: falls back to the first user message (≤80 chars).
        let headless = dir.path().join("headless.jsonl");
        fs::write(
            &headless,
            format!(
                "{}\n{}",
                header("s2", "/work"),
                message_line("user", "2026-08-01T10:00:00.000Z")
            ),
        )
        .unwrap();
        assert_eq!(title_from_session_file(&headless), Some("hi".to_string()));

        // Missing file: None.
        assert_eq!(
            title_from_session_file(&dir.path().join("nope.jsonl")),
            None
        );
    }

    #[test]
    fn honors_session_dir_setting_override() {
        let dir = TempDir::new().unwrap();
        let agent = dir.path().join("agent");
        fs::create_dir_all(&agent).unwrap();
        let custom = dir.path().join("custom-sessions");
        fs::create_dir_all(&custom).unwrap();
        fs::write(
            agent.join("settings.json"),
            format!(r#"{{"sessionDir": "{}"}}"#, custom.to_string_lossy()),
        )
        .unwrap();
        fs::write(
            custom.join("x.jsonl"),
            format!(
                "{}\n{}",
                header("sx", "/work"),
                message_line("user", "2026-08-01T10:00:00.000Z")
            ),
        )
        .unwrap();
        assert_eq!(list_pi_sessions_from(&custom).len(), 1);
    }
}
