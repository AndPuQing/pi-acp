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
    if bytes.len() < 19 {
        return None;
    }
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }

    let year = parse_digits(bytes, 0, 4)? as i64;
    let month = parse_digits(bytes, 5, 2)?;
    let day = parse_digits(bytes, 8, 2)?;
    let hour = parse_digits(bytes, 11, 2)?;
    let minute = parse_digits(bytes, 14, 2)?;
    let second = parse_digits(bytes, 17, 2)?;
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }

    let mut index = 19;
    let milliseconds = if bytes.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if start == index {
            return None;
        }
        let mut value = 0u32;
        let mut digits = 0;
        for byte in &bytes[start..index] {
            if digits == 3 {
                break;
            }
            value = value * 10 + u32::from(byte - b'0');
            digits += 1;
        }
        while digits < 3 {
            value *= 10;
            digits += 1;
        }
        value
    } else {
        0
    };

    let offset_minutes = match bytes.get(index).copied() {
        None => 0i64,
        Some(b'Z' | b'z') if index + 1 == bytes.len() => 0,
        Some(b'+' | b'-') => {
            let sign = if bytes[index] == b'+' { 1i64 } else { -1i64 };
            index += 1;
            let offset_hours = parse_digits(bytes, index, 2)?;
            index += 2;
            if bytes.get(index) == Some(&b':') {
                index += 1;
            }
            let offset_minutes = parse_digits(bytes, index, 2)?;
            index += 2;
            if index != bytes.len() || offset_hours > 23 || offset_minutes > 59 {
                return None;
            }
            sign * (i64::from(offset_hours) * 60 + i64::from(offset_minutes))
        }
        Some(_) => return None,
    };

    let local_days = days_from_civil(year, month, day);
    let local_seconds =
        local_days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    let utc_seconds = local_seconds - offset_minutes * 60;
    let utc_days = utc_seconds.div_euclid(86_400);
    let seconds_of_day = utc_seconds.rem_euclid(86_400);
    let (utc_year, utc_month, utc_day) = civil_from_days(utc_days);
    if !(0..=9999).contains(&utc_year) {
        return None;
    }
    let utc_hour = seconds_of_day / 3_600;
    let utc_minute = (seconds_of_day % 3_600) / 60;
    let utc_second = seconds_of_day % 60;
    Some(format!(
        "{utc_year:04}-{utc_month:02}-{utc_day:02}T{utc_hour:02}:{utc_minute:02}:{utc_second:02}.{milliseconds:03}Z"
    ))
}

fn parse_digits(bytes: &[u8], start: usize, len: usize) -> Option<u32> {
    let end = start.checked_add(len)?;
    let digits = bytes.get(start..end)?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(
        digits
            .iter()
            .fold(0u32, |value, digit| value * 10 + u32::from(digit - b'0')),
    )
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

// Proleptic Gregorian calendar conversion without adding a date dependency.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year / 400
    } else {
        (adjusted_year - 399) / 400
    };
    let year_of_era = adjusted_year - era * 400;
    let month_prime = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let adjusted_days = days + 719_468;
    let era = if adjusted_days >= 0 {
        adjusted_days / 146_097
    } else {
        (adjusted_days - 146_096) / 146_097
    };
    let day_of_era = adjusted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month, day)
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
    fn updated_at_converts_offsets_to_utc() {
        assert_eq!(
            normalize_timestamp("2026-08-02T00:30:00.1+08:00"),
            Some("2026-08-01T16:30:00.100Z".to_string())
        );
        assert_eq!(
            normalize_timestamp("2026-08-01T23:30:00-0200"),
            Some("2026-08-02T01:30:00.000Z".to_string())
        );
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
