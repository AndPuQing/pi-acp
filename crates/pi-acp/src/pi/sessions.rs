//! pi session-file scanning.
//!
//! Ports `acp/pi-sessions.ts`. S7 (W-454) implements:
//! - walk `~/.pi/agent/sessions/**/*.jsonl`
//! - read first line (header: `id`, `cwd`, `type: "session"`)
//! - read tail for latest `session_info.name` (title) and `message.timestamp` (updatedAt)
//! - honor `PI_CODING_AGENT_DIR` and settings `sessionDir` (fixes #88)
