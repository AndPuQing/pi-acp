//! `session-map.json` persistence: `sessionId -> {cwd, sessionFile, updatedAt}`.
//!
//! S7 (W-454): in-memory cache + atomic write (tempfile + rename). Ports
//! `acp/session-store.ts` (which rewrites the whole file on every access).
