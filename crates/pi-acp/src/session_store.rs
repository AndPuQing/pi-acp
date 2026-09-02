//! `session-map.json` persistence: `sessionId -> {cwd, sessionFile, updatedAt}`.
//!
//! Ports `acp/session-store.ts` (which rewrites the whole file on every access)
//! with the design's hardening: **in-memory cache + atomic write** (tempfile +
//! rename, design D7). S6 (W-453) uses the store to locate a session's pi
//! session file for `session/load` / `session/delete` and to record the
//! mapping after `session/new`.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::settings::agent_dir;
use crate::time::utc_now_iso8601;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Serializes map-file read-modify-write transactions across all stores in
/// this process. An ACP agent normally owns one store, but tests and embedded
/// callers can create more than one store for the same path.
fn session_store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// One stored session entry (mirrors TS `StoredSession`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoredSession {
    pub session_id: String,
    pub cwd: String,
    pub session_file: String,
    pub updated_at: String,
}

/// The map file shape (`{ version: 1, sessions: {...} }`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionMapFile {
    version: u8,
    sessions: HashMap<String, StoredSession>,
}

impl Default for SessionMapFile {
    fn default() -> Self {
        Self {
            version: 1,
            sessions: HashMap::new(),
        }
    }
}

/// Default location of the session map: `<agent dir>/pi-acp/session-map.json`
/// (the TS reference uses `~/.pi/pi-acp/session-map.json`; the agent dir is
/// used so `PI_CODING_AGENT_DIR` overrides are honored, fixes #88).
pub fn session_map_path() -> PathBuf {
    agent_dir().join("pi-acp").join("session-map.json")
}

/// In-memory-cached, atomically-written session map.
///
/// Reads happen from the cache once loaded; every mutation rewrites the file
/// via a temp file + rename (crash-safe). The cache is per-process — matching
/// the TS reference's behavior of reloading the file on every access would
/// defeat the point of the cache (a `reload` hook for external
/// changes).
#[derive(Debug, Default)]
pub struct SessionStore {
    path: PathBuf,
    cache: Mutex<Option<SessionMapFile>>,
}

impl SessionStore {
    /// Create a store backed by the default [`session_map_path`].
    pub fn new() -> Self {
        Self::at(session_map_path())
    }

    /// Create a store at an explicit path (testable / injectable).
    pub fn at(path: PathBuf) -> Self {
        Self {
            path,
            cache: Mutex::new(None),
        }
    }

    fn load(&self) -> SessionMapFile {
        let mut cache = self.cache.lock().expect("session store cache poisoned");
        if let Some(loaded) = cache.as_ref() {
            return loaded.clone();
        }
        let loaded = load_file(&self.path);
        *cache = Some(loaded.clone());
        loaded
    }

    fn update<F>(&self, update: F)
    where
        F: FnOnce(&mut SessionMapFile) -> bool,
    {
        // The file lock covers the disk reload, mutation, and replacement. Do
        // not use a possibly stale per-store cache as the transaction base:
        // another SessionStore may have committed since this store was read.
        let _file_lock = session_store_lock()
            .lock()
            .expect("session store file lock poisoned");
        let mut cache = self.cache.lock().expect("session store cache poisoned");
        let mut db = load_file(&self.path);
        if update(&mut db) && atomic_write_json(&self.path, &db) {
            *cache = Some(db);
        }
    }

    /// The stored entry for a session id, if any.
    pub fn get(&self, session_id: &str) -> Option<StoredSession> {
        self.load().sessions.get(session_id).cloned()
    }

    /// Insert or refresh an entry (updates `updatedAt` to now).
    pub fn upsert(&self, session_id: &str, cwd: &str, session_file: &str) {
        self.update(|db| {
            db.sessions.insert(
                session_id.to_string(),
                StoredSession {
                    session_id: session_id.to_string(),
                    cwd: cwd.to_string(),
                    session_file: session_file.to_string(),
                    updated_at: utc_now_iso8601(),
                },
            );
            true
        });
    }

    /// Remove an entry; no-op when absent.
    pub fn delete(&self, session_id: &str) {
        self.update(|db| db.sessions.remove(session_id).is_some());
    }
}

/// Parse the map file; missing/malformed/non-v1 files yield an empty map.
fn load_file(path: &Path) -> SessionMapFile {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(_) => return SessionMapFile::default(),
    };
    match serde_json::from_str::<SessionMapFile>(&raw) {
        Ok(db) if db.version == 1 => db,
        _ => SessionMapFile::default(),
    }
}

/// Write the map file through a same-directory temp file and replacement.
fn atomic_write_json(path: &Path, db: &SessionMapFile) -> bool {
    let Some(parent) = path.parent() else {
        return false;
    };
    if let Err(e) = fs::create_dir_all(parent) {
        tracing::warn!(error = %e, ?path, "failed to create session-map parent dir");
        return false;
    }
    let body = serde_json::to_string_pretty(db)
        .map(|mut s| {
            s.push('\n');
            s
        })
        .unwrap_or_else(|_| "{}\n".to_string());
    let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(
        ".session-map.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let write = (|| -> std::io::Result<()> {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
        replace_file(&tmp, path)?;
        Ok(())
    })();
    if let Err(e) = write {
        tracing::warn!(error = %e, ?path, "failed to write session-map");
        let _ = fs::remove_file(&tmp);
        false
    } else {
        true
    }
}

/// Replace the destination with the completed temp file. Unix rename replaces
/// an existing file atomically. The standard Windows rename API rejects an
/// existing destination, so use its best available two-step fallback there;
/// all callers are serialized by [`session_store_lock`].
fn replace_file(tmp: &Path, path: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        match fs::rename(tmp, path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                fs::remove_file(path)?;
                fs::rename(tmp, path)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(not(windows))]
    {
        fs::rename(tmp, path)
    }
}

/// The raw map JSON (used by tests / diagnostics).
pub fn map_as_json(store: &SessionStore) -> Value {
    serde_json::to_value(store.load()).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn upsert_get_delete_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = SessionStore::at(dir.path().join("map.json"));
        assert!(store.get("s1").is_none());

        store.upsert("s1", "/work", "/tmp/s1.jsonl");
        let entry = store.get("s1").unwrap();
        assert_eq!(entry.session_id, "s1");
        assert_eq!(entry.cwd, "/work");
        assert_eq!(entry.session_file, "/tmp/s1.jsonl");
        assert!(entry.updated_at.ends_with('Z'));

        // upsert refreshes
        store.upsert("s1", "/work", "/tmp/s1-new.jsonl");
        assert_eq!(store.get("s1").unwrap().session_file, "/tmp/s1-new.jsonl");

        store.delete("s1");
        assert!(store.get("s1").is_none());
        store.delete("s1"); // idempotent no-op
    }

    #[test]
    fn map_file_is_persisted_atomically_and_reloadable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("map.json");
        {
            let store = SessionStore::at(path.clone());
            store.upsert("a", "/w", "/f");
        }
        // A fresh store reads the file back.
        let fresh = SessionStore::at(path.clone());
        assert_eq!(fresh.get("a").unwrap().cwd, "/w");
        assert!(path.exists());
        // No leftover temp files.
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp files should be renamed away");
    }

    #[test]
    fn malformed_map_yields_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("map.json");
        fs::write(&path, "{ not json").unwrap();
        let store = SessionStore::at(path.clone());
        assert!(store.get("x").is_none());
        // And a subsequent write repairs the file.
        store.upsert("x", "/w", "/f");
        assert_eq!(SessionStore::at(path).get("x").unwrap().cwd, "/w");
    }

    #[test]
    fn concurrent_updates_preserve_all_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("map.json");
        let store = std::sync::Arc::new(SessionStore::at(path.clone()));
        let handles: Vec<_> = (0..16)
            .map(|i| {
                let store = store.clone();
                std::thread::spawn(move || {
                    let id = format!("session-{i}");
                    store.upsert(&id, "/work", &format!("/tmp/{id}.jsonl"));
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let fresh = SessionStore::at(path);
        for i in 0..16 {
            assert!(fresh.get(&format!("session-{i}")).is_some());
        }
    }

    #[test]
    fn independent_stores_do_not_lose_read_modify_write_updates() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("map.json");
        let first = SessionStore::at(path.clone());
        let second = SessionStore::at(path.clone());

        // Prime both caches before either writer runs. A per-instance cache
        // lock alone would make the second upsert overwrite the first entry.
        assert!(first.get("first").is_none());
        assert!(second.get("second").is_none());

        first.upsert("first", "/one", "/one/session.jsonl");
        second.upsert("second", "/two", "/two/session.jsonl");

        let fresh = SessionStore::at(path);
        assert!(fresh.get("first").is_some());
        assert!(fresh.get("second").is_some());
    }

    #[test]
    fn wrong_version_yields_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("map.json");
        fs::write(&path, r#"{"version": 2, "sessions": {"a": {}}}"#).unwrap();
        let store = SessionStore::at(path);
        assert!(store.get("a").is_none());
    }
}
