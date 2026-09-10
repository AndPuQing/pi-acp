//! `session-map.json` persistence: `sessionId -> {cwd, sessionFile,
//! additionalDirectories, updatedAt}`.
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
    /// The complete ACP additional-root list. Missing in older map files is
    /// treated as an empty list by serde.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_directories: Vec<PathBuf>,
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
        // A map file that exists on disk but cannot be parsed (corruption, or
        // a future version) is loaded as an empty map; the write below would
        // destroy its entries. Preserve the raw bytes first so they are
        // recoverable instead of silently lost.
        let mut db = load_file(&self.path);
        if map_file_unreadable(&self.path) {
            let _ = backup_unreadable_map(&self.path);
        }
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
        self.upsert_with_additional_directories(session_id, cwd, session_file, &[]);
    }

    /// Insert or refresh an entry with its complete ACP additional-root list.
    pub fn upsert_with_additional_directories(
        &self,
        session_id: &str,
        cwd: &str,
        session_file: &str,
        additional_directories: &[PathBuf],
    ) {
        self.update(|db| {
            db.sessions.insert(
                session_id.to_string(),
                StoredSession {
                    session_id: session_id.to_string(),
                    cwd: cwd.to_string(),
                    session_file: session_file.to_string(),
                    additional_directories: additional_directories.to_vec(),
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

/// True when the map file exists on disk but cannot be parsed as a v1 map
/// (corruption, malformed JSON, or a future version).
fn map_file_unreadable(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    match fs::read_to_string(path) {
        Ok(raw) => serde_json::from_str::<SessionMapFile>(&raw)
            .map(|db| db.version != 1)
            .unwrap_or(true),
        Err(_) => true,
    }
}

/// Preserve the raw bytes of an unreadable map file as `<path>.corrupt`
/// (only if no backup exists yet) and log where they went. Called BEFORE the
/// overwriting write; the corrupted entries are then recoverable manually.
fn backup_unreadable_map(path: &Path) -> std::io::Result<()> {
    let backup = path.with_file_name(format!(
        "{}.corrupt",
        path.file_name().unwrap().to_string_lossy()
    ));
    if backup.exists() {
        return Ok(());
    }
    let result = fs::rename(path, &backup);
    match &result {
        Ok(()) => tracing::error!(
            ?path,
            ?backup,
            "session-map file was unreadable (corrupt or unknown version); preserved it and continued with an empty map — restore entries from the backup if needed"
        ),
        Err(e) => tracing::error!(
            error = %e,
            ?path,
            "session-map file was unreadable and could not be backed up; its entries will be lost on the next write"
        ),
    }
    result
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
        assert!(entry.additional_directories.is_empty());
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
    fn additional_directories_roundtrip_and_old_maps_default_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("map.json");
        fs::write(
            &path,
            r#"{"version":1,"sessions":{"old":{"sessionId":"old","cwd":"/work","sessionFile":"/tmp/old.jsonl","updatedAt":"2026-08-01T00:00:00.000Z"}}}"#,
        )
        .unwrap();

        let old = SessionStore::at(path.clone());
        assert!(old.get("old").unwrap().additional_directories.is_empty());

        let roots = vec![PathBuf::from("/repo-a"), PathBuf::from("/repo-b")];
        old.upsert_with_additional_directories("new", "/work", "/tmp/new.jsonl", &roots);
        let entry = SessionStore::at(path.clone()).get("new").unwrap();
        assert_eq!(entry.additional_directories, roots);

        let raw = fs::read_to_string(path).unwrap();
        assert!(raw.contains("additionalDirectories"));
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
    fn corrupt_map_is_preserved_as_backup_before_overwrite() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("map.json");
        // Malformed bytes that still contain a recognizable entry, so the
        // backup's recoverability is observable.
        fs::write(&path, "{ not json but mentions \"old\"").unwrap();

        let store = SessionStore::at(path.clone());
        store.upsert("new", "/w", "/f");

        let backup = path.with_file_name("map.json.corrupt");
        assert!(backup.exists(), "corrupt map should be preserved as a backup");
        let backup_raw = fs::read_to_string(&backup).unwrap();
        assert!(backup_raw.contains("old"));
        // The live file is the repaired v1 map with only the new entry.
        assert!(store.get("new").is_some());
        assert!(store.get("old").is_none());
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
