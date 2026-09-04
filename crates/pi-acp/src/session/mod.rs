//! Per-session state machine (S5, W-452).
//!
//! Implements `SessionManager` + `PiAcpSession`, porting `acp/session.ts`:
//! - [`PiAcpSession`] — TurnQueue (client-side `one-at-a-time`), the pi event
//!   pump, monotonic tool-call status, edit/write structured diffs, the
//!   extension UI bridge, and turn completion keyed on `agent_settled`.
//! - [`SessionManager`] — `Arc<Mutex<HashMap<SessionId, Arc<Session>>>>` with
//!   `close` / `close_all_except` / `dispose_all`.
//! - [`OutboundMessage`] — the single ordered channel all session → client
//!   traffic flows through ([`spawn_outbound_connector`] bridges it to the ACP
//!   SDK connection; tests drive it directly).
//!
//! Acceptance (per W-452): turn queueing / cancel / monotonic tool status /
//! diff generation are exercised in `tests/session.rs` against the mock pi
//! (`--mock-rpc` fixture) driven by scripted event sequences.

// Design doc layout: `session/session.rs` holds PiAcpSession next to the
// manager in `session/mod.rs` (clippy module_inception is intentional).
#[allow(clippy::module_inception)]
mod session;

pub use session::{
    spawn_outbound_connector, OutboundMessage, PiAcpSession, SessionParams, StopReason,
};

use std::collections::HashMap;
use std::sync::Arc;

use agent_client_protocol::schema::v1::SessionId;
use tokio::sync::Mutex;

use crate::error::{AcpxError, Result};

/// Owns the registered sessions, keyed by session id.
///
/// `Arc<Mutex<HashMap<SessionId, Arc<Session>>>>` per the design doc §3/§8.1:
/// the map guards registration only; each session's state machine runs in its
/// own pump task.
pub struct SessionManager {
    sessions: Arc<Mutex<HashMap<SessionId, Arc<PiAcpSession>>>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The registered session, if any (no throw — TS `maybeGet`).
    pub async fn maybe_get(&self, session_id: &SessionId) -> Option<Arc<PiAcpSession>> {
        self.sessions.lock().await.get(session_id).cloned()
    }

    /// The registered session, or [`AcpxError::UnknownSession`].
    pub async fn get(&self, session_id: &SessionId) -> Result<Arc<PiAcpSession>> {
        self.maybe_get(session_id)
            .await
            .ok_or_else(|| AcpxError::UnknownSession(session_id.0.to_string()))
    }

    /// Register a session (spawned by the caller, e.g. `session/new`). A
    /// replaced instance is disposed after ownership leaves the map.
    pub async fn insert(&self, session: Arc<PiAcpSession>) {
        let replaced = self
            .sessions
            .lock()
            .await
            .insert(session.session_id().clone(), session.clone());
        if let Some(previous) = replaced {
            if !Arc::ptr_eq(&previous, &session) {
                previous.dispose().await;
            }
        }
    }

    /// Spawn a fresh session (pi subprocess + pump) and register it.
    pub async fn create(&self, params: SessionParams) -> Result<Arc<PiAcpSession>> {
        let session = PiAcpSession::spawn(params).await?;
        self.insert(session.clone()).await;
        Ok(session)
    }

    /// Dispose a session's pi process and remove it from the manager.
    pub async fn close(&self, session_id: &SessionId) {
        // Remove ownership before awaiting process teardown. Otherwise an
        // insert of the same id during dispose could be removed by the stale
        // close after it finishes.
        let session = self.sessions.lock().await.remove(session_id);
        if let Some(session) = session {
            session.dispose().await;
        }
    }

    /// Close every session except the one to keep (TS `closeAllExcept`).
    pub async fn close_all_except(&self, keep: &SessionId) {
        let ids: Vec<SessionId> = self
            .sessions
            .lock()
            .await
            .keys()
            .filter(|id| *id != keep)
            .cloned()
            .collect();
        for id in ids {
            self.close(&id).await;
        }
    }

    /// Close every session (graceful shutdown path; design §8.3).
    pub async fn dispose_all(&self) {
        let sessions: Vec<Arc<PiAcpSession>> = self
            .sessions
            .lock()
            .await
            .drain()
            .map(|(_, session)| session)
            .collect();
        for session in sessions {
            session.dispose().await;
        }
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn manager_unknown_session_is_explicit_error() {
        let manager = SessionManager::new();
        let sid: SessionId = "nope".into();
        assert!(manager.maybe_get(&sid).await.is_none());
        match manager.get(&sid).await {
            Err(AcpxError::UnknownSession(id)) => assert_eq!(id, "nope"),
            other => panic!("expected UnknownSession, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn manager_close_and_close_all_except_are_noops_on_empty() {
        let manager = SessionManager::new();
        let sid: SessionId = "x".into();
        manager.close(&sid).await; // unknown id: no-op
        manager.close_all_except(&sid).await;
        manager.dispose_all().await;
        assert!(manager.maybe_get(&sid).await.is_none());
    }
}
