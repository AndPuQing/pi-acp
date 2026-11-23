//! ACP-owned retained message identities and insertion-key deduplication.
//! This is a conversation cache, not provider accounting or execution recovery.
use crate::error::Result;
use agent_client_protocol_schema::v2;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

#[derive(Default, Clone, Serialize, Deserialize)]
struct History {
    #[serde(default)]
    deleted: bool,
    keys: HashMap<String, String>,
    updates: Vec<Value>,
}
fn lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
fn path(id: &str) -> PathBuf {
    let key = id
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    crate::settings::agent_dir()
        .join("pi-acp/conversations")
        .join(format!("{key}.json"))
}
fn load(id: &str) -> Result<History> {
    let path = path(id);
    if !path.exists() {
        return Ok(History::default());
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
pub fn lookup(id: &str, key: Option<&str>) -> Result<Option<v2::MessageId>> {
    let _lock = lock().lock().unwrap();
    let history = load(id)?;
    Ok(key
        .and_then(|key| history.keys.get(key).cloned())
        .map(v2::MessageId::new))
}
pub fn insert(
    id: &str,
    key: Option<&str>,
    content: Vec<v2::ContentBlock>,
) -> Result<(v2::MessageId, bool)> {
    let _lock = lock().lock().unwrap();
    let mut history = load(id)?;
    if history.deleted {
        return Err(crate::error::AcpxError::UnknownSession(id.into()));
    }
    if let Some(existing) = key.and_then(|k| history.keys.get(k)) {
        return Ok((v2::MessageId::new(existing.clone()), false));
    }
    let message_id = v2::MessageId::new(uuid::Uuid::new_v4().to_string());
    if let Some(key) = key {
        history
            .keys
            .insert(key.to_string(), message_id.0.to_string());
    }
    history
        .updates
        .push(serde_json::to_value(v2::SessionUpdate::UserMessage(
            v2::UserMessage::new(message_id.clone()).content(content),
        ))?);
    crate::bounded::atomic_json(&path(id), &history)?;
    Ok((message_id, true))
}
pub fn record(notification: &v2::UpdateSessionNotification) -> Result<()> {
    let update = serde_json::to_value(&notification.update)?;
    let kind = update["sessionUpdate"].as_str().unwrap_or("");
    if !matches!(
        kind,
        "user_message" | "agent_message" | "tool_call" | "tool_call_update"
    ) {
        return Ok(());
    }
    let _lock = lock().lock().unwrap();
    let id = notification.session_id.0.as_ref();
    let mut history = load(id)?;
    if history.deleted {
        return Ok(());
    }
    if matches!(kind, "user_message" | "agent_message") {
        if let Some(existing) = history
            .updates
            .iter_mut()
            .find(|v| v["sessionUpdate"] == kind && v["messageId"] == update["messageId"])
        {
            let fields = existing.as_object_mut().expect("recorded update object");
            for (key, value) in update.as_object().expect("update object") {
                if key == "_meta" && value.is_object() {
                    let meta = fields
                        .entry(key.clone())
                        .or_insert_with(|| serde_json::json!({}));
                    if !meta.is_object() {
                        *meta = serde_json::json!({});
                    }
                    let target = meta.as_object_mut().unwrap();
                    for (name, value) in value.as_object().unwrap() {
                        if value.is_null() {
                            target.remove(name);
                        } else {
                            target.insert(name.clone(), value.clone());
                        }
                    }
                } else {
                    fields.insert(key.clone(), value.clone());
                }
            }
        } else {
            history.updates.push(update);
        }
    } else {
        history.updates.push(update);
    }
    crate::bounded::atomic_json(&path(id), &history)
}
pub fn replay(id: &str) -> Result<Vec<v2::UpdateSessionNotification>> {
    let _lock = lock().lock().unwrap();
    load(id)?
        .updates
        .into_iter()
        .map(|v| {
            Ok(v2::UpdateSessionNotification::new(
                v2::SessionId::new(id),
                serde_json::from_value(v)?,
            ))
        })
        .collect()
}

pub fn delete(id: &str) -> Result<()> {
    let _lock = lock().lock().unwrap();
    // Empty tombstone rejects late frames from a disposed connector without
    // retaining message content or insertion keys.
    crate::bounded::atomic_json(
        &path(id),
        &History {
            deleted: true,
            ..History::default()
        },
    )
}

pub fn fork(source: &str, target: &str) -> Result<()> {
    let _lock = lock().lock().unwrap();
    let mut history = load(source)?;
    history.keys.clear(); // insertion keys are scoped to the parent session
    crate::bounded::atomic_json(&path(target), &history)
}
