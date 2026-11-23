//! Endpoint-owned restriction and crash-uncertainty records. These are native
//! adapter internals, never a client-specific status dialect or an accounting journal.
use crate::{
    error::{AcpxError, Result},
    mcp::McpServerSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

fn refused(message: &str) -> AcpxError {
    AcpxError::RpcFailed {
        command: "bounded execution".into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn runtime(root: &Path) -> BoundedRuntime {
        let module = root.join("host-managed.ts");
        fs::write(&module, "// deterministic fixture module").unwrap();
        let runtime = BoundedRuntime::prepare(&module, &[]).unwrap();
        atomic_json(
            &runtime.scratch.path().join("state.json"),
            &json!({
                "version":1,"ready":true,"uncertain":false,"authorized":false,"inFlight":0
            }),
        )
        .unwrap();
        runtime
    }

    #[test]
    fn completed_explicit_close_allows_quiet_recovery() {
        let root = tempfile::tempdir().unwrap();
        let journal = root.path().join("lifecycle.json");
        let first = runtime(root.path());
        first.bind_at(journal.clone(), false).unwrap();
        first.begin("edit arithmetic.py").unwrap();
        assert!(first.finish(true).unwrap());
        first.close(false).unwrap();
        let restored = runtime(root.path());
        restored.bind_at(journal, true).unwrap();
        assert!(!restored.uncertain());
        assert!(restored.begin("a new authorized task").is_ok());
    }

    #[test]
    fn interrupted_work_is_not_settled_by_quiet_replacement() {
        let root = tempfile::tempdir().unwrap();
        let journal = root.path().join("lifecycle.json");
        let first = runtime(root.path());
        first.bind_at(journal.clone(), false).unwrap();
        first.begin("remote call outstanding").unwrap();
        drop(first); // models hard endpoint death: no close or settlement
        let restored = runtime(root.path());
        assert!(
            restored.bind_at(journal.clone(), true).is_err(),
            "unreconciled ownership must refuse replacement"
        );
        let record: Lifecycle = serde_json::from_slice(&fs::read(journal).unwrap()).unwrap();
        assert_eq!(record.state, PriorState::Active);
    }

    #[test]
    fn remote_delivery_uncertainty_and_missing_guard_are_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let runtime = runtime(root.path());
        runtime
            .bind_at(root.path().join("lifecycle.json"), false)
            .unwrap();
        runtime.begin("call").unwrap();
        atomic_json(
            &runtime.scratch.path().join("state.json"),
            &json!({
                "version":1,"ready":true,"uncertain":true,"authorized":false,"inFlight":0
            }),
        )
        .unwrap();
        assert!(!runtime.finish(true).unwrap());
        assert!(runtime.uncertain());
        assert!(runtime.begin("retry").is_err());
        atomic_json(
            &runtime.scratch.path().join("state.json"),
            &json!({
                "version":1,"ready":true,"uncertain":false,"authorized":false,"inFlight":0
            }),
        )
        .unwrap();
        assert!(
            !runtime.finish(true).unwrap(),
            "later quietness cannot erase uncertainty"
        );
        fs::remove_file(runtime.scratch.path().join("state.json")).unwrap();
        assert!(runtime
            .bind_at(root.path().join("another.json"), true)
            .is_err());
    }

    #[test]
    fn restrictions_preserve_coding_and_disable_discovery_and_shell() {
        let root = tempfile::tempdir().unwrap();
        let runtime = runtime(root.path());
        let args = runtime.args();
        for flag in [
            "--no-extensions",
            "--no-mcp",
            "--no-skills",
            "--no-prompt-templates",
            "--tools",
        ] {
            assert!(args.iter().any(|a| a == flag));
        }
        assert!(!CODING_TOOLS.contains(&"bash"));
        assert!(CODING_TOOLS.contains(&"edit") && CODING_TOOLS.contains(&"write"));
        runtime
            .bind_at(root.path().join("lifecycle.json"), false)
            .unwrap();
        assert!(runtime.begin("/extension-command").is_err());
    }
}

pub(crate) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| refused("invalid lifecycle path"))?;
    fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(file.as_file_mut(), value)?;
    file.as_file_mut().sync_all()?;
    file.persist(path).map_err(|e| AcpxError::Io(e.error))?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// Only these native coding tools remain available. In particular, no shell,
/// arbitrary extension command, model-switching tool or tool-install surface.
pub const CODING_TOOLS: &[&str] = &["read", "write", "edit", "grep", "find", "ls"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PriorState {
    Idle,
    Active,
    Closed,
    Uncertain,
}

#[derive(Debug, Serialize, Deserialize)]
struct Lifecycle {
    version: u8,
    state: PriorState,
}

/// One owned subprocess lifetime. Its scratch state cannot be reused to settle
/// a previous process. Only an explicitly clean close is recoverable as quiet.
#[derive(Debug)]
pub struct BoundedRuntime {
    scratch: tempfile::TempDir,
    journal: Mutex<Option<PathBuf>>,
    unresolved: Mutex<bool>,
    lease: Mutex<Option<PathBuf>>,
}

impl BoundedRuntime {
    pub fn prepare(module: &Path, specs: &[McpServerSpec]) -> Result<Self> {
        if !module.is_absolute() || !module.is_file() {
            return Err(refused(
                "PI_ACP_BOUNDED_MCP_MODULE must name an absolute public host-managed module",
            ));
        }
        if specs
            .iter()
            .any(|s| s.definition.url.is_none() || s.definition.command.is_some())
        {
            return Err(refused("bounded mode supports only ACP HTTP MCP servers"));
        }
        let scratch = tempfile::tempdir()?;
        let source = include_str!("bounded_runtime.ts").replace(
            "__HOST_MANAGED_MODULE__",
            &serde_json::to_string(&module.to_string_lossy())?,
        );
        fs::write(scratch.path().join("extension.ts"), source)?;
        atomic_json(&scratch.path().join("servers.json"), &specs.iter().map(|s| {
            json!({"name":s.name,"url":s.definition.url,"headers":s.definition.headers})
        }).collect::<Vec<_>>())?;
        Ok(Self {
            scratch,
            journal: Mutex::new(None),
            unresolved: Mutex::new(false),
            lease: Mutex::new(None),
        })
    }

    pub fn controls_outside(&self, workspace: &Path) -> bool {
        self.scratch
            .path()
            .canonicalize()
            .is_ok_and(|path| !path.starts_with(workspace))
    }

    pub fn args(&self) -> Vec<String> {
        vec![
            "--no-extensions",
            "--no-mcp",
            "--no-skills",
            "--no-prompt-templates",
            "--tools",
            "read,write,edit,grep,find,ls,*_*",
            "--extension",
        ]
        .into_iter()
        .map(str::to_owned)
        .chain([self
            .scratch
            .path()
            .join("extension.ts")
            .to_string_lossy()
            .into_owned()])
        .collect()
    }
    pub fn environment(&self) -> Vec<(String, String)> {
        vec![(
            "PI_ACP_BOUNDED_DIR".into(),
            self.scratch.path().to_string_lossy().into_owned(),
        )]
    }

    /// Called only after the native session identity has been checked. A live
    /// or uncertain old record cannot be cleared by the new process's flags.
    pub fn bind(&self, session_id: &str, restoring: bool) -> Result<()> {
        let name = session_id
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let path = crate::settings::agent_dir()
            .join("pi-acp/bounded")
            .join(format!("{name}.json"));
        self.bind_at(path, restoring)
    }

    fn bind_at(&self, path: PathBuf, restoring: bool) -> Result<()> {
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| refused("invalid lifecycle path"))?,
        )?;
        let lease = path.with_extension("owner");
        fs::create_dir(&lease).map_err(|_| {
            refused("prior process ownership is unresolved; refusing replacement or blind retry")
        })?;
        *self.lease.lock().unwrap() = Some(lease);
        let prior = if path.exists() {
            let record: Lifecycle = serde_json::from_slice(&fs::read(&path)?)?;
            if record.version != 1 {
                return Err(refused("unknown lifecycle record version"));
            }
            Some(record.state)
        } else {
            None
        };
        let report: Value =
            serde_json::from_slice(&fs::read(self.scratch.path().join("state.json"))?)?;
        if report["version"] != 1
            || report["ready"] != true
            || report["authorized"] != false
            || report["inFlight"] != 0
            || report["uncertain"] != false
        {
            return Err(refused("bounded guard failed to establish quiet startup"));
        }
        let unknown = (restoring || prior.is_some()) && prior != Some(PriorState::Closed);
        *self.unresolved.lock().unwrap() = unknown;
        *self.journal.lock().unwrap() = Some(path);
        self.record(if unknown {
            PriorState::Uncertain
        } else {
            PriorState::Idle
        })
    }

    fn record(&self, state: PriorState) -> Result<()> {
        if let Some(path) = self.journal.lock().unwrap().as_ref() {
            if self.lease.lock().unwrap().is_none() {
                return Err(refused("stale execution owner"));
            }
            atomic_json(path, &Lifecycle { version: 1, state })?;
        }
        Ok(())
    }
    pub fn uncertain(&self) -> bool {
        *self.unresolved.lock().unwrap()
    }

    pub fn mark_uncertain(&self) -> Result<()> {
        *self.unresolved.lock().unwrap() = true;
        self.record(PriorState::Uncertain)
    }

    pub fn begin(&self, message: &str) -> Result<()> {
        if self.uncertain() {
            return Err(refused(
                "prior work outcome is unresolved; automatic retry is forbidden",
            ));
        }
        if message.trim_start().starts_with('/') {
            return Err(refused(
                "slash/extension commands are disabled in bounded mode",
            ));
        }
        self.record(PriorState::Active)?;
        atomic_json(
            &self.scratch.path().join("grant.json"),
            &json!({"message":message}),
        )
    }

    /// Read the owned guard's result, not an inference from a quiet native
    /// replacement. Missing/corrupt state is uncertainty, never empty activity.
    pub fn finish(&self, success: bool) -> Result<bool> {
        let report = fs::read(self.scratch.path().join("state.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
        let clean = success
            && !self.uncertain()
            && report.as_ref().is_some_and(|r| {
                r["version"] == 1
                    && r["ready"] == true
                    && r["uncertain"] == false
                    && r["inFlight"] == 0
                    && r["authorized"] == false
            });
        if !clean {
            *self.unresolved.lock().unwrap() = true;
        }
        let _ = fs::remove_file(self.scratch.path().join("grant.json"));
        self.record(if clean {
            PriorState::Idle
        } else {
            PriorState::Uncertain
        })?;
        Ok(clean)
    }

    pub fn close(&self, was_active: bool) -> Result<()> {
        if was_active {
            *self.unresolved.lock().unwrap() = true;
        }
        self.record(if self.uncertain() {
            PriorState::Uncertain
        } else {
            PriorState::Closed
        })?;
        if let Some(path) = self.lease.lock().unwrap().take() {
            fs::remove_dir(path)?;
        }
        Ok(())
    }
}
