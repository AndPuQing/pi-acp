//! Global + project `settings.json` merge (project overrides global).
//!
//! Ports `acp/pi-settings.ts`: deep-merge semantics and the `quietStartup` /
//! `enableSkillCommands` lookups (including legacy key aliases). S6 (W-453)
//! wires these into startup-info emission; `sessionDir` handling lives with
//! session persistence (S7, W-454).

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// The pi agent directory: `PI_CODING_AGENT_DIR` when set, else
/// `~/.pi/agent`. Mirrors TS `getAgentDir` (pi-settings.ts).
pub fn agent_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("PI_CODING_AGENT_DIR") {
        if !dir.is_empty() {
            let p = PathBuf::from(dir);
            if p.is_absolute() {
                return p;
            }
            return std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(p);
        }
    }
    dirs::home_dir()
        .map(|home| home.join(".pi").join("agent"))
        .unwrap_or_else(|| PathBuf::from(".pi/agent"))
}

/// Deep-merge two JSON values, mirroring TS `deepMerge`: when both sides are
/// JSON objects the merge recurses; any other pair (scalar, array, null) lets
/// the overlay win wholesale. Arrays are replaced, never merged.
pub fn deep_merge(base: &Value, overlay: &Value) -> Value {
    match (base, overlay) {
        (Value::Object(a), Value::Object(b)) => {
            let mut out = a.clone();
            for (k, v) in b {
                match out.get(k) {
                    Some(existing) => out.insert(k.clone(), deep_merge(existing, v)),
                    None => out.insert(k.clone(), v.clone()),
                };
            }
            Value::Object(out)
        }
        _ => overlay.clone(),
    }
}

/// Read a JSON settings file; missing files, malformed JSON, and non-object
/// payloads all yield `Value::Null` (mirrors TS `readJsonFile` returning `{}`).
pub fn read_json_file(path: &Path) -> Value {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(_) => return Value::Null,
    };
    match serde_json::from_str::<Value>(&raw) {
        Ok(value) if value.is_object() => value,
        _ => Value::Null,
    }
}

/// Load and merge the global + project settings for `cwd` (project wins).
///
/// Path-injectable core of [`get_merged_settings`] (testable without touching
/// the real agent dir).
pub fn load_merged_settings(agent_dir: &Path, cwd: &Path) -> Value {
    let global_path = agent_dir.join("settings.json");
    let project_path = cwd.join(".pi").join("settings.json");
    let global = read_json_file(&global_path);
    let project = read_json_file(&project_path);
    deep_merge(&global, &project)
}

/// Merged settings for `cwd` using the real agent dir.
pub fn get_merged_settings(cwd: &Path) -> Value {
    load_merged_settings(&agent_dir(), cwd)
}

/// `quietStartup` lookup on merged settings; falls back to the legacy
/// `quietStart` key; defaults to `false`. Mirrors TS `getQuietStartup`.
pub fn quiet_startup(merged: &Value) -> bool {
    if let Some(v) = merged.get("quietStartup").and_then(Value::as_bool) {
        return v;
    }
    merged
        .get("quietStart")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// `enableSkillCommands` lookup on merged settings: direct boolean, then the
/// legacy nested `skills.enableSkillCommands`; defaults to `true`. Mirrors TS
/// `getEnableSkillCommands`.
pub fn enable_skill_commands(merged: &Value) -> bool {
    if let Some(v) = merged.get("enableSkillCommands").and_then(Value::as_bool) {
        return v;
    }
    merged
        .get("skills")
        .and_then(|s| s.get("enableSkillCommands"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

/// `getQuietStartup(cwd)` convenience wrapper.
pub fn get_quiet_startup(cwd: &Path) -> bool {
    quiet_startup(&get_merged_settings(cwd))
}

/// `getEnableSkillCommands(cwd)` convenience wrapper.
pub fn get_enable_skill_commands(cwd: &Path) -> bool {
    enable_skill_commands(&get_merged_settings(cwd))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    // --- deep_merge ---

    #[test]
    fn overlay_wins_on_scalars_and_arrays() {
        let merged = deep_merge(
            &json!({ "a": 1, "b": "global", "arr": [1, 2], "nested": { "keep": true, "over": 1 } }),
            &json!({ "a": 2, "arr": [3], "nested": { "over": 2, "add": "x" } }),
        );
        assert_eq!(merged["a"], 2);
        assert_eq!(merged["b"], "global");
        // arrays replaced wholesale (not merged)
        assert_eq!(merged["arr"], json!([3]));
        assert_eq!(merged["nested"]["keep"], true);
        assert_eq!(merged["nested"]["over"], 2);
        assert_eq!(merged["nested"]["add"], "x");
    }

    #[test]
    fn deep_merge_null_base_behaves_like_empty() {
        let merged = deep_merge(&Value::Null, &json!({ "a": 1 }));
        assert_eq!(merged, json!({ "a": 1 }));
        assert_eq!(deep_merge(&json!({ "a": 1 }), &Value::Null), Value::Null);
    }

    // --- read_json_file ---

    #[test]
    fn read_json_file_tolerates_missing_and_bad_json() {
        let dir = TempDir::new().unwrap();
        assert_eq!(read_json_file(&dir.path().join("nope.json")), Value::Null);

        let bad = dir.path().join("bad.json");
        fs::write(&bad, "{ not json").unwrap();
        assert_eq!(read_json_file(&bad), Value::Null);

        let scalar = dir.path().join("scalar.json");
        fs::write(&scalar, "42").unwrap();
        assert_eq!(read_json_file(&scalar), Value::Null);
    }

    // --- merged settings ---

    #[test]
    fn project_overrides_global_and_keys_merge() {
        let agent = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        fs::create_dir_all(agent.path()).unwrap();
        fs::create_dir_all(project.path().join(".pi")).unwrap();
        fs::write(
            agent.path().join("settings.json"),
            json!({ "quietStartup": true, "keep": "global" }).to_string(),
        )
        .unwrap();
        fs::write(
            project.path().join(".pi").join("settings.json"),
            json!({ "quietStartup": false, "newKey": "project" }).to_string(),
        )
        .unwrap();

        let merged = load_merged_settings(agent.path(), project.path());
        assert_eq!(merged["quietStartup"], false);
        assert_eq!(merged["keep"], "global");
        assert_eq!(merged["newKey"], "project");
    }

    #[test]
    fn missing_settings_files_yield_defaults() {
        let agent = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        let merged = load_merged_settings(agent.path(), project.path());
        assert!(!quiet_startup(&merged));
        assert!(enable_skill_commands(&merged));
    }

    // --- lookup helpers ---

    #[test]
    fn quiet_startup_direct_and_legacy() {
        assert!(quiet_startup(&json!({ "quietStartup": true })));
        assert!(!quiet_startup(&json!({ "quietStartup": false })));
        // legacy alias
        assert!(quiet_startup(&json!({ "quietStart": true })));
        // direct wins over legacy
        assert!(!quiet_startup(
            &json!({ "quietStartup": false, "quietStart": true })
        ));
        assert!(!quiet_startup(&json!({ "quietStartup": "yes" })));
        assert!(!quiet_startup(&Value::Null));
    }

    #[test]
    fn enable_skill_commands_direct_nested_and_default() {
        assert!(enable_skill_commands(
            &json!({ "enableSkillCommands": true })
        ));
        assert!(!enable_skill_commands(
            &json!({ "enableSkillCommands": false })
        ));
        // nested legacy shape
        assert!(enable_skill_commands(
            &json!({ "skills": { "enableSkillCommands": true } })
        ));
        assert!(!enable_skill_commands(
            &json!({ "skills": { "enableSkillCommands": false } })
        ));
        // direct wins over nested
        assert!(enable_skill_commands(&json!({
            "enableSkillCommands": true,
            "skills": { "enableSkillCommands": false }
        })));
        // default true
        assert!(enable_skill_commands(&json!({ "other": 1 })));
        assert!(enable_skill_commands(&Value::Null));
    }
}
