//! Global + project `settings.json` merge (project overrides global).
//!
//! Ports `acp/pi-settings.ts`: deep-merge semantics and the `quietStartup` /
//! `enableSkillCommands` lookups (including legacy key aliases). S6 (W-453)
//! wires these into startup-info emission; `sessionDir` handling lives with
//! session persistence (S7, W-454).

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

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
/// payloads all yield an empty object `{}` (mirrors TS `readJsonFile`, which
/// returns `{}` rather than `null`). Returning `{}` — not `Value::Null` — is
/// what keeps a project without `.pi/settings.json` from wiping the global
/// settings during [`deep_merge`]: a `null` overlay would win wholesale.
pub fn read_json_file(path: &Path) -> Value {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(_) => return json!({}),
    };
    match serde_json::from_str::<Value>(&raw) {
        Ok(value) if value.is_object() => value,
        _ => json!({}),
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

/// Read the optional `enabledModels` model patterns from merged settings.
/// Empty or non-string entries are ignored; an empty result means no filter.
pub fn enabled_models(merged: &Value) -> Option<Vec<String>> {
    let patterns = merged
        .get("enabledModels")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    (!patterns.is_empty()).then_some(patterns)
}

/// `enabledModels` lookup for a working directory.
pub fn get_enabled_models(cwd: &Path) -> Option<Vec<String>> {
    enabled_models(&get_merged_settings(cwd))
}

/// Check one model against an `enabledModels` pattern.
///
/// This follows pi CLI's model-scope matching: patterns are case-insensitive
/// globs checked against both `provider/id` and the bare model id. A thinking
/// level suffix such as `:high` is not part of the model identity.
pub fn model_matches_enabled_pattern(provider: &str, model_id: &str, pattern: &str) -> bool {
    let provider = provider.trim();
    let model_id = strip_thinking_suffix(model_id.trim());
    let pattern = strip_thinking_suffix(pattern.trim());
    if provider.is_empty() || model_id.is_empty() || pattern.is_empty() {
        return false;
    }

    let full_id = format!("{provider}/{model_id}");
    glob_matches(&full_id, pattern) || glob_matches(model_id, pattern)
}

/// Return whether a model is allowed by an optional `enabledModels` list.
/// Missing or empty settings preserve pi's unrestricted behavior.
pub fn is_model_enabled(provider: &str, model_id: &str, patterns: Option<&[String]>) -> bool {
    match patterns {
        Some(patterns) if !patterns.is_empty() => patterns
            .iter()
            .any(|pattern| model_matches_enabled_pattern(provider, model_id, pattern)),
        _ => true,
    }
}

const THINKING_SUFFIXES: [&str; 8] = [
    "off", "minimal", "low", "medium", "high", "xhigh", "max", "thinking",
];

fn strip_thinking_suffix(value: &str) -> &str {
    let Some(colon) = value.rfind(':') else {
        return value;
    };
    let suffix = &value[colon + 1..];
    if THINKING_SUFFIXES
        .iter()
        .any(|candidate| suffix.eq_ignore_ascii_case(candidate))
    {
        &value[..colon]
    } else {
        value
    }
}

/// Match the small glob language used by pi's model patterns. `*` and `?`
/// stay within one slash-delimited segment; `**` can span segments and
/// bracket classes support the usual `[abc]`, `[a-z]`, `[!a]` forms.
fn glob_matches(value: &str, pattern: &str) -> bool {
    let value = value.to_lowercase().chars().collect::<Vec<_>>();
    let pattern = pattern.to_lowercase().chars().collect::<Vec<_>>();
    let mut memo = vec![vec![None; value.len() + 1]; pattern.len() + 1];
    glob_matches_at(&value, &pattern, 0, 0, &mut memo)
}

fn glob_matches_at(
    value: &[char],
    pattern: &[char],
    pattern_index: usize,
    value_index: usize,
    memo: &mut [Vec<Option<bool>>],
) -> bool {
    if let Some(result) = memo[pattern_index][value_index] {
        return result;
    }

    let result = if pattern_index == pattern.len() {
        value_index == value.len()
    } else {
        match pattern[pattern_index] {
            '*' => {
                let mut next_pattern = pattern_index + 1;
                while next_pattern < pattern.len() && pattern[next_pattern] == '*' {
                    next_pattern += 1;
                }
                let crosses_slashes = next_pattern - pattern_index > 1;
                let max_value = if crosses_slashes {
                    value.len()
                } else {
                    value[value_index..]
                        .iter()
                        .position(|ch| *ch == '/')
                        .map_or(value.len(), |offset| value_index + offset)
                };
                (value_index..=max_value).any(|next_value| {
                    glob_matches_at(value, pattern, next_pattern, next_value, memo)
                })
            }
            '?' if value_index < value.len() && value[value_index] != '/' => {
                glob_matches_at(value, pattern, pattern_index + 1, value_index + 1, memo)
            }
            '[' if value_index < value.len() => {
                if let Some((next_pattern, matches)) =
                    char_class_match(pattern, pattern_index, value[value_index])
                {
                    matches && glob_matches_at(value, pattern, next_pattern, value_index + 1, memo)
                } else {
                    value[value_index] == '['
                        && glob_matches_at(value, pattern, pattern_index + 1, value_index + 1, memo)
                }
            }
            '\\' if pattern_index + 1 < pattern.len() => {
                value_index < value.len()
                    && value[value_index] == pattern[pattern_index + 1]
                    && glob_matches_at(value, pattern, pattern_index + 2, value_index + 1, memo)
            }
            literal => {
                value_index < value.len()
                    && value[value_index] == literal
                    && glob_matches_at(value, pattern, pattern_index + 1, value_index + 1, memo)
            }
        }
    };

    memo[pattern_index][value_index] = Some(result);
    result
}

fn char_class_match(pattern: &[char], start: usize, value: char) -> Option<(usize, bool)> {
    let mut end = start + 1;
    while end < pattern.len() && pattern[end] != ']' {
        end += 1;
    }
    if end == pattern.len() {
        return None;
    }

    let mut cursor = start + 1;
    let negated = matches!(pattern.get(cursor), Some('!') | Some('^'));
    if negated {
        cursor += 1;
    }
    if cursor == end {
        return None;
    }

    let mut matched = false;
    while cursor < end {
        let first = pattern[cursor];
        if first == '\\' && cursor + 1 < end {
            matched |= pattern[cursor + 1] == value;
            cursor += 2;
        } else if cursor + 2 < end && pattern[cursor + 1] == '-' {
            let last = pattern[cursor + 2];
            matched |= first <= value && value <= last;
            cursor += 3;
        } else {
            matched |= first == value;
            cursor += 1;
        }
    }

    Some((end + 1, if negated { !matched } else { matched }))
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
    }

    #[test]
    fn global_settings_survive_missing_project_file() {
        let agent = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        fs::write(
            agent.path().join("settings.json"),
            json!({ "quietStartup": true, "enabledModels": ["anthropic/*"] })
                .to_string(),
        )
        .unwrap();

        let merged = load_merged_settings(agent.path(), project.path());
        assert_eq!(merged["quietStartup"], true);
        assert_eq!(merged["enabledModels"], json!(["anthropic/*"]));
    }

    // --- read_json_file ---

    #[test]
    fn read_json_file_tolerates_missing_and_bad_json() {
        let dir = TempDir::new().unwrap();
        // Missing/malformed/non-object files read as `{}`, not `null`: a
        // `null` overlay would wipe the base during `deep_merge` (a project
        // without `.pi/settings.json` would lose all global settings).
        assert_eq!(read_json_file(&dir.path().join("nope.json")), json!({}));

        let bad = dir.path().join("bad.json");
        fs::write(&bad, "{ not json").unwrap();
        assert_eq!(read_json_file(&bad), json!({}));

        let scalar = dir.path().join("scalar.json");
        fs::write(&scalar, "42").unwrap();
        assert_eq!(read_json_file(&scalar), json!({}));
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

    // --- enabled models ---

    #[test]
    fn enabled_models_reads_non_empty_string_patterns() {
        assert_eq!(
            enabled_models(&json!({
                "enabledModels": [" anthropic/* ", 42, "", "claude-*"]
            })),
            Some(vec!["anthropic/*".to_string(), "claude-*".to_string()])
        );
        assert_eq!(enabled_models(&json!({ "enabledModels": [] })), None);
        assert_eq!(
            enabled_models(&json!({ "enabledModels": "claude-*" })),
            None
        );
        assert_eq!(enabled_models(&Value::Null), None);
    }

    #[test]
    fn enabled_model_patterns_match_pi_cli_forms() {
        // Canonical provider/id and bare id are both exact matches.
        assert!(model_matches_enabled_pattern(
            "Anthropic",
            "claude-sonnet-4",
            "anthropic/claude-sonnet-4"
        ));
        assert!(model_matches_enabled_pattern(
            "anthropic",
            "claude-sonnet-4",
            "claude-sonnet-4"
        ));

        // Prefix and provider globs use the same case-insensitive matcher.
        assert!(model_matches_enabled_pattern(
            "anthropic",
            "claude-sonnet-4",
            "CLAUDE-*"
        ));
        assert!(model_matches_enabled_pattern(
            "anthropic",
            "claude-sonnet-4",
            "anthropic/*"
        ));
        assert!(!model_matches_enabled_pattern(
            "openai",
            "gpt-5",
            "anthropic/*"
        ));

        // Thinking is a selector suffix, not part of the model identity.
        assert!(model_matches_enabled_pattern(
            "anthropic",
            "claude-sonnet-4",
            "anthropic/claude-sonnet-4:thinking"
        ));
        assert!(model_matches_enabled_pattern(
            "anthropic",
            "claude-sonnet-4:high",
            "claude-sonnet-4"
        ));
    }

    #[test]
    fn missing_or_empty_enabled_models_leave_models_unrestricted() {
        assert!(is_model_enabled("openai", "gpt-5", None));
        assert!(is_model_enabled("openai", "gpt-5", Some(&[])));
        let patterns = vec!["anthropic/*".to_string()];
        assert!(!is_model_enabled("openai", "gpt-5", Some(&patterns)));
        assert!(is_model_enabled(
            "anthropic",
            "claude-sonnet-4",
            Some(&patterns)
        ));
    }
}
