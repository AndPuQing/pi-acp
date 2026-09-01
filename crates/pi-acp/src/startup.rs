//! Startup info assembly + version check (S6, W-453).
//!
//! Ports `buildStartupInfo` / `buildUpdateNotice` from TS `acp/agent.ts`:
//! - [`build_startup_info`] renders the startup prelude (pi version header +
//!   Context / Skills / Prompts / Extensions sections + optional update
//!   notice). The TS reference probes `pi --version` synchronously here;
//!   the Rust version takes the version as an argument so the caller fetches
//!   it **asynchronously** (design D6 — no sync subprocess probe on the
//!   `session/new` critical path).
//! - [`build_update_notice`] runs the npm-registry version check. It is
//!   **disabled by default** (decision 2, fixes #72): the caller only invokes
//!   it when `PI_ACP_VERSION_CHECK=true`, and should run it on a spawned task
//!   with a hard timeout so a slow registry never blocks session setup.

use std::path::{Path, PathBuf};

use crate::settings::agent_dir;

/// Default npm lookup timeout (mirrors the TS `timeout: 800`).
const NPM_VIEW_TIMEOUT_MS: u64 = 800;

/// A discovered skill/prompt/extension file (rendered as a `- item` line).
struct Section {
    title: &'static str,
    items: Vec<String>,
}

/// Render the startup info markdown prelude.
///
/// Mirrors TS `buildStartupInfo`:
/// - `pi v<version>` header (when known);
/// - Context: `<cwd>/AGENTS.md` (when present);
/// - Skills: `<agent dir>/skills`, `~/.agents/skills`, `<cwd>/.pi/skills` —
///   direct `.md` files plus recursive `SKILL.md` (skipping `node_modules` /
///   `.git`);
/// - Prompts: `<agent dir>/prompts/*.md` as `/name`;
/// - Extensions: `<agent dir>/extensions/*.{ts,js}` + `packages` entries from
///   the merged settings;
/// - the update notice, appended after a `---` divider.
///
/// `quietStartup` handling lives in the caller: it suppresses everything but
/// the update notice.
pub fn build_startup_info(
    cwd: &Path,
    pi_version: Option<&str>,
    update_notice: Option<&str>,
) -> String {
    build_startup_info_at(&agent_dir(), cwd, pi_version, update_notice)
}

/// [`build_startup_info`] against an explicit agent dir (testable without
/// touching the real `~/.pi` / env vars).
pub fn build_startup_info_at(
    agent: &Path,
    cwd: &Path,
    pi_version: Option<&str>,
    update_notice: Option<&str>,
) -> String {
    let mut md: Vec<String> = Vec::new();

    if let Some(v) = pi_version {
        md.push(format!("pi v{v}"));
        md.push("---".to_string());
        md.push(String::new());
    }

    let agent = agent.to_path_buf();

    // Context
    let mut context_items = Vec::new();
    let context_path = cwd.join("AGENTS.md");
    if context_path.exists() {
        context_items.push(context_path.to_string_lossy().to_string());
    }
    add_section(
        &mut md,
        &Section {
            title: "Context",
            items: context_items,
        },
    );

    // Skills
    let mut skills_items = Vec::new();
    push_skills_from_root(&agent.join("skills"), &mut skills_items);
    if let Some(home) = std::env::var_os("HOME") {
        push_skills_from_root(
            &PathBuf::from(home).join(".agents").join("skills"),
            &mut skills_items,
        );
    }
    push_skills_from_root(&cwd.join(".pi").join("skills"), &mut skills_items);
    add_section(
        &mut md,
        &Section {
            title: "Skills",
            items: skills_items,
        },
    );

    // Prompts
    let mut prompts_items = Vec::new();
    let prompts_dir = agent.join("prompts");
    if let Ok(entries) = std::fs::read_dir(&prompts_dir) {
        let mut names: Vec<String> = entries
            .flatten()
            .filter(|e| {
                e.file_type().map(|t| t.is_file()).unwrap_or(false)
                    && e.file_name().to_string_lossy().ends_with(".md")
            })
            .map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                format!("/{}", name.trim_end_matches(".md"))
            })
            .collect();
        names.sort();
        prompts_items.extend(names);
    }
    add_section(
        &mut md,
        &Section {
            title: "Prompts",
            items: prompts_items,
        },
    );

    // Extensions
    let mut ext_items = Vec::new();
    let ext_dir = agent.join("extensions");
    if let Ok(entries) = std::fs::read_dir(&ext_dir) {
        let mut files: Vec<String> = entries
            .flatten()
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                e.file_type().map(|t| t.is_file()).unwrap_or(false)
                    && (name.ends_with(".ts") || name.ends_with(".js"))
            })
            .map(|e| e.path().to_string_lossy().to_string())
            .collect();
        files.sort();
        ext_items.extend(files);
    }
    // npm packages from settings (global + project)
    for settings_path in [
        agent.join("settings.json"),
        cwd.join(".pi").join("settings.json"),
    ] {
        if let Ok(raw) = std::fs::read_to_string(&settings_path) {
            if let Ok(settings) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let Some(pkgs) = settings
                    .get("packages")
                    .and_then(serde_json::Value::as_array)
                {
                    for pkg in pkgs {
                        if let Some(s) = pkg.as_str() {
                            if let Some(npm) = s.strip_prefix("npm:") {
                                ext_items.push(format!("{npm}\n  - index.ts"));
                            } else {
                                ext_items.push(s.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    add_section(
        &mut md,
        &Section {
            title: "Extensions",
            items: ext_items,
        },
    );

    if let Some(notice) = update_notice {
        md.push("---".to_string());
        md.push(notice.to_string());
        md.push(String::new());
    }

    let joined = md.join("\n");
    joined.trim().to_string() + "\n"
}

/// Scan a skills root: direct `.md` files at the top level plus recursive
/// `SKILL.md` files in subdirectories (skipping `node_modules` / `.git`).
fn push_skills_from_root(root: &Path, out: &mut Vec<String>) {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        let name = entry.file_name().to_string_lossy().to_string();
        if ft.is_file() && name.to_lowercase().ends_with(".md") {
            out.push(path.to_string_lossy().to_string());
        }
    }
    // Recursive SKILL.md scan.
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if name == "node_modules" || name == ".git" {
                continue;
            }
            let Ok(ft) = entry.file_type() else {
                continue;
            };
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() && name == "SKILL.md" {
                out.push(path.to_string_lossy().to_string());
            }
        }
    }
}

fn add_section(md: &mut Vec<String>, section: &Section) {
    let cleaned: Vec<String> = section
        .items
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if cleaned.is_empty() {
        return;
    }
    md.push(format!("## {}", section.title));
    for item in cleaned {
        md.push(format!("- {item}"));
    }
    md.push(String::new());
}

/// Fetch the installed pi version (`pi --version`), best-effort.
///
/// Async wrapper so the caller never blocks on a subprocess probe inside the
/// dispatch loop (design D6). Requires a semver-shaped answer (real pi prints
/// `vX.Y.Z`; anything else — e.g. an error banner — is dropped).
pub async fn fetch_pi_version(pi_command: &str) -> Option<String> {
    tokio::time::timeout(std::time::Duration::from_millis(1500), async {
        let output = tokio::process::Command::new(pi_command)
            .arg("--version")
            .output()
            .await
            .ok()?;
        let raw = String::from_utf8_lossy(if output.stdout.is_empty() {
            &output.stderr
        } else {
            &output.stdout
        });
        let v = raw.trim().trim_start_matches('v').trim().to_string();
        is_semver(&v).then_some(v)
    })
    .await
    .ok()
    .flatten()
}

/// Run the npm-registry version check: `pi --version` vs the latest published
/// `@earendil-works/pi-coding-agent`. Returns the notice text when a newer
/// version exists, else `None` (any failure → `None`, best-effort).
///
/// This is intentionally **not** called unless the version check is enabled
/// (decision 2) and should run with an overall timeout on a spawned task.
pub async fn build_update_notice(pi_command: &str) -> Option<String> {
    let installed = fetch_pi_version(pi_command).await?;
    if !is_semver(&installed) {
        return None;
    }
    let latest = tokio::time::timeout(
        std::time::Duration::from_millis(NPM_VIEW_TIMEOUT_MS),
        async {
            let output = tokio::process::Command::new("npm")
                .args(["view", "@earendil-works/pi-coding-agent", "version"])
                .output()
                .await
                .ok()?;
            let raw = String::from_utf8_lossy(&output.stdout);
            let v = raw.trim().trim_start_matches('v').trim().to_string();
            (!v.is_empty()).then_some(v)
        },
    )
    .await
    .ok()
    .flatten()?;
    if !is_semver(&latest) || compare_semver(&latest, &installed) <= 0 {
        return None;
    }
    Some(format!(
        "New version available: v{latest} (installed v{installed}). Run: `npm i -g @earendil-works/pi-coding-agent`"
    ))
}

/// `x.y.z` (+ optional pre-release/build) semver shape check.
fn is_semver(v: &str) -> bool {
    let v = v.trim();
    let core = v.split(['-', '+']).next().unwrap_or(v);
    let mut parts = core.split('.');
    let (Some(a), Some(b), Some(c)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    !a.is_empty()
        && !b.is_empty()
        && !c.is_empty()
        && parts.next().is_none()
        && a.chars().all(|c| c.is_ascii_digit())
        && b.chars().all(|c| c.is_ascii_digit())
        && c.chars().all(|c| c.is_ascii_digit())
}

/// Compare two semver strings on their `x.y.z` core (pre-release/build tags do
/// not affect ordering beyond the base, matching the TS comparator).
fn compare_semver(a: &str, b: &str) -> i32 {
    let nums = |s: &str| -> Vec<i64> {
        s.split(['-', '+'])
            .next()
            .unwrap_or(s)
            .split('.')
            .filter_map(|n| n.parse().ok())
            .collect()
    };
    let pa = nums(a);
    let pb = nums(b);
    for i in 0..3 {
        let da = pa.get(i).copied().unwrap_or(0);
        let db = pb.get(i).copied().unwrap_or(0);
        if da > db {
            return 1;
        }
        if da < db {
            return -1;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn semver_validation_and_comparison() {
        assert!(is_semver("1.2.3"));
        assert!(is_semver("1.2.3-beta.1"));
        assert!(is_semver("1.2.3+build"));
        assert!(!is_semver("v1.2.3"));
        assert!(!is_semver("1.2"));
        assert!(!is_semver("1.2.x"));
        assert!(!is_semver(""));

        assert_eq!(compare_semver("1.2.3", "1.2.3"), 0);
        assert_eq!(compare_semver("1.2.4", "1.2.3"), 1);
        assert_eq!(compare_semver("1.2.3", "1.10.0"), -1);
        assert_eq!(compare_semver("2.0.0-beta", "1.9.9"), 1);
    }

    #[test]
    fn startup_info_lists_context_skills_prompts_extensions() {
        let dir = TempDir::new().unwrap();
        let cwd = dir.path().join("project");
        let agent = dir.path().join("agent");
        fs::create_dir_all(&cwd).unwrap();
        fs::create_dir_all(&agent).unwrap();

        // Context
        fs::write(cwd.join("AGENTS.md"), "context").unwrap();
        // Project skills: direct md + nested SKILL.md
        fs::create_dir_all(cwd.join(".pi").join("skills")).unwrap();
        fs::write(cwd.join(".pi").join("skills").join("notes.md"), "n").unwrap();
        fs::create_dir_all(cwd.join(".pi").join("skills").join("deep")).unwrap();
        fs::write(
            cwd.join(".pi").join("skills").join("deep").join("SKILL.md"),
            "s",
        )
        .unwrap();
        // Prompts + extensions in the agent dir
        fs::create_dir_all(agent.join("prompts")).unwrap();
        fs::write(agent.join("prompts").join("fix.md"), "p").unwrap();
        fs::create_dir_all(agent.join("extensions")).unwrap();
        fs::write(agent.join("extensions").join("tool.ts"), "t").unwrap();

        let out = build_startup_info_at(&agent, &cwd, Some("1.2.3"), None);

        assert!(out.starts_with("pi v1.2.3\n---"), "header first: {out}");
        assert!(out.contains("## Context\n- "), "{out}");
        assert!(out.contains("notes.md"), "{out}");
        assert!(out.contains("SKILL.md"), "{out}");
        assert!(out.contains("## Prompts\n- /fix"), "{out}");
        assert!(out.contains("## Extensions\n- "), "{out}");
        assert!(out.contains("tool.ts"), "{out}");
        assert!(!out.contains("New version available"), "{out}");
    }

    #[test]
    fn startup_info_appends_update_notice_and_trims_empty_sections() {
        let dir = TempDir::new().unwrap();
        let cwd = dir.path();
        let agent = dir.path().join("agent");
        fs::create_dir_all(&agent).unwrap();
        let out = build_startup_info_at(&agent, cwd, None, Some("New version available: v9.9.9"));
        // No sections, no header — only the notice.
        assert_eq!(out, "---\nNew version available: v9.9.9\n");
    }

    #[test]
    fn startup_info_handles_npm_packages_in_extensions() {
        let dir = TempDir::new().unwrap();
        let cwd = dir.path().join("project");
        let agent = dir.path().join("agent");
        fs::create_dir_all(&cwd).unwrap();
        fs::create_dir_all(cwd.join(".pi")).unwrap();
        fs::create_dir_all(&agent).unwrap();
        fs::write(
            cwd.join(".pi").join("settings.json"),
            r#"{"packages": ["npm:@earendil-works/pi-codex", "local-ext"]}"#,
        )
        .unwrap();

        let out = build_startup_info_at(&agent, &cwd, None, None);

        assert!(out.contains("## Extensions"), "{out}");
        assert!(
            out.contains("@earendil-works/pi-codex\n  - index.ts"),
            "{out}"
        );
        assert!(out.contains("local-ext"), "{out}");
    }
}
