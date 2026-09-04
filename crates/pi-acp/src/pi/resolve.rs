//! pi executable resolution (S9 / W-456, fixes pi-acp #27).
//!
//! On **Windows** `pi` is installed as an npm global: the real entry point is a
//! batch wrapper `pi.cmd` in the npm `PATH` dir, not a native `pi.exe`. Relying
//! on `std::process::Command`'s implicit Windows resolution (CreateProcess
//! PATHEXT search + auto-`cmd /c` wrapping) is exactly what #27 tripped over —
//! bare-name launches can fail or misfire depending on `PATH`/`PATHEXT` and
//! whether the wrapper path has spaces.
//!
//! So we resolve **explicitly**: given the configured command name, expand a
//! bare name (no path separator, no extension) against `PATH` (and `PATHEXT`
//! on Windows) to a concrete file. If that file is a Windows `.bat`/`.cmd`
//! wrapper, launch it through `cmd.exe /d /s /c` so the batch file actually
//! runs with our arguments. Everything is a pure, dependency-free function so
//! the Windows branch is unit-testable on any host (see `tests`).

use std::path::Path;

/// How to launch the resolved pi executable.
///
/// The final `Command` is built as:
/// ```ignore
/// Command::new(&resolved.program).args(&resolved.cmd_args).args(pi_args)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPi {
    /// Program to pass to `Command::new` (the resolved pi path, or `cmd.exe`
    /// when the wrapper is launched through the shell).
    pub program: String,
    /// Arguments inserted between the program and the real pi args. Empty on
    /// unix / for a native binary; `["/d","/s","/c","<pi.cmd>"]` when a Windows
    /// batch wrapper must be launched through `cmd.exe`.
    pub cmd_args: Vec<String>,
}

/// A PATHEXT / file extension that denotes a batch wrapper requiring `cmd.exe`.
/// Accepts both the dotted form (PATHEXT `.CMD`) and the bare form (`cmd`).
fn is_batch_ext(ext: &str) -> bool {
    let e = ext.strip_prefix('.').unwrap_or(ext);
    e.eq_ignore_ascii_case("CMD") || e.eq_ignore_ascii_case("BAT")
}

/// Quote a Windows path for use inside a `cmd /c` command line **only** when it
/// contains whitespace (avoids cmd's quote-stripping quirk for the common
/// space-free npm dir, while still surviving `C:\Users\John Smith\...`).
fn maybe_quote_win(path: &str) -> String {
    if path.chars().any(|c| c.is_whitespace()) {
        format!("\"{path}\"")
    } else {
        path.to_string()
    }
}

/// The default extension order for a bare Windows command name. Mirrors the
/// stock `PATHEXT` ordering (`.COM`/`.EXE` first) with the batch wrappers last.
const DEFAULT_PATHEXT: &[&str] = &[".COM", ".EXE", ".BAT", ".CMD"];

/// Resolve `pi_command` into a launchable [`ResolvedPi`.
///
/// `os` is the target OS name (`"windows"` / `"unix"`); `path` is the `PATH`
/// value to search; `pathext` is the Windows `PATHEXT` value (ignored off
/// Windows). Passing `None` for either falls back to the best default for the
/// platform. All resolution is pure so the Windows branch is testable on any
/// host.
pub fn resolve_pi_command(
    pi_command: &str,
    os: &str,
    path: Option<&str>,
    pathext: Option<&str>,
) -> ResolvedPi {
    let is_windows = os.eq_ignore_ascii_case("windows");
    let trimmed = pi_command.trim();

    // A name that already names a file — an absolute/relative path with a
    // separator, or one that carries its own extension — is used as-is (we only
    // add the `cmd` wrapper on Windows for a bare `.bat`/`.cmd`).
    let has_separator = trimmed.contains('/') || trimmed.contains('\\');
    let ext = split_ext(trimmed).1;
    let has_ext = ext.is_some();

    if has_separator || has_ext {
        if is_windows && is_batch_ext(ext.unwrap_or("")) {
            // Explicit `.cmd`/`.bat` path: launch through cmd.exe.
            return ResolvedPi {
                program: "cmd.exe".to_string(),
                cmd_args: vec![
                    "/d".into(),
                    "/s".into(),
                    "/c".into(),
                    maybe_quote_win(trimmed),
                ],
            };
        }
        // Native binary / absolute path / unix: use directly.
        return ResolvedPi {
            program: trimmed.to_string(),
            cmd_args: Vec::new(),
        };
    }

    // Bare name, no extension, no separator: resolve against PATH.
    if is_windows {
        // Search PATH x PATHEXT (default PATHEXT if unset).
        let exts: Vec<&str> = match pathext.filter(|e| !e.trim().is_empty()) {
            Some(e) => e
                .split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect(),
            None => DEFAULT_PATHEXT.to_vec(),
        };
        if let Some(p) = path {
            for dir in split_path(p, true) {
                let parent = Path::new(&dir);
                for ext in &exts {
                    // Resolve to the *actual* on-disk filename (correct casing),
                    // since PATHEXT is upper-case but the wrapper is often
                    // lower-case on disk.
                    let want = format!("{trimmed}{ext}");
                    if let Some(real) = find_real_name(parent, &want) {
                        let path = parent.join(&real);
                        if is_batch_ext(ext) {
                            return ResolvedPi {
                                program: "cmd.exe".to_string(),
                                cmd_args: vec![
                                    "/d".into(),
                                    "/s".into(),
                                    "/c".into(),
                                    maybe_quote_win(&path.to_string_lossy()),
                                ],
                            };
                        }
                        return ResolvedPi {
                            program: path.to_string_lossy().into_owned(),
                            cmd_args: Vec::new(),
                        };
                    }
                }
            }
        }
        // Could not resolve on disk (e.g. PATH unset in a unit test). Fall back
        // to the bare name and let Windows CreateProcess do its PATHEXT search —
        // never worse than before.
        ResolvedPi {
            program: trimmed.to_string(),
            cmd_args: Vec::new(),
        }
    } else {
        // Unix: search PATH for an extless executable; use the first hit. If
        // not found, return the bare name and let the OS resolve it.
        if let Some(p) = path {
            for dir in split_path(p, false) {
                let candidate = Path::new(&dir).join(trimmed);
                if candidate_exists(&candidate) {
                    return ResolvedPi {
                        program: candidate.to_string_lossy().into_owned(),
                        cmd_args: Vec::new(),
                    };
                }
            }
        }
        ResolvedPi {
            program: trimmed.to_string(),
            cmd_args: Vec::new(),
        }
    }
}

/// Resolve using the current process environment (real spawn path).
pub fn resolve_current_env(pi_command: &str) -> ResolvedPi {
    let os = std::env::consts::OS;
    let path = std::env::var("PATH").ok();
    let pathext = std::env::var("PATHEXT").ok();
    resolve_pi_command(pi_command, os, path.as_deref(), pathext.as_deref())
}

/// Split a command name into `(stem, extension-without-dot)`.
fn split_ext(name: &str) -> (&str, Option<&str>) {
    match name.rfind('.') {
        // A leading dot (e.g. ".cmd") or a dot in a dir component is not an
        // extension for our purposes; require the dot not to be first.
        Some(i) if i > 0 => (&name[..i], Some(&name[i + 1..])),
        _ => (name, None),
    }
}

/// Split a `PATH`-like value on the platform separator: `;` on Windows
/// (drive-letter `:` in each entry must NOT split it), `:` elsewhere. Pass
/// `is_windows` so a Windows `C:\...` PATH is split on `;`, not `:`.
fn split_path(p: &str, is_windows: bool) -> Vec<String> {
    let sep = if is_windows { ';' } else { ':' };
    p.split(sep)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Existence check for the unix branch (plain `is_file`; `exists` on Windows).
#[cfg(unix)]
fn candidate_exists(p: &Path) -> bool {
    p.is_file()
}
#[cfg(windows)]
fn candidate_exists(p: &Path) -> bool {
    p.exists()
}

/// Find `want` inside `parent`, returning the real on-disk filename (correct
/// casing). Exact match first (fast path; on a real Windows box the case-
/// insensitive FS makes this hit regardless of PATHEXT casing), then a
/// case-insensitive directory scan so the windows branch behaves correctly in
/// cross-platform unit tests (Linux is case-sensitive).
fn find_real_name(parent: &Path, want: &str) -> Option<String> {
    if parent.join(want).exists() {
        return Some(want.to_string());
    }
    let want_lc = want.to_ascii_lowercase();
    let Ok(entries) = std::fs::read_dir(parent) else {
        return None;
    };
    for e in entries.flatten() {
        let name = e.file_name();
        if name.to_string_lossy().to_ascii_lowercase() == want_lc {
            return Some(name.to_string_lossy().into_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unix() -> &'static str {
        "unix"
    }
    fn win() -> &'static str {
        "windows"
    }

    #[test]
    fn bare_name_unix_resolves_in_path() {
        let r = resolve_pi_command("pi", unix(), Some("/usr/bin:/opt/bin"), None);
        // No files actually exist at those paths in this sandbox, so it falls
        // back to the bare name (the OS resolves it at spawn).
        assert_eq!(r.program, "pi");
        assert!(r.cmd_args.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn bare_name_unix_picks_existing_path_entry() {
        // Use a real, existing file path as a stand-in directory entry so the
        // resolution can actually find something: point PATH at a dir that
        // contains a file named "pi".
        let tmp = tempfile::tempdir().unwrap();
        let pi = tmp.path().join("pi");
        std::fs::write(&pi, b"#!/bin/sh\n").unwrap();
        let r = resolve_pi_command("pi", unix(), Some(tmp.path().to_str().unwrap()), None);
        assert_eq!(r.program, pi.to_string_lossy());
        assert!(r.cmd_args.is_empty());
    }

    #[test]
    fn absolute_path_used_verbatim_on_unix() {
        let r = resolve_pi_command("/home/u/.local/bin/pi", unix(), Some("/usr/bin"), None);
        assert_eq!(r.program, "/home/u/.local/bin/pi");
        assert!(r.cmd_args.is_empty());
    }

    #[test]
    fn windows_bare_name_prefers_exe_over_cmd() {
        let r = resolve_pi_command("pi", win(), Some("C:\\npm"), Some(".CMD;.EXE"));
        // Neither file exists in the sandbox; fall back to the bare name.
        assert_eq!(r.program, "pi");
        assert!(r.cmd_args.is_empty());
    }

    #[test]
    fn windows_bare_name_wraps_existing_cmd() {
        // Build a real .cmd so resolution finds it.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_string_lossy().into_owned();
        std::fs::write(tmp.path().join("pi.cmd"), "@echo off\r\n").unwrap();
        let r = resolve_pi_command("pi", win(), Some(&dir), Some(".CMD"));
        assert_eq!(r.program, "cmd.exe");
        let joined = r.cmd_args.join(" ");
        assert!(joined.starts_with("/d /s /c "), "cmd_args: {joined}");
        assert!(
            joined.to_ascii_lowercase().ends_with("pi.cmd"),
            "cmd_args: {joined}"
        );
    }

    #[test]
    fn windows_bare_name_prefers_native_exe_when_present() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_string_lossy().into_owned();
        std::fs::write(tmp.path().join("pi.exe"), b"MZ").unwrap();
        std::fs::write(tmp.path().join("pi.cmd"), "@echo off\r\n").unwrap();
        // PATHEXT order: .CMD first would pick the wrapper; with .EXE first the
        // native binary wins (no shell needed).
        let r = resolve_pi_command("pi", win(), Some(&dir), Some(".EXE;.CMD"));
        assert!(r.cmd_args.is_empty(), "should not shell-wrap: {r:?}");
        assert!(
            r.program.to_ascii_lowercase().ends_with("pi.exe"),
            "program: {}",
            r.program
        );
    }

    #[test]
    fn windows_explicit_cmd_path_wraps_via_cmd() {
        let r = resolve_pi_command("C:\\npm\\pi.cmd", win(), Some("C:\\npm"), None);
        assert_eq!(r.program, "cmd.exe");
        let joined = r.cmd_args.join(" ");
        assert!(joined.starts_with("/d /s /c "), "cmd_args: {joined}");
        assert!(joined.contains("pi.cmd"), "cmd_args: {joined}");
    }

    #[test]
    fn windows_explicit_exe_path_used_verbatim() {
        let r = resolve_pi_command("C:\\npm\\pi.exe", win(), None, None);
        assert_eq!(r.program, "C:\\npm\\pi.exe");
        assert!(r.cmd_args.is_empty());
    }

    #[test]
    fn windows_cmd_with_spaces_is_quoted() {
        let tmp = tempfile::tempdir().unwrap();
        let spaced = tmp.path().join("My Npm Dir");
        std::fs::create_dir_all(&spaced).unwrap();
        std::fs::write(spaced.join("pi.cmd"), "@echo off\r\n").unwrap();
        // PATH must point at the spaced directory itself so `pi.cmd` resolves.
        let dir = spaced.to_string_lossy().into_owned();
        let r = resolve_pi_command("pi", win(), Some(&dir), Some(".CMD"));
        // The wrapper path contains a space, so the /c target must be quoted.
        assert_eq!(r.program, "cmd.exe");
        let last = r.cmd_args.last().unwrap();
        assert!(
            last.starts_with('"') && last.ends_with('"'),
            "target: {last}"
        );
        assert!(last.contains("My Npm Dir"), "target: {last}");
    }
}
