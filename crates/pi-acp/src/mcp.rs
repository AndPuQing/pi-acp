//! MCP wiring (W-483): session-scoped `runtime-register`接线.
//!
//! ## Who provides what (read before touching this file)
//!
//! MCP servers are **always provided by the ACP caller (IDE)** via
//! `session/new { mcp_servers }` / `session/load { mcp_servers }`. pi-acp
//! itself runs no MCP server — it only wires the caller's menu through:
//!
//! ```text
//! IDE --session/new { mcp_servers }--> pi-acp (validate, spawn, gate)
//!   --PI_ACP_MCP_SERVERS_JSON + --extension registrar--> pi child
//!     --pi-mcp-adapter:runtime-register:v1--> pi-mcp-adapter (in-pi)
//!       --stdio/http/sse--> MCP servers
//! ```
//!
//! - Only the `runtime-register` route is implemented: one
//!   `pi-mcp-adapter:runtime-register:v1` event per server, `{ name,
//!   definition }`, whose synchronous `request.result` carries a registration
//!   with `dispose()`. No `--mcp-config` temp-file fallback, no MCP-over-ACP
//!   (`mcp/connect|message|disconnect`, shim, bridge) — those are explicitly
//!   out of scope for W-483.
//! - Only stdio / http / sse transports. Anything else (including a future
//!   `acp` transport) is rejected with an explicit error, never silently
//!   dropped.
//! - pi-acp never reads or writes the user project's `.pi/mcp.json` and
//!   never creates `.bak` files. Injection is purely additive and
//!   session-scoped: the payload travels in the pi child's environment and
//!   registrations are process-local + non-persisted, so killing pi always
//!   cleans up.
//!
//! ## `PI_MCP_CONFIG_MODE=exclusive`: decided OFF (not set)
//!
//! With the runtime-register route there is no config file to scope, so the
//! only question is whether the adapter should *also* ignore the user's own
//! (global/project) MCP configuration. Decision: leave the variable unset.
//!
//! - Injection stays additive: the user's own MCP tools keep working in
//!   pi-acp sessions; the IDE-declared servers are added on top.
//! - Name collisions fail closed instead of shadowing: the adapter rejects a
//!   runtime registration whose name collides with a configured server (and
//!   vice versa at init), and pi-acp surfaces that as an explicit per-server
//!   error. Hiding the user's config (exclusive) would trade a loud,
//!   diagnosable conflict for silent tool disappearance — worse.
//!
//! ## Capability honesty
//!
//! `initialize` advertises `http`/`sse` **only** when `PI_ACP_ENABLE_MCP=true`
//! *and* a pi-mcp-adapter is installed as a pi package (see
//! [`adapter_available`]). There is no `acp` capability item: the schema
//! feature for it is off and MCP-over-ACP is out of scope. Server-initiated
//! abilities (sampling, elicitation, …) are the adapter's domain; pi-acp
//! advertises transport reachability only and never claims full MCP
//! semantics — anything unsupported surfaces as a protocol-level error from
//! the adapter, not as a silent drop here.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_client_protocol::schema::v1::McpServer;
use serde::{Deserialize, Serialize};

/// Env gate: `initialize` advertises MCP transports only when this is `true`
/// (case-insensitive). Unset/anything-else means off; `false` force-closes.
pub const MCP_ENABLE_ENV: &str = "PI_ACP_ENABLE_MCP";
/// Scraped from the pi child's stderr, never stdout: pi routes everything
/// its extensions print (including raw `process.stdout.write`) to the
/// child's stderr, so the registrar's `console.log` markers land there and
/// pi-acp pipes + filters them (see `pi::process`).
/// Per-pi-child payload: JSON array of `{ name, definition }` pairs the
/// in-pi registrar (`mcp_registrar.js`) registers. Set per child process via
/// `Command::env`, never globally, never on disk.
pub const MCP_SERVERS_ENV: &str = "PI_ACP_MCP_SERVERS_JSON";
/// Stderr marker prefix the registrar reports through (single lines pi-acp
/// scrapes from the child's stderr, never protocol).
pub const MCP_MARKER_PREFIX: &str = "PI_ACP_MCP:";
/// How long `session/new|load` waits for every requested server's marker
/// before failing the handshake explicitly (registration itself is
/// synchronous in pi; this only bounds a missing/dead registrar).
pub const MCP_REGISTER_TIMEOUT: Duration = Duration::from_secs(15);
/// Bumped whenever `mcp_registrar.js` changes so a stale materialized copy is
/// never reused.
const REGISTRAR_VERSION: &str = "w483-v1";
/// Embedded registrar source (materialized next to temp, see
/// [`materialize_registrar`]).
const REGISTRAR_JS: &str = include_str!("mcp_registrar.js");

/// Adapter `ServerEntry` subset pi-acp produces (adapter `types.ts`):
/// stdio (`command/args/env`) and http/sse (`url/headers`) plus the
/// transport pin for sse. Unknown adapter fields are never sent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServerEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    /// Pinned to `"sse"` for ACP SSE servers (adapter `httpTransport`:
    /// forces the transport instead of client fallback).
    #[serde(rename = "httpTransport", skip_serializing_if = "Option::is_none")]
    pub http_transport: Option<String>,
}

/// One validated server: the registry name plus its adapter definition.
#[derive(Debug, Clone, PartialEq)]
pub struct McpServerSpec {
    pub name: String,
    pub definition: ServerEntry,
}

/// Validate + normalize ACP `mcp_servers` into adapter definitions.
///
/// Every rejection is an explicit `Err(String)` naming the offending server —
/// the caller maps it onto ACP `invalidParams`. Unsupported transports
/// (wildcard arm: future variants such as MCP-over-ACP) are rejected, never
/// skipped.
pub fn normalize_mcp_servers(servers: &[McpServer]) -> Result<Vec<McpServerSpec>, String> {
    let mut out = Vec::with_capacity(servers.len());
    let mut seen = HashSet::new();
    for server in servers {
        let spec = normalize_one(server)?;
        if !seen.insert(spec.name.clone()) {
            return Err(format!(
                "MCP server \"{}\" is declared more than once",
                spec.name
            ));
        }
        out.push(spec);
    }
    Ok(out)
}

fn normalize_one(server: &McpServer) -> Result<McpServerSpec, String> {
    match server {
        McpServer::Stdio(stdio) => {
            let name = checked_name(&stdio.name)?;
            if stdio.command.as_os_str().is_empty() {
                return Err(format!(
                    "MCP server \"{name}\": stdio transport requires a non-empty command"
                ));
            }
            let mut env = HashMap::new();
            for var in &stdio.env {
                if var.name.trim().is_empty() {
                    return Err(format!(
                        "MCP server \"{name}\": env variable with an empty name"
                    ));
                }
                env.insert(var.name.clone(), var.value.clone());
            }
            Ok(McpServerSpec {
                name,
                definition: ServerEntry {
                    command: Some(stdio.command.to_string_lossy().to_string()),
                    args: Some(stdio.args.clone()),
                    env: if env.is_empty() { None } else { Some(env) },
                    url: None,
                    headers: None,
                    http_transport: None,
                },
            })
        }
        McpServer::Http(http) => {
            let name = checked_name(&http.name)?;
            let url = checked_http_url(&http.url, &name)?;
            let headers = headers_map(&http.headers, &name)?;
            Ok(McpServerSpec {
                name,
                definition: ServerEntry {
                    command: None,
                    args: None,
                    env: None,
                    url: Some(url),
                    headers,
                    http_transport: None,
                },
            })
        }
        McpServer::Sse(sse) => {
            let name = checked_name(&sse.name)?;
            let url = checked_http_url(&sse.url, &name)?;
            let headers = headers_map(&sse.headers, &name)?;
            Ok(McpServerSpec {
                name,
                definition: ServerEntry {
                    command: None,
                    args: None,
                    env: None,
                    url: Some(url),
                    headers,
                    http_transport: Some("sse".to_string()),
                },
            })
        }
        // Future/unstable transports (e.g. MCP-over-ACP) land here: reject
        // loudly so the caller can never silently drop a requested server.
        _ => Err(
            "unsupported MCP transport: only stdio, http and sse servers are supported \
             (MCP-over-ACP is not implemented)"
                .to_string(),
        ),
    }
}

/// Names must be non-empty (adapter fails closed on those) and single-line
/// (they travel in line-based stdout markers).
fn checked_name(name: &str) -> Result<String, String> {
    if name.trim().is_empty() {
        return Err("MCP server name must be a non-empty string".to_string());
    }
    if name.contains('\n') || name.contains('\r') {
        return Err(format!(
            "MCP server \"{name}\": name must not contain line breaks"
        ));
    }
    Ok(name.to_string())
}

fn checked_http_url(url: &str, server: &str) -> Result<String, String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "MCP server \"{server}\": http/sse transport requires a non-empty url"
        ));
    }
    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(format!(
            "MCP server \"{server}\": url must use http:// or https:// (got {trimmed:?})"
        ));
    }
    Ok(trimmed.to_string())
}

fn headers_map(
    headers: &[agent_client_protocol::schema::v1::HttpHeader],
    server: &str,
) -> Result<Option<HashMap<String, String>>, String> {
    let mut map = HashMap::new();
    for h in headers {
        if h.name.trim().is_empty() {
            return Err(format!(
                "MCP server \"{server}\": header with an empty name"
            ));
        }
        map.insert(h.name.clone(), h.value.clone());
    }
    Ok(if map.is_empty() { None } else { Some(map) })
}

/// Whether MCP wiring is switched on (`PI_ACP_ENABLE_MCP=true`,
/// case-insensitive; unset/false force off).
pub fn mcp_enabled() -> bool {
    std::env::var(MCP_ENABLE_ENV)
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Whether a pi-mcp-adapter is installed as a pi package
/// (`<agent dir>/npm/node_modules/pi-mcp-adapter`, i.e. `pi install
/// npm:pi-mcp-adapter`). `initialize` carries no cwd, so only the global
/// install is probed — never a project directory.
///
/// Note: the install must be *global*. pi in `--mode rpc` loads global
/// extensions plus explicit `--extension` paths, but does NOT discover
/// project `.pi/extensions` (verified against pi 0.85) — a project-local
/// adapter would advertise here yet never load in the child.
pub fn adapter_available() -> bool {
    adapter_available_at(&crate::settings::agent_dir())
}

/// [`adapter_available`] against an explicit agent dir (testable without
/// touching the real `~/.pi`).
pub fn adapter_available_at(agent_dir: &Path) -> bool {
    agent_dir
        .join("npm")
        .join("node_modules")
        .join("pi-mcp-adapter")
        .join("package.json")
        .is_file()
}

/// The `(http, sse)` pair for `initialize.mcpCapabilities`: advertised only
/// when the flag is on *and* the adapter is installed. No `acp` item exists
/// (schema feature off, MCP-over-ACP out of scope).
pub fn advertise_mcp_capabilities(enabled: bool, adapter_present: bool) -> (bool, bool) {
    let on = enabled && adapter_present;
    (on, on)
}

// ---------------------------------------------------------------------------
// Registrar markers
// ---------------------------------------------------------------------------

/// Per-server handshake outcome parsed out of the registrar's stderr markers.
#[derive(Debug, Default, PartialEq)]
pub struct McpHandshake {
    pub registered: Vec<String>,
    pub failed: Vec<(String, String)>,
}

/// True for registrar protocol lines (pi diagnostics never carry this prefix).
pub fn is_mcp_marker(line: &str) -> bool {
    line.starts_with(MCP_MARKER_PREFIX)
}

/// Parse registrar markers. Malformed marker lines are ignored (the registrar
/// only emits the two shapes below; anything else is someone else's stdout).
pub fn parse_mcp_markers(lines: &[String]) -> McpHandshake {
    let mut out = McpHandshake::default();
    for line in lines {
        let Some(body) = line.strip_prefix(MCP_MARKER_PREFIX) else {
            continue;
        };
        if let Some(name) = body.strip_prefix("registered:") {
            if !name.is_empty() {
                out.registered.push(name.to_string());
            }
        } else if let Some(rest) = body.strip_prefix("failed:") {
            let (name, reason) = rest
                .split_once(':')
                .map(|(n, r)| (n.to_string(), r.to_string()))
                .unwrap_or_else(|| (rest.to_string(), "registration failed".to_string()));
            if !name.is_empty() {
                out.failed.push((name, reason));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Per-session manager
// ---------------------------------------------------------------------------

/// Owns one ACP session's MCP wiring on the Rust side.
///
/// The actual registrations live inside the pi child (held by the registrar
/// extension, disposed via `registration.dispose()` on `session_shutdown`
/// and always discarded on process exit). This manager owns the validated
/// spec set, the spawn payload, and the marker gate: the session handshake
/// only completes once every requested server reported back, and teardown is
/// the session's own dispose (which kills pi, hence every registration).
/// No auto-respawn anywhere (decision 1): a dead pi surfaces loudly through
/// the existing `PiExited` paths.
#[derive(Debug)]
pub struct McpSessionManager {
    specs: Vec<McpServerSpec>,
    registered: HashSet<String>,
    failed: Vec<(String, String)>,
}

impl McpSessionManager {
    pub fn new(specs: Vec<McpServerSpec>) -> Self {
        Self {
            specs,
            registered: HashSet::new(),
            failed: Vec::new(),
        }
    }

    /// Names requested by the caller (handshake gate target set).
    pub fn requested_names(&self) -> Vec<String> {
        self.specs.iter().map(|s| s.name.clone()).collect()
    }

    /// The `PI_ACP_MCP_SERVERS_JSON` payload for the pi child:
    /// `[{ name, definition }]`.
    pub fn payload_json(&self) -> Result<String, serde_json::Error> {
        let pairs: Vec<serde_json::Value> = self
            .specs
            .iter()
            .map(|s| serde_json::json!({ "name": s.name, "definition": s.definition }))
            .collect();
        serde_json::to_string(&pairs)
    }

    /// Fold newly observed prelude lines into the gate state. Markers emit
    /// once per server, so re-applying overlapping snapshots is idempotent.
    pub fn apply_markers(&mut self, lines: &[String]) {
        let parsed = parse_mcp_markers(lines);
        self.registered.extend(parsed.registered);
        for (name, reason) in parsed.failed {
            if !self.failed.iter().any(|(n, _)| n == &name) {
                self.failed.push((name, reason));
            }
        }
    }

    /// Still-awaited server names.
    pub fn pending(&self) -> Vec<String> {
        let mut pending: Vec<String> = self
            .requested_names()
            .into_iter()
            .filter(|n| !self.registered.contains(n) && !self.failed.iter().any(|(f, _)| f == n))
            .collect();
        pending.sort();
        pending
    }

    /// The gate is settled once every requested server either registered or
    /// failed — the caller then either proceeds or fails explicitly naming
    /// the failed servers. Partial silence (no marker at all) keeps the gate
    /// open until the caller times out.
    pub fn is_settled(&self) -> bool {
        self.pending().is_empty()
    }

    /// Explicit per-server failure detail (`None` when nothing failed).
    /// Names exactly the servers that failed — never a generic blob — so one
    /// server's failure is reported as that server's failure (isolation).
    pub fn failure_message(&self) -> Option<String> {
        if self.failed.is_empty() {
            return None;
        }
        let mut parts: Vec<String> = self
            .failed
            .iter()
            .map(|(n, r)| format!("\"{n}\": {r}"))
            .collect();
        parts.sort();
        Some(format!(
            "MCP server registration failed: {}",
            parts.join("; ")
        ))
    }

    pub fn registered_count(&self) -> usize {
        self.registered.len()
    }
}

// ---------------------------------------------------------------------------
// Registrar materialization
// ---------------------------------------------------------------------------

/// Write the embedded registrar to a version-pinned path under the system
/// temp dir and return it (written once, reused; rewritten when the embedded
/// source changes).
///
/// This is extension *code*, not MCP configuration: it carries no server
/// definitions and no secrets (those travel per-child in the environment),
/// so sharing one copy across sessions is safe. Unix files get `0600`.
pub fn materialize_registrar() -> std::io::Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("pi-acp-{REGISTRAR_VERSION}"));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("mcp-registrar.js");
    let current = std::fs::read_to_string(&path).ok();
    if current.as_deref() != Some(REGISTRAR_JS) {
        let tmp = dir.join("mcp-registrar.js.tmp");
        std::fs::write(&tmp, REGISTRAR_JS)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(&tmp, &path)?;
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{
        EnvVariable, HttpHeader, McpServerHttp, McpServerSse, McpServerStdio,
    };

    fn stdio(name: &str) -> McpServer {
        McpServer::Stdio(
            McpServerStdio::new(name, "/usr/bin/fake-mcp")
                .args(vec!["--serve".to_string()])
                .env(vec![EnvVariable::new("TOKEN", "secret")]),
        )
    }

    fn http(name: &str) -> McpServer {
        McpServer::Http(
            McpServerHttp::new(name, "http://localhost:8000/mcp")
                .headers(vec![HttpHeader::new("Authorization", "Bearer x")]),
        )
    }

    fn sse(name: &str) -> McpServer {
        McpServer::Sse(McpServerSse::new(name, "https://example.com/sse"))
    }

    #[test]
    fn normalizes_all_three_transports() {
        let specs = normalize_mcp_servers(&[stdio("a"), http("b"), sse("c")]).unwrap();
        assert_eq!(specs.len(), 3);
        let std = &specs[0];
        assert_eq!(std.name, "a");
        assert_eq!(std.definition.command.as_deref(), Some("/usr/bin/fake-mcp"));
        assert_eq!(
            std.definition.args.as_deref(),
            Some(&["--serve".to_string()][..])
        );
        assert_eq!(
            std.definition.env.as_ref().unwrap()["TOKEN"],
            "secret".to_string()
        );
        let h = &specs[1];
        assert_eq!(
            h.definition.url.as_deref(),
            Some("http://localhost:8000/mcp")
        );
        assert_eq!(
            h.definition.headers.as_ref().unwrap()["Authorization"],
            "Bearer x".to_string()
        );
        assert_eq!(h.definition.http_transport, None);
        let s = &specs[2];
        assert_eq!(s.definition.url.as_deref(), Some("https://example.com/sse"));
        assert_eq!(s.definition.http_transport.as_deref(), Some("sse"));
    }

    #[test]
    fn rejects_empty_names_and_missing_command() {
        assert!(normalize_mcp_servers(&[stdio("")]).is_err());
        assert!(normalize_mcp_servers(&[stdio("   ")]).is_err());
        let bad = McpServer::Stdio(McpServerStdio::new("x", ""));
        let err = normalize_mcp_servers(&[bad]).unwrap_err();
        assert!(err.contains("\"x\"") && err.contains("command"), "{err}");
    }

    #[test]
    fn rejects_bad_urls_and_empty_headers() {
        let bad = McpServer::Http(McpServerHttp::new("h", "ftp://x/y"));
        let err = normalize_mcp_servers(&[bad]).unwrap_err();
        assert!(err.contains("\"h\"") && err.contains("http"), "{err}");
        let empty = McpServer::Sse(McpServerSse::new("s", ""));
        assert!(normalize_mcp_servers(&[empty]).is_err());
        let bad_header = McpServer::Http(
            McpServerHttp::new("h", "http://x").headers(vec![HttpHeader::new("", "v")]),
        );
        assert!(normalize_mcp_servers(&[bad_header]).is_err());
    }

    #[test]
    fn rejects_duplicates_and_multiline_names() {
        let err = normalize_mcp_servers(&[stdio("a"), http("a")]).unwrap_err();
        assert!(
            err.contains("\"a\"") && err.contains("more than once"),
            "{err}"
        );
        let evil = McpServer::Http(McpServerHttp::new("a\nb", "http://x"));
        assert!(normalize_mcp_servers(&[evil]).is_err());
    }

    #[test]
    fn capability_matrix_needs_flag_and_adapter() {
        assert_eq!(advertise_mcp_capabilities(false, false), (false, false));
        assert_eq!(advertise_mcp_capabilities(true, false), (false, false));
        assert_eq!(advertise_mcp_capabilities(false, true), (false, false));
        assert_eq!(advertise_mcp_capabilities(true, true), (true, true));
    }

    #[test]
    fn adapter_probe_reads_pi_package_dir() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!adapter_available_at(tmp.path()));
        let pkg = tmp.path().join("npm/node_modules/pi-mcp-adapter");
        std::fs::create_dir_all(&pkg).unwrap();
        assert!(!adapter_available_at(tmp.path()));
        std::fs::write(pkg.join("package.json"), "{}").unwrap();
        assert!(adapter_available_at(tmp.path()));
    }

    #[test]
    fn markers_parse_and_gate_settles() {
        let lines = vec![
            "some banner".to_string(),
            "PI_ACP_MCP:registered:a".to_string(),
            "PI_ACP_MCP:failed:b:already registered".to_string(),
            "PI_ACP_MCP:garbage".to_string(),
        ];
        assert!(!is_mcp_marker(&lines[0]));
        assert!(is_mcp_marker(&lines[1]));
        let parsed = parse_mcp_markers(&lines);
        assert_eq!(parsed.registered, vec!["a".to_string()]);
        assert_eq!(
            parsed.failed,
            vec![("b".to_string(), "already registered".to_string())]
        );

        let specs = normalize_mcp_servers(&[stdio("a"), http("b")]).unwrap();
        let mut mgr = McpSessionManager::new(specs);
        assert_eq!(mgr.pending(), vec!["a".to_string(), "b".to_string()]);
        assert!(!mgr.is_settled());
        mgr.apply_markers(&lines);
        assert!(mgr.is_settled());
        assert_eq!(mgr.registered_count(), 1);
        let msg = mgr.failure_message().unwrap();
        assert!(
            msg.contains("\"b\"") && msg.contains("already registered"),
            "{msg}"
        );
        // Re-applying overlapping snapshots is idempotent.
        mgr.apply_markers(&lines);
        assert_eq!(mgr.registered_count(), 1);
        assert_eq!(mgr.failure_message().unwrap(), msg);
    }

    #[test]
    fn payload_json_carries_name_and_definition() {
        let specs = normalize_mcp_servers(&[stdio("a")]).unwrap();
        let mgr = McpSessionManager::new(specs);
        let payload = mgr.payload_json().unwrap();
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v[0]["name"], "a");
        assert_eq!(v[0]["definition"]["command"], "/usr/bin/fake-mcp");
    }

    #[test]
    fn registrar_materializes_with_source() {
        let path = materialize_registrar().unwrap();
        assert_eq!(path.file_name().unwrap(), "mcp-registrar.js");
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("pi-mcp-adapter:runtime-register:v1"));
        // Second call reuses the file without rewriting.
        let again = materialize_registrar().unwrap();
        assert_eq!(path, again);
    }
}
