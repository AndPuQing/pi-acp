//! W-484 acceptance (real chain): fake MCP servers + REAL pi-mcp-adapter +
//! REAL mcp_registrar.js + (where marked) REAL pi.
//!
//! Suite map (acceptance items 1-4; item 5 is CI itself):
//! - `harness_*`: the stub-pi harness drives the real adapter + real
//!   registrar over real MCP wire protocol to zero-dependency fake servers
//!   (stdio, Streamable HTTP, SSE). Proves tools/list + tools/call
//!   roundtrips, per-server failure isolation, session-scoped snapshots and
//!   dispose — the data plane session/new hands off to.
//! - `real_pi_*`: full ACP `session/new` through pi-acp spawning REAL pi
//!   with the real adapter installed and the registrar extension. Proves the
//!   IDE-side injection path (env payload, extension args, stderr markers,
//!   handshake gate) plus replacement and file-residue cleanup.
//! - pi-acp's own bookkeeping (concurrency, silent/dead failures,
//!   load-swap) runs dependency-free in `mcp_acceptance.rs`.
//!
//! Provisioning: these tests `npm install` a pinned adapter + runner into a
//! versioned temp dir (reused across runs) and install the adapter into a
//! cached pi agent-dir template. Anything missing (node/npm offline,
//! unreachable registry, no pi binary) SKIP with a loud message instead of
//! failing — CI provides all of it, so CI never skips.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, OnceLock};

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::schema::{
    DeleteSessionRequest, InitializeRequest, McpServer, McpServerHttp, McpServerSse,
    McpServerStdio, NewSessionRequest, SessionNotification, SessionUpdate,
};
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::{on_receive_notification, AcpAgent, AcpAgentConfig, Client};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");
/// Adapter version Stage 1 was built against (W-482/W-483 reference).
const ADAPTER_PIN: &str = "2.32.1";
/// Real-pi host version this acceptance is verified against.
const PI_PIN: &str = "0.85.0";
/// Bump when the fixture layout or provisioning recipe changes.
const CHAIN_CACHE_VERSION: &str = "v1";

static MCP_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn acquire_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    MCP_TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcp-chain")
}

fn registrar_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/mcp_registrar.js")
}

// ---------------------------------------------------------------------------
// Host tool resolution (Windows runs .cmd shims via cmd.exe)
// ---------------------------------------------------------------------------

/// Resolve `name` on PATH (with PATHEXT-style suffixes on Windows).
fn find_on_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    let suffixes: &[&str] = if cfg!(windows) {
        &["", ".exe", ".cmd", ".bat"]
    } else {
        &[""]
    };
    for dir in std::env::split_paths(&path_var) {
        for suffix in suffixes {
            let candidate = dir.join(format!("{name}{suffix}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// Build a host-tool command: direct on unix, via `cmd /d /s /c` when the
/// resolved program is a `.cmd`/`.bat` shim (npm, pi on Windows).
fn host_command(program: &Path) -> std::process::Command {
    let needs_shell = cfg!(windows)
        && matches!(
            program.extension().and_then(|e| e.to_str()),
            Some("cmd") | Some("bat")
        );
    if needs_shell {
        let mut cmd = std::process::Command::new("cmd");
        cmd.args(["/d", "/s", "/c"]).arg(program);
        cmd
    } else {
        std::process::Command::new(program)
    }
}

fn npm_install(cwd: &Path, npm: &Path, packages: &[String]) -> Result<(), String> {
    let mut cmd = host_command(npm);
    cmd.current_dir(cwd)
        .arg("install")
        .arg("--no-save")
        .arg("--no-audit")
        .arg("--no-fund")
        // Single deterministic path: the adapter's optional peers overlap
        // the pi host packages pinned below.
        .arg("--legacy-peer-deps");
    for pkg in packages {
        cmd.arg(pkg);
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = cmd.output().map_err(|e| format!("spawn npm failed: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "npm install failed: {}",
            clip(&String::from_utf8_lossy(&output.stderr), 800)
        ))
    }
}

fn clip(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

// ---------------------------------------------------------------------------
// Chain env provisioning (adapter + tsx + host shims, cached in temp dir)
// ---------------------------------------------------------------------------

struct ChainEnv {
    /// Dir with node_modules (adapter, tsx, typebox, pi host pkgs).
    env_dir: PathBuf,
    /// adapterDir for the harness: *.ts sources copied out of node_modules
    /// (node refuses type-stripping *inside* node_modules).
    adapter_src: PathBuf,
    node: PathBuf,
}

/// Provision (or reuse) the harness env. `None` = skip: no node/npm or the
/// registry is unreachable. Never fails the suite on its own.
fn ensure_chain_env() -> Option<ChainEnv> {
    ensure_chain_env_with_versions(ADAPTER_PIN, PI_PIN)
}

fn ensure_chain_env_with_versions(adapter_pin: &str, pi_pin: &str) -> Option<ChainEnv> {
    let node = find_on_path("node").or_else(|| {
        eprintln!("SKIP: `node` not on PATH (real-chain tests need Node 20+)");
        None
    })?;
    let npm = find_on_path("npm").or_else(|| {
        eprintln!("SKIP: `npm` not on PATH (real-chain tests need npm)");
        None
    })?;

    let base = std::env::temp_dir()
        .join("pi-acp-mcp-chain")
        .join(CHAIN_CACHE_VERSION);
    let env_dir = base.join(format!("adapter-{adapter_pin}"));
    let ready = env_dir.join(".ready");
    let ready_ok = fs::read_to_string(&ready)
        .ok()
        .map(|s| s.trim().to_string())
        == Some(adapter_pin.to_string());
    if !ready_ok {
        let _ = fs::remove_dir_all(&env_dir);
        fs::create_dir_all(&env_dir).ok()?;
        if env_dir.join("package.json").missing() {
            let _ = fs::write(
                env_dir.join("package.json"),
                "{\"name\":\"pi-acp-chain-env\"}",
            );
        }
        let packages = [
            format!("pi-mcp-adapter@{adapter_pin}"),
            "tsx".to_string(),
            "typebox".to_string(),
            format!("@earendil-works/pi-coding-agent@{pi_pin}"),
            format!("@earendil-works/pi-tui@{pi_pin}"),
            format!("@earendil-works/pi-ai@{pi_pin}"),
        ];
        if let Err(e) = npm_install(&env_dir, &npm, &packages) {
            eprintln!("SKIP: chain env provisioning failed (offline registry?): {e}");
            return None;
        }
        // Copy the adapter sources out of node_modules for type-stripping.
        // The package.json (`"type": "module"`) must come along so the
        // TS loader keeps treating the sources as ESM (without it tsx
        // falls back to require() and dies with ERR_REQUIRE_CYCLE_MODULE).
        let src = env_dir.join("node_modules/pi-mcp-adapter");
        let dst = env_dir.join("adapter-src");
        let _ = fs::remove_dir_all(&dst);
        fs::create_dir_all(&dst).ok()?;
        fs::copy(src.join("package.json"), dst.join("package.json")).ok()?;
        let entries = fs::read_dir(&src).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("ts") {
                fs::copy(&path, dst.join(path.file_name()?)).ok()?;
            }
        }
        if !dst.join("index.ts").is_file() {
            eprintln!("SKIP: provisioned adapter has no index.ts");
            return None;
        }
        fs::write(&ready, adapter_pin).ok()?;
    }
    let adapter_src = env_dir.join("adapter-src");
    if !adapter_src.join("index.ts").is_file() {
        return None;
    }
    Some(ChainEnv {
        env_dir,
        adapter_src,
        node,
    })
}

/// Optional package prepared by prepare-local-bridge.py. Legacy acceptance
/// keeps its published pins; modern acceptance can use an exact local artifact.
fn local_chain_env() -> Option<ChainEnv> {
    let setup = std::env::var_os("PI_ACP_TEST_BRIDGE_SETUP")?;
    let setup: Value =
        serde_json::from_str(&fs::read_to_string(setup).expect("read local bridge setup"))
            .expect("valid bridge setup JSON");
    assert_eq!(setup["schemaVersion"], 1);
    assert_eq!(setup["nativePiVersion"], "1.5.2");
    let path = |key: &str| PathBuf::from(setup[key].as_str().expect("setup path"));
    let env = ChainEnv {
        env_dir: path("envDir"),
        adapter_src: path("adapterSrc"),
        node: path("node"),
    };
    assert!(env.adapter_src.join("index.ts").is_file());
    assert!(env.env_dir.join("node_modules/tsx/package.json").is_file());
    Some(env)
}

fn modern_chain_env() -> ChainEnv {
    local_chain_env().unwrap_or_else(|| {
        ensure_chain_env_with_versions("7.0.1", "1.5.2")
            .expect("modern acceptance requires Node/npm and the pinned bridge packages")
    })
}

trait MissingExt {
    fn missing(&self) -> bool;
}
impl MissingExt for PathBuf {
    fn missing(&self) -> bool {
        !self.exists()
    }
}

// ---------------------------------------------------------------------------
// Fake servers + harness driver
// ---------------------------------------------------------------------------

struct FakeHttp {
    child: tokio::process::Child,
    port: u16,
    log: PathBuf,
}

async fn spawn_fake_http(env: &ChainEnv, tag: &str, dir: &Path) -> Option<FakeHttp> {
    spawn_http_script(env, tag, dir, "fake-mcp-http.mjs").await
}

async fn spawn_http_script(
    env: &ChainEnv,
    tag: &str,
    dir: &Path,
    script: &str,
) -> Option<FakeHttp> {
    let log = dir.join(format!("fake-{tag}.log"));
    let mut child = tokio::process::Command::new(&env.node)
        .arg(fixtures_dir().join(script))
        .arg("--tag")
        .arg(tag)
        .arg("--port")
        .arg("0")
        .arg("--log")
        .arg(&log)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let mut lines = BufReader::new(stdout).lines();
    let port = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let line = lines.next_line().await.ok()??;
            if let Some(n) = line.strip_prefix("PORT:") {
                return n.trim().parse::<u16>().ok();
            }
        }
    })
    .await
    .ok()??;
    Some(FakeHttp { child, port, log })
}

impl Drop for FakeHttp {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[derive(Debug)]
struct ChainEvent {
    event: String,
    server: Option<String>,
    payload: Value,
}

/// Run the harness once; returns all CHAIN: events (exit status asserted).
async fn run_harness(
    env: &ChainEnv,
    agent_dir: &Path,
    servers: &Value,
    script: &Value,
    dir: &Path,
) -> Option<Vec<ChainEvent>> {
    let servers_file = dir.join("servers.json");
    let script_file = dir.join("script.json");
    fs::write(&servers_file, serde_json::to_string(servers).ok()?).ok()?;
    fs::write(&script_file, serde_json::to_string(script).ok()?).ok()?;

    let mut child = tokio::process::Command::new(&env.node)
        .arg("--import")
        .arg("tsx/esm")
        .arg(fixtures_dir().join("chain-harness.mjs"))
        .arg("--adapter-dir")
        .arg(&env.adapter_src)
        .arg("--registrar")
        .arg(registrar_path())
        .arg("--servers-file")
        .arg(&servers_file)
        .arg("--script")
        .arg(&script_file)
        .current_dir(&env.env_dir)
        .env("PI_CODING_AGENT_DIR", agent_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;

    let stdout = child.stdout.take()?;
    let stderr = child.stderr.take()?;
    let mut events = Vec::new();
    let mut out_lines = BufReader::new(stdout).lines();
    let mut err_buf = String::new();
    let mut err_lines = BufReader::new(stderr).lines();
    let reader = async {
        loop {
            tokio::select! {
                line = out_lines.next_line() => {
                    let line = line.ok()??;
                    if let Some(body) = line.strip_prefix("CHAIN:") {
                        if let Ok(v) = serde_json::from_str::<Value>(body) {
                            events.push(ChainEvent {
                                event: v.get("event").and_then(|e| e.as_str()).unwrap_or("").to_string(),
                                server: v.get("server").and_then(|s| s.as_str()).map(str::to_string),
                                payload: v,
                            });
                        }
                    }
                }
                line = err_lines.next_line() => {
                    let line = line.ok()??;
                    err_buf.push_str(&line);
                    err_buf.push('\n');
                }
            }
        }
        #[allow(unreachable_code)]
        Some(())
    };
    let status = tokio::time::timeout(std::time::Duration::from_secs(240), async {
        let _ = reader.await;
        child.wait().await.ok()
    })
    .await;
    match status {
        Ok(Some(exit)) if exit.success() => Some(events),
        Ok(Some(exit)) => {
            eprintln!(
                "harness exited {exit}; stderr tail:\n{}",
                tail(&err_buf, 3000)
            );
            None
        }
        _ => {
            let _ = child.start_kill();
            eprintln!("harness timed out; stderr tail:\n{}", tail(&err_buf, 3000));
            None
        }
    }
}

fn tail(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        s[s.len() - max..].to_string()
    }
}

fn find<'a>(events: &'a [ChainEvent], event: &str, server: Option<&str>) -> Option<&'a Value> {
    events
        .iter()
        .find(|e| e.event == event && e.server.as_deref() == server)
        .map(|e| &e.payload)
}

fn server_log_methods(log: &Path) -> Vec<String> {
    let content = fs::read_to_string(log).unwrap_or_default();
    content
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| v.get("method")?.as_str().map(str::to_string))
        .collect()
}

// ---------------------------------------------------------------------------
// Harness tests: real adapter + real registrar + real MCP wire
// ---------------------------------------------------------------------------

/// Item 1 (data plane): stdio + Streamable-HTTP fakes register through the
/// real registrar/adapter, become visible as pi tools, and list/call
/// roundtrip correctly — asserted on both the harness side and the fake
/// servers' wire logs. Snapshot + dispose prove session-scoped lifecycle.
#[tokio::test]
async fn harness_stdio_http_list_call_roundtrip() {
    let _guard = acquire_test_lock().await;
    let Some(env) = ensure_chain_env() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&agent_dir).unwrap();

    let stdio_log = tmp.path().join("stdio-wire.log");
    let mut http = spawn_fake_http(&env, "chainhttp", tmp.path())
        .await
        .expect("fake HTTP server starts");
    let stdio_def = json!({
        "command": env.node.to_string_lossy(),
        "args": [
            fixtures_dir().join("fake-mcp-stdio.mjs").to_string_lossy(),
            "--tag", "chainstdio",
            "--log", stdio_log.to_string_lossy(),
        ],
    });
    let servers = json!([
        {"name": "chain-stdio", "definition": stdio_def},
        {"name": "chain-http", "definition": {"url": format!("http://127.0.0.1:{}/mcp", http.port)}},
    ]);
    let script = json!([
        {"op": "connect", "server": "chain-stdio"},
        {"op": "connect", "server": "chain-http"},
        {"op": "tools"},
        {"op": "list", "server": "chain-stdio"},
        {"op": "list", "server": "chain-http"},
        {"op": "call", "server": "chain-stdio", "tool": "chainstdio_echo", "args": {"text": "hello"}},
        {"op": "call", "server": "chain-http", "tool": "chainhttp_add", "args": {"a": 40, "b": 2}},
        {"op": "snapshot", "server": "chain-stdio"},
        {"op": "register-dispose", "server": "chain-ephemeral",
         "definition": {"url": format!("http://127.0.0.1:{}/mcp", http.port)}},
    ]);
    let events = run_harness(&env, &agent_dir, &servers, &script, tmp.path())
        .await
        .expect("harness completes");

    // Registration markers from the REAL registrar.
    let markers = find(&events, "markers", None).expect("markers event");
    let markers = markers
        .get("markers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        markers
            .iter()
            .any(|m| m == "PI_ACP_MCP:registered:chain-stdio"),
        "{markers:?}"
    );
    assert!(
        markers
            .iter()
            .any(|m| m == "PI_ACP_MCP:registered:chain-http"),
        "{markers:?}"
    );

    // Visibility: per-server namespace tools appear in pi's tool surface.
    let tools = find(&events, "tools", None).expect("tools event");
    let tools = tools
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let names: Vec<&str> = tools.iter().filter_map(Value::as_str).collect();
    assert!(names.contains(&"mcp__chain_stdio"), "pi tools: {names:?}");
    assert!(names.contains(&"mcp__chain_http"), "pi tools: {names:?}");

    // List results carry both fake tools per server.
    for server in ["chain-stdio", "chain-http"] {
        let list = find(&events, "list", Some(server)).expect("list event");
        let reported = list
            .pointer("/result/details/tools")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert_eq!(reported.len(), 2, "list {server}: {list}");
    }

    // Call roundtrips return the fakes' answers through the real adapter.
    let call = find(&events, "call", Some("chain-stdio")).expect("stdio call");
    assert_eq!(
        call.pointer("/result/content/0/text")
            .and_then(Value::as_str),
        Some("echo:hello"),
        "{call}"
    );
    let call = find(&events, "call", Some("chain-http")).expect("http call");
    assert_eq!(
        call.pointer("/result/content/0/text")
            .and_then(Value::as_str),
        Some("sum:42"),
        "{call}"
    );

    // Wire proof: both fakes saw initialize + tools/list + tools/call.
    for log in [&stdio_log, &http.log] {
        let methods = server_log_methods(log);
        for want in ["initialize", "tools/list", "tools/call"] {
            assert!(
                methods.contains(&want.to_string()),
                "{want} missing in {log:?}: {methods:?}"
            );
        }
    }

    // Snapshot: session-scoped, never persisted.
    let snap = find(&events, "snapshot", Some("chain-stdio")).expect("snapshot");
    assert_eq!(
        snap.pointer("/snapshot/runtime").and_then(Value::as_bool),
        Some(true),
        "{snap}"
    );
    assert_eq!(
        snap.pointer("/snapshot/persisted").and_then(Value::as_bool),
        Some(false),
        "{snap}"
    );

    // Dispose removes exactly that registration (mirrors session teardown).
    let dispose = find(&events, "register-dispose", Some("chain-ephemeral")).expect("dispose");
    assert!(
        dispose.pointer("/before/name").and_then(Value::as_str) == Some("chain-ephemeral"),
        "registered before dispose: {dispose}"
    );
    let after = dispose
        .pointer("/after/error")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        after.contains("not registered"),
        "gone after dispose: {dispose}"
    );

    let _ = http.child.start_kill();
}

/// Item 1 (SSE) + item 3 (adapter side): the SSE-pinned fake roundtrips
/// while dead servers fail explicitly per server — the good server is
/// unaffected, and every failure names its server (no silent drops).
#[tokio::test]
async fn harness_sse_roundtrip_and_failure_isolation() {
    let _guard = acquire_test_lock().await;
    let Some(env) = ensure_chain_env() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&agent_dir).unwrap();

    let mut sse = spawn_fake_http(&env, "chainsse", tmp.path())
        .await
        .expect("fake SSE server starts");
    let servers = json!([
        {"name": "chain-sse",
         "definition": {"url": format!("http://127.0.0.1:{}/sse", sse.port), "httpTransport": "sse"}},
        {"name": "chain-dead-http", "definition": {"url": "http://127.0.0.1:1/mcp"}},
        {"name": "chain-dead-stdio", "definition": {"command": "pi-acp-definitely-missing-binary-xyz"}},
    ]);
    let script = json!([
        {"op": "connect", "server": "chain-sse"},
        {"op": "call", "server": "chain-sse", "tool": "chainsse_echo", "args": {"text": "via-sse"}},
        {"op": "connect", "server": "chain-dead-http"},
        {"op": "connect", "server": "chain-dead-stdio"},
        {"op": "call", "server": "chain-dead-http", "tool": "whatever", "args": {}},
        {"op": "status"},
    ]);
    let events = run_harness(&env, &agent_dir, &servers, &script, tmp.path())
        .await
        .expect("harness completes");

    // SSE roundtrip through the real adapter over the real SSE wire format.
    let call = find(&events, "call", Some("chain-sse")).expect("sse call");
    assert_eq!(
        call.pointer("/result/content/0/text")
            .and_then(Value::as_str),
        Some("echo:via-sse"),
        "{call}"
    );
    let methods = server_log_methods(&sse.log);
    for want in ["initialize", "tools/list", "tools/call"] {
        assert!(
            methods.contains(&want.to_string()),
            "{want} missing over SSE: {methods:?}"
        );
    }

    // Registration itself is config-only (lazy lifecycle): dead servers
    // still report `registered` — reachability failures surface explicitly
    // at connect/call time, naming the exact server.
    let markers = find(&events, "markers", None).expect("markers");
    let markers = markers
        .get("markers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        markers
            .iter()
            .any(|m| m == "PI_ACP_MCP:registered:chain-dead-http"),
        "{markers:?}"
    );

    for dead in ["chain-dead-http", "chain-dead-stdio"] {
        let connect = find(&events, "connect", Some(dead)).expect("dead connect");
        let err = connect
            .pointer("/result/details/error")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert_eq!(
            err, "connect_failed",
            "explicit connect failure for {dead}: {connect}"
        );
        let msg = connect
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(msg.contains(dead), "failure names its server: {connect}");
    }
    let call = find(&events, "call", Some("chain-dead-http")).expect("dead call");
    let err = call
        .pointer("/result/details/error")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        !err.is_empty(),
        "call on a dead server errors explicitly: {call}"
    );

    // Status aggregates honestly: 1/3 connected, failures listed.
    let status = find(&events, "status", None).expect("status");
    assert_eq!(
        status
            .pointer("/result/details/connectedCount")
            .and_then(Value::as_u64),
        Some(1),
        "{status}"
    );

    let _ = sse.child.start_kill();
}

/// Item 2 (data plane): two isolated adapter instances register the SAME
/// server names against DIFFERENT backends — calls route to each instance's
/// own backend (no cross-talk through shared machine state).
#[tokio::test]
async fn harness_concurrent_same_names_stay_isolated() {
    let _guard = acquire_test_lock().await;
    let Some(env) = ensure_chain_env() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();

    let run_one = |tag: &str| {
        let env_dir = env.env_dir.clone();
        let adapter_src = env.adapter_src.clone();
        let node = env.node.clone();
        let dir = tmp.path().join(tag);
        std::fs::create_dir_all(&dir).unwrap();
        let tag = tag.to_string();
        async move {
            let log = dir.join("fake.log");
            let mut fake = tokio::process::Command::new(&node)
                .arg(fixtures_dir().join("fake-mcp-http.mjs"))
                .arg("--tag")
                .arg(&tag)
                .arg("--port")
                .arg("0")
                .arg("--log")
                .arg(&log)
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;
            let stdout = fake.stdout.take()?;
            let mut lines = BufReader::new(stdout).lines();
            let port = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop {
                    let line = lines.next_line().await.ok()??;
                    if let Some(n) = line.strip_prefix("PORT:") {
                        return n.trim().parse::<u16>().ok();
                    }
                }
            })
            .await
            .ok()??;
            let agent_dir = dir.join("agent");
            std::fs::create_dir_all(&agent_dir).ok()?;
            let servers = json!([
                {"name": "shared-name",
                 "definition": {"url": format!("http://127.0.0.1:{port}/mcp")}},
            ]);
            // Call the tag-specific tool: the answer proves which backend served it.
            let script = json!([
                {"op": "connect", "server": "shared-name"},
                {"op": "call", "server": "shared-name",
                 "tool": format!("{tag}_echo"), "args": {"text": tag}},
            ]);
            let env = ChainEnv {
                env_dir,
                adapter_src,
                node,
            };
            let events = run_harness(&env, &agent_dir, &servers, &script, &dir).await?;
            let call = find(&events, "call", Some("shared-name"))?.clone();
            let text = call
                .pointer("/result/content/0/text")?
                .as_str()?
                .to_string();
            let _ = fake.start_kill();
            Some((tag, text))
        }
    };

    // Serialized (suite-wide), but separate OS processes + agent dirs with
    // identical server names — the registrar/adapter must keep them apart.
    let a = run_one("backend-a").await.expect("backend A chain works");
    let b = run_one("backend-b").await.expect("backend B chain works");
    assert_eq!(a, ("backend-a".to_string(), "echo:backend-a".to_string()));
    assert_eq!(b, ("backend-b".to_string(), "echo:backend-b".to_string()));
}

// ---------------------------------------------------------------------------
// Real pi through ACP session/new
// ---------------------------------------------------------------------------

/// Cached agent-dir template with the real adapter `pi install`ed.
/// `None` = skip (no pi binary, or the install failed).
fn ensure_adapter_template() -> Option<PathBuf> {
    let pi = std::env::var_os("PI_ACP_TEST_PI_BIN")
        .map(PathBuf::from)
        .or_else(|| find_on_path("pi"))
        .or_else(|| {
            eprintln!("SKIP: no `pi` binary (set PI_ACP_TEST_PI_BIN or put pi on PATH)");
            None
        })?;
    let base = std::env::temp_dir()
        .join("pi-acp-realpi")
        .join(CHAIN_CACHE_VERSION);
    let template = base.join(format!("adapter-{ADAPTER_PIN}"));
    let ready = template.join(".ready");
    let ready_ok = fs::read_to_string(&ready)
        .ok()
        .map(|s| s.trim().to_string())
        == Some(ADAPTER_PIN.to_string());
    if !ready_ok {
        let _ = fs::remove_dir_all(&template);
        fs::create_dir_all(&template).ok()?;
        let mut cmd = host_command(&pi);
        cmd.env("PI_CODING_AGENT_DIR", &template)
            .arg("install")
            .arg(format!("npm:pi-mcp-adapter@{ADAPTER_PIN}"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = cmd.output().ok()?;
        if !output.status.success() {
            eprintln!(
                "SKIP: `pi install pi-mcp-adapter` failed (offline?): {}",
                clip(&String::from_utf8_lossy(&output.stderr), 500)
            );
            return None;
        }
        if !template
            .join("npm/node_modules/pi-mcp-adapter/package.json")
            .is_file()
        {
            eprintln!("SKIP: adapter install produced no package dir");
            return None;
        }
        fs::write(&ready, ADAPTER_PIN).ok()?;
    }
    Some(template)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let (from, to) = (entry.path(), dst.join(entry.file_name()));
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else {
            fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

/// Item 1 (IDE path) + item 4: ACP `initialize` advertises, `session/new`
/// with stdio+http menus handshakes against REAL pi + REAL adapter (markers
/// through the real registrar), the observer proves the `mcp` gateway is
/// visible inside pi, a second `session/new` with the same names replaces
/// cleanly, and nothing lands in `.pi/mcp.json` (or any `.bak`).
#[tokio::test]
async fn real_pi_session_new_registers_and_replaces() {
    let _guard = acquire_test_lock().await;
    let Some(env) = ensure_chain_env() else {
        return;
    };
    let pi_bin = std::env::var_os("PI_ACP_TEST_PI_BIN")
        .map(PathBuf::from)
        .or_else(|| find_on_path("pi"));
    let Some(pi_bin) = pi_bin else {
        eprintln!("SKIP: no `pi` binary (set PI_ACP_TEST_PI_BIN or put pi on PATH)");
        return;
    };
    let Some(template) = ensure_adapter_template() else {
        return;
    };

    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = tmp.path().join("agent");
    copy_dir_recursive(&template, &agent_dir).expect("copy adapter template");
    // Observer: global extension dumping pi's tool surface to a file.
    let observe_dir = agent_dir.join("extensions/observe");
    fs::create_dir_all(&observe_dir).unwrap();
    fs::copy(
        fixtures_dir().join("observe/index.ts"),
        observe_dir.join("index.ts"),
    )
    .unwrap();
    let observe_log = tmp.path().join("observe.log");

    // Fakes: stdio server + Streamable-HTTP server (ports resolved at runtime).
    let stdio_log = tmp.path().join("stdio-wire.log");
    let mut http = spawn_fake_http(&env, "realhttp", tmp.path())
        .await
        .expect("fake HTTP server starts");
    let stdio_cmd = env.node.to_string_lossy().to_string();
    let stdio_script = fixtures_dir()
        .join("fake-mcp-stdio.mjs")
        .to_string_lossy()
        .to_string();

    let mut extra_env: Vec<(String, String)> = vec![
        ("PI_ACP_ENABLE_MCP".to_string(), "true".to_string()),
        (
            "PI_ACP_PI_COMMAND".to_string(),
            pi_bin.to_string_lossy().to_string(),
        ),
        (
            "PI_CODING_AGENT_DIR".to_string(),
            agent_dir.to_string_lossy().to_string(),
        ),
        // Hermetic: no startup network use; model catalog is static data.
        ("PI_OFFLINE".to_string(), "1".to_string()),
        (
            "PI_ACP_OBSERVE_LOG".to_string(),
            observe_log.to_string_lossy().to_string(),
        ),
    ];
    // The model catalog only lists providers with a (possibly dummy) key;
    // session/new needs a non-empty list. Never override a real key.
    for key in [
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "GOOGLE_GENERATIVE_AI_API_KEY",
    ] {
        if std::env::var_os(key).is_none() {
            extra_env.push((key.to_string(), "pi-acp-acceptance-dummy".to_string()));
            break;
        }
    }

    let log: Arc<Mutex<Vec<(String, SessionUpdate)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();
    let mut cfg = AcpAgentConfig::new(BIN);
    for (k, v) in &extra_env {
        cfg = cfg.env(k, v);
    }
    // NOTE: PI_ACP_MOCK deliberately unset — this client drives REAL pi.
    let agent = AcpAgent::new(cfg);
    let stdio_log_c = stdio_log.clone();
    let cwd_c = cwd.clone();
    let http_port = http.port;
    Client
        .v1()
        .name("mcp-realpi-client")
        .on_receive_notification(
            async move |notif: SessionNotification, _cx| {
                log_in_handler
                    .lock()
                    .await
                    .push((notif.session_id.0.to_string(), notif.update.clone()));
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(
            agent,
            |cx: ConnectionTo<agent_client_protocol::Agent>| async move {
                let init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await
                    .expect("initialize against real pi");
                let caps = &init.agent_capabilities.mcp_capabilities;
                assert!(
                    caps.http && caps.sse,
                    "flag + installed adapter must advertise"
                );

                let stdio_entry =
                    McpServer::Stdio(McpServerStdio::new("real-stdio", &stdio_cmd).args(vec![
                        stdio_script,
                        "--tag".to_string(),
                        "realstdio".to_string(),
                        "--log".to_string(),
                        stdio_log_c.to_string_lossy().to_string(),
                    ]));
                let http_entry = McpServer::Http(McpServerHttp::new(
                    "real-http",
                    format!("http://127.0.0.1:{http_port}/mcp"),
                ));
                let first = cx
                    .send_request(
                        NewSessionRequest::new(cwd_c.clone())
                            .mcp_servers(vec![stdio_entry, http_entry]),
                    )
                    .block_task()
                    .await
                    .expect("session/new registers stdio+http through real pi");

                // Same names, different menus: replacement must handshake cleanly
                // (old pi + its registrations torn down first).
                let second = cx
                    .send_request(NewSessionRequest::new(cwd_c.clone()).mcp_servers(vec![
                        McpServer::Sse(McpServerSse::new(
                            "real-stdio",
                            format!("http://127.0.0.1:{http_port}/sse"),
                        )),
                        McpServer::Http(McpServerHttp::new(
                            "real-http",
                            format!("http://127.0.0.1:{http_port}/mcp"),
                        )),
                    ]))
                    .block_task()
                    .await
                    .expect("replacement menu with the same names registers");
                assert!(!second.session_id.0.is_empty());

                let _ = cx
                    .send_request(DeleteSessionRequest::new(second.session_id.clone()))
                    .block_task()
                    .await;
                let _ = first;
                Ok(())
            },
        )
        .await
        .expect("real-pi connection should complete");

    // Observer proof: the `mcp` gateway was visible inside real pi.
    let mut observed_tools: Vec<String> = Vec::new();
    for _ in 0..30 {
        if let Ok(content) = fs::read_to_string(&observe_log) {
            for line in content.lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    for name in v
                        .get("tools")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                    {
                        if let Some(name) = name.as_str() {
                            if !observed_tools.contains(&name.to_string()) {
                                observed_tools.push(name.to_string());
                            }
                        }
                    }
                }
            }
            if observed_tools.contains(&"mcp".to_string()) {
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    assert!(
        observed_tools.contains(&"mcp".to_string()),
        "mcp gateway visible inside real pi, saw: {observed_tools:?}"
    );

    // Cleanup proof: no MCP config or backup files anywhere near the project
    // or the agent dir (registrations were env-payload + in-memory only).
    assert!(
        !cwd.join(".pi").exists(),
        "project .pi dir must not be created"
    );
    let mut offenders = Vec::new();
    for root in [&agent_dir, &cwd] {
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name == "mcp.json" || name.ends_with(".bak") {
                        offenders.push(path);
                    }
                }
            }
        }
    }
    assert!(offenders.is_empty(), "config residue: {offenders:?}");

    let _ = http.child.start_kill();
}

/// Modern-only wire acceptance with the selected bridge, real registrar and
/// normalized ACP v2 HTTP descriptor. No pi model/provider is contacted.
#[tokio::test]
async fn harness_modern_http_pin_calls_tool_without_legacy_fallback() {
    use pi_acp::mcp::{apply_http_protocol_version, normalize_mcp_servers_v2, McpSessionManager};
    let _guard = acquire_test_lock().await;
    let env = modern_chain_env();
    let tmp = tempfile::tempdir().unwrap();
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&agent_dir).unwrap();
    let modern = spawn_http_script(&env, "modern", tmp.path(), "fake-mcp-modern-http.mjs")
        .await
        .expect("modern-only HTTP server starts");
    let legacy = spawn_fake_http(&env, "legacy", tmp.path())
        .await
        .expect("legacy-only HTTP server starts");
    let descriptors: Vec<agent_client_protocol_schema::v2::McpServer> = serde_json::from_value(json!([
        {"type": "http", "name": "modern", "url": format!("http://127.0.0.1:{}/mcp", modern.port), "headers": []},
        {"type": "http", "name": "incompatible", "url": format!("http://127.0.0.1:{}/mcp", legacy.port), "headers": []}
    ])).unwrap();
    let mut specs = normalize_mcp_servers_v2(&descriptors).unwrap();
    apply_http_protocol_version(&mut specs, Some("2026-07-28")).unwrap();
    let servers =
        serde_json::from_str(&McpSessionManager::new(specs).payload_json().unwrap()).unwrap();
    let script = json!([
        {"op": "connect", "server": "modern"},
        {"op": "list", "server": "modern"},
        {"op": "call", "server": "modern", "tool": "get_assignment", "args": {}},
        {"op": "connect", "server": "incompatible"}
    ]);
    let events = run_harness(&env, &agent_dir, &servers, &script, tmp.path())
        .await
        .expect("modern harness completes");
    let call = find(&events, "call", Some("modern")).expect("modern tool call");
    assert_eq!(
        call.pointer("/result/content/0/text")
            .and_then(Value::as_str),
        Some("MODERN_MCP_FIXTURE_OK"),
        "{call}"
    );
    let methods = server_log_methods(&modern.log);
    for method in ["server/discover", "tools/list", "tools/call"] {
        assert!(
            methods.contains(&method.to_string()),
            "missing {method}: {methods:?}"
        );
    }
    assert!(!methods.contains(&"initialize".to_string()), "{methods:?}");
    let incompatible = find(&events, "connect", Some("incompatible")).unwrap();
    assert_eq!(
        incompatible
            .pointer("/result/details/error")
            .and_then(Value::as_str),
        Some("connect_failed"),
        "{incompatible}"
    );
    if std::env::var_os("PI_ACP_TEST_BRIDGE_SETUP").is_some() {
        assert_no_legacy_http(&modern.log);
        assert_no_legacy_http(&legacy.log);
    }
    // Bridge 7.0.1 enriches connection errors with an independent endpoint
    // probe that can send legacy initialize. The pinned connection still
    // fails: do not mistake the diagnostic probe for successful fallback.
    // This test proves no legacy traffic only on the successful modern path.
    let modern_requests = fs::read_to_string(&modern.log).unwrap();
    for line in modern_requests.lines() {
        let request: Value = serde_json::from_str(line).unwrap();
        assert_eq!(request["accepted"], true, "{request}");
    }
    if let Some(directory) = std::env::var_os("PI_ACP_TEST_MCP_EVIDENCE_DIR") {
        let directory = PathBuf::from(directory);
        fs::create_dir_all(&directory).unwrap();
        fs::copy(&modern.log, directory.join("modern-http.jsonl")).unwrap();
        fs::copy(&legacy.log, directory.join("incompatible-http.jsonl")).unwrap();
        fs::write(
            directory.join("registrations.json"),
            serde_json::to_vec_pretty(&servers).unwrap(),
        )
        .unwrap();
        let report = json!({
            "bridgeVersion": serde_json::from_str::<Value>(&fs::read_to_string(env.adapter_src.join("package.json")).unwrap()).unwrap()["version"],
            "localBridge": std::env::var_os("PI_ACP_TEST_BRIDGE_SETUP").is_some(),
            "host": "stub pi ExtensionAPI; real registrar and selected bridge; no model",
            "modernRequests": server_log_methods(&modern.log),
            "incompatibleRequests": server_log_methods(&legacy.log),
            "modernMarkerReturned": true,
            "modernRequestsAllAccepted": true,
            "incompatibleConnectionFailed": true,
            "noLegacyRequestsOnSuccess": true,
            "noLegacyRequestsOnFailure": !server_log_methods(&legacy.log).contains(&"initialize".to_string()),
            "events": events.iter().map(|event| &event.payload).collect::<Vec<_>>(),
        });
        fs::write(
            directory.join("report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
    }
}

/// Inspect HTTP envelopes, including GET/SSE attempts, rather than assuming
/// absence of a successful legacy connection means absence of legacy traffic.
fn assert_no_legacy_http(log: &Path) {
    let requests = fs::read_to_string(log).expect("wire log exists");
    let mut count = 0;
    for line in requests.lines() {
        let entry: Value = serde_json::from_str(line).unwrap();
        let request = entry.get("httpRequest").unwrap_or(&entry);
        if request.get("httpMethod").is_none() {
            continue;
        }
        count += 1;
        assert_eq!(request["httpMethod"], "POST", "no GET/SSE: {request}");
        assert_eq!(request["protocolVersionHeader"], "2026-07-28", "{request}");
        assert_eq!(request["sessionHeaderPresent"], false, "{request}");
        let message = request.get("message").unwrap_or(request);
        assert_ne!(message["method"], "initialize", "{request}");
        assert_eq!(
            message["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"], "2026-07-28",
            "{request}"
        );
        assert!(
            message["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"].is_object(),
            "{request}"
        );
        assert_eq!(request["methodHeader"], message["method"], "{request}");
    }
    assert!(count > 0, "must observe actual wire requests");
}

/// Required local-candidate acceptance: every connect/error diagnostic stays
/// modern, and diagnostics recognize a valid modern discovery response.
#[tokio::test]
async fn harness_local_bridge_modern_errors_and_discovery() {
    use pi_acp::mcp::{apply_http_protocol_version, normalize_mcp_servers_v2, McpSessionManager};
    let _guard = acquire_test_lock().await;
    let Some(env) = local_chain_env() else {
        eprintln!("SKIP: set PI_ACP_TEST_BRIDGE_SETUP to run local-candidate error acceptance");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let agent = tmp.path().join("agent");
    fs::create_dir_all(&agent).unwrap();
    let modern = spawn_http_script(&env, "errors", tmp.path(), "fake-mcp-modern-http.mjs")
        .await
        .expect("modern HTTP fixture");
    let legacy = spawn_fake_http(&env, "legacy-errors", tmp.path())
        .await
        .expect("legacy HTTP fixture");
    let mut descriptors = Vec::new();
    let mut script = Vec::new();
    for path in ["incompatible", "error", "malformed", "auth"] {
        descriptors.push(json!({"type":"http", "name":path, "url":format!("http://127.0.0.1:{}/{path}", modern.port), "headers":[]}));
        script.push(json!({"op":"connect", "server":path}));
    }
    descriptors.push(json!({"type":"http", "name":"sse", "url":format!("http://127.0.0.1:{}/sse", legacy.port), "headers":[]}));
    script.push(json!({"op":"connect", "server":"sse"}));
    script.push(json!({"op":"probe", "server":"valid-discovery", "url":format!("http://127.0.0.1:{}/mcp", modern.port), "protocolVersion":"2026-07-28"}));
    let descriptors: Vec<agent_client_protocol_schema::v2::McpServer> =
        serde_json::from_value(json!(descriptors)).unwrap();
    let mut specs = normalize_mcp_servers_v2(&descriptors).unwrap();
    apply_http_protocol_version(&mut specs, Some("2026-07-28")).unwrap();
    let mut servers: Value =
        serde_json::from_str(&McpSessionManager::new(specs).payload_json().unwrap()).unwrap();
    // This data-plane host explicitly selects the bridge's public no-OAuth
    // policy. Ordinary pi-acp does not add an ACP descriptor OAuth field.
    // The bounded host-managed API is verified separately without authProvider.
    for server in servers.as_array_mut().unwrap() {
        server["definition"]["oauth"] = json!(false);
    }
    let events = run_harness(&env, &agent, &servers, &json!(script), tmp.path())
        .await
        .expect("local bridge harness");
    if let Some(directory) = std::env::var_os("PI_ACP_TEST_MCP_EVIDENCE_DIR") {
        let directory = PathBuf::from(directory).join("error-matrix");
        fs::create_dir_all(&directory).unwrap();
        fs::copy(&modern.log, directory.join("modern-http.jsonl")).unwrap();
        fs::copy(&legacy.log, directory.join("sse-endpoint-http.jsonl")).unwrap();
        fs::write(
            directory.join("events.json"),
            serde_json::to_vec_pretty(&events.iter().map(|e| &e.payload).collect::<Vec<_>>())
                .unwrap(),
        )
        .unwrap();
    }
    for name in ["incompatible", "error", "malformed", "auth", "sse"] {
        let connect = find(&events, "connect", Some(name)).expect("explicit connect result");
        assert_eq!(
            connect
                .pointer("/result/details/error")
                .and_then(Value::as_str),
            Some("connect_failed"),
            "{connect}"
        );
    }
    let discovery = find(&events, "probe", Some("valid-discovery")).expect("diagnostic result");
    assert_eq!(
        discovery.pointer("/result/isMcp"),
        Some(&json!(true)),
        "valid modern discovery recognized: {discovery}"
    );
    assert_no_legacy_http(&modern.log);
    assert_no_legacy_http(&legacy.log);
}

/// A corrected local bridge must not remove modes chosen by other consumers.
#[tokio::test]
async fn harness_local_bridge_legacy_and_auto_still_connect() {
    use pi_acp::mcp::{apply_http_protocol_version, normalize_mcp_servers_v2, McpSessionManager};
    let _guard = acquire_test_lock().await;
    let Some(env) = local_chain_env() else {
        eprintln!("SKIP: set PI_ACP_TEST_BRIDGE_SETUP for local compatibility acceptance");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let legacy = spawn_fake_http(&env, "compat", tmp.path())
        .await
        .expect("legacy fixture");
    for selection in ["legacy", "auto"] {
        let directory = tmp.path().join(selection);
        fs::create_dir_all(&directory).unwrap();
        let agent = directory.join("agent");
        fs::create_dir_all(&agent).unwrap();
        let descriptor = serde_json::from_value(json!({"type":"http", "name":"compat", "url":format!("http://127.0.0.1:{}/mcp", legacy.port), "headers":[]})).unwrap();
        let mut specs = normalize_mcp_servers_v2(&[descriptor]).unwrap();
        apply_http_protocol_version(&mut specs, Some(selection)).unwrap();
        let servers =
            serde_json::from_str(&McpSessionManager::new(specs).payload_json().unwrap()).unwrap();
        let script = json!([{"op":"connect","server":"compat"},{"op":"call","server":"compat","tool":"compat_echo","args":{"text":selection}}]);
        let events = run_harness(&env, &agent, &servers, &script, &directory)
            .await
            .expect("compatibility harness");
        let call = find(&events, "call", Some("compat")).unwrap();
        let expected = format!("echo:{selection}");
        assert_eq!(
            call.pointer("/result/content/0/text")
                .and_then(Value::as_str),
            Some(expected.as_str()),
            "{call}"
        );
    }
}

/// Verify the documented native Pi TypeScript extension path, without a model
/// prompt, a normal profile, or assuming a compiled host-managed.js exists.
#[tokio::test]
async fn real_pi_local_host_managed_typescript_loads_over_acp_v2() {
    use agent_client_protocol::v2::schema as v2;
    let _guard = acquire_test_lock().await;
    let Some(setup_path) = std::env::var_os("PI_ACP_TEST_BRIDGE_SETUP") else {
        eprintln!("SKIP: set PI_ACP_TEST_BRIDGE_SETUP for native TS loading acceptance");
        return;
    };
    let setup: Value = serde_json::from_str(&fs::read_to_string(setup_path).unwrap()).unwrap();
    assert_eq!(setup["nativePiVersion"], "1.5.2");
    let module = PathBuf::from(setup["envDir"].as_str().unwrap())
        .join("node_modules/pi-mcp-adapter/host-managed.ts");
    assert!(module.is_file(), "supported public TS module");
    let tmp = tempfile::tempdir().unwrap();
    let agent_dir = tmp.path().join("agent");
    let extension_dir = agent_dir.join("extensions");
    fs::create_dir_all(&extension_dir).unwrap();
    let log = tmp.path().join("loaded.json");
    let quoted_module = serde_json::to_string(&module.to_string_lossy()).unwrap();
    let extension = format!(
        r#"
import {{ createHostManagedMcpAdapter }} from {quoted_module};
import fs from "node:fs";
export default async function(pi) {{
  const adapter = createHostManagedMcpAdapter({{
    servers: {{}}, onToolCall: async call => call.dispatch(),
  }});
  await adapter.ready();
  adapter.extensionFactory(pi);
  fs.writeFileSync(process.env.PI_ACP_HOST_LOAD_LOG, JSON.stringify({{ loaded: true }}));
  pi.on("session_shutdown", () => adapter.close());
}}
"#
    );
    fs::write(extension_dir.join("host-managed-load.ts"), extension).unwrap();
    fs::write(
        agent_dir.join("settings.json"),
        r#"{"extensions":["-builtin:mcp"]}"#,
    )
    .unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let cfg = AcpAgentConfig::new(BIN)
        .env("PI_ACP_PI_COMMAND", setup["nativePi"].as_str().unwrap())
        .env("PI_CODING_AGENT_DIR", agent_dir.to_str().unwrap())
        .env("PI_OFFLINE", "1")
        // Synthetic value permits a catalog; no prompt or provider call occurs.
        .env("ANTHROPIC_API_KEY", "pi-acp-loading-test-dummy")
        .env("PI_ACP_HOST_LOAD_LOG", log.to_str().unwrap());
    Client
        .v2()
        .name("local-host-module-loading")
        .connect_with(AcpAgent::new(cfg), async move |cx| {
            cx.send_request(v2::InitializeRequest::new(
                ProtocolVersion::V2,
                v2::Implementation::new("loading-test", "1"),
            ))
            .block_task()
            .await?;
            let session = cx
                .send_request(v2::NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            let _ = cx
                .send_request(v2::CloseSessionRequest::new(session.session_id))
                .block_task()
                .await?;
            Ok(())
        })
        .await
        .expect("native Pi session loads the supported TS extension");
    let loaded: Value =
        serde_json::from_str(&fs::read_to_string(log).expect("actual native extension load"))
            .unwrap();
    assert_eq!(loaded["loaded"], true);
    // Native Pi may initialize its own auth storage in this temporary profile.
    // The test does not inspect or copy it; no normal profile is accessed.
}

/// The selected host-managed path uses its public client-era option, its own
/// SDK dependency root, and a transport with no authProvider. Exercise it in
/// actual native Pi, still without a model prompt or normal profile.
#[tokio::test]
async fn real_pi_local_host_managed_modern_success_and_errors() {
    use agent_client_protocol::v2::schema as v2;
    let _guard = acquire_test_lock().await;
    let Some(env) = local_chain_env() else {
        eprintln!("SKIP: set PI_ACP_TEST_BRIDGE_SETUP for native host-managed wire acceptance");
        return;
    };
    let setup: Value = serde_json::from_str(
        &fs::read_to_string(std::env::var_os("PI_ACP_TEST_BRIDGE_SETUP").unwrap()).unwrap(),
    )
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let modern = spawn_http_script(&env, "native-host", tmp.path(), "fake-mcp-modern-http.mjs")
        .await
        .expect("native host modern fixture");
    let legacy = spawn_fake_http(&env, "native-sse", tmp.path())
        .await
        .expect("native host SSE-only fixture");
    let module = PathBuf::from(setup["envDir"].as_str().unwrap())
        .join("node_modules/pi-mcp-adapter/host-managed.ts");
    let extension = r#"
import { createHostManagedMcpAdapter } from __MODULE__;
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
export default async function(pi) {
  const sdk = await import(pathToFileURL(createRequire(__MODULE__).resolve("@modelcontextprotocol/client")).href);
  const server = url => ({ protocolVersion: "2026-07-28", createTransport: () => new sdk.StreamableHTTPClientTransport(new URL(url)) });
  const calls = [];
  const adapter = createHostManagedMcpAdapter({
    servers: { fixture: server(__BASE__ + "/mcp") },
    onToolCall: async call => call.dispatch(), requestTimeoutMs: 3000,
  });
  await adapter.ready();
  adapter.extensionFactory({
    registerTool: tool => { calls.push(tool); pi.registerTool(tool); },
    events: { emit: (_name, request) => {
      if (typeof request?.claim === "function") request.claim(() =>
        request.serverName === "fixture" && request.originalToolName === "get_assignment" ? "allow_once" : "deny");
    } },
  });
  if (calls.length !== 1) throw new Error("Unexpected host-managed tool catalog");
  const result = await calls[0].execute("native-fixture-call", {}, undefined);
  await adapter.close();
  const failures = {};
  for (const path of ["incompatible", "error", "malformed", "auth", "sse"]) {
    const url = path === "sse" ? __SSE__ : __BASE__ + "/" + path;
    const negative = createHostManagedMcpAdapter({servers:{fixture:server(url)},onToolCall:async call => call.dispatch(),requestTimeoutMs:3000});
    try { await negative.ready(); failures[path] = false; }
    catch { failures[path] = true; }
    finally { await negative.close(); }
  }
  fs.writeFileSync(process.env.PI_ACP_HOST_LOAD_LOG, JSON.stringify({ result, failures }));
}
"#
        .replace("__MODULE__", &serde_json::to_string(&module.to_string_lossy()).unwrap())
        .replace("__BASE__", &serde_json::to_string(&format!("http://127.0.0.1:{}", modern.port)).unwrap())
        .replace("__SSE__", &serde_json::to_string(&format!("http://127.0.0.1:{}/sse", legacy.port)).unwrap());
    let agent = tmp.path().join("agent");
    let extensions = agent.join("extensions");
    fs::create_dir_all(&extensions).unwrap();
    fs::write(extensions.join("host-managed-wire.ts"), &extension).unwrap();
    fs::write(
        agent.join("settings.json"),
        r#"{"extensions":["-builtin:mcp"]}"#,
    )
    .unwrap();
    let log = tmp.path().join("native-result.json");
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let cfg = AcpAgentConfig::new(BIN)
        .env("PI_ACP_PI_COMMAND", setup["nativePi"].as_str().unwrap())
        .env("PI_CODING_AGENT_DIR", agent.to_str().unwrap())
        .env("PI_OFFLINE", "1")
        .env("ANTHROPIC_API_KEY", "pi-acp-loading-test-dummy")
        .env("PI_ACP_HOST_LOAD_LOG", log.to_str().unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        Client
            .v2()
            .name("native-host-wire")
            .connect_with(AcpAgent::new(cfg), async move |cx| {
                cx.send_request(v2::InitializeRequest::new(
                    ProtocolVersion::V2,
                    v2::Implementation::new("native-host-wire", "1"),
                ))
                .block_task()
                .await?;
                let session = cx
                    .send_request(v2::NewSessionRequest::new(&cwd))
                    .block_task()
                    .await?;
                cx.send_request(v2::CloseSessionRequest::new(session.session_id))
                    .block_task()
                    .await?;
                Ok(())
            }),
    )
    .await
    .expect("native host acceptance bounded")
    .expect("ACP v2 session");
    let result: Value =
        serde_json::from_str(&fs::read_to_string(&log).expect("native host result")).unwrap();
    if let Some(directory) = std::env::var_os("PI_ACP_TEST_MCP_EVIDENCE_DIR") {
        let directory = PathBuf::from(directory).join("native-host-managed");
        fs::create_dir_all(&directory).unwrap();
        fs::copy(&modern.log, directory.join("modern-http.jsonl")).unwrap();
        fs::copy(&legacy.log, directory.join("sse-endpoint-http.jsonl")).unwrap();
        fs::copy(&log, directory.join("result.json")).unwrap();
        fs::write(directory.join("extension.ts"), extension).unwrap();
    }
    assert_eq!(
        result
            .pointer("/result/content/0/text")
            .and_then(Value::as_str),
        Some("MODERN_MCP_FIXTURE_OK"),
        "{result}"
    );
    for path in ["incompatible", "error", "malformed", "auth", "sse"] {
        assert_eq!(result["failures"][path], true, "{result}");
    }
    assert_no_legacy_http(&modern.log);
    assert_no_legacy_http(&legacy.log);
    let methods = server_log_methods(&modern.log);
    for method in ["server/discover", "tools/list", "tools/call"] {
        assert!(methods.contains(&method.to_string()), "{methods:?}");
    }
}
