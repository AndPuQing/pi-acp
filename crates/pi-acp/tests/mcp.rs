//! W-483 acceptance tests: MCP `runtime-register` wiring against the mock pi.
//!
//! The mock stands in for (real pi + registrar extension + adapter): when it
//! sees the session payload (`PI_ACP_MCP_SERVERS_JSON`) it prints one
//! `PI_ACP_MCP:*` marker per requested server — exactly the lines the real
//! registrar emits — and `PI_ACP_MOCK_MCP_FAIL=<name>` makes one server fail.
//! This exercises the full agent-side handshake (validate → spawn with
//! extension+env → marker gate → store/replace/forget) with no real pi.

use std::fs;
use std::sync::{Arc, OnceLock};

use agent_client_protocol::schema::v1::{
    InitializeRequest, LoadSessionRequest, McpServer, McpServerHttp, McpServerStdio,
    NewSessionRequest, SessionNotification, SessionUpdate,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{on_receive_notification, AcpAgent, AcpAgentConfig, Client};
use serde_json::json;
use tokio::sync::Mutex;

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");

type NotifLog = Arc<Mutex<Vec<(String, SessionUpdate)>>>;

static MCP_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn acquire_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    MCP_TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

/// A fake agent dir containing an installed pi-mcp-adapter package marker.
fn agent_dir_with_adapter(base: &std::path::Path) -> std::path::PathBuf {
    let agent = base.join("agent");
    let pkg = agent.join("npm/node_modules/pi-mcp-adapter");
    fs::create_dir_all(&pkg).unwrap();
    fs::write(pkg.join("package.json"), "{\"name\":\"pi-mcp-adapter\"}").unwrap();
    agent
}

fn stdio_server(name: &str) -> McpServer {
    McpServer::Stdio(McpServerStdio::new(name, "fake-mcp-server").args(vec!["--serve".to_string()]))
}

fn http_server(name: &str) -> McpServer {
    McpServer::Http(McpServerHttp::new(name, "http://localhost:9/mcp"))
}

/// Drive one connected client with `extra_env` on the agent child.
async fn run_client<F, Fut>(extra_env: Vec<(&str, &str)>, agent_dir: &std::path::Path, f: F)
where
    F: FnOnce(agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>) -> Fut
        + Send
        + 'static,
    Fut: std::future::Future<Output = agent_client_protocol::Result<()>> + Send + 'static,
{
    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();
    let mut cfg = AcpAgentConfig::new(BIN)
        .env("PI_ACP_MOCK", "1")
        .env("PI_ACP_PI_COMMAND", BIN)
        .env("PI_CODING_AGENT_DIR", agent_dir.to_str().unwrap());
    for (k, v) in extra_env {
        cfg = cfg.env(k, v);
    }
    let agent = AcpAgent::new(cfg);
    Client
        .builder()
        .name("mcp-e2e-client")
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
        .connect_with(agent, |cx| async move { f(cx).await })
        .await
        .expect("connection should complete");
}

async fn initialize_caps(
    cx: &agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>,
) -> (bool, bool) {
    let init = cx
        .send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .expect("initialize");
    let caps = &init.agent_capabilities.mcp_capabilities;
    (caps.http, caps.sse)
}

/// Default: no flag → transports off, and a menu is rejected (never dropped).
#[tokio::test]
async fn mcp_off_by_default_and_menu_rejected() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(&agent_dir).unwrap();

    run_client(vec![], &agent_dir, move |cx| async move {
        let (http, sse) = initialize_caps(&cx).await;
        assert!(!http && !sse, "MCP must be off by default");

        let err = cx
            .send_request(NewSessionRequest::new(cwd.clone()).mcp_servers(vec![stdio_server("a")]))
            .block_task()
            .await
            .expect_err("menu with MCP disabled must error");
        assert!(err.to_string().contains("disabled"), "error: {err}");
        Ok(())
    })
    .await;
}

/// Flag on but no adapter installed → still off, still an explicit error.
#[tokio::test]
async fn mcp_enabled_without_adapter_rejects() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(&agent_dir).unwrap();

    run_client(
        vec![("PI_ACP_ENABLE_MCP", "true")],
        &agent_dir,
        move |cx| async move {
            let (http, sse) = initialize_caps(&cx).await;
            assert!(!http && !sse, "no adapter installed: must stay off");

            let err = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone()).mcp_servers(vec![stdio_server("a")]),
                )
                .block_task()
                .await
                .expect_err("menu without adapter must error");
            assert!(err.to_string().contains("pi-mcp-adapter"), "error: {err}");
            Ok(())
        },
    )
    .await;
}

/// Flag on + adapter present → advertised, and `session/new` with a menu
/// succeeds once every server reports back (mock markers).
#[tokio::test]
async fn mcp_happy_path_registers_all_servers() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    run_client(
        vec![("PI_ACP_ENABLE_MCP", "true")],
        &agent_dir,
        move |cx| async move {
            let (http, sse) = initialize_caps(&cx).await;
            assert!(http && sse, "flag + adapter: must advertise");

            let new_session = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone())
                        .mcp_servers(vec![stdio_server("a"), http_server("b")]),
                )
                .block_task()
                .await
                .expect("all servers register");
            assert!(!new_session.session_id.0.is_empty());

            // Same-named menu in a second session coexists: registrations are
            // process-local, so replacement never collides (ordering proof).
            // (The mock always reports the same pi session id; success of
            // both handshakes is the assertion.)
            let second = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone())
                        .mcp_servers(vec![stdio_server("a"), http_server("b")]),
                )
                .block_task()
                .await
                .expect("replacement session with same names");
            assert!(!second.session_id.0.is_empty());
            Ok(())
        },
    )
    .await;
}

/// One server failing names exactly that server (isolation), and the session
/// is not created.
#[tokio::test]
async fn mcp_single_server_failure_names_only_it() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    run_client(
        vec![("PI_ACP_ENABLE_MCP", "true"), ("PI_ACP_MOCK_MCP_FAIL", "b")],
        &agent_dir,
        move |cx| async move {
            let err = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone())
                        .mcp_servers(vec![stdio_server("a"), stdio_server("b")]),
                )
                .block_task()
                .await
                .expect_err("one failed server must fail the handshake");
            let msg = err.to_string();
            assert!(msg.contains("\"b\""), "names the failed server: {msg}");
            assert!(
                msg.contains("mock registration refused"),
                "carries the cause: {msg}"
            );
            Ok(())
        },
    )
    .await;
}

/// Validation rejects before any spawn: no pi process is ever started.
#[tokio::test]
async fn mcp_validation_rejects_before_spawn() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());
    let command_log = tmp.path().join("commands.log");

    run_client(
        vec![
            ("PI_ACP_ENABLE_MCP", "true"),
            ("PI_ACP_MOCK_COMMAND_LOG", command_log.to_str().unwrap()),
        ],
        &agent_dir,
        move |cx| async move {
            // Bad URL.
            let err = cx
                .send_request(NewSessionRequest::new(cwd.clone()).mcp_servers(vec![
                    McpServer::Http(McpServerHttp::new("h", "ftp://nope/x")),
                ]))
                .block_task()
                .await
                .expect_err("bad url must error");
            assert!(err.to_string().contains("\"h\""), "error: {err}");

            // Duplicate names.
            let err = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone())
                        .mcp_servers(vec![stdio_server("a"), stdio_server("a")]),
                )
                .block_task()
                .await
                .expect_err("duplicates must error");
            assert!(err.to_string().contains("more than once"), "error: {err}");
            Ok(())
        },
    )
    .await;

    assert!(
        !command_log.exists(),
        "validation must fail before pi is spawned (no mock commands)"
    );
}

/// `session/load` consumes a replacement menu; an empty menu clears it.
#[tokio::test]
async fn mcp_load_replaces_menu() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    // A stored pi session the loader can restore.
    let sessions = agent_dir.join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    let header = json!({ "type": "session", "id": "loadable", "cwd": cwd.to_string_lossy() });
    fs::write(
        sessions.join("loadable.jsonl"),
        format!("{header}\n{{\"type\":\"message\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"),
    )
    .unwrap();
    // The session map the agent reads (mirrors SessionStore layout).
    let store_dir = agent_dir.join("pi-acp");
    fs::create_dir_all(&store_dir).unwrap();
    fs::write(
        store_dir.join("session-map.json"),
        json!({ "sessions": { "loadable": {
            "sessionId": "loadable",
            "cwd": cwd.to_string_lossy(),
            "sessionFile": sessions.join("loadable.jsonl").to_string_lossy(),
        }}})
        .to_string(),
    )
    .unwrap();

    run_client(
        vec![("PI_ACP_ENABLE_MCP", "true")],
        &agent_dir,
        move |cx| async move {
            let loaded = cx
                .send_request(
                    LoadSessionRequest::new("loadable", cwd.clone())
                        .mcp_servers(vec![stdio_server("a")]),
                )
                .block_task()
                .await
                .expect("load with menu");
            let _ = loaded;

            // Empty menu on the next load clears the wiring (no markers
            // needed, no gate).
            let _ = cx
                .send_request(LoadSessionRequest::new("loadable", cwd.clone()))
                .block_task()
                .await
                .expect("load without menu clears");
            Ok(())
        },
    )
    .await;
}
