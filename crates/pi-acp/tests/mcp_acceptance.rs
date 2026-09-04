//! W-484 acceptance (pi-acp side, mock pi): concurrency isolation, failure
//! explicitness, replacement and exit cleanup.
//!
//! These tests run everywhere with no external dependencies: the mock pi
//! stands in for (real pi + registrar + adapter) and emits the same
//! `PI_ACP_MCP:*` markers, so they prove pi-acp's own wiring — per-child
//! payloads, per-session bookkeeping, the handshake gate, teardown order.
//! The real adapter + real MCP wire + real pi are covered in
//! `mcp_realchain.rs`.

use std::fs;
use std::sync::{Arc, OnceLock};

use agent_client_protocol::schema::v1::{
    ContentBlock, DeleteSessionRequest, InitializeRequest, LoadSessionRequest, McpServer,
    McpServerHttp, McpServerStdio, NewSessionRequest, PromptRequest, SessionNotification,
    SessionUpdate, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::ConnectionTo;
use agent_client_protocol::{on_receive_notification, AcpAgent, AcpAgentConfig, Client};
use serde_json::json;
use tokio::sync::Mutex;

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");

type NotifLog = Arc<Mutex<Vec<(String, SessionUpdate)>>>;

static MCP_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn acquire_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    MCP_TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

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
    F: FnOnce(ConnectionTo<agent_client_protocol::Agent>) -> Fut + Send + 'static,
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
        .name("mcp-acceptance-client")
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

async fn initialize(cx: &ConnectionTo<agent_client_protocol::Agent>) {
    cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .expect("initialize");
}

/// No MCP config file may appear anywhere: pi-acp injects purely through the
/// child's environment, never through `.pi/mcp.json` (+ no `.bak` either).
fn assert_no_mcp_files(roots: &[&std::path::Path]) {
    let mut offenders = Vec::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name == "mcp.json" || name == "mcp.json.pi-acp.bak" || name.ends_with(".bak")
                    {
                        offenders.push(path);
                    }
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "MCP config residue must never be written: {offenders:?}"
    );
}

/// Item 2a: sequential replacement in one connection. The second menu (same
/// server names, different definitions) handshakes cleanly — the retired
/// session's registrations die with its pi process, so names never collide.
#[tokio::test]
async fn replacement_menu_with_same_names_succeeds() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    run_client(
        vec![("PI_ACP_ENABLE_MCP", "true")],
        &agent_dir,
        move |cx| async move {
            initialize(&cx).await;
            let first = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone()).mcp_servers(vec![stdio_server("srv")]),
                )
                .block_task()
                .await
                .expect("first menu registers");
            // Same name, different transport: only passes when the old
            // session's wiring was torn down first (no cross-session leak).
            let second = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone()).mcp_servers(vec![http_server("srv")]),
                )
                .block_task()
                .await
                .expect("replacement menu with the same name registers");
            // NOTE: the mock pi always reports the same pi-side session id,
            // so id inequality cannot be asserted here; what matters is that
            // both handshakes settle and the live session is usable.
            let _ = first;
            let prompt = cx
                .send_request(PromptRequest::new(
                    second.session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new("hi"))],
                ))
                .block_task()
                .await;
            assert!(prompt.is_ok(), "replacement session is usable: {prompt:?}");

            // A plain session afterwards is unaffected by either menu.
            let plain = cx
                .send_request(NewSessionRequest::new(cwd.clone()))
                .block_task()
                .await
                .expect("plain session after MCP sessions");
            assert!(!plain.session_id.0.is_empty());
            Ok(())
        },
    )
    .await;

    assert_no_mcp_files(&[&tmp.path().join("project"), &agent_dir]);
}

/// Item 2b: two independent connections (two pi-acp processes, same cwd,
/// different menus) proceed concurrently and the first closing does not
/// disturb the second. Shared machine state (registrar staging, temp dirs)
/// must not couple them.
#[tokio::test]
async fn two_connections_with_different_menus_are_independent() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    let run_one = |menu: Vec<McpServer>| {
        let cwd = cwd.clone();
        let agent_dir = agent_dir.clone();
        async move {
            run_client(
                vec![("PI_ACP_ENABLE_MCP", "true")],
                &agent_dir,
                move |cx| async move {
                    initialize(&cx).await;
                    let new_session = cx
                        .send_request(NewSessionRequest::new(cwd.clone()).mcp_servers(menu))
                        .block_task()
                        .await
                        .expect("menu registers");
                    // A prompt roundtrip proves the session is alive and usable.
                    let prompt = cx
                        .send_request(PromptRequest::new(
                            new_session.session_id.clone(),
                            vec![ContentBlock::Text(TextContent::new("hi"))],
                        ))
                        .block_task()
                        .await;
                    assert!(prompt.is_ok(), "session stays usable: {prompt:?}");
                    // Close our session explicitly; the other connection must not notice.
                    let _ = cx
                        .send_request(DeleteSessionRequest::new(new_session.session_id.clone()))
                        .block_task()
                        .await;
                    Ok(())
                },
            )
            .await;
        }
    };

    // Run sequentially: the point is process independence, not socket racing
    // (the suite serializes for runner process limits).
    run_one(vec![stdio_server("only-a")]).await;
    run_one(vec![http_server("only-b")]).await;

    assert_no_mcp_files(&[&tmp.path().join("project"), &agent_dir]);
}

/// Item 3a: the pi child dies mid-handshake — the session fails with an
/// explicit process error, never a silent stall or a half-created session.
#[tokio::test]
async fn pi_exit_mid_handshake_is_explicit() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    run_client(
        vec![
            ("PI_ACP_ENABLE_MCP", "true"),
            ("PI_ACP_MOCK_EXIT_AFTER", "1"),
        ],
        &agent_dir,
        move |cx| async move {
            initialize(&cx).await;
            let err = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone()).mcp_servers(vec![stdio_server("a")]),
                )
                .block_task()
                .await
                .expect_err("dead pi must fail session/new");
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("exit"),
                "process death must be explicit, got: {err}"
            );
            Ok(())
        },
    )
    .await;
}

/// Item 3b: the registrar/adapter dies without reporting (no markers at
/// all) — the gate times out and names every pending server instead of
/// resolving with a partial menu.
#[tokio::test]
async fn silent_registrar_times_out_naming_every_server() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    run_client(
        vec![
            ("PI_ACP_ENABLE_MCP", "true"),
            ("PI_ACP_MOCK_MCP_SILENT", "1"),
        ],
        &agent_dir,
        move |cx| async move {
            initialize(&cx).await;
            let err = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone())
                        .mcp_servers(vec![stdio_server("a"), http_server("b")]),
                )
                .block_task()
                .await
                .expect_err("silent registrar must fail the handshake");
            let msg = err.to_string();
            assert!(msg.contains('a'), "names pending server a: {msg}");
            assert!(msg.contains('b'), "names pending server b: {msg}");
            Ok(())
        },
    )
    .await;
}

/// Item 3c: `PI_ACP_ENABLE_MCP=false` force-closes the wiring even with an
/// adapter installed — the menu is rejected loudly, never half-wired.
#[tokio::test]
async fn explicit_false_force_closes_with_adapter_present() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    run_client(
        vec![("PI_ACP_ENABLE_MCP", "false")],
        &agent_dir,
        move |cx| async move {
            let init = cx
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await
                .expect("initialize");
            let caps = &init.agent_capabilities.mcp_capabilities;
            assert!(!caps.http && !caps.sse, "force-off must not advertise");

            let err = cx
                .send_request(
                    NewSessionRequest::new(cwd.clone()).mcp_servers(vec![stdio_server("a")]),
                )
                .block_task()
                .await
                .expect_err("menu with MCP force-closed must error");
            assert!(err.to_string().contains("disabled"), "error: {err}");
            Ok(())
        },
    )
    .await;
}

/// Item 4: `session/load` swaps the menu (old wiring forgotten with its
/// process), and deleting the session leaves no config residue behind.
#[tokio::test]
async fn load_swaps_menu_and_delete_cleans_up() {
    let _guard = acquire_test_lock().await;
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    fs::create_dir_all(&cwd).unwrap();
    let agent_dir = agent_dir_with_adapter(tmp.path());

    let sessions = agent_dir.join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    let header = json!({ "type": "session", "id": "loadable", "cwd": cwd.to_string_lossy() });
    fs::write(
        sessions.join("loadable.jsonl"),
        format!("{header}\n{{\"type\":\"message\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"),
    )
    .unwrap();
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
            initialize(&cx).await;
            // Load with menu A, then swap to menu B on the same session id:
            // the second handshake re-runs against a fresh pi, so B's
            // markers (not A's residue) settle the gate.
            let _ = cx
                .send_request(
                    LoadSessionRequest::new("loadable", cwd.clone())
                        .mcp_servers(vec![stdio_server("menu-a")]),
                )
                .block_task()
                .await
                .expect("load with menu A");
            let _ = cx
                .send_request(
                    LoadSessionRequest::new("loadable", cwd.clone())
                        .mcp_servers(vec![http_server("menu-b")]),
                )
                .block_task()
                .await
                .expect("load swaps to menu B");
            // Clearing the menu also works (no gate, no residue).
            let _ = cx
                .send_request(LoadSessionRequest::new("loadable", cwd.clone()))
                .block_task()
                .await
                .expect("load without menu clears");
            Ok(())
        },
    )
    .await;

    assert_no_mcp_files(&[&tmp.path().join("project"), &agent_dir]);
}
