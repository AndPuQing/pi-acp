//! S3 (W-450) acceptance tests for the pi RPC client.
//!
//! These drive [`PiProcess`] against a **mock pi** — a hidden `--mock-rpc`
//! fixture inside the `pi-acp` binary (see `main.rs`) that speaks the same
//! JSONL protocol as real pi. This keeps the client's error handling (timeout,
//! mid-flight exit, prelude capture, `agent_settled` semantics) testable
//! without a real pi + LLM backend, and is fully cross-platform (the mock is
//! the same binary on all CI targets).
//!
//! Acceptance per W-450: normal request/response / timeout / child mid-exit /
//! prelude / `agent_settled`; no panics.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use pi_acp::error::AcpxError;
use pi_acp::pi::process::PiProcess;
use pi_acp::pi::process::SKILL_DIR_ENV;
use pi_acp::pi::rpc::{QueueMode, RpcCommand, RpcEvent, RpcSessionState};

/// Path to the `pi-acp` binary under test (set by cargo for integration tests).
const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");

/// Short deadline so timeout tests stay fast (the mock hangs → real timeout).
const FAST_TIMEOUT: Duration = Duration::from_millis(500);

async fn spawn_mock(extra: &[&str]) -> PiProcess {
    // The mock fixture is switched on by `--mock-rpc` in the `pi-acp` binary;
    // behavior flags (prelude/hang/exit/delay) follow.
    let mut args = vec!["--mock-rpc"];
    args.extend_from_slice(extra);
    PiProcess::spawn_with_args(BIN, &args, None, FAST_TIMEOUT)
        .await
        .unwrap()
}

#[tokio::test]
async fn spawn_in_dir_sets_pi_cwd() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd_log = tmp.path().join("cwd.log");
    let mut pi = PiProcess::spawn_with_args_in_dir(
        BIN,
        &["--mock-rpc", "--mock-cwd-log", cwd_log.to_str().unwrap()],
        None,
        tmp.path(),
        FAST_TIMEOUT,
    )
    .await
    .unwrap();

    pi.get_state().await.unwrap();
    let reported = fs::canonicalize(fs::read_to_string(cwd_log).unwrap().trim()).unwrap();
    assert_eq!(reported, fs::canonicalize(tmp.path()).unwrap());
    pi.dispose().await;
}

/// Serializes the skill-dir spawn tests: `std::env` is process-global and
/// the harness runs tests on threads in the same process (async-aware mutex
/// so the guard may be held across awaits).
static SKILL_DIR_ENV_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn acquire_skill_dir_env_lock() -> tokio::sync::MutexGuard<'static, ()> {
    SKILL_DIR_ENV_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// W-495 (upstream #99): with `PI_CODING_AGENT_SKILL_DIR` set, the spawned
/// pi child receives `--no-skills --skill <dir>` before `--session`.
#[tokio::test]
async fn spawn_passes_skill_dir_flags_to_pi() {
    let _guard = acquire_skill_dir_env_lock().await;
    let prev = std::env::var_os(SKILL_DIR_ENV);
    let tmp = tempfile::tempdir().unwrap();
    let skill_dir = tmp.path().join("tenant-skills");
    let argv_log = tmp.path().join("argv.log");
    let session_file = tmp.path().join("s.jsonl");
    // SAFETY: under `skill_dir_env_lock`; restored below.
    unsafe { std::env::set_var(SKILL_DIR_ENV, &skill_dir) };

    let argv_log_arg = argv_log.to_string_lossy().into_owned();
    let mut pi = PiProcess::spawn_with_args(
        BIN,
        &["--mock-rpc", "--mock-argv-log", &argv_log_arg],
        Some(&session_file),
        FAST_TIMEOUT,
    )
    .await
    .unwrap();
    pi.get_state().await.unwrap();
    pi.dispose().await;

    match prev {
        Some(v) => unsafe { std::env::set_var(SKILL_DIR_ENV, v) },
        None => unsafe { std::env::remove_var(SKILL_DIR_ENV) },
    }

    let argv = fs::read_to_string(&argv_log).unwrap();
    let lines: Vec<&str> = argv.lines().collect();
    let pos = |flag: &str| lines.iter().position(|l| *l == flag).unwrap();
    let no_skills = pos("--no-skills");
    let skill = pos("--skill");
    let session = pos("--session");
    assert_eq!(lines[skill + 1], skill_dir.to_string_lossy());
    assert_eq!(
        lines[session + 1],
        session_file.to_string_lossy(),
        "argv:\n{argv}"
    );
    assert!(
        no_skills < skill && skill + 1 < session,
        "skill flags must precede --session, argv:\n{argv}"
    );
}

/// W-495 default: without the env var the spawn argv carries no skill flags.
#[tokio::test]
async fn spawn_without_skill_dir_passes_no_skill_flags() {
    let _guard = acquire_skill_dir_env_lock().await;
    let prev = std::env::var_os(SKILL_DIR_ENV);
    // SAFETY: under `skill_dir_env_lock`; restored below.
    unsafe { std::env::remove_var(SKILL_DIR_ENV) };

    let tmp = tempfile::tempdir().unwrap();
    let argv_log = tmp.path().join("argv.log");
    let argv_log_arg = argv_log.to_string_lossy().into_owned();
    let mut pi = PiProcess::spawn_with_args(
        BIN,
        &["--mock-rpc", "--mock-argv-log", &argv_log_arg],
        None,
        FAST_TIMEOUT,
    )
    .await
    .unwrap();
    pi.get_state().await.unwrap();
    pi.dispose().await;

    match prev {
        Some(v) => unsafe { std::env::set_var(SKILL_DIR_ENV, v) },
        None => unsafe { std::env::remove_var(SKILL_DIR_ENV) },
    }

    let argv = fs::read_to_string(&argv_log).unwrap();
    assert!(
        !argv.lines().any(|l| l == "--skill" || l == "--no-skills"),
        "argv:\n{argv}"
    );
}

/// Normal request/response round-trip with typed `get_state` payload.
#[tokio::test]
async fn request_response_roundtrip() {
    let mut pi = spawn_mock(&[]).await;

    let state: RpcSessionState = pi
        .request(&RpcCommand::GetState)
        .await
        .and_then(|data| serde_json::from_value(data).map_err(Into::into))
        .unwrap();

    assert_eq!(state.session_id, "mock-session-id");
    assert_eq!(
        state.session_file.as_deref(),
        Some("/tmp/mock-session.jsonl")
    );
    assert_eq!(state.thinking_level, pi_acp::pi::rpc::ThinkingLevel::Medium);
    assert_eq!(state.steering_mode, QueueMode::OneAtATime);
    assert!(!state.is_streaming);
}

/// Typed wrapper parity.
#[tokio::test]
async fn typed_wrappers_parse_payloads() {
    let mut pi = spawn_mock(&[]).await;

    let state = pi.get_state().await.unwrap();
    assert_eq!(state.session_id, "mock-session-id");

    let models = pi.get_available_models().await.unwrap();
    assert_eq!(models.len(), 3);
    assert_eq!(models[0].id, "mock-model");
    assert_eq!(models[0].provider, "mock");
    assert_eq!(models[1].id, "mock-fast");
    assert_eq!(models[1].context_window, Some(8000));
    // `mock-limited` carries a restricted `thinkingLevelMap` for the
    // per-model dynamic selector tests (W-478).
    assert_eq!(models[2].id, "mock-limited");
    assert!(models[2].reasoning);
    assert!(models[2].thinking_level_map.is_some());

    let path = pi.export_html(None).await.unwrap();
    assert_eq!(path, "/tmp/mock.html");

    pi.set_thinking_level(pi_acp::pi::rpc::ThinkingLevel::High)
        .await
        .unwrap();
    pi.abort().await.unwrap();
}

/// Two requests in flight resolve independently (pending map, out-of-order safe).
#[tokio::test]
async fn concurrent_requests_resolve_independently() {
    let pi = Arc::new(tokio::sync::Mutex::new(
        spawn_mock(&["--mock-delay-ms", "150"]).await,
    ));
    let a = pi.clone();
    let b = pi.clone();

    let (r1, r2) = tokio::join!(
        async move { a.lock().await.request(&RpcCommand::GetState).await },
        async move {
            b.lock()
                .await
                .request(&RpcCommand::GetAvailableModels)
                .await
        },
    );

    let state: RpcSessionState = serde_json::from_value(r1.unwrap()).unwrap();
    assert_eq!(state.session_id, "mock-session-id");

    let models = r2.unwrap();
    assert_eq!(models["models"][0]["id"], "mock-model");
}

/// Human-readable prelude lines (ANSI-styled) are captured and stripped.
#[tokio::test]
async fn prelude_lines_are_captured_and_ansi_stripped() {
    let mut pi = spawn_mock(&["--mock-prelude", "3"]).await;

    // Force the reader to have consumed the banner: the prelude lines precede
    // the response in the pipe, so after a successful round-trip they are all
    // collected.
    pi.get_state().await.unwrap();

    let prelude = pi.consume_prelude_lines();
    assert_eq!(prelude.len(), 3, "prelude lines: {prelude:?}");
    assert!(prelude[0].contains("Context"), "line: {}", prelude[0]);
    assert!(prelude[1].contains("Skills"), "line: {}", prelude[1]);
    assert!(
        prelude.iter().all(|l| !l.contains('\u{1b}')),
        "ANSI escapes must be stripped: {prelude:?}"
    );

    // Consumed on read.
    assert!(pi.consume_prelude_lines().is_empty());
}

/// A hanging pi must produce `RpcTimeout`, not an infinite await (fixes #94).
#[tokio::test]
async fn request_times_out_when_pi_hangs() {
    let mut pi = spawn_mock(&["--mock-hang"]).await;

    let err = pi.request(&RpcCommand::GetState).await.unwrap_err();
    match err {
        AcpxError::RpcTimeout { cmd, secs } => {
            assert_eq!(cmd, "get_state");
            assert_eq!(secs, FAST_TIMEOUT.as_secs());
        }
        other => panic!("expected RpcTimeout, got {other:?}"),
    }
}

/// A response that arrives *after* our deadline must not resolve the timed-out
/// request; it lands on the event stream as `UnmatchedResponse` (TS parity).
#[tokio::test]
async fn late_response_after_timeout_is_routed_to_event_stream() {
    // Client deadline (150ms) fires before the mock's delayed response (400ms).
    let mut pi = PiProcess::spawn_with_args(
        BIN,
        &["--mock-rpc", "--mock-delay-ms", "400"],
        None,
        Duration::from_millis(150),
    )
    .await
    .unwrap();

    let err = pi.request(&RpcCommand::GetState).await.unwrap_err();
    assert!(matches!(err, AcpxError::RpcTimeout { .. }), "got {err:?}");

    // The stale response shows up as an event instead of vanishing.
    match pi.next_event().await {
        Some(RpcEvent::UnmatchedResponse { raw }) => {
            assert_eq!(raw["command"], "get_state");
        }
        other => panic!("expected UnmatchedResponse, got {other:?}"),
    }
}

/// An event type pi does not know yet must not break the stream (no panic); it
/// is surfaced as `RpcEvent::Unknown` for consumers to log and ignore.
#[tokio::test]
async fn unknown_event_types_do_not_break_the_stream() {
    let mut pi = spawn_mock(&["--mock-unknown-event"]).await;

    // The unknown event is buffered (emitted at startup, before any command);
    // the request itself still completes normally.
    assert_eq!(pi.get_state().await.unwrap().session_id, "mock-session-id");

    match pi.next_event().await {
        Some(RpcEvent::Unknown { raw }) => {
            assert_eq!(raw["type"], "brand_new_event");
        }
        other => panic!("expected Unknown, got {other:?}"),
    }
}

/// A child that dies mid-flight rejects the pending request with `PiExited`
/// (fixes #82) and is detected on subsequent requests.
#[tokio::test]
async fn child_exit_rejects_pending_and_marks_dead() {
    let mut pi = spawn_mock(&["--mock-exit-after", "1"]).await;

    let err = pi.request(&RpcCommand::GetState).await.unwrap_err();
    match err {
        AcpxError::PiExited { code, signal } => {
            assert_eq!(code, Some(42));
            assert_eq!(signal, None);
        }
        other => panic!("expected PiExited, got {other:?}"),
    }

    assert!(pi.is_dead());
    assert_eq!(pi.exit_status(), Some((Some(42), None)));

    // Fail-fast on the dead process (no silent empty end_turn).
    let err2 = pi.request(&RpcCommand::GetState).await.unwrap_err();
    assert!(matches!(err2, AcpxError::PiExited { .. }), "got {err2:?}");
}

/// A turn ends at `agent_settled`, streaming text deltas along the way
/// (S2 constraint 2/3 — never truncate on the early prompt response).
#[tokio::test]
async fn prompt_settles_on_agent_settled_and_streams_text() {
    let mut pi = spawn_mock(&[]).await;

    let mut deltas: Vec<String> = Vec::new();
    let result = pi
        .prompt_until_settled("hi", |event| {
            if let Some(delta) = pi_acp::pi::process::text_delta_of(event) {
                deltas.push(delta.to_string());
            }
        })
        .await;

    result.unwrap();
    assert_eq!(deltas, vec!["hello from mock".to_string()]);
}

/// `dispose()` terminates the child (SIGTERM → SIGKILL) and reaps it.
#[tokio::test]
async fn dispose_terminates_and_reaps_child() {
    let mut pi = spawn_mock(&[]).await;
    assert!(!pi.is_dead());

    pi.dispose().await;

    assert!(pi.is_dead(), "child must be terminated by dispose");
    assert!(pi.exit_status().is_some());
    // Event stream is closed for any pump.
    assert!(pi.next_event().await.is_none());
}

/// `extension_ui_response` is a fire-and-forget write (no response expected).
#[tokio::test]
async fn extension_ui_response_is_fire_and_forget() {
    let mut pi = spawn_mock(&[]).await;

    pi.send_extension_ui_response(pi_acp::pi::rpc::ExtensionUiResponse::Value {
        id: "ui-1".into(),
        value: "pick-a".into(),
    })
    .await
    .unwrap();
    pi.send_extension_ui_response(pi_acp::pi::rpc::ExtensionUiResponse::Cancelled {
        id: "ui-2".into(),
        cancelled: true,
    })
    .await
    .unwrap();

    // Client stays usable afterwards.
    assert_eq!(pi.get_state().await.unwrap().session_id, "mock-session-id");
}
