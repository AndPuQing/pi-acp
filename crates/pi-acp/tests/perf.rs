//! W-479 performance baseline: reproducible handshake/refresh RPC budgets,
//! latency breakdown, first-token adapter overhead, and memory probes.
//!
//! All measurements run against the in-binary mock pi (`PI_ACP_MOCK=1`),
//! so they are hermetic in CI. Wall-time assertions use generous bounds
//! (loaded CI runners); the primary output is the printed table
//! (`cargo test --test perf -- --nocapture`).
//!
//! Reproduce locally:
//! ```sh
//! cargo test --test perf -- --nocapture
//! ls -la target/release/pi-acp   # binary size (release)
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, NewSessionRequest, PromptRequest, SessionId,
    SessionNotification, SessionUpdate, SetSessionModeRequest, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{on_receive_notification, AcpAgent, AcpAgentConfig, Client};
use pi_acp::session::{OutboundMessage, PiAcpSession, SessionParams};
use tokio::sync::{mpsc, Mutex};

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");
const TIMEOUT: Duration = Duration::from_secs(15);
/// Injected per-RPC latency for stage-breakdown measurements.
const RPC_DELAY_MS: u64 = 50;

type NotifLog = Arc<Mutex<Vec<SessionUpdate>>>;

fn read_command_log(path: &Path) -> Vec<String> {
    let raw = fs::read_to_string(path).unwrap_or_default();
    raw.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

fn count_commands(cmds: &[String]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for c in cmds {
        *counts.entry(c.clone()).or_insert(0) += 1;
    }
    counts
}

/// Spawn a session directly against the mock pi with a per-RPC delay and a
/// command log. Returns the session plus a recorder of outbound updates.
struct SessionFixture {
    _tmp: tempfile::TempDir,
    session: Arc<PiAcpSession>,
    recorded: Arc<Mutex<Vec<(SessionUpdate, Instant)>>>,
    command_log: PathBuf,
}

async fn spawn_session_with_delay(delay_ms: u64) -> SessionFixture {
    let tmp = tempfile::tempdir().unwrap();
    let command_log = tmp.path().join("commands.log");
    let args = vec![
        "--mock-rpc".to_string(),
        "--mock-command-log".to_string(),
        command_log.to_str().unwrap().to_string(),
        "--mock-delay-ms".to_string(),
        delay_ms.to_string(),
    ];
    let (outbound_tx, mut outbound_rx) = mpsc::channel(512);
    let recorded: Arc<Mutex<Vec<(SessionUpdate, Instant)>>> = Arc::new(Mutex::new(Vec::new()));
    let rec = recorded.clone();
    tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            match msg {
                OutboundMessage::Notify(notif) => {
                    rec.lock().await.push((notif.update, Instant::now()));
                }
                OutboundMessage::RequestPermission(_, respond) => {
                    let _ =
                        respond.send(Err(pi_acp::error::AcpxError::SessionClosed("perf".into())));
                }
                OutboundMessage::Flush(ack) => {
                    let _ = ack.send(());
                }
            }
        }
    });

    let session = PiAcpSession::spawn(SessionParams {
        pi_command: BIN.to_string(),
        extra_args: args,
        timeout: TIMEOUT,
        settle_timeout: Duration::ZERO,
        cwd: tmp.path().to_path_buf(),
        outbound: outbound_tx,
        session_path: None,
        session_id_override: None,
        file_commands: vec![],
    })
    .await
    .expect("session spawn");

    SessionFixture {
        _tmp: tmp,
        session,
        recorded,
        command_log,
    }
}

/// Stage-by-stage handshake latency with a known per-RPC delay.
///
/// Mirrors the `session/new` handshake sequence
/// (spawn → get_state+get_models → thinking levels) so the number of
/// sequential RPC stages — the critical-path cost — is visible.
#[tokio::test]
async fn handshake_stage_latency_with_50ms_rpc_delay() {
    let t0 = Instant::now();
    let fx = spawn_session_with_delay(RPC_DELAY_MS).await;
    let spawn_ms = t0.elapsed().as_millis();

    let t1 = Instant::now();
    let (state_res, models_res) =
        tokio::join!(fx.session.get_state(), fx.session.get_available_models());
    let fetch_ms = t1.elapsed().as_millis();
    assert!(state_res.is_ok());
    assert!(!models_res.unwrap().is_empty());

    let t2 = Instant::now();
    let levels = fx.session.available_thinking_levels().await;
    let levels_ms = t2.elapsed().as_millis();
    assert!(!levels.is_empty());

    let total_ms = t0.elapsed().as_millis();
    let cmds = read_command_log(&fx.command_log);
    println!(
        "[perf] handshake stages (50ms/RPC mock): spawn={spawn_ms}ms \
         get_state+get_models(join)={fetch_ms}ms available_levels={levels_ms}ms \
         total={total_ms}ms pi_commands={cmds:?}"
    );
    // Loose bounds: each stage is dominated by exactly one 50ms delay plus
    // process spawn / IPC overhead; CI runners just need headroom.
    assert!(spawn_ms < 10_000, "spawn took {spawn_ms}ms");
    assert!(fetch_ms < 5_000, "parallel fetch took {fetch_ms}ms");
    assert!(levels_ms < 5_000, "levels fetch took {levels_ms}ms");
}

/// Refresh-chain RPC cost: the single triple-join shape the agent refresh
/// path uses post-optimization (models + state + levels concurrently).
/// With a 50ms per-RPC delay the whole refresh must complete in one stage.
#[tokio::test]
async fn refresh_chain_single_stage() {
    let fx = spawn_session_with_delay(RPC_DELAY_MS).await;
    let before = read_command_log(&fx.command_log).len();
    let t0 = Instant::now();
    let (models, state, levels) = tokio::join!(
        fx.session.get_available_models(),
        fx.session.get_state(),
        fx.session.available_thinking_levels()
    );
    let stage_ms = t0.elapsed().as_millis();
    assert!(!models.unwrap().is_empty());
    assert!(state.is_ok());
    assert!(!levels.is_empty());
    let cmds = read_command_log(&fx.command_log);
    let new_cmds = &cmds[before..];
    println!("[perf] refresh triple-join: {stage_ms}ms pi_commands={new_cmds:?}");
    assert_eq!(
        new_cmds.len(),
        3,
        "refresh must cost exactly 3 pi RPCs (models + state + levels)"
    );
    // One 50ms stage plus IPC overhead; generous headroom for CI.
    assert!(
        stage_ms < 5_000,
        "triple-join refresh took {stage_ms}ms, expected ~{RPC_DELAY_MS}ms"
    );
}

/// First-token adapter overhead: prompt → first streamed text chunk,
/// mock pi responds instantly so this is pure adapter forwarding cost.
#[tokio::test]
async fn first_chunk_adapter_overhead() {
    let fx = spawn_session_with_delay(0).await;
    fx.recorded.lock().await.clear();
    let t0 = Instant::now();
    let reason = fx
        .session
        .prompt("hello".to_string(), vec![])
        .await
        .expect("prompt settles");
    let settle_ms = t0.elapsed().as_millis();
    assert_eq!(reason, pi_acp::session::StopReason::EndTurn);

    let first_chunk_ms = fx
        .recorded
        .lock()
        .await
        .iter()
        .filter_map(|(u, at)| match u {
            SessionUpdate::AgentMessageChunk(_) => Some(at.saturating_duration_since(t0)),
            _ => None,
        })
        .next();
    println!(
        "[perf] first text chunk after {:?}, turn settled after {settle_ms}ms \
         (instant mock: pure adapter overhead)",
        first_chunk_ms
    );
    assert!(first_chunk_ms.unwrap_or(Duration::MAX) < Duration::from_secs(5));
    assert!(settle_ms < 10_000);
}

/// End-to-end `session/new` + `set_mode` pi-RPC budget through the real ACP
/// agent. This guards the W-479 P0 optimizations: the handshake must not
/// re-fetch state the spawn already has, and the config refresh must fetch
/// state exactly once.
#[tokio::test]
async fn e2e_handshake_and_refresh_rpc_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    let agent_dir = tmp.path().join("agent");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(&agent_dir).unwrap();
    let command_log = tmp.path().join("commands.log");

    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN)
            .env("PI_ACP_MOCK_COMMAND_LOG", command_log.to_str().unwrap())
            .env("PI_CODING_AGENT_DIR", agent_dir.to_str().unwrap()),
    );
    let log: NotifLog = Arc::new(Mutex::new(Vec::new()));
    let log_in_handler = log.clone();

    let mut counts_after_new = HashMap::new();
    let mut new_ms = 0u128;
    let mut init_ms = 0u128;
    let mut set_mode_ms = 0u128;
    let mut counts_after_set_mode;
    let mut sid_holder: Option<SessionId> = None;

    Client
        .builder()
        .name("perf-budget-client")
        .on_receive_notification(
            async move |notif: SessionNotification, _cx| {
                log_in_handler.lock().await.push(notif.update);
                Ok(())
            },
            on_receive_notification!(),
        )
        .connect_with(agent, async |cx| {
            let t0 = Instant::now();
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            init_ms = t0.elapsed().as_millis();

            let t1 = Instant::now();
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd.clone()))
                .block_task()
                .await?;
            new_ms = t1.elapsed().as_millis();
            sid_holder = Some(new_session.session_id.clone());
            // Post-response advertise_commands (get_commands) races the
            // response; filter to handshake commands only.
            counts_after_new = count_commands(&read_command_log(&command_log));

            let sid = new_session.session_id.clone();
            let t2 = Instant::now();
            cx.send_request(SetSessionModeRequest::new(sid.clone(), "low"))
                .block_task()
                .await?;
            set_mode_ms = t2.elapsed().as_millis();
            Ok(())
        })
        .await
        .expect("e2e perf client");
    // The mock emits `thinking_level_changed` after set_thinking_level and
    // the pump refreshes selectors on it; poll until that async work lands
    // so the budget below covers the full chain deterministically.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        counts_after_set_mode = count_commands(&read_command_log(&command_log));
        let settled = counts_after_set_mode.get("get_state").copied().unwrap_or(0) >= 3
            && counts_after_set_mode
                .get("get_available_models")
                .copied()
                .unwrap_or(0)
                >= 3
            && counts_after_set_mode
                .get("get_available_thinking_levels")
                .copied()
                .unwrap_or(0)
                >= 3;
        if settled || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let sid = sid_holder.expect("session/new must succeed");
    println!(
        "[perf] e2e (instant mock): initialize={init_ms}ms session/new={new_ms}ms \
         set_mode={set_mode_ms}ms session={}",
        sid.0.as_ref()
    );
    println!("[perf] pi commands after session/new: {counts_after_new:?}");
    println!("[perf] pi commands after set_mode: {counts_after_set_mode:?}");

    // Handshake budget (W-479 P0): exactly one get_state (at spawn, reused
    // from `initial_state`), one get_available_models, one
    // get_available_thinking_levels. (`get_commands` is post-response and
    // intentionally excluded from the critical-path budget.)
    assert_eq!(counts_after_new.get("get_state"), Some(&1));
    assert_eq!(counts_after_new.get("get_available_models"), Some(&1));
    assert_eq!(
        counts_after_new.get("get_available_thinking_levels"),
        Some(&1)
    );

    // set_mode budget: one set_thinking_level, one triple-join refresh
    // (models + state + levels), plus the pump's `thinking_level_changed`
    // refresh (state + models + levels, already concurrent — P1 coalescing
    // candidate, see the W-479 report).
    let delta = |k: &str| {
        counts_after_set_mode.get(k).copied().unwrap_or(0)
            - counts_after_new.get(k).copied().unwrap_or(0)
    };
    assert_eq!(delta("set_thinking_level"), 1);
    assert_eq!(delta("get_available_models"), 2);
    assert_eq!(delta("get_state"), 2);
    assert_eq!(delta("get_available_thinking_levels"), 2);

    // Loose latency bounds for CI (instant mock: everything is local IPC).
    assert!(init_ms < 10_000, "initialize took {init_ms}ms");
    assert!(new_ms < 15_000, "session/new took {new_ms}ms");
    assert!(set_mode_ms < 10_000, "set_mode took {set_mode_ms}ms");
}

/// Prompt request shape helper.
#[allow(dead_code)]
fn prompt_for(session_id: &SessionId, s: &str) -> PromptRequest {
    PromptRequest::new(
        session_id.clone(),
        vec![ContentBlock::Text(TextContent::new(s.to_string()))],
    )
}

/// Resident memory probe: spawn a session (adapter is in-process here, pi
/// is a `pi-acp --mock-rpc` child) and sum the RSS of `pi-acp` executables
/// that are descendants of this test process (parallel sibling tests are
/// excluded by construction).
#[tokio::test]
async fn resident_memory_of_agent_plus_mock_pi() {
    let me = std::process::id();
    // Sibling tests run in the same process; diff the descendant set across
    // the spawn so only this test's mock pi is measured.
    let before: std::collections::HashSet<u32> = descendant_pi_acp_rss_kb(me)
        .into_iter()
        .map(|(pid, _)| pid)
        .collect();
    let fx = spawn_session_with_delay(0).await;
    let _ = fx.session.get_state().await;
    let added: Vec<(u32, u64)> = descendant_pi_acp_rss_kb(me)
        .into_iter()
        .filter(|(pid, _)| !before.contains(pid))
        .collect();
    let total_kb: u64 = added.iter().map(|(_, rss)| rss).sum();
    println!("[perf] new mock-pi child (pid:RSS_KB): {added:?} total={total_kb}KB (debug build)");
    assert_eq!(added.len(), 1, "one session spawns exactly one pi child");
    assert!(total_kb < 200_000, "mock pi child uses {total_kb}KB");
}

fn process_rss_kb(pid: u32) -> u64 {
    let raw = fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    raw.lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// `(pid, rss_kb)` of `pi-acp`-executable processes descended from `root`.
/// Ancestry is resolved via /proc ppid chains, so parallel sibling tests
/// never pollute the measurement.
fn descendant_pi_acp_rss_kb(root: u32) -> Vec<(u32, u64)> {
    // pid -> ppid for the whole table (single pass).
    let mut ppid_of: HashMap<u32, u32> = HashMap::new();
    let mut candidates: Vec<u32> = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
        // comm may contain spaces/parens: ppid is the 4th field after the
        // last ')'.
        if let Some(after) = stat.rsplit_once(')') {
            if let Some(ppid) = after
                .1
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse().ok())
            {
                ppid_of.insert(pid, ppid);
            }
        }
        // Match only the `pi-acp` executable basename (test binaries live
        // under a `pi-acp/` workdir path and must not match).
        let cmdline = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
        let argv0 = cmdline.split(|b| *b == 0).next().unwrap_or_default();
        let base = argv0.rsplit(|b| *b == b'/').next().unwrap_or_default();
        if base == b"pi-acp" && pid != root {
            candidates.push(pid);
        }
    }
    candidates
        .into_iter()
        .filter(|pid| {
            let mut p = *pid;
            while let Some(pp) = ppid_of.get(&p).copied() {
                if pp == root {
                    return true;
                }
                if pp == 0 || pp == p {
                    return false;
                }
                p = pp;
            }
            false
        })
        .map(|pid| (pid, process_rss_kb(pid)))
        .collect()
}
