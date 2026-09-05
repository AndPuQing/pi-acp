//! Entry point for the `pi-acp` binary.
//!
//! Two modes:
//! - `--terminal-login`: launch `pi` interactively (inherited stdio) so the user
//!   can configure API keys / OAuth login. Mirrors the TS pi-acp behavior and is
//!   what ACP "Terminal Auth" invokes.
//! - default: run the ACP agent over stdio, bridging to a `pi --mode rpc`
//!   subprocess (see [`pi_acp::agent::run`]).
//!
//! The whole thing runs on a `tokio` multi-thread runtime — the S2 spike
//! (W-449) validates that the ACP SDK's `Stdio` transport is driven correctly
//! under tokio (design D9 / §5.3).

use anyhow::Result;
use pi_acp::agent::AcpAgent;
use pi_acp::config::Config;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::signal;

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // Keep short-lived probes from creating a Tokio worker pool. These paths
    // are used during every session handshake, so starting the runtime first
    // needlessly raises the process/thread peak when pi is itself pi-acp.
    if args.iter().any(|a| a == "--terminal-login") {
        return terminal_login();
    }
    if args.iter().any(|a| a == "--version") {
        println!("v{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // The in-process mock is deliberately single-threaded. ACP integration
    // tests nest a mock pi inside the adapter, and the mock only performs
    // sequential stdin/stdout work; an extra worker pool needlessly raises
    // the process/thread peak on Unix runners.
    if is_mock_mode(&args) {
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run_mock_rpc());
    }

    // The adapter-side ACP fixture also sets `PI_ACP_MOCK`, even though it
    // does not carry the `--mode` marker. Keep those short-lived test
    // processes single-threaded too, so adjacent fixtures can be torn down
    // without exhausting Unix runner process resources.
    if std::env::var_os("PI_ACP_MOCK").is_some() {
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async_main());
    }

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(async_main())
}

fn is_mock_mode(args: &[String]) -> bool {
    args.iter().any(|a| a == "--mock-rpc")
        || (std::env::var_os("PI_ACP_MOCK").is_some() && args.iter().any(|a| a == "--mode"))
}

async fn async_main() -> Result<()> {
    // Structured logging (env-filter driven, e.g. RUST_LOG=pi_acp=debug).
    // Logs go to stderr: stdout carries the ACP JSONL stream and any stray
    // line breaks strict clients' framing (W-479 P1).
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    let cfg = Config::from_env();
    tracing::info!(pi_command = %cfg.pi_command, "pi-acp (Rust) starting");
    let agent = Arc::new(AcpAgent::new(cfg));
    tokio::select! {
        result = agent.run() => {
            result.map_err(|e| anyhow::anyhow!("ACP error: {e:?}"))?;
        }
        // Graceful shutdown on SIGINT/SIGTERM (design §8.3): the pi subprocess
        // runs in its own process group, so a terminal Ctrl+C / `kill` on
        // pi-acp never reaches it — dispose every session explicitly so no pi
        // is orphaned. Without this branch the default signal disposition
        // would kill the process mid-session.
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received; disposing pi sessions");
            let shutdown = agent.shutdown();
            // Bound the graceful teardown: a pi that ignores SIGTERM must not
            // block exit forever (PiProcess::dispose escalates to SIGKILL, but
            // only after its own grace window).
            tokio::select! {
                _ = shutdown => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    tracing::warn!("graceful shutdown exceeded 10s; exiting anyway");
                }
            }
        }
    }
    Ok(())
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM — the graceful-shutdown trigger
/// (design §8.3). Unix handles both; other platforms get Ctrl+C only.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            signal::unix::signal(signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}

/// Launch `pi` with inherited stdio for interactive login/setup (ACP Terminal
/// Auth, §6.3): spawn `pi` without any RPC flags so the user can configure API
/// keys / OAuth in the interactive TUI, and propagate its exit code. A missing
/// `pi` binary surfaces a clear install hint.
fn terminal_login() -> Result<()> {
    let cfg = Config::from_env();
    let pi_command = cfg.pi_command.clone();
    tracing::info!(pi_command, "launching pi for terminal login");

    // Resolve for the Windows `pi.cmd` wrapper (fixes pi-acp #27): a bare `pi`
    // expands to the npm global and is launched via `cmd.exe /d /s /c`.
    let resolved = pi_acp::pi::resolve::resolve_current_env(&pi_command);
    let status = std::process::Command::new(&resolved.program)
        .args(&resolved.cmd_args)
        .status()
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to launch `{pi_command}` (resolved to {}) for terminal login: {e}. \n\
                 Is pi installed? Install it with `npm i -g @earendil-works/pi-coding-agent` \n\
                 or set PI_ACP_PI_COMMAND (on Windows this is the npm global `pi.cmd`).",
                resolved.program
            )
        })?;
    std::process::exit(status.code().unwrap_or(1));
}

/// Test-only mock `pi --mode rpc` server.
///
/// Hidden fixture behind `--mock-rpc` (spawned by `tests/pi_process.rs` and
/// `tests/session.rs`) or the `PI_ACP_MOCK=1` env var (spawned by the ACP e2e
/// tests, which cannot pass argv to the inner pi): speaks the same JSONL
/// protocol as pi so [`pi_acp::pi::process::PiProcess`], the session state
/// machine, and the ACP agent (S6) can be exercised without a real pi + LLM
/// backend. All other args (`--mode rpc --no-themes --session <path>`) are
/// ignored.
///
/// Behavior flags (any combination):
/// - `--mock-prelude <n>`   emit `n` ANSI-styled human-readable lines before NDJSON
/// - `--mock-hang`          read commands but never respond (request-timeout tests)
/// - `--mock-exit-after <n>` exit(42) after reading `n` commands, without responding
/// - `--mock-delay-ms <n>`  delay each response by `n` ms (concurrency tests)
/// - `--mock-unknown-event` emit one unknown event type (protocol-evolution guard)
/// - `--mock-scenario <dir>` per-prompt event replay from `<dir>/<n>.jsonl`
///   (`n` = 1-based prompt ordinal; also settable via `PI_ACP_MOCK_SCENARIO` so
///   the ACP e2e can configure it without argv). Each line is a JSON event
///   emitted after the prompt response; `{"__directive__":"write_file",...}` /
///   `{"__directive__":"delete_file",...}` lines mutate the filesystem
///   instead of emitting. `agent_settled` is auto-appended unless the file
///   already contains one or `--mock-no-settle` is set.
/// - `--mock-no-settle`      never auto-append `agent_settled` (cancel tests)
/// - `--mock-no-settle-first` suppress the auto `agent_settled` for the first
///   prompt only (settle-timeout recovery tests: the stuck turn times out,
///   the post-timeout `abort` still settles, and the retry settles normally)
/// - `--mock-abort-error <text>` answer `abort` with `success:false`
///   (settle-timeout recovery failure tests)
/// - `--mock-abort-delay-ms <n>` delay the abort response and settle event
///   (settle-timeout recovery cancellation tests)
/// - `--mock-event-delay-ms <n>` sleep `n` ms before each scenario event
/// - `--mock-command-log <path>`  append each received command type
/// - `--mock-extension-log <path>` append each received `extension_ui_response`
/// - `--mock-cwd-log <path>`      write the mock's startup cwd
/// - `--mock-argv-log <path>`     write the mock's full argv (one per line;
///   used to assert which flags pi-acp passed to the child)
/// - `--mock-prompt-error <text>`   answer `prompt` with `success:false` and this
///   error text (auth/error-surfacing tests)
/// - `--mock-prompt-hang`          accept/read `prompt` but withhold its response;
///   a later `abort` is still processed (cancel race tests)
/// - `--mock-prompt-response-after-events-ms <n>` delay each successful prompt
///   response until its scenario events have been emitted (channel-order tests)
/// - `--mock-models-error <text>`   answer `get_available_models` with
///   `success:false` and this error text (authRequired-on-new tests)
/// - `PI_ACP_MOCK_SESSION_FILE`     session file path for a new mock session;
///   the mock writes a valid session header after the first successful prompt
///
/// Default: respond `success: true` to every command (with a fixed `get_state`
/// payload), and after a `prompt` command emit a `text_delta` message_update
/// with an empty usage snapshot, the final assistant `message_end` with token
/// usage, and then `agent_settled` (mirroring pi's early-response + settled-
/// event semantics, S2 constraint 2). After an `abort`, emit `agent_settled`
/// (pi settles once the aborted turn unwinds).
async fn run_mock_rpc() -> Result<()> {
    use std::path::PathBuf;

    let env_scenario = std::env::var_os("PI_ACP_MOCK_SCENARIO").map(PathBuf::from);

    let mut prelude = 0usize;
    let mut hang = false;
    let mut exit_after: Option<usize> = None;
    let mut delay_ms: u64 = 0;
    let mut unknown_event = false;
    let mut scenario_dir: Option<PathBuf> = None;
    let mut no_settle = false;
    let mut no_settle_first = false;
    let mut abort_error: Option<String> = None;
    let mut abort_delay_ms: u64 = 0;
    let mut event_delay_ms: u64 = 0;
    let mut command_log: Option<PathBuf> = None;
    let mut extension_log: Option<PathBuf> = None;
    let mut cwd_log: Option<PathBuf> = None;
    let mut argv_log: Option<PathBuf> = None;
    let mut prompt_error: Option<String> = None;
    let mut prompt_hang = false;
    let mut prompt_response_after_events_ms: u64 = 0;
    let mut models_error: Option<String> = None;
    let env_session_file = std::env::var_os("PI_ACP_MOCK_SESSION_FILE").map(PathBuf::from);
    let mut session_arg: Option<PathBuf> = None;
    let mut prompt_count: usize = 0;
    // Stateful mock: real pi keeps these across RPC calls, and the agent's
    // config-option / slash-command handlers read them back via get_state.
    let mut mock_thinking_level = "medium".to_string();
    let mut mock_model = "mock-model".to_string();
    let mut mock_auto_compaction = false;
    let mut mock_steering_mode = "one-at-a-time".to_string();
    let mut mock_follow_up_mode = "one-at-a-time".to_string();
    let mut mock_session_name = "Mock Session".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--mock-rpc" => {}
            "--mock-prelude" => prelude = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--mock-hang" => hang = true,
            "--mock-exit-after" => exit_after = args.next().and_then(|v| v.parse().ok()),
            "--mock-delay-ms" => delay_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--mock-unknown-event" => unknown_event = true,
            "--mock-scenario" => scenario_dir = args.next().map(PathBuf::from),
            "--mock-no-settle" => no_settle = true,
            "--mock-no-settle-first" => no_settle_first = true,
            "--mock-abort-error" => abort_error = args.next(),
            "--mock-abort-delay-ms" => {
                abort_delay_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(0)
            }
            "--mock-event-delay-ms" => {
                event_delay_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(0)
            }
            "--mock-command-log" => command_log = args.next().map(PathBuf::from),
            "--mock-extension-log" => extension_log = args.next().map(PathBuf::from),
            "--mock-cwd-log" => cwd_log = args.next().map(PathBuf::from),
            "--mock-argv-log" => argv_log = args.next().map(PathBuf::from),
            "--mock-prompt-error" => prompt_error = args.next(),
            "--mock-prompt-hang" => prompt_hang = true,
            "--mock-prompt-response-after-events-ms" => {
                prompt_response_after_events_ms =
                    args.next().and_then(|v| v.parse().ok()).unwrap_or(0)
            }
            "--mock-models-error" => models_error = args.next(),
            "--session" => session_arg = args.next().map(PathBuf::from),
            _ => {}
        }
    }
    if scenario_dir.is_none() {
        scenario_dir = env_scenario;
    }
    if command_log.is_none() {
        command_log = std::env::var_os("PI_ACP_MOCK_COMMAND_LOG").map(PathBuf::from);
    }
    if prompt_error.is_none() {
        prompt_error = std::env::var("PI_ACP_MOCK_PROMPT_ERROR").ok();
    }
    if models_error.is_none() {
        models_error = std::env::var("PI_ACP_MOCK_MODELS_ERROR").ok();
    }
    if exit_after.is_none() {
        exit_after = std::env::var("PI_ACP_MOCK_EXIT_AFTER")
            .ok()
            .and_then(|v| v.parse().ok());
    }

    let mock_session_file = session_arg
        .clone()
        .or_else(|| env_session_file.clone())
        .unwrap_or_else(|| PathBuf::from("/tmp/mock-session.jsonl"));
    let mock_session_id = session_arg
        .as_deref()
        .and_then(read_mock_session_id)
        .unwrap_or_else(|| "mock-session-id".to_string());
    let persist_new_session = session_arg.is_none() && env_session_file.is_some();

    if argv_log.is_none() {
        argv_log = std::env::var_os("PI_ACP_MOCK_ARGV_LOG").map(PathBuf::from);
    }
    if let Some(log) = &argv_log {
        let argv = std::env::args().skip(1).collect::<Vec<_>>().join("\n");
        append_log(log, &argv);
    }
    if let Some(log) = &cwd_log {
        if let Ok(cwd) = std::env::current_dir() {
            append_log(log, &cwd.to_string_lossy());
        }
    }

    let mut stdout = tokio::io::stdout();

    // Prelude: human-readable banner with ANSI colors, before any NDJSON.
    // These are raw text lines (not JSON) — exactly what real pi emits.
    let ansi = [
        "\u{1b}[32mContext\u{1b}[0m: mock context loaded",
        "\u{1b}[1mSkills\u{1b}[0m: mock skill (2)",
        "\u{1b}[34mExtensions\u{1b}[0m: none",
    ];
    for line in ansi.iter().take(prelude) {
        mock_write_line(&mut stdout, line.as_bytes()).await?;
    }

    // Protocol-evolution guard: an event type this client does not know yet.
    if unknown_event {
        mock_write_line(&mut stdout, b"{\"type\":\"brand_new_event\",\"payload\":1}").await?;
    }

    // W-483 fixture: simulate the in-pi MCP registrar. When the adapter
    // passes a session payload (`PI_ACP_MCP_SERVERS_JSON`, set per child by
    // the MCP handshake), the mock emits one `PI_ACP_MCP:*` marker per
    // requested server — exactly the lines the real registrar prints.
    // Markers go to stderr, mirroring real pi (which routes everything its
    // extensions print to the child's stderr); pi-acp scrapes them there.
    // `PI_ACP_MOCK_MCP_FAIL=<name>` makes that server report failure
    // instead (explicit per-server error path). `PI_ACP_MOCK_MCP_SILENT=1`
    // emits no markers at all: the registrar/adapter died without reporting,
    // so the session handshake must time out loudly (W-484), never hang or
    // resolve with a partial menu.
    if let Ok(payload) = std::env::var("PI_ACP_MCP_SERVERS_JSON") {
        use tokio::io::AsyncWriteExt as _;
        let silent = std::env::var_os("PI_ACP_MOCK_MCP_SILENT").is_some();
        let fail_name = std::env::var("PI_ACP_MOCK_MCP_FAIL").ok();
        if silent {
            // Registrar died without reporting: stay alive (the RPC loop
            // below still runs) but emit nothing, so the handshake gate
            // must time out loudly naming the pending servers.
        } else if let Ok(specs) = serde_json::from_str::<serde_json::Value>(&payload) {
            if let Some(items) = specs.as_array() {
                let mut stderr = tokio::io::stderr();
                for item in items {
                    let name = item
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    if name.is_empty() {
                        continue;
                    }
                    let line = if fail_name.as_deref() == Some(name) {
                        format!("PI_ACP_MCP:failed:{name}:mock registration refused")
                    } else {
                        format!("PI_ACP_MCP:registered:{name}")
                    };
                    stderr.write_all(line.as_bytes()).await?;
                    stderr.write_all(b"\n").await?;
                }
                stderr.flush().await?;
            }
        }
    }

    if exit_after == Some(0) {
        std::process::exit(42);
    }

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    let mut handled: usize = 0;

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // stdin EOF — pi exits on EOF too
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(after) = exit_after {
            if handled + 1 >= after {
                // Read `after` commands, then die without answering them.
                std::process::exit(42);
            }
        }
        if hang {
            // Read and ignore — the client is expected to time out.
            continue;
        }

        let command: serde_json::Value = serde_json::from_str(trimmed)?;
        let id = command
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let ty = command
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");

        if let Some(log) = &command_log {
            append_log(log, ty);
        }

        // Keep reading after a prompt without producing its early response so
        // the client can prove that cancellation does not wait on the prompt
        // request's ownership guard. The abort branch below still settles it.
        if prompt_hang && ty == "prompt" {
            handled += 1;
            continue;
        }

        // Fire-and-forget extension answers: pi matches these by the request's
        // own id; record them and emit no response.
        if ty == "extension_ui_response" {
            if let Some(log) = &extension_log {
                append_log(log, trimmed);
            }
            continue;
        }

        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }

        let data = match ty {
            "get_state" => serde_json::json!({
                "model": mock_model_definition(&mock_model),
                "thinkingLevel": mock_thinking_level,
                "isStreaming": false,
                "isCompacting": false,
                "steeringMode": mock_steering_mode,
                "followUpMode": mock_follow_up_mode,
                "sessionFile": mock_session_file.to_string_lossy(),
                "sessionId": mock_session_id,
                "sessionName": mock_session_name,
                "autoCompactionEnabled": mock_auto_compaction,
                "messageCount": 3,
                "pendingMessageCount": 0
            }),
            "get_available_models" => serde_json::json!({
                "models": [
                    mock_model_definition("mock-model"),
                    mock_model_definition("mock-fast"),
                    mock_model_definition("mock-limited")
                ]
            }),
            // Native per-model levels (pi `AgentSession.getAvailableThinkingLevels`).
            "get_available_thinking_levels" => serde_json::json!({
                "levels": mock_supported_levels(&mock_model)
            }),
            "export_html" => serde_json::json!({"path": "/tmp/mock.html"}),
            "set_model" => command
                .get("modelId")
                .and_then(serde_json::Value::as_str)
                .map(mock_model_definition)
                .unwrap_or_else(|| mock_model_definition(&mock_model)),
            "compact" => serde_json::json!({
                "tokensBefore": 1500,
                "summary": "The conversation was summarized."
            }),
            "get_session_stats" => serde_json::json!({
                "sessionId": mock_session_id,
                "sessionFile": mock_session_file.to_string_lossy(),
                "totalMessages": 3,
                "cost": 0.0123,
                "tokens": {"input": 100, "output": 50, "cacheRead": 10, "cacheWrite": 5, "total": 165}
            }),
            "get_commands" => serde_json::json!({
                "commands": [
                    {"name": "review", "description": "Review the current diff", "source": "prompt"},
                    {"name": "skill:deploy", "description": "Deploy to staging", "source": "skill"},
                    {"name": "ext-thing", "description": "An extension command", "source": "extension"}
                ]
            }),
            "get_messages" => serde_json::json!({
                "messages": [
                    {"role": "user", "content": "hello there"},
                    {"role": "assistant", "content": [{"type": "text", "text": "hi! how can I help?"}]},
                    {"role": "toolResult", "toolName": "bash", "toolCallId": "bash-1", "isError": false, "command": "ls", "content": {"stdout": "a.txt\nb.txt\n", "exitCode": 0}, "details": {}},
                    {"role": "toolResult", "toolName": "read", "toolCallId": "read-1", "isError": false, "content": {"output": "file contents", "exitCode": 0}, "details": {"path": "a.txt"}}
                ]
            }),
            _ => serde_json::Value::Null,
        };

        // Apply stateful mutations (real pi persists these; the agent reads
        // them back via get_state for configOptions / slash-command displays).
        // Mirror pi: thinking changes clamp to the model's levels and only
        // emit `thinking_level_changed` when the effective level moves.
        let mut thinking_event: Option<serde_json::Value> = None;
        match ty {
            "set_thinking_level" => {
                if let Some(l) = command.get("level").and_then(serde_json::Value::as_str) {
                    let effective = mock_clamp_level(&mock_model, l);
                    if effective != mock_thinking_level {
                        mock_thinking_level = effective.clone();
                        thinking_event = Some(serde_json::json!({
                            "type": "thinking_level_changed", "level": effective
                        }));
                    }
                }
            }
            "set_model" => {
                if let Some(m) = command.get("modelId").and_then(serde_json::Value::as_str) {
                    mock_model = m.to_string();
                }
                // pi re-resolves + clamps thinking on a model switch.
                let effective = mock_clamp_level(&mock_model, &mock_thinking_level.clone());
                if effective != mock_thinking_level {
                    mock_thinking_level = effective.clone();
                    thinking_event = Some(serde_json::json!({
                        "type": "thinking_level_changed", "level": effective
                    }));
                }
            }
            "set_auto_compaction" => {
                mock_auto_compaction = command
                    .get("enabled")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
            }
            "set_steering_mode" => {
                if let Some(m) = command.get("mode").and_then(serde_json::Value::as_str) {
                    mock_steering_mode = m.to_string();
                }
            }
            "set_follow_up_mode" => {
                if let Some(m) = command.get("mode").and_then(serde_json::Value::as_str) {
                    mock_follow_up_mode = m.to_string();
                }
            }
            "set_session_name" => {
                if let Some(n) = command.get("name").and_then(serde_json::Value::as_str) {
                    mock_session_name = n.to_string();
                }
            }
            _ => {}
        }

        let mut response = serde_json::json!({
            "id": id,
            "type": "response",
            "command": ty,
            "success": true,
        });
        // Error injection: `success:false` responses with the configured text
        // (auth / error-surfacing tests).
        let (success, error) = match ty {
            "prompt" => (prompt_error.is_none(), prompt_error.clone()),
            "abort" => (abort_error.is_none(), abort_error.clone()),
            "get_available_models" => (models_error.is_none(), models_error.clone()),
            _ => (true, None),
        };
        response["success"] = serde_json::Value::Bool(success);
        if let Some(e) = error {
            response["error"] = serde_json::Value::String(e);
        } else if !data.is_null() {
            response["data"] = data;
        }
        let delay_prompt_response =
            success && ty == "prompt" && prompt_response_after_events_ms > 0;
        if success && ty == "abort" && abort_delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(abort_delay_ms)).await;
        }
        if !delay_prompt_response {
            mock_write_line(&mut stdout, response.to_string().as_bytes()).await?;
        }
        handled += 1;

        // Mirror pi: `setSessionName` emits `session_info_changed` on the event
        // stream (pi-agent-core `AgentSessionEvent`), which the adapter
        // forwards as ACP `session_info_update` (live thread title, #102/#24).
        if success && ty == "set_session_name" {
            let event =
                serde_json::json!({ "type": "session_info_changed", "name": mock_session_name });
            mock_write_line(&mut stdout, event.to_string().as_bytes()).await?;
        }

        // Mirror pi: a thinking change that moves the effective level emits
        // `thinking_level_changed` (W-478 clamp parity).
        if success {
            if let Some(event) = thinking_event {
                mock_write_line(&mut stdout, event.to_string().as_bytes()).await?;
            }
        }

        // Mirror pi: the prompt response arrives early; the streaming events
        // and the real turn-completion signal (`agent_settled`) follow after.
        if success && ty == "prompt" {
            prompt_count += 1;
            // Suppress the auto settle for the first prompt only when asked
            // (recovery tests); the post-timeout `abort` below still settles.
            let suppress_settle = no_settle || (no_settle_first && prompt_count == 1);
            if let Some(dir) = &scenario_dir {
                let scenario = dir.join(format!("{prompt_count}.jsonl"));
                if scenario.exists() {
                    let content = std::fs::read_to_string(&scenario)?;
                    replay_scenario(
                        &mut stdout,
                        &content,
                        &mut reader,
                        suppress_settle,
                        event_delay_ms,
                        extension_log.as_deref(),
                    )
                    .await?;
                } else if !suppress_settle {
                    emit_default_prompt_response(&mut stdout).await?;
                }
            } else if !suppress_settle {
                emit_default_prompt_response(&mut stdout).await?;
            }
            if delay_prompt_response {
                tokio::time::sleep(std::time::Duration::from_millis(
                    prompt_response_after_events_ms,
                ))
                .await;
                mock_write_line(&mut stdout, response.to_string().as_bytes()).await?;
            }
            if persist_new_session {
                persist_mock_session_file(&mock_session_file, &mock_session_id)?;
            }
        } else if success && ty == "abort" {
            // pi settles once an aborted turn unwinds.
            mock_write_line(&mut stdout, b"{\"type\":\"agent_settled\"}").await?;
        }
    }

    Ok(())
}

/// Mock model definition for `get_state` / `get_available_models` /
/// `set_model` responses. `mock-model` + `mock-fast` advertise the full pi
/// thinking ladder; `mock-limited` only supports a subset (no `minimal`,
/// `xhigh`, `max`) so tests can prove the per-model dynamic selector
/// (W-478). Shapes mirror real pi payloads (`thinkingLevelMap` included).
fn mock_model_definition(id: &str) -> serde_json::Value {
    match id {
        "mock-limited" => serde_json::json!({
            "id": "mock-limited", "name": "Mock Limited", "provider": "mock",
            "reasoning": true,
            "thinkingLevelMap": {"off": "none", "low": "low", "medium": "medium", "high": "high"},
            "contextWindow": 4000, "maxTokens": 100
        }),
        "mock-fast" => serde_json::json!({
            "id": "mock-fast", "name": "Mock Fast", "provider": "mock",
            "reasoning": true,
            "thinkingLevelMap": {"off": "none", "minimal": "minimal", "low": "low", "medium": "medium", "high": "high", "xhigh": "xhigh", "max": "max"},
            "contextWindow": 8000, "maxTokens": 100
        }),
        _ => serde_json::json!({
            "id": "mock-model", "name": "Mock Model", "provider": "mock",
            "reasoning": true,
            "thinkingLevelMap": {"off": "none", "minimal": "minimal", "low": "low", "medium": "medium", "high": "high", "xhigh": "xhigh", "max": "max"},
            "contextWindow": 1000, "maxTokens": 100
        }),
    }
}

/// Native per-model thinking levels for the mock (`getSupportedThinkingLevels`
/// parity: `null`/missing entries exclude `xhigh`/`max`; a `null` entry
/// excludes its level everywhere). Unknown model ids get the full ladder.
fn mock_supported_levels(id: &str) -> Vec<&'static str> {
    match id {
        "mock-limited" => vec!["off", "low", "medium", "high"],
        _ => vec!["off", "minimal", "low", "medium", "high", "xhigh", "max"],
    }
}

/// Clamp a requested level to the mock model's levels (pi-ai
/// `clampThinkingLevel` parity: exact hit, else nearest higher, else nearest
/// lower). Unknown levels fall back to the first supported level.
fn mock_clamp_level(id: &str, level: &str) -> String {
    const LADDER: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];
    let supported = mock_supported_levels(id);
    if supported.contains(&level) {
        return level.to_string();
    }
    let requested = LADDER.iter().position(|l| *l == level);
    if let Some(i) = requested {
        if let Some(up) = LADDER.iter().skip(i + 1).find(|l| supported.contains(*l)) {
            return up.to_string();
        }
        if let Some(down) = LADDER[..i].iter().rev().find(|l| supported.contains(*l)) {
            return down.to_string();
        }
    }
    supported.first().unwrap_or(&"off").to_string()
}

/// Read the native id from a persisted pi session header. A missing or
/// malformed file intentionally falls back to the default mock id, which lets
/// restore tests exercise the adapter's mismatch check.
fn read_mock_session_id(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let first = raw.lines().next()?.trim();
    let value: serde_json::Value = serde_json::from_str(first).ok()?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("session") {
        return None;
    }
    let id = value.get("id").and_then(serde_json::Value::as_str)?.trim();
    (!id.is_empty()).then(|| id.to_string())
}

/// Simulate pi flushing a new session after its first successful turn. The
/// caller can still use a scenario directive to provide richer file content;
/// an existing file is therefore left untouched.
fn persist_mock_session_file(path: &Path, session_id: &str) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let cwd = std::env::current_dir()?;
    let header = serde_json::json!({
        "type": "session",
        "id": session_id,
        "cwd": cwd.to_string_lossy(),
    });
    std::fs::write(path, format!("{header}\n"))?;
    Ok(())
}

/// The default post-prompt event sequence: a `text_delta` message_update with
/// an empty usage snapshot, the final assistant `message_end` with token
/// usage, and `agent_settled`.
async fn emit_default_prompt_response(stdout: &mut tokio::io::Stdout) -> Result<()> {
    let update = serde_json::json!({
        "type": "message_update",
        "usage": {},
        "assistantMessageEvent": {
            "type": "text_delta",
            "contentIndex": 0,
            "delta": "hello from mock"
        }
    });
    mock_write_line(stdout, update.to_string().as_bytes()).await?;
    let end = serde_json::json!({
        "type": "message_end",
        "message": {
            "role": "assistant",
            "content": [{"type": "text", "text": "hello from mock"}],
            "usage": {"input": 10, "output": 5, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 15}
        }
    });
    mock_write_line(stdout, end.to_string().as_bytes()).await?;
    mock_write_line(stdout, b"{\"type\":\"agent_settled\"}").await?;
    Ok(())
}

/// Replay one prompt's scenario file: emit each JSON event in order, honor
/// `__directive__` lines, pause after `extension_ui_request` until the client
/// answers, and auto-append `agent_settled` unless the scenario already
/// contains one or `--mock-no-settle` suppresses it.
async fn replay_scenario(
    stdout: &mut tokio::io::Stdout,
    content: &str,
    reader: &mut BufReader<tokio::io::Stdin>,
    no_settle: bool,
    event_delay_ms: u64,
    extension_log: Option<&Path>,
) -> Result<()> {
    let mut saw_settle = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(trimmed)?;
        match value
            .get("__directive__")
            .and_then(serde_json::Value::as_str)
        {
            Some("write_file") => {
                let path = value["path"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("write_file directive needs a path"))?;
                let content = value["content"].as_str().unwrap_or("");
                if let Some(parent) = Path::new(path).parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(path, content)?;
                continue;
            }
            Some("delete_file") => {
                let path = value["path"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("delete_file directive needs a path"))?;
                let _ = std::fs::remove_file(path);
                continue;
            }
            Some("wait_ms") => {
                // Give the client time to observe the previous event (e.g. the
                // session's pre-mutation snapshot) before the next directive
                // mutates the filesystem.
                let ms = value["ms"].as_u64().unwrap_or(0);
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                continue;
            }
            _ => {}
        }

        if event_delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(event_delay_ms)).await;
        }
        if value.get("type").and_then(serde_json::Value::as_str) == Some("agent_settled") {
            saw_settle = true;
        }
        mock_write_line(stdout, trimmed.as_bytes()).await?;

        // pi blocks its turn on an extension UI request until the client
        // answers; wait for the `extension_ui_response` line before continuing.
        if value.get("type").and_then(serde_json::Value::as_str) == Some("extension_ui_request") {
            wait_for_extension_response(reader, extension_log).await?;
        }
    }
    if !no_settle && !saw_settle {
        mock_write_line(stdout, b"{\"type\":\"agent_settled\"}").await?;
    }
    Ok(())
}

/// Read stdin until the client answers an `extension_ui_request`; the answer
/// is recorded (when a log path is given) and consumed without a response.
async fn wait_for_extension_response(
    reader: &mut BufReader<tokio::io::Stdin>,
    log: Option<&Path>,
) -> Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(()); // stdin closed
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if value.get("type").and_then(serde_json::Value::as_str)
                == Some("extension_ui_response")
            {
                if let Some(log) = log {
                    append_log(log, trimmed);
                }
                return Ok(());
            }
        }
    }
}

/// Append one line to a log file (command log / extension response log).
fn append_log(path: &Path, line: &str) {
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "{line}");
    }
}

/// Write one JSON/text line to the mock's stdout and flush it. Piped stdout is
/// block-buffered, so a flush per line is required or the client never sees it.
async fn mock_write_line(stdout: &mut tokio::io::Stdout, line: &[u8]) -> std::io::Result<()> {
    stdout.write_all(line).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}
