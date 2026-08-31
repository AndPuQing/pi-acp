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
use pi_acp::agent;
use pi_acp::config::Config;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<()> {
    // Structured logging (env-filter driven, e.g. RUST_LOG=pi_acp=debug).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    if std::env::args().skip(1).any(|a| a == "--terminal-login") {
        return terminal_login();
    }

    // Hidden test fixture (see tests/pi_process.rs): a mock `pi --mode rpc`
    // server so the RPC client can be tested without a real pi + LLM backend.
    if std::env::args().skip(1).any(|a| a == "--mock-rpc") {
        return run_mock_rpc().await;
    }

    let cfg = Config::from_env();
    tracing::info!(pi_command = %cfg.pi_command, "pi-acp (Rust) starting");
    agent::run()
        .await
        .map_err(|e| anyhow::anyhow!("ACP error: {e:?}"))?;
    Ok(())
}

/// Launch `pi` with inherited stdio for interactive login/setup.
fn terminal_login() -> Result<()> {
    let cfg = Config::from_env();
    let pi_command = cfg.pi_command.clone();
    tracing::info!(pi_command, "launching pi for terminal login");

    // TODO(S3, W-450): spawn `pi` (inherited stdio) and propagate its exit code;
    // surface a clear "install pi" message on ENOENT.
    eprintln!(
        "pi-acp (Rust): --terminal-login not yet wired (scaffold). Would launch: {pi_command}"
    );
    Ok(())
}

/// Test-only mock `pi --mode rpc` server.
///
/// Hidden fixture behind `--mock-rpc` (spawned by `tests/pi_process.rs`): speaks
/// the same JSONL protocol as pi so [`pi_acp::pi::process::PiProcess`] can be
/// exercised without a real pi + LLM backend. All other args (`--mode rpc
/// --no-themes --session <path>`) are ignored.
///
/// Behavior flags (any combination):
/// - `--mock-prelude <n>`   emit `n` ANSI-styled human-readable lines before NDJSON
/// - `--mock-hang`          read commands but never respond (request-timeout tests)
/// - `--mock-exit-after <n>` exit(42) after reading `n` commands, without responding
/// - `--mock-delay-ms <n>`  delay each response by `n` ms (concurrency tests)
/// - `--mock-unknown-event` emit one unknown event type (protocol-evolution guard)
///
/// Default: respond `success: true` to every command (with a fixed `get_state`
/// payload), and after a `prompt` command emit a `text_delta` message_update
/// followed by `agent_settled` (mirroring pi's early-response + settled-event
/// semantics, S2 constraint 2).
async fn run_mock_rpc() -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let mut prelude = 0usize;
    let mut hang = false;
    let mut exit_after: Option<usize> = None;
    let mut delay_ms: u64 = 0;
    let mut unknown_event = false;

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--mock-rpc" => {}
            "--mock-prelude" => prelude = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--mock-hang" => hang = true,
            "--mock-exit-after" => exit_after = args.next().and_then(|v| v.parse().ok()),
            "--mock-delay-ms" => delay_ms = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--mock-unknown-event" => unknown_event = true,
            _ => {}
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

        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }

        let data = match ty {
            "get_state" => serde_json::json!({
                "model": {"id": "mock-model", "name": "Mock Model", "provider": "mock", "reasoning": false, "contextWindow": 1000, "maxTokens": 100},
                "thinkingLevel": "medium",
                "isStreaming": false,
                "isCompacting": false,
                "steeringMode": "one-at-a-time",
                "followUpMode": "one-at-a-time",
                "sessionFile": "/tmp/mock-session.jsonl",
                "sessionId": "mock-session-id",
                "sessionName": "Mock Session",
                "autoCompactionEnabled": false,
                "messageCount": 0,
                "pendingMessageCount": 0
            }),
            "get_available_models" => serde_json::json!({
                "models": [
                    {"id": "mock-model", "name": "Mock Model", "provider": "mock", "reasoning": false, "contextWindow": 1000, "maxTokens": 100}
                ]
            }),
            "export_html" => serde_json::json!({"path": "/tmp/mock.html"}),
            "set_model" => serde_json::json!({
                "id": "mock-model", "name": "Mock Model", "provider": "mock", "reasoning": false
            }),
            _ => serde_json::Value::Null,
        };

        let mut response = serde_json::json!({
            "id": id,
            "type": "response",
            "command": ty,
            "success": true,
        });
        if !data.is_null() {
            response["data"] = data;
        }
        mock_write_line(&mut stdout, response.to_string().as_bytes()).await?;
        handled += 1;

        // Mirror pi: the prompt response arrives early; the streaming events
        // and the real turn-completion signal (`agent_settled`) follow after.
        if ty == "prompt" {
            let update = serde_json::json!({
                "type": "message_update",
                "usage": {},
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": "hello from mock"
                }
            });
            mock_write_line(&mut stdout, update.to_string().as_bytes()).await?;
            mock_write_line(&mut stdout, b"{\"type\":\"agent_settled\"}").await?;
        }
    }

    Ok(())
}

/// Write one JSON/text line to the mock's stdout and flush it. Piped stdout is
/// block-buffered, so a flush per line is required or the client never sees it.
async fn mock_write_line(stdout: &mut tokio::io::Stdout, line: &[u8]) -> std::io::Result<()> {
    stdout.write_all(line).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}
