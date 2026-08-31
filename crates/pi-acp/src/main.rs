//! Entry point for the `pi-acp` binary.
//!
//! Two modes:
//! - `--terminal-login`: launch `pi` interactively (inherited stdio) so the user
//!   can configure API keys / OAuth login. Mirrors the TS pi-acp behavior and is
//!   what ACP "Terminal Auth" invokes.
//! - default: run the ACP agent over stdio. The ACP wiring lands in the S2 spike
//!   (issue W-449); this scaffold prints a notice until then.

use anyhow::Result;
use pi_acp::config::Config;

fn main() -> Result<()> {
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

    let _cfg = Config::from_env();

    tracing::info!("pi-acp (Rust) scaffold starting");
    // S2 spike (W-449) brings up:
    //   agent_client_protocol::Agent::builder()
    //       .on_receive_request(..., on_receive_request!())
    //       .connect_to(agent_client_protocol::Stdio::new())
    eprintln!("pi-acp (Rust) scaffold — ACP server not yet wired (pending W-449 / S2 spike).");
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
