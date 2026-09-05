//! Runtime configuration sourced from environment variables and CLI flags.
//!
//! Recognized variables:
//! - `PI_ACP_PI_COMMAND` — override the `pi` executable name/path.
//! - `PI_ACP_ENABLE_EMBEDDED_CONTEXT` — advertise ACP `embeddedContext` support (`true` to enable).
//! - `PI_ACP_VERSION_CHECK` — enable the startup update notice (default: **off**, decision 2).
//! - `PI_ACP_RPC_TIMEOUT_SECS` — per-request pi RPC deadline (default `30`).
//! - `PI_ACP_SETTLE_TIMEOUT_SECS` — deadline for a turn's `agent_settled`
//!   after pi accepts the prompt (default `600`; `0` disables — design §11
//!   risk #84 mitigation).
//! - `PI_ACP_ENABLE_MCP` — advertise ACP MCP transports and wire
//!   `session/new|load` `mcp_servers` through pi-mcp-adapter's
//!   `runtime-register` event (default: **off**; W-483).
//! - `PI_ACP_PI_EXTRA_ARGS` — extra CLI flags appended to every pi spawn
//!   (upstream svkozak/pi-acp#38; W-496). Shell-like splitting on
//!   whitespace with single/double-quote grouping (see
//!   [`crate::commands::parse_command_args`]); unset/empty means no extra
//!   args (zero behavior change). Passed as separate argv entries through
//!   the existing [`crate::pi::resolve`] spawn path, so no manual
//!   shell quoting is ever built here — Windows batch-wrapper quoting stays
//!   exactly where it already lives.

/// Default per-request pi RPC deadline in seconds (design D2).
pub const DEFAULT_RPC_TIMEOUT_SECS: u64 = 30;

/// Default settle deadline for a turn's `agent_settled` in seconds (design
/// §11 risk #84 mitigation): a pi that accepts a prompt but never settles
/// (e.g. an extension slash command that never enters the agent loop) must
/// not hang `session/prompt` forever. Generous enough to never fire on
/// legitimate long turns; `PI_ACP_SETTLE_TIMEOUT_SECS=0` opts out.
pub const DEFAULT_SETTLE_TIMEOUT_SECS: u64 = 600;

/// Extra pi CLI flags from `PI_ACP_PI_EXTRA_ARGS` (W-496).
pub const PI_EXTRA_ARGS_ENV: &str = "PI_ACP_PI_EXTRA_ARGS";

/// Parse the raw `PI_ACP_PI_EXTRA_ARGS` value into argv entries: trim,
/// then shell-like split (whitespace separated, single/double-quote
/// grouping). Empty/whitespace-only yields `[]` (zero behavior change).
pub fn parse_pi_extra_args(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    crate::commands::parse_command_args(trimmed)
}

/// Resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// `pi` executable to spawn.
    pub pi_command: String,
    /// Extra CLI flags appended to every pi spawn (W-496; empty by default).
    pub pi_extra_args: Vec<String>,
    /// Whether to advertise ACP `promptCapabilities.embeddedContext`.
    pub enable_embedded_context: bool,
    /// Whether to run the startup version/update check (default off).
    pub enable_version_check: bool,
    /// Whether MCP wiring is switched on: `initialize` advertises the
    /// transports (only together with an installed pi-mcp-adapter) and
    /// `session/new|load` consume `mcp_servers` (W-483). Default off.
    pub enable_mcp: bool,
    /// Per-request pi RPC deadline in seconds.
    pub rpc_timeout_secs: u64,
    /// Deadline for a turn's `agent_settled` after pi accepts the prompt
    /// (seconds; `0` disables — design §11 risk #84 mitigation).
    pub settle_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pi_command: "pi".to_string(),
            pi_extra_args: Vec::new(),
            enable_embedded_context: false,
            enable_version_check: false,
            enable_mcp: false,
            rpc_timeout_secs: DEFAULT_RPC_TIMEOUT_SECS,
            settle_timeout_secs: DEFAULT_SETTLE_TIMEOUT_SECS,
        }
    }
}

impl Config {
    /// Build a [`Config`] from the current process environment.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Some(cmd) = std::env::var("PI_ACP_PI_COMMAND")
            .ok()
            .filter(|s| !s.trim().is_empty())
        {
            cfg.pi_command = cmd;
        }

        cfg.enable_embedded_context = std::env::var("PI_ACP_ENABLE_EMBEDDED_CONTEXT")
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        cfg.enable_version_check = std::env::var("PI_ACP_VERSION_CHECK")
            .map(|v| v.trim().eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        cfg.rpc_timeout_secs = std::env::var("PI_ACP_RPC_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_RPC_TIMEOUT_SECS);

        cfg.settle_timeout_secs = std::env::var("PI_ACP_SETTLE_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_SETTLE_TIMEOUT_SECS);

        cfg.pi_extra_args = std::env::var(PI_EXTRA_ARGS_ENV)
            .ok()
            .map(|v| parse_pi_extra_args(&v))
            .unwrap_or_default();

        cfg.enable_mcp = crate::mcp::mcp_enabled();

        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_safe() {
        let cfg = Config::default();
        assert_eq!(cfg.pi_command, "pi");
        assert!(!cfg.enable_embedded_context);
        // Decision 2: version check is off by default.
        assert!(!cfg.enable_version_check);
        // W-483: MCP wiring is off by default.
        assert!(!cfg.enable_mcp);
        assert!(cfg.pi_extra_args.is_empty());
        assert_eq!(cfg.rpc_timeout_secs, DEFAULT_RPC_TIMEOUT_SECS);
        // Settle fallback is on by default with a generous bound (0 = off).
        assert_eq!(cfg.settle_timeout_secs, DEFAULT_SETTLE_TIMEOUT_SECS);
    }

    #[test]
    fn pi_extra_args_split_plain_and_quoted() {
        assert!(parse_pi_extra_args("").is_empty());
        assert!(parse_pi_extra_args("   ").is_empty());
        assert_eq!(
            parse_pi_extra_args("--tools read,bash --model x"),
            vec!["--tools", "read,bash", "--model", "x"],
        );
        // Quoted segment with a space stays one argv entry (W-496).
        assert_eq!(
            parse_pi_extra_args("--label \"hello world\" --tools read,bash"),
            vec!["--label", "hello world", "--tools", "read,bash"],
        );
        assert_eq!(
            parse_pi_extra_args("--prompt 'say hi now'"),
            vec!["--prompt", "say hi now"],
        );
    }

    #[test]
    fn from_env_reads_pi_extra_args_and_defaults_empty() {
        let _guard = env_lock().lock().unwrap();
        let prev = std::env::var_os(PI_EXTRA_ARGS_ENV);
        // SAFETY: under `env_lock`.
        unsafe { std::env::remove_var(PI_EXTRA_ARGS_ENV) };
        assert!(Config::from_env().pi_extra_args.is_empty());
        // SAFETY: under `env_lock`.
        unsafe { std::env::set_var(PI_EXTRA_ARGS_ENV, "   ") };
        assert!(Config::from_env().pi_extra_args.is_empty());
        // SAFETY: under `env_lock`.
        unsafe { std::env::set_var(PI_EXTRA_ARGS_ENV, "--tools read,bash --label \"hi there\"") };
        assert_eq!(
            Config::from_env().pi_extra_args,
            vec!["--tools", "read,bash", "--label", "hi there"],
        );
        match prev {
            // SAFETY: the caller holds `env_lock`.
            Some(v) => unsafe { std::env::set_var(PI_EXTRA_ARGS_ENV, v) },
            None => unsafe { std::env::remove_var(PI_EXTRA_ARGS_ENV) },
        }
    }

    /// Serializes env-mutating tests: `std::env` is process-global and the
    /// harness runs tests on threads in the same process.
    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        &LOCK
    }
}
