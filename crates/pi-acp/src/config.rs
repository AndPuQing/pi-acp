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

/// Default per-request pi RPC deadline in seconds (design D2).
pub const DEFAULT_RPC_TIMEOUT_SECS: u64 = 30;

/// Default settle deadline for a turn's `agent_settled` in seconds (design
/// §11 risk #84 mitigation): a pi that accepts a prompt but never settles
/// (e.g. an extension slash command that never enters the agent loop) must
/// not hang `session/prompt` forever. Generous enough to never fire on
/// legitimate long turns; `PI_ACP_SETTLE_TIMEOUT_SECS=0` opts out.
pub const DEFAULT_SETTLE_TIMEOUT_SECS: u64 = 600;

/// Resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// `pi` executable to spawn.
    pub pi_command: String,
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
        assert_eq!(cfg.rpc_timeout_secs, DEFAULT_RPC_TIMEOUT_SECS);
        // Settle fallback is on by default with a generous bound (0 = off).
        assert_eq!(cfg.settle_timeout_secs, DEFAULT_SETTLE_TIMEOUT_SECS);
    }
}
