//! Runtime configuration sourced from environment variables and CLI flags.
//!
//! Recognized variables:
//! - `PI_ACP_PI_COMMAND` — override the `pi` executable name/path.
//! - `PI_ACP_ENABLE_EMBEDDED_CONTEXT` — advertise ACP `embeddedContext` support (`true` to enable).
//! - `PI_ACP_VERSION_CHECK` — enable the startup update notice (default: **off**, decision 2).
//! - `PI_ACP_RPC_TIMEOUT_SECS` — per-request pi RPC deadline (default `30`).

/// Default per-request pi RPC deadline in seconds (design D2).
pub const DEFAULT_RPC_TIMEOUT_SECS: u64 = 30;

/// Resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// `pi` executable to spawn.
    pub pi_command: String,
    /// Whether to advertise ACP `promptCapabilities.embeddedContext`.
    pub enable_embedded_context: bool,
    /// Whether to run the startup version/update check (default off).
    pub enable_version_check: bool,
    /// Per-request pi RPC deadline in seconds.
    pub rpc_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pi_command: "pi".to_string(),
            enable_embedded_context: false,
            enable_version_check: false,
            rpc_timeout_secs: DEFAULT_RPC_TIMEOUT_SECS,
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
        assert_eq!(cfg.rpc_timeout_secs, DEFAULT_RPC_TIMEOUT_SECS);
    }
}
