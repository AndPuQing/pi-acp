//! Terminal Auth + error -> ACP `AuthRequired` detection.
//!
//! Ports `acp/auth.ts` + `acp/auth-required.ts`:
//! - [`get_auth_methods`] advertises `pi_terminal_login` (terminal type) with
//!   the `--terminal-login` relaunch spec, plus Zed's `_meta["terminal-auth"]`
//!   banner payload when the client asks for it.
//! - [`maybe_auth_required_error`] matches pi/provider error text against
//!   credential keywords and promotes it to an ACP `AuthRequired`.

use std::collections::HashMap;

use agent_client_protocol::schema::v1::{AuthMethod, AuthMethodTerminal, Meta};
use serde_json::Map;

/// The terminal auth method id (Zed + registry).
pub const PI_SETUP_METHOD_ID: &str = "pi_terminal_login";

/// Build the `authMethods` array for the initialize response.
///
/// Mirrors TS `getAuthMethods`: always includes the registry shape
/// (`type: "terminal"`, `args: ["--terminal-login"]`, `env: {}`); when the
/// client advertises Zed's `_meta["terminal-auth"]` capability, also attach
/// the launch spec under `_meta["terminal-auth"]` so Zed renders the
/// "Authenticate" banner + button.
pub fn get_auth_methods(supports_terminal_auth_meta: bool) -> Vec<AuthMethod> {
    let mut meta: Meta = Map::new();
    if supports_terminal_auth_meta {
        let (command, args) = terminal_auth_launch_spec();
        let mut spec = Map::new();
        spec.insert("command".to_string(), serde_json::Value::String(command));
        spec.insert(
            "args".to_string(),
            serde_json::Value::Array(args.into_iter().map(serde_json::Value::String).collect()),
        );
        spec.insert(
            "label".to_string(),
            serde_json::Value::String("Launch pi".to_string()),
        );
        meta.insert("terminal-auth".to_string(), serde_json::Value::Object(spec));
    }

    let terminal = AuthMethodTerminal::new(PI_SETUP_METHOD_ID, "Launch pi in the terminal")
        .description("Start pi in an interactive terminal to configure API keys or login")
        .args(vec!["--terminal-login".to_string()])
        .env(HashMap::new());
    let terminal = if meta.is_empty() {
        terminal
    } else {
        terminal.meta(meta)
    };

    vec![AuthMethod::Terminal(terminal)]
}

/// The command Zed should launch for terminal auth: reuse `node <dist>.js`
/// when that's how we were launched (most reliable in dev), else `pi-acp`.
fn terminal_auth_launch_spec() -> (String, Vec<String>) {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 2 {
        let argv0 = &args[0];
        let argv1 = &args[1];
        let is_node = argv0.contains("node");
        let is_js = argv1.ends_with(".js");
        if is_node && is_js {
            return (
                argv0.clone(),
                vec![argv1.clone(), "--terminal-login".to_string()],
            );
        }
    }
    ("pi-acp".to_string(), vec!["--terminal-login".to_string()])
}

/// Detect missing-credentials / auth-failure error text from pi or a provider.
///
/// Mirrors TS `maybeAuthRequiredError`: keyword match against common patterns;
/// returns `Some(message)` when the text looks like an auth problem. The caller
/// surfaces it as an ACP `authRequired` error (with the auth methods attached).
pub fn maybe_auth_required_error(message: &str) -> Option<String> {
    let s = message.to_lowercase();
    const PATTERNS: &[&str] = &[
        "api key",
        "apikey",
        "missing key",
        "no key",
        "not configured",
        "unauthorized",
        "authentication",
        "permission denied",
        "forbidden",
        "401",
        "403",
    ];
    if PATTERNS.iter().any(|p| s.contains(p)) {
        Some(message.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_methods_carry_terminal_shape() {
        let methods = get_auth_methods(false);
        assert_eq!(methods.len(), 1);
        match &methods[0] {
            AuthMethod::Terminal(t) => {
                assert_eq!(t.id.0.as_ref(), PI_SETUP_METHOD_ID);
                assert_eq!(t.args, vec!["--terminal-login".to_string()]);
                assert!(t.env.is_empty());
                assert!(t.meta.is_none());
            }
            other => panic!("expected terminal auth method, got {other:?}"),
        }
    }

    #[test]
    fn auth_methods_include_terminal_auth_meta_when_supported() {
        let methods = get_auth_methods(true);
        match &methods[0] {
            AuthMethod::Terminal(t) => {
                let meta = t.meta.as_ref().expect("terminal-auth meta");
                let spec = meta.get("terminal-auth").expect("terminal-auth key");
                let command = spec
                    .get("command")
                    .and_then(serde_json::Value::as_str)
                    .unwrap();
                assert!(!command.is_empty());
                let args = spec
                    .get("args")
                    .and_then(serde_json::Value::as_array)
                    .unwrap();
                assert_eq!(args.len(), 1);
                assert_eq!(
                    args[0].as_str().unwrap(),
                    "--terminal-login",
                    "the launch spec must end with --terminal-login"
                );
            }
            other => panic!("expected terminal auth method, got {other:?}"),
        }
    }

    #[test]
    fn auth_error_detection_matches_common_patterns() {
        assert!(maybe_auth_required_error("Missing API key for anthropic").is_some());
        assert!(maybe_auth_required_error("unauthorized: 401").is_some());
        assert!(maybe_auth_required_error("no key configured for provider").is_some());
        assert!(maybe_auth_required_error("403 Forbidden").is_some());
        assert!(maybe_auth_required_error("something else entirely").is_none());
        assert!(maybe_auth_required_error("").is_none());
    }
}
