//! Terminal Auth + error -> ACP `AuthRequired` detection.
//!
//! Ports `acp/auth.ts` + `acp/auth-required.ts`. Advertises `pi_terminal_login`
//! (type: terminal) and maps pi auth errors to ACP `AuthRequired`.
