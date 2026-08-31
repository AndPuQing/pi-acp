//! Per-session state machine.
//!
//! S5 (W-452) implements `SessionManager` + `PiAcpSession`:
//! - TurnQueue (client-side `one-at-a-time` queueing)
//! - event pump: consume pi events -> state machine -> ordered outbound
//! - monotonic tool-call status (pending -> in_progress -> completed)
//! - edit/write file snapshots -> ACP structured diff
//! - extension UI bridge (select/confirm -> request_permission)
//! - turn completion keyed on `agent_settled`
