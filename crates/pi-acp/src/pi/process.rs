//! pi subprocess RPC client.
//!
//! Ports `pi-rpc/process.ts`. S3 (W-450) implements:
//! - spawn `pi --mode rpc --no-themes [--session <path>]`
//! - `request(cmd)` with a `tokio::time::timeout` wrapper (fixes #94)
//! - stdout line reader: `response` resolves the matching pending id, else events
//! - `Child::wait()` watcher: on exit, reject all pending + mark session dead (fixes #82)
//! - `Drop` kills the child (SIGTERM -> SIGKILL)
//! - prelude capture: human-readable lines before NDJSON starts (ANSI-stripped)
