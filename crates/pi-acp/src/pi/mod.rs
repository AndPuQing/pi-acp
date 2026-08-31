//! Integration with the `pi` subprocess and its session files.
//!
//! - [`process`] — spawn + JSONL RPC client (request/response with timeout, event
//!   stream, child-exit watch, `Drop` cleanup, prelude capture). Built in S3 (W-450).
//! - [`rpc`] — pi RPC command / event / model serde types. Built in S3 (W-450).
//! - [`sessions`] — scan `~/.pi/agent/sessions/**/*.jsonl` for list/load. Built in S7 (W-454).

pub mod process;
pub mod rpc;
pub mod sessions;
