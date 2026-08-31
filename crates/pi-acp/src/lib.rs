//! # pi-acp (Rust)
//!
//! ACP (Agent Client Protocol) adapter for the [pi coding agent](https://github.com/earendil-works/pi).
//!
//! This crate is the **bridge** between an ACP client (e.g. Zed) and pi's
//! `--mode rpc` subprocess. It speaks ACP JSON-RPC 2.0 over stdio to the client
//! and pi's JSONL RPC protocol to the `pi` child process, translating between
//! the two. The LLM loop, tools, and session management live in pi itself —
//! this adapter does not re-implement pi.
//!
//! ## Module map
//! - [`agent`] — ACP `Agent` role implementation (initialize / new / prompt / ...).
//! - [`session`] — per-session state machine (turn queue, event pump, tool tracking).
//! - [`pi`] — pi subprocess RPC client + session-file scanning.
//! - [`translate`] — pure pi ⇄ ACP translation functions.
//! - [`commands`] — slash commands (file-based + built-in + skills).
//! - [`settings`] — global + project `settings.json` merge.
//! - [`session_store`] — `session-map.json` persistence (atomic).
//! - [`auth`] — Terminal Auth + error → ACP `AuthRequired` detection.
//! - [`startup`] — startup info assembly + (disabled-by-default) version check.
//!
//! See the workspace README and design doc (issue W-446 / W-447) for the full plan.

pub mod agent;
pub mod auth;
pub mod commands;
pub mod config;
pub mod error;
pub mod pi;
pub mod session;
pub mod session_store;
pub mod settings;
pub mod startup;
pub mod translate;

pub use error::AcpxError;
