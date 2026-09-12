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
//! ## Library use (in-process driving)
//!
//! The crate is also usable as a library: [`agent::AcpAgent`] owns the whole
//! ACP translation stack and can be driven **in-process** over any transport,
//! not just as a stdio child. Call [`agent::AcpAgent::run_with`] with any
//! [`ConnectTo<Agent>`](agent_client_protocol::ConnectTo) component — e.g. one
//! end of a [`Channel::duplex`](agent_client_protocol::Channel::duplex) pair;
//! [`agent::AcpAgent::run`] is exactly `run_with(Stdio::new())`, so the binary
//! and any external client keep the same behavior.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use agent_client_protocol::{Channel, ConnectTo};
//! use pi_acp::agent::AcpAgent;
//! use pi_acp::config::Config;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let agent = Arc::new(AcpAgent::new(Config::default()));
//! // The agent end of an in-process duplex pair; drive the other end with an
//! // ACP client (e.g. `Client.builder()...connect_to(other_end)`).
//! let (agent_end, _peer) = Channel::duplex();
//! agent.run_with(agent_end).await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the workspace README and design doc (issue W-446 / W-447) for the full plan.

pub mod agent;
pub mod auth;
pub mod commands;
pub mod config;
pub mod error;
pub mod mcp;
pub mod pi;
pub mod session;
pub mod session_store;
pub mod settings;
pub mod startup;
pub mod time;
pub mod translate;

pub use error::AcpxError;
