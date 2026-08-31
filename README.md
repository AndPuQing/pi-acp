# pi-acp (Rust)

ACP ([Agent Client Protocol](https://agentclientprotocol.com)) adapter for the
[pi coding agent](https://github.com/earendil-works/pi), written in **Rust**.

`pi-acp` speaks **ACP JSON-RPC 2.0 over stdio** to an ACP client (e.g. Zed) and
spawns `pi --mode rpc`, bridging requests/events between the two. The LLM loop,
tools, and session management live in **pi** itself — this adapter is only the
bridge + translation layer. It does **not** re-implement pi.

> This is a Rust rewrite of
> [svkozak/pi-acp](https://github.com/svkozak/pi-acp) (TypeScript). See the
> design doc (Multica issues **W-446** / **W-447**) for rationale, architecture,
> and roadmap.

## Status

🚧 **Scaffold.** The workspace, module skeleton, error/config foundation, and
cross-platform CI are in place. The ACP server wiring begins in the **S2 spike**
(issue W-449).

## Layout

```
crates/pi-acp/src/
├── main.rs            # entry: --terminal-login + (S2) Stdio ACP server
├── lib.rs
├── agent.rs           # ACP Agent role (S6)
├── config.rs          # env/CLI config
├── error.rs           # AcpxError (unified error type)
├── session/           # per-session state machine (S5)
├── pi/
│   ├── process.rs     # pi subprocess RPC client (S3)
│   ├── rpc.rs         # pi RPC serde types (S3)
│   └── sessions.rs    # session-file scanning (S7)
├── translate/         # pure pi <-> ACP translation (S4)
│   ├── messages.rs
│   ├── tools.rs
│   ├── bash.rs
│   └── prompt.rs
├── commands.rs        # slash commands (S6)
├── settings.rs        # settings.json merge (S6)
├── session_store.rs   # session-map.json persistence (S7)
├── auth.rs            # Terminal Auth (S6/S8)
└── startup.rs         # startup info + version check (S6)
```

## Build

Requires a Rust toolchain (see `rust-toolchain.toml`) and `pi` on your `PATH`
at runtime.

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

## Roadmap (Multica sub-issues)

| Stage | Issue | Scope |
|-------|-------|-------|
| 1 | W-448 | Cargo scaffold + module skeleton + CI (Win/Mac/Linux) |
| 1 | W-449 | ACP SDK × tokio runtime spike |
| 2 | W-450 | pi RPC client |
| 2 | W-451 | Translation layer + unit tests |
| 3 | W-452 | Session state machine |
| 3 | W-453 | ACP Agent methods + commands + settings + startup |
| 4 | W-454 | Session persistence + replay |
| 4 | W-455 | Reliability fixes |
| 5 | W-456 | Cross-platform polish + release |

## Decisions (reviewed 2026-08-31)

1. Dead `pi` child: **no auto-respawn** — detect and report a clear error.
2. Startup version check: **off by default** (`PI_ACP_VERSION_CHECK=true` to enable).
3. First release includes ACP `usage_update` notifications.
4. Target platforms: **Linux / macOS / Windows** (CI on all three).

## License

MIT