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

✅ **Ready for use.** All stages (S1–S9) are merged: the ACP SDK × tokio spike
(S2), pi RPC client (S3), pure translate layer (S4), session state machine
(S5), the full ACP agent method set with slash commands / settings / startup /
`usage_update` (S6), session persistence + replay (S7), reliability hardening
(S8), and cross-platform polish + release (S9). A single static `pi-acp`
binary is produced per platform via the GitHub Release pipeline.

`pi-acp` is a **drop-in replacement** for the TypeScript `pi-acp` in an existing
Zed `agent_servers` config — same ACP method surface, same stdio transport.

## Install

`pi-acp` still needs the **pi coding agent** on your `PATH` at runtime (it
spawns `pi --mode rpc`). Install pi first:

```bash
npm i -g @earendil-works/pi-coding-agent
```

Then get the `pi-acp` binary:

**Option A — prebuilt binary (recommended).** Download the latest release for
your platform from [GitHub Releases](https://github.com/AndPuQing/pi-acp/releases)
and put it on your `PATH`:

| Platform        | Asset                    |
|-----------------|--------------------------|
| Linux x86-64    | `pi-acp-linux-x64`       |
| Linux arm64     | `pi-acp-linux-arm64`     |
| macOS x86-64    | `pi-acp-macos-x64`       |
| macOS Apple Silicon | `pi-acp-macos-arm64`     |
| Windows x86-64  | `pi-acp-windows-x64.exe` |

Linux binaries are **musl / statically linked** (no glibc requirement); verify a
checksum against `CHECKSUMS.txt`.

```bash
# Linux/macOS
chmod +x pi-acp-linux-x64 && sudo mv pi-acp-linux-x64 /usr/local/bin/pi-acp
# Windows: put pi-acp-windows-x64.exe on your PATH
```

**Option B — build from source** (requires a Rust toolchain; see
`rust-toolchain.toml`):

```bash
git clone git@github.com:AndPuQing/pi-acp.git && cd pi-acp
cargo build --release
# binary at target/release/pi-acp
```

## Configure (Zed, drop-in)

In Zed's `settings.json`, point the `pi` agent server at the binary. This
replaces the TypeScript `pi-acp` command one-for-one:

```json
{
  "agent_servers": {
    "pi": {
      "command": "pi-acp",
      "args": [],
      "env": {}
    }
  }
}
```

Use an absolute `command` (e.g. `"/usr/local/bin/pi-acp"`) if `pi-acp` is not on
the `PATH` that Zed inherits. Any `env` you set here is passed to `pi-acp` (and
inherited by the `pi` child it spawns), so `PI_PROVIDER` / `PI_MODEL` etc. work
here too.

## Environment variables

| Variable                     | Default | Meaning |
|------------------------------|---------|---------|
| `PI_ACP_PI_COMMAND`          | `pi`    | Executable to spawn for `pi --mode rpc`. Set this if pi is not named `pi` / not on `PATH`. On Windows this is typically the npm global `pi.cmd`. |
| `PI_ACP_VERSION_CHECK`       | `false` | Enable the startup "update available" notice (decision 2: **off by default** to keep startup fast). |
| `PI_ACP_ENABLE_EMBEDDED_CONTEXT` | `false` | Advertise ACP `promptCapabilities.embeddedContext`. |
| `PI_ACP_RPC_TIMEOUT_SECS`    | `30`    | Per-request `pi` RPC deadline in seconds (fixes the "first prompt hangs forever" class of bugs). |
| `PI_ACP_SETTLE_TIMEOUT_SECS` | `600`   | Deadline for a turn's `agent_settled` after `pi` accepts the prompt (design §11 risk #84 mitigation: a `pi` that accepts but never settles must not hang `session/prompt` forever). `0` disables the fallback. |
| `RUST_LOG`                   | `warn`  | Structured log level (e.g. `RUST_LOG=pi_acp=debug`). Logs go to **stderr**, never stdout (stdout is the ACP protocol channel). |

## Platform notes

- **Windows** — `pi` is an npm global, i.e. a `pi.cmd` batch wrapper, not a
  native `pi.exe`. `pi-acp` resolves a bare `pi` against `PATH`/`PATHEXT` and
  launches a `.cmd`/`.bat` wrapper via `cmd.exe /d /s /c`, so it works with the
  default npm install out of the box (fixes upstream pi-acp #27). If your npm
  dir is not on `PATH`, set `PI_ACP_PI_COMMAND` to the full path of `pi.cmd`.
- **Missing pi** — if the configured pi command cannot be found, `pi-acp` fails
  fast with an actionable install hint (npm package + `PI_ACP_PI_COMMAND` + the
  Windows `pi.cmd` note) instead of hanging.
- **Dead pi child** — `pi-acp` detects a dead pi subprocess and surfaces a clear
  error; it does **not** auto-respawn (decision 1). Start a new session
  (`session/new`) to recover.

## Layout

```
crates/pi-acp/src/
├── main.rs            # entry: --terminal-login / --version / ACP stdio server + mock fixture
├── lib.rs
├── agent.rs           # ACP Agent role (S6)
├── config.rs          # env/CLI config
├── error.rs           # AcpxError (unified error type + ACP RequestError mapping)
├── session/           # per-session state machine (S5)
├── pi/
│   ├── process.rs     # pi subprocess RPC client (S3)
│   ├── rpc.rs         # pi RPC serde types (S3)
│   ├── sessions.rs    # session-file scanning (S7)
│   └── resolve.rs     # pi command resolution / Windows pi.cmd handling (S9)
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

## Build & test

```bash
cargo build
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

CI runs `fmt` + `clippy -D warnings` + `test` on **Windows / macOS / Linux**,
plus a `cargo check` smoke across the full release matrix
(Linux x64/arm64 musl, macOS x64/arm64, Windows x64).

## Release

Tag a `vX.Y.Z` (or run the **Release** workflow manually) to build the
three-platform static binary matrix and publish it as a GitHub Release with
`CHECKSUMS.txt`. See `.github/workflows/release.yml`.

## Decisions (reviewed 2026-08-31)

1. Dead `pi` child: **no auto-respawn** — detect and report a clear error.
2. Startup version check: **off by default** (`PI_ACP_VERSION_CHECK=true` to enable).
3. First release includes ACP `usage_update` notifications.
4. Target platforms: **Linux / macOS / Windows** (CI on all three).

## Roadmap (Multica sub-issues)

| Stage | Issue | Scope                                                        | Status |
|-------|-------|--------------------------------------------------------------|--------|
| 1     | W-448 | Cargo scaffold + module skeleton + CI (Win/Mac/Linux)        | ✅ |
| 1     | W-449 | ACP SDK × tokio runtime spike                                 | ✅ |
| 2     | W-450 | pi RPC client                                                 | ✅ |
| 2     | W-451 | Translation layer + unit tests                                | ✅ |
| 3     | W-452 | Session state machine                                         | ✅ |
| 3     | W-453 | ACP Agent methods + commands + settings + startup             | ✅ |
| 4     | W-454 | Session persistence + replay                                  | ✅ |
| 4     | W-455 | Reliability fixes                                             | ✅ |
| 5     | W-456 | Cross-platform polish + release                               | ✅ |

## License

MIT