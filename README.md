# pi-acp

Use the [pi coding agent](https://github.com/earendil-works/pi) from [Zed](https://zed.dev/)
and other [Agent Client Protocol (ACP)](https://agentclientprotocol.com) clients.

`pi-acp` connects an ACP client to pi over a local process boundary. It starts
pi when a session is opened and forwards prompts, tool activity, session history,
model settings, and usage information. pi still owns the agent loop, providers,
credentials, and tools; `pi-acp` is the adapter between pi and the client.

## Requirements

- Node.js and npm, to install pi
- pi installed and available on your `PATH`
- Zed or another ACP-compatible client
- Linux (x86-64 or arm64), macOS (Intel or Apple Silicon), or Windows (x86-64)

## Install

### 1. Install pi

```bash
npm install --global @earendil-works/pi-coding-agent
pi --version
```

### 2. Install pi-acp

Download the latest release for your platform from [GitHub Releases](https://github.com/AndPuQing/pi-acp/releases).

| Platform | Release asset |
| --- | --- |
| Linux x86-64 | `pi-acp-linux-x64` |
| Linux arm64 | `pi-acp-linux-arm64` |
| macOS Intel | `pi-acp-macos-x64` |
| macOS Apple Silicon | `pi-acp-macos-arm64` |
| Windows x86-64 | `pi-acp-windows-x64.exe` |

On Linux and macOS, make the downloaded file executable and move it to a
directory on your `PATH`:

```bash
chmod +x pi-acp-<platform>
sudo mv pi-acp-<platform> /usr/local/bin/pi-acp
pi-acp --version
```

On Windows, keep the `.exe` suffix and add the directory containing
`pi-acp-windows-x64.exe` to `PATH`. You can also point Zed directly at the full
path to the executable. The release includes `CHECKSUMS.txt`; verify the
download's SHA-256 checksum when installing from a release.

## Configure Zed

Add `pi-acp` to Zed's `settings.json`:

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

Restart or open a new agent thread in Zed. If Zed cannot find a binary installed
on your shell's `PATH`, use an absolute path instead:

```json
{
  "agent_servers": {
    "pi": {
      "command": "/usr/local/bin/pi-acp",
      "args": [],
      "env": {}
    }
  }
}
```

Environment variables in this block are inherited by pi. This is where you can
set pi options such as `PI_PROVIDER` and `PI_MODEL` if your pi setup uses them.

## Sign in to pi

If pi needs an API key or provider login, run its interactive setup once:

```bash
pi-acp --terminal-login
```

Complete the setup in the terminal, then start a new agent session in Zed. ACP
clients that support terminal authentication can also launch this action from
their authentication prompt.

## Configuration

Most users can keep the defaults. Set these variables in Zed's `env` block or
in the environment that launches `pi-acp`:

| Variable | Default | Use |
| --- | --- | --- |
| `PI_ACP_PI_COMMAND` | `pi` | Path or command name for pi. On Windows, use the full path to the npm-generated `pi.cmd` when it is not on `PATH`. |
| `PI_ACP_VERSION_CHECK` | `false` | Set to `true` to show a startup notice when a newer pi version is available. |
| `PI_ACP_ENABLE_EMBEDDED_CONTEXT` | `false` | Set to `true` for ACP clients that send embedded context. |
| `PI_ACP_RPC_TIMEOUT_SECS` | `30` | Maximum time to wait for an individual pi request. |
| `PI_ACP_SETTLE_TIMEOUT_SECS` | `600` | Maximum time to wait for pi to finish a turn after accepting a prompt. Set to `0` to disable this fallback. |
| `RUST_LOG` | `warn` | Set to `pi_acp=debug` for diagnostic logs. Logs are written to stderr so ACP stdout remains available for protocol messages. |

## Troubleshooting

### `pi-acp` or `pi` cannot be found

Check both commands from a terminal:

```bash
pi --version
pi-acp --version
```

If they work in a terminal but not in Zed, use absolute paths in Zed's
`command` or set `PI_ACP_PI_COMMAND` to the full path of pi. On Windows, pi is
usually an npm `pi.cmd` wrapper rather than a native `pi.exe`.

### Authentication fails

Run `pi-acp --terminal-login` and complete the provider setup. If you use
environment variables for credentials, make sure they are present in Zed's
`env` block as well as in your shell.

### A session stops unexpectedly

`pi-acp` reports when its pi child process exits and does not restart it
automatically. Start a new agent session after fixing the underlying pi error.
Set `RUST_LOG=pi_acp=debug` temporarily if you need more detail.

## Build from source

Prebuilt releases are recommended. To build locally, install the Rust toolchain
specified by [`rust-toolchain.toml`](rust-toolchain.toml), then run:

```bash
git clone https://github.com/AndPuQing/pi-acp.git
cd pi-acp
cargo build --release
```

The binary is written to `target/release/pi-acp`.

## License

MIT
