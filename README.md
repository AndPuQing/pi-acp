# pi-acp-rs

Use the [pi coding agent](https://github.com/earendil-works/pi) from [Zed](https://zed.dev/)
and other [Agent Client Protocol (ACP)](https://agentclientprotocol.com) clients.

`pi-acp-rs` connects an ACP client to pi over a local process boundary. It starts
pi when a session is opened and forwards prompts, tool activity, session history,
model settings, and usage information. pi still owns the agent loop, providers,
credentials, and tools; `pi-acp-rs` is the adapter between pi and the client.

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

### 2. Install pi-acp-rs

The easiest option is the npm package, which selects the native binary for the
current platform:

```bash
npm install --global pi-acp-rs
pi-acp-rs --version
```

You can also download the latest standalone release for your platform from
[GitHub Releases](https://github.com/AndPuQing/pi-acp/releases).

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
sudo mv pi-acp-<platform> /usr/local/bin/pi-acp-rs
pi-acp-rs --version
```

On Windows, keep the `.exe` suffix and add the directory containing
`pi-acp-windows-x64.exe` to `PATH`. You can also point Zed directly at the full
path to the executable. The release includes `CHECKSUMS.txt`; verify the
download's SHA-256 checksum when installing from a release.

## Configure Zed

Add `pi-acp-rs` to Zed's `settings.json`:

```json
{
  "agent_servers": {
    "pi": {
      "command": "pi-acp-rs",
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
      "command": "/usr/local/bin/pi-acp-rs",
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
pi-acp-rs --terminal-login
```

Complete the setup in the terminal, then start a new agent session in Zed. ACP
clients that support terminal authentication can also launch this action from
their authentication prompt.

## Configuration

Most users can keep the defaults. Set these variables in Zed's `env` block or
in the environment that launches `pi-acp-rs`:

| Variable | Default | Use |
| --- | --- | --- |
| `PI_ACP_PI_COMMAND` | `pi` | Path or command name for pi. On Windows, use the full path to the npm-generated `pi.cmd` when it is not on `PATH`. |
| `PI_ACP_VERSION_CHECK` | `false` | Set to `true` to show a startup notice when a newer pi version is available. |
| `PI_ACP_ENABLE_EMBEDDED_CONTEXT` | `false` | Set to `true` for ACP clients that send embedded context. |
| `PI_ACP_ENABLE_MCP` | `false` | Set to `true` to accept and wire non-empty ACP `mcpServers` through `pi-mcp-adapter`. |
| `PI_ACP_SESSION_MAP` | `<agent dir>/pi-acp/session-map.json` | Full file path for `session-map.json`. Missing parent directories are created on first write. Unset or empty keeps the default. |
| `PI_ACP_RPC_TIMEOUT_SECS` | `30` | Maximum time to wait for an individual pi request. |
| `PI_ACP_SETTLE_TIMEOUT_SECS` | `600` | Maximum time to wait for pi to finish a turn after accepting a prompt. Set to `0` to disable this fallback. |
| `RUST_LOG` | `warn` | Set to `pi_acp=debug` for diagnostic logs. Logs are written to stderr so ACP stdout remains available for protocol messages. |

## MCP support

MCP support is opt-in. `pi-acp-rs` does not discover Zed's MCP settings and does
not read or write `.pi/mcp.json`. The supported path is:

```text
Zed -- ACP session/new|load mcpServers --> pi-acp-rs
    -- runtime-register --> pi-mcp-adapter inside pi
    -- stdio/http/sse --> MCP servers
```

To enable this path:

1. Install the adapter as a global pi package in the same environment and pi
   agent directory used by Zed:

   ```bash
   pi install npm:pi-mcp-adapter
   ```

2. Set the flag in the Zed `agent_servers` entry. Putting it only in a shell
   that was not used to launch Zed may have no effect:

   ```json
   {
     "agent_servers": {
       "pi": {
         "command": "pi-acp-rs",
         "args": [],
         "env": {
           "PI_ACP_ENABLE_MCP": "true"
         }
       }
     }
   }
   ```

3. Configure servers in Zed's `context_servers` settings. A custom stdio
   server uses `source: "stdio"`; an HTTP server uses `source: "http"`:

   ```json
   {
     "context_servers": {
       "my-local-server": {
         "source": "stdio",
         "command": "/path/to/my-mcp-server",
         "args": [],
         "env": {}
       },
       "my-http-server": {
         "source": "http",
         "url": "https://example.example/mcp"
       }
     }
   }
   ```

   Create a new ACP session after changing either configuration. The server
   command, arguments, and environment must be usable by the process that runs
   pi. MCP registrations are session-scoped and are not persisted to disk.

### Remote development

For a remote Zed project, Zed only sends stdio servers that have
`"remote": true`. This applies to both custom and extension-provided context
servers. Set that flag only when the command is installed and can run in the
remote agent environment:

```json
{
  "source": "stdio",
  "remote": true,
  "command": "/usr/local/bin/my-mcp-server",
  "args": []
}
```

If the server must keep running on the local machine while the agent is remote,
use an HTTP endpoint reachable by the remote agent. A local stdio process needs
a Zed-side relay or MCP-over-ACP support; `pi-acp-rs` cannot recover it from an
empty ACP `mcpServers` list.

### Diagnose an empty server list

Inspect the raw ACP `session/new` or `session/load` request:

- `"mcpServers": []` means Zed did not pass any server. Changing
  `PI_ACP_ENABLE_MCP` cannot add one; check Zed `context_servers`, enabled
  state, project scope, and the `remote` flag.
- A non-empty list with `MCP wiring is disabled` means
  `PI_ACP_ENABLE_MCP=true` was not inherited by pi-acp.
- A non-empty list with an adapter error means `pi-mcp-adapter` is not
  installed in the pi agent directory used by that session.

The default is `false` deliberately: accepting an ACP MCP menu can start
external commands and connect to network services. Explicit opt-in preserves
existing pi behavior and keeps the advertised ACP capabilities honest when the
optional adapter is unavailable.

## Troubleshooting

### `pi-acp-rs` or `pi` cannot be found

Check both commands from a terminal:

```bash
pi --version
pi-acp-rs --version
```

If they work in a terminal but not in Zed, use absolute paths in Zed's
`command` or set `PI_ACP_PI_COMMAND` to the full path of pi. On Windows, pi is
usually an npm `pi.cmd` wrapper rather than a native `pi.exe`.

### Authentication fails

Run `pi-acp-rs --terminal-login` and complete the provider setup. If you use
environment variables for credentials, make sure they are present in Zed's
`env` block as well as in your shell.

### A session stops unexpectedly

`pi-acp-rs` reports when its pi child process exits and does not restart it
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
