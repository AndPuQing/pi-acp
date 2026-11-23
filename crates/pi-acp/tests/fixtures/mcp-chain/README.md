# Local bridge integration

The ordinary real-chain tests retain their published package pins. A local
bridge candidate can be tested independently from its immutable npm artifact.
No global package install or normal Pi profile modification is required.

Build pi-acp, then prepare a **new** output directory:

```sh
cargo build --locked -p pi-acp
python3 crates/pi-acp/tests/fixtures/mcp-chain/prepare-local-bridge.py \
  --bridge-package /absolute/path/to/pi-mcp-adapter-candidate.tgz \
  --pi /absolute/path/to/native/pi \
  --endpoint /absolute/path/to/pi-acp \
  --output /absolute/path/to/fresh-local-setup
```

Preparation verifies native Pi `1.5.2`, installs the candidate and pinned Pi
host packages into an isolated directory with npm lifecycle scripts disabled,
and records the artifact, source, dependency-lock and executable hashes in
`setup.json`. Separate empty npm configuration files avoid loading user npm
credentials. The setup code does not inspect native credential files.

`hostManagedModule` names the installed public `host-managed.ts` entry, whose
SDK dependencies resolve from that installation. Native Pi loads an explicit
TypeScript extension importing this entry through its supported extension
loader; Node integration fixtures use `tsx`. No compiled JS entry is assumed.

Run the local-candidate acceptance with a fresh evidence directory:

```sh
PI_ACP_TEST_BRIDGE_SETUP=/absolute/path/to/fresh-local-setup/setup.json \
PI_ACP_TEST_MCP_EVIDENCE_DIR=/absolute/path/to/fresh-wire-evidence \
cargo test --locked --test mcp_realchain -- --nocapture --test-threads=1
```

Local acceptance checks:

- Modern discovery, listing, tool invocation and the returned marker.
- Incompatible-version, HTTP 500, malformed-response, authentication-error
  and SSE-only endpoint failures. Every observed request must remain a
  modern POST with matching metadata; legacy `initialize`, GET/SSE attempts
  and transport sessions fail the assertion.
- Diagnostic recognition of a valid modern discovery response.
- Continued legacy and auto-mode tool calls for consumers choosing them.
- Actual native Pi `1.5.2` loading of the public host-managed TS module via
  an ACP v2 session, without a model prompt or provider call.
- Native host-managed discovery/list/call/marker and the same error paths,
  using its public per-server `protocolVersion` option and SDK dependency
  resolved from the module root, with no auth provider.

The local-only tests report a skip when no setup is supplied. Running the
default suite without it does not establish local-candidate acceptance. The
published `7.0.1` bridge is an expected negative control: its error probes
send legacy traffic and misclassify valid modern discovery. Its successful
modern connection does not establish zero legacy requests on error paths.

The ordinary bridge error matrix sets its existing public `oauth: false`
configuration on the fixture registrations. It does not claim that ACP server
descriptors contain an OAuth policy field or that pi-acp forwards one. OAuth
well-known discovery is supporting authorization traffic, distinct from MCP
initialization/SSE; its trace remains retained separately when observing the
OAuth-enabled bridge. The selected host-managed tests provide no auth provider
and independently require every request to remain a modern MCP POST.

The native host-managed wire test invokes the registered public tool from its
test extension. It proves native loading and wire behavior, not a real model
coding task or production execution-grant/lifecycle enforcement.

The generated `endpoint.json` is an unauthenticated, isolated ordinary-mode
launch descriptor for separate ACP testing. Authentication and any real
provider prompts require their own authorized test setup. These fixtures do
not establish bounded execution, current-state recovery, billing, or general
background coverage. Native Pi may initialize its own storage inside a
temporary test profile; tests do not inspect or copy its auth storage.
