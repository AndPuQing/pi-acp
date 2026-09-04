// pi-acp MCP registrar extension (W-483).
//
// Runs INSIDE the pi subprocess (loaded via `pi --extension <this file>`).
// It bridges the per-session MCP wiring that pi-acp prepared outside:
//
// - pi-acp validates the ACP `session/new|load` `mcp_servers` in Rust,
//   serializes them as `{ name, definition }` pairs into the
//   `PI_ACP_MCP_SERVERS_JSON` env var, and spawns pi with this extension.
// - This extension emits one `pi-mcp-adapter:runtime-register:v1` event per
//   server against the pi extension bus. When pi-mcp-adapter is installed in
//   the same pi instance it answers synchronously on `request.result` with a
//   `{ registration }` carrying `dispose()`; otherwise `result` stays
//   `undefined` (adapter missing) and the attempt is retried at
//   `session_start`.
// - Outcomes are reported back to pi-acp as single stderr lines
//   (`PI_ACP_MCP:registered:<name>` / `PI_ACP_MCP:failed:<name>:<reason>`).
//   pi routes everything its extensions print to the child's stderr (even raw
//   `process.stdout.write`), so pi-acp pipes stderr and scrapes these
//   markers there. pi speaks JSONL on stdout; markers never touch it and are
//   never parsed as protocol.
// - On `session_shutdown` every live registration is disposed (bounded,
//   best-effort). Registrations are also process-local and non-persisted, so
//   killing pi always cleans up; pi-acp additionally tears the pi process
//   down on session close / shutdown.
//
// Per-server failure isolation: one server throwing (bad definition,
// duplicate name, adapter-internal error) is recorded as its own `failed`
// marker and never prevents the remaining servers from being attempted.
//
// This file is dependency-free (no import of pi-mcp-adapter): the event name
// + `{ version, name, definition }` shape is the whole contract, mirroring
// the adapter's cross-extension fallback path (`registerMcpServer` without a
// shared module).

const REGISTER_EVENT = "pi-mcp-adapter:runtime-register:v1";
const REGISTER_VERSION = 1;
const SERVERS_ENV_VAR = "PI_ACP_MCP_SERVERS_JSON";
const MARKER_PREFIX = "PI_ACP_MCP:";

/** Keep markers to one line so pi-acp can parse them out of stdout. */
function oneLine(value, max = 500) {
  return String(value ?? "")
    .replace(/[\r\n]+/g, " ")
    .trim()
    .slice(0, max);
}

function readSpecs() {
  const raw = (typeof process !== "undefined" && process.env?.[SERVERS_ENV_VAR]) || "";
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      console.log(`${MARKER_PREFIX}failed:_payload:${SERVERS_ENV_VAR} must be a JSON array`);
      return [];
    }
    return parsed;
  } catch (err) {
    console.log(
      `${MARKER_PREFIX}failed:_payload:${SERVERS_ENV_VAR} is not valid JSON: ${oneLine(err?.message ?? err)}`,
    );
    return [];
  }
}

export default function piAcpMcpRegistrar(pi) {
  const specs = readSpecs();
  if (specs.length === 0) return;

  /** Server names with an already-reported outcome (markers emit once). */
  const reported = new Set();
  /** Live registrations, disposed on `session_shutdown`. */
  const live = [];

  function report(name, ok, reason) {
    if (reported.has(name)) return;
    reported.add(name);
    if (ok) {
      console.log(`${MARKER_PREFIX}registered:${name}`);
    } else {
      console.log(`${MARKER_PREFIX}failed:${name}:${oneLine(reason) || "registration failed"}`);
    }
  }

  /** Names whose load-time emit found no adapter handler yet. */
  const deferred = new Set();

  function registerOne(spec, isRetry) {
    const name = typeof spec?.name === "string" ? spec.name : "";
    const definition = spec?.definition;
    if (name === "" || reported.has(name)) {
      if (name === "" && !reported.has("_payload")) {
        report("_payload", false, "server entry without a name");
      }
      return;
    }
    const request = { version: REGISTER_VERSION, name, definition };
    try {
      pi.events.emit(REGISTER_EVENT, request);
    } catch (err) {
      report(name, false, err?.message ?? String(err));
      return;
    }
    if (!request.result) {
      if (isRetry) {
        // Still no handler after every extension loaded: the adapter is
        // genuinely absent. Fail explicitly so pi-acp never waits out a
        // timeout for a registration that cannot happen.
        report(name, false, "pi-mcp-adapter is not installed for this Pi instance");
      } else {
        // Adapter loads as a separate extension and may come after this
        // one. Retry at `session_start`, which fires after every extension
        // is loaded.
        deferred.add(name);
      }
      return;
    }
    deferred.delete(name);
    if (request.result.ok) {
      live.push(request.result.registration);
      report(name, true);
    } else {
      report(name, false, request.result.error?.message ?? "registration failed");
    }
  }

  function registerPending(isRetry) {
    for (const spec of specs) registerOne(spec, isRetry);
  }

  // Attempt at load (covers adapter-first extension order) plus one retry
  // for anything still pending once the session starts (registrar-first
  // order, or a definitive adapter-missing failure).
  registerPending(false);
  pi.on("session_start", () => {
    registerPending(true);
  });
  pi.on("session_shutdown", async () => {
    for (const registration of live.splice(0)) {
      try {
        await registration.dispose();
      } catch {
        // Best-effort teardown: the process exit itself discards any
        // registration left behind (non-persisted, process-local).
      }
    }
  });
}
