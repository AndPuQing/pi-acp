// Stub-pi harness (W-484): drives the REAL pi-mcp-adapter + the REAL
// mcp_registrar.js against fake MCP servers, with no real pi and no model.
//
// The stub implements just enough of pi's ExtensionAPI for the adapter to
// install and initialize (events, tool registry, flags/commands). Tool calls
// go through the captured `mcp` proxy tool's `execute` — the exact function
// pi itself would invoke for a model-issued call — so list/call roundtrips
// exercise the genuine adapter code path over real MCP wire protocol.
//
// Runtime needs (provisioned by the Rust test, never checked in):
// - `--adapter-dir`: directory holding the adapter's *.ts sources (copied
//   out of node_modules — node refuses type-stripping *inside* node_modules)
//   with a resolvable node_modules above it (pi-mcp-adapter + tsx + typebox
//   + pi host packages). Run as `node --import tsx/esm chain-harness.mjs`.
// - `--registrar`: path to the repo's mcp_registrar.js (copied aside before
//   import so the TS loader never treats it as CJS).
//
// Emits CHAIN:<json> lines on stdout for the test driver to assert.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const args = process.argv.slice(2);
const opt = (name, def) => {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};
const adapterDir = opt("--adapter-dir");
const registrarPath = opt("--registrar");
const serversFile = opt("--servers-file");
const scriptFile = opt("--script", "");

const emit = (obj) => console.log("CHAIN:" + JSON.stringify(obj));

function createStubPi() {
  const handlers = new Map(); // pi.on(event) handlers
  const eventHandlers = new Map(); // pi.events handlers
  const tools = new Map();
  const pi = {
    events: {
      on: (name, fn) => {
        if (!eventHandlers.has(name)) eventHandlers.set(name, []);
        eventHandlers.get(name).push(fn);
      },
      emit: (name, payload) => {
        for (const fn of eventHandlers.get(name) ?? []) fn(payload);
      },
    },
    on: (name, fn) => {
      if (!handlers.has(name)) handlers.set(name, []);
      handlers.get(name).push(fn);
    },
    registerTool: (def) => {
      tools.set(def.name, def);
    },
    getAllTools: () => [...tools.keys()].map((name) => ({ name })),
    getActiveTools: () => [...tools.keys()],
    setActiveTools: () => {},
    registerFlag: () => {},
    getFlag: () => undefined,
    registerCommand: () => {},
    registerShortcut: () => {},
    registerMessageRenderer: () => {},
  };
  return { pi, handlers, tools };
}

const stubCtx = {
  mode: "rpc",
  hasUI: false,
  cwd: process.cwd(),
  model: undefined,
  modelRegistry: undefined,
  signal: undefined,
  ui: { notify: () => {} },
};

async function fire(handlers, name, event) {
  for (const fn of handlers.get(name) ?? []) await fn(event, stubCtx);
}

async function waitForTools(tools, names, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (names.every((n) => tools.has(n))) return true;
    if (Date.now() > deadline) return false;
    await new Promise((r) => setTimeout(r, 100));
  }
}

const { pi, handlers, tools } = createStubPi();
// File URL: Windows paths (backslashes, drive letters) are not valid
// module specifiers as plain strings.
const adapterEntry = pathToFileURL(path.join(adapterDir, "index.ts")).href;
const { createMcpAdapter, registerMcpServer, getRuntimeMcpServerSnapshot } =
  await import(adapterEntry);
const install = createMcpAdapter({ config: { mcpServers: {} } });
install(pi);
emit({ event: "adapter-installed" });

await fire(handlers, "session_start", {});

const hasMcp = await waitForTools(tools, ["mcp"], 60000);
emit({ event: "tool-surface", tools: [...tools.keys()], hasMcp });
if (!hasMcp) {
  emit({ event: "fatal", message: "mcp proxy tool never registered" });
  process.exit(2);
}

// Load the real registrar with the session payload and capture its markers.
const markers = [];
const origLog = console.log;
process.env.PI_ACP_MCP_SERVERS_JSON = fs.readFileSync(serversFile, "utf8");
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pi-acp-chain-"));
const registrarCopy = path.join(scratch, "mcp-registrar.harness.mjs");
fs.copyFileSync(registrarPath, registrarCopy);
const { default: registrar } = await import(pathToFileURL(registrarCopy).href);
console.log = (...a) => {
  const line = a.join(" ");
  if (line.startsWith("PI_ACP_MCP:")) markers.push(line);
  else origLog(...a);
};
try {
  registrar(pi);
} finally {
  console.log = origLog;
}
// session_start retry path (covers registrar-first extension order).
await fire(handlers, "session_start", {});
await new Promise((r) => setTimeout(r, 500));
emit({ event: "markers", markers });

const mcpTool = tools.get("mcp");
const run = (params) =>
  mcpTool.execute("chain-call", params, undefined, undefined, stubCtx);

let script = [];
if (scriptFile) script = JSON.parse(fs.readFileSync(scriptFile, "utf8"));
for (const step of script) {
  try {
    if (step.op === "connect" || step.op === "list") {
      // connect: lazy connect + metadata refresh (its result already carries
      // the tool list); list: serve the list from live metadata.
      const res =
        step.op === "connect"
          ? await run({ connect: step.server })
          : await run({ server: step.server });
      emit({ event: step.op, server: step.server, result: res });
    } else if (step.op === "call") {
      const res = await run({
        tool: step.tool,
        args: step.args ?? {},
        server: step.server,
      });
      emit({ event: "call", server: step.server, tool: step.tool, result: res });
    } else if (step.op === "tools") {
      emit({ event: "tools", tools: [...tools.keys()] });
    } else if (step.op === "status") {
      emit({ event: "status", result: await run({}) });
    } else if (step.op === "snapshot") {
      // Runtime snapshot API: proves the registration is session-scoped and
      // non-persisted (runtime:true, persisted:false).
      try {
        const snap = getRuntimeMcpServerSnapshot({ pi, name: step.server });
        emit({ event: "snapshot", server: step.server, snapshot: snap });
      } catch (e) {
        emit({
          event: "snapshot",
          server: step.server,
          error: String(e?.message ?? e),
        });
      }
    } else if (step.op === "register-dispose") {
      // Direct register/dispose roundtrip on top of the installed adapter:
      // proves teardown removes exactly that registration (failed-marker
      // isolation's mirror image on the cleanup side).
      const reg = registerMcpServer({
        pi,
        name: step.server,
        definition: step.definition,
      });
      let before = null;
      let after = null;
      try {
        before = getRuntimeMcpServerSnapshot({ pi, name: step.server });
      } catch (e) {
        before = { error: String(e?.message ?? e) };
      }
      await reg.dispose();
      try {
        after = getRuntimeMcpServerSnapshot({ pi, name: step.server });
      } catch (e) {
        after = { error: String(e?.message ?? e) };
      }
      emit({ event: "register-dispose", server: step.server, before, after });
    }
  } catch (e) {
    emit({
      event: step.op,
      server: step.server,
      error: String(e?.message ?? e),
    });
  }
}

await fire(handlers, "session_shutdown", {});
emit({ event: "shutdown-complete" });
process.exit(0);
