// Endpoint-owned explicit extension. Loaded only by the supported Pi TS loader.
// The host-managed bridge receives a narrow API, never model/follow-up APIs.
import { createHostManagedMcpAdapter } from __HOST_MANAGED_MODULE__;
import { createRequire } from "node:module";
import { readFileSync, writeFileSync, renameSync, unlinkSync, realpathSync } from "node:fs";
import { join, resolve, dirname, relative, isAbsolute } from "node:path";
import { pathToFileURL } from "node:url";

const coding = ["read", "write", "edit", "grep", "find", "ls"];
export const stopCacheWarming = async () => ({ action: "stop" as const });

export function insideWorkspace(cwd: string, candidate: string): boolean {
  const root = realpathSync(cwd);
  let path = resolve(root, candidate);
  for (;;) {
    try { path = realpathSync(path); break; }
    catch { const parent = dirname(path); if (parent === path) return false; path = parent; }
  }
  const rel = relative(root, path);
  return rel === "" || (!rel.startsWith("..") && !isAbsolute(rel));
}

export default async function boundedRpc(pi: any) {
  // Native cache warming can dispatch directly after settlement without a
  // before_agent_start event. Use its supported decision control, not errors.
  pi.on("cache_warming_decision", stopCacheWarming);
  const dir = process.env.PI_ACP_BOUNDED_DIR;
  if (!dir) throw new Error("bounded execution configuration missing");
  const state = { version: 1, ready: false, uncertain: false, authorized: false, inFlight: 0 };
  const save = () => {
    const path = join(dir, "state.json");
    writeFileSync(path + ".tmp", JSON.stringify(state), { mode: 0o600 });
    renameSync(path + ".tmp", path);
  };
  save();
  // Pi may compact before before_agent_start. This hook is its supported
  // pre-routing cancellation point; only an already owned workflow may do so.
  pi.on("session_before_compact", async () =>
    !state.authorized || state.uncertain ? { cancel: true } : undefined);
  const specs = JSON.parse(readFileSync(join(dir, "servers.json"), "utf8"));
  const toolNames = new Set(coding);
  let sealed = false;
  const narrowApi = Object.freeze({
    events: pi.events,
    registerTool(tool: any) {
      if (sealed || toolNames.has(tool.name)) throw new Error("late or colliding tool registration refused");
      toolNames.add(tool.name);
      pi.registerTool(tool);
    },
  });
  const requireFromBridge = createRequire(__HOST_MANAGED_MODULE__);
  const { StreamableHTTPClientTransport } = await import(pathToFileURL(requireFromBridge.resolve("@modelcontextprotocol/client")).href);
  const servers = Object.fromEntries(specs.map((spec: any) => [spec.name, {
    protocolVersion: "2026-07-28",
    createTransport() {
      return new StreamableHTTPClientTransport(new URL(spec.url), {
        requestInit: { headers: spec.headers ?? {} },
      });
    },
  }]));
  const adapter = createHostManagedMcpAdapter({
    servers,
    async onToolCall(call: any) {
      if (!state.authorized || state.uncertain || !toolNames.has(call.toolName)) {
        throw new Error("tool call outside owned foreground work refused");
      }
      state.inFlight++; save();
      try {
        const result = await call.dispatch();
        if (result.resultType !== undefined && result.resultType !== "complete") {
          state.uncertain = true;
          throw new Error("remote task handles are unsupported in bounded mode");
        }
        return result;
      } catch (error: any) {
        // Only not_sent establishes that no remote work began. A server error,
        // invalid task/result or abort may leave remote work unresolved.
        if (error?.delivery !== "not_sent") state.uncertain = true;
        throw error;
      } finally { state.inFlight--; save(); }
    },
  });
  pi.events.on("pi-mcp-adapter:tool-approval-request", (request: any) => {
    request.claim(async () => state.authorized && !state.uncertain
      && toolNames.has(request.prefixedToolName) ? "allow_once" : "deny");
  });
  await adapter.ready();
  adapter.extensionFactory(narrowApi);
  sealed = true;
  const activate = () => pi.setActiveTools([...toolNames]);
  pi.on("session_start", async () => { activate(); });
  pi.on("input", async (event: any) => {
    if (state.uncertain || state.authorized || event.source !== "rpc") {
      return { action: "handled" };
    }
    try {
      const path = join(dir, "grant.json");
      const grant = JSON.parse(readFileSync(path, "utf8"));
      unlinkSync(path); // consume before any possible model activity
      if (grant.message !== event.text || event.text.trimStart().startsWith("/")) return { action: "handled" };
      state.authorized = true; save();
      return { action: "continue" };
    } catch { return { action: "handled" }; }
  });
  pi.on("before_agent_start", async (_event: any, ctx: any) => {
    if (!state.authorized || state.uncertain) {
      await ctx.abort(); // Pi checks the startup abort before its first provider request.
      return;
    }
    activate();
  });
  pi.on("tool_call", async (event: any, ctx: any) => {
    if (!state.authorized || state.uncertain || !toolNames.has(event.toolName)) {
      return { block: true, reason: "tool is outside the bounded execution contract" };
    }
    if (coding.includes(event.toolName)
      && !insideWorkspace(ctx.cwd, event.input?.path ?? ".")) {
      return { block: true, reason: "coding tools are restricted to the session workspace" };
    }
  });
  // Pi emits this after retries/compaction/continuations and awaits extension
  // handlers before publishing the same settlement on its RPC stream.
  pi.on("agent_settled", async () => { state.authorized = false; save(); });
  pi.on("session_shutdown", async () => {
    if (state.authorized || state.inFlight) state.uncertain = true;
    state.authorized = false;
    save();
    await adapter.close();
  });
  state.ready = true; save();
  for (const spec of specs) console.log(`PI_ACP_MCP:registered:${spec.name}`);
}
