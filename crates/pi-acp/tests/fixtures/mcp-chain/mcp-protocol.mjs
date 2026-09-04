// Shared hand-rolled MCP server core (zero dependencies) for W-484 acceptance.
//
// Speaks MCP over JSON-RPC: `initialize` / `notifications/initialized` /
// `tools/list` / `tools/call` / `ping`. Every inbound method is appended to
// the request log so tests can assert the real pi-mcp-adapter actually
// performed list/call across the wire. The `initialize` answer pins
// protocolVersion 2025-11-25, which the adapter's client accepts.
import fs from "node:fs";

export const PROTOCOL_VERSION = "2025-11-25";

export function makeToolSet(tag) {
  return [
    {
      name: `${tag}_echo`,
      description: `Echo tool on ${tag}`,
      inputSchema: {
        type: "object",
        properties: { text: { type: "string" } },
        required: ["text"],
      },
    },
    {
      name: `${tag}_add`,
      description: `Add two numbers on ${tag}`,
      inputSchema: {
        type: "object",
        properties: { a: { type: "number" }, b: { type: "number" } },
        required: ["a", "b"],
      },
    },
  ];
}

export function createLog(logPath) {
  const append = (obj) => {
    if (logPath) fs.appendFileSync(logPath, JSON.stringify(obj) + "\n");
  };
  return { append };
}

export function handleMessage(msg, tools, log) {
  if (Array.isArray(msg))
    return msg.map((m) => handleMessage(m, tools, log)).filter(Boolean);
  if (msg === null || typeof msg !== "object") return null;
  const { id, method, params } = msg;
  if (method !== undefined) log.append({ method, params: params ?? null });
  const ok = (result) =>
    id === undefined ? null : { jsonrpc: "2.0", id, result };
  const err = (code, message) =>
    id === undefined
      ? null
      : { jsonrpc: "2.0", id, error: { code, message } };
  switch (method) {
    case "initialize":
      return ok({
        protocolVersion: PROTOCOL_VERSION,
        capabilities: { tools: {} },
        serverInfo: { name: "fake-mcp", version: "0.0.1" },
      });
    case "notifications/initialized":
    case "notifications/cancelled":
      return null;
    case "ping":
      return ok({});
    case "tools/list":
      return ok({ tools });
    case "tools/call": {
      const name = params?.name ?? "";
      const args = params?.arguments ?? {};
      const tool = tools.find((t) => t.name === name);
      if (!tool) return err(-32602, `Unknown tool: ${name}`);
      let text;
      if (name.endsWith("_echo")) text = `echo:${args.text ?? ""}`;
      else if (name.endsWith("_add"))
        text = `sum:${Number(args.a) + Number(args.b)}`;
      else text = `ok:${JSON.stringify(args)}`;
      return ok({ content: [{ type: "text", text }] });
    }
    default:
      if (method === undefined) return null;
      return err(-32601, `Method not found: ${method}`);
  }
}
