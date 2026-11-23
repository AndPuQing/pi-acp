// Modern-only stateless HTTP fixture. No initialize, sessions, SSE or models.
import http from "node:http";
import fs from "node:fs";

const args = process.argv.slice(2);
const flag = (key, fallback) => {
  const i = args.indexOf(key);
  return i < 0 ? fallback : args[i + 1];
};
const version = "2026-07-28";
const prefix = "io.modelcontextprotocol/";
const logPath = flag("--log", "");
const serverInfo = { name: "modern-only-test", version: "1.0.0" };
const tool = {
  name: "get_assignment",
  description: "Return a synthetic acceptance marker.",
  inputSchema: { type: "object", properties: {}, additionalProperties: false },
};

const server = http.createServer(async (req, res) => {
  let message;
  let status = 200;
  let result;
  let error;
  let malformed = false;
  try {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    message = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    const meta = message.params?._meta ?? {};
    if (req.method !== "POST" || !["/mcp", "/incompatible", "/error", "/malformed", "/auth"].includes(req.url)) {
      status = 405;
      error = { code: -32601, message: "Only stateless POST /mcp is supported" };
    } else if (
      meta[prefix + "protocolVersion"] !== version ||
      req.headers["mcp-protocol-version"] !== version ||
      req.headers["mcp-method"] !== message.method ||
      typeof meta[prefix + "clientCapabilities"] !== "object" ||
      meta[prefix + "clientCapabilities"] === null ||
      req.headers["mcp-session-id"] !== undefined ||
      (message.method === "tools/call" && req.headers["mcp-name"] !== message.params?.name)
    ) {
      status = 400;
      error = { code: -32022, message: "Modern-only request metadata required" };
    } else if (req.url === "/incompatible") {
      status = 400;
      error = { code: -32022, message: "Unsupported protocol version", data: { supported: ["2025-11-25"] } };
    } else if (req.url === "/error") {
      status = 500;
      error = { code: -32603, message: "Synthetic server error" };
    } else if (req.url === "/auth") {
      status = 401;
      error = { code: -32001, message: "Synthetic authentication failure" };
    } else if (req.url === "/malformed") {
      malformed = true;
      result = { unexpected: true };
    } else if (message.method === "server/discover") {
      result = { resultType: "complete", supportedVersions: [version], capabilities: { tools: {} }, ttlMs: 0, cacheScope: "public" };
    } else if (message.method === "tools/list") {
      result = { resultType: "complete", tools: [tool], ttlMs: 0, cacheScope: "public" };
    } else if (message.method === "tools/call" && message.params?.name === tool.name) {
      result = { resultType: "complete", content: [{ type: "text", text: "MODERN_MCP_FIXTURE_OK" }], isError: false };
    } else {
      status = 404;
      error = { code: -32601, message: "No legacy initialization or fallback" };
    }
  } catch {
    status = 400;
    error = { code: -32700, message: "Invalid JSON request" };
  }
  if (logPath) fs.appendFileSync(logPath, JSON.stringify({
    method: message?.method, params: message?.params, accepted: !error,
    httpMethod: req.method, path: req.url,
    protocolVersionHeader: req.headers["mcp-protocol-version"] ?? null,
    methodHeader: req.headers["mcp-method"] ?? null,
    sessionHeaderPresent: req.headers["mcp-session-id"] !== undefined,
  }) + "\n");
  const response = { jsonrpc: "2.0", id: message?.id ?? null };
  if (error) response.error = error;
  else response.result = malformed ? result : { ...result, _meta: { [prefix + "serverInfo"]: serverInfo } };
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(response));
});

server.listen(Number(flag("--port", "0")), "127.0.0.1", () => {
  console.log(`PORT:${server.address().port}`);
});
