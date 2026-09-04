// Fake MCP server over HTTP, zero dependencies. Serves both transports the
// adapter speaks:
// - Streamable HTTP: POST /mcp (stateless plain-JSON responses), used for
//   ACP `Http` entries (with SSE fallback the adapter never needs here).
// - Legacy SSE: GET /sse (event stream carrying the `endpoint` event) plus
//   POST /messages?sessionId=..., used for ACP `Sse` entries
//   (`httpTransport: "sse"` pin).
// Usage: node fake-mcp-http.mjs --tag <tag> --port <port|0> --log <path>.
// With --port 0 the process prints `PORT:<n>` on stdout once listening.
import http from "node:http";
import { randomUUID } from "node:crypto";
import { makeToolSet, createLog, handleMessage } from "./mcp-protocol.mjs";

const args = process.argv.slice(2);
const flag = (name, def) => {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};
const tag = flag("--tag", "http");
const port = Number(flag("--port", "8377"));
const tools = makeToolSet(tag);
const log = createLog(flag("--log", ""));

const sseClients = new Map(); // sessionId -> res

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", "http://localhost");
  try {
    if (req.method === "GET" && url.pathname === "/sse") {
      const sessionId = randomUUID();
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });
      res.write(`event: endpoint\ndata: /messages?sessionId=${sessionId}\n\n`);
      sseClients.set(sessionId, res);
      req.on("close", () => sseClients.delete(sessionId));
      return;
    }
    if (
      req.method === "POST" &&
      (url.pathname === "/mcp" || url.pathname === "/messages")
    ) {
      const body = await readBody(req);
      let msg;
      try {
        msg = JSON.parse(body);
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            id: null,
            error: { code: -32700, message: "Parse error" },
          }),
        );
        return;
      }
      const out = handleMessage(msg, tools, log);
      if (url.pathname === "/messages") {
        // Legacy SSE: ack the POST, deliver the response on the event stream.
        res.writeHead(202, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
        const sessionId = url.searchParams.get("sessionId") ?? "";
        const stream = sseClients.get(sessionId);
        const items =
          msg !== null && typeof msg === "object" && !Array.isArray(msg)
            ? [out]
            : (out ?? []);
        for (const item of items) {
          if (item !== null && item !== undefined)
            stream?.write(
              `event: message\ndata: ${JSON.stringify(item)}\n\n`,
            );
        }
        return;
      }
      // Streamable HTTP: stateless plain-JSON response.
      if (out === null || out === undefined) {
        res.writeHead(202, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(out));
      return;
    }
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
  } catch (e) {
    try {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(e?.message ?? e) }));
    } catch {
      // Response already on its way out; nothing left to prove.
    }
  }
});

server.listen(port, "127.0.0.1", () => {
  const addr = server.address();
  const actual = typeof addr === "object" && addr ? addr.port : port;
  console.log(`PORT:${actual}`);
});
