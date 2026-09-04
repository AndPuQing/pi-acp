// Fake MCP server over stdio (newline-delimited JSON-RPC), zero dependencies.
// Usage: node fake-mcp-stdio.mjs --tag <tag> --log <path>
import readline from "node:readline";
import { makeToolSet, createLog, handleMessage } from "./mcp-protocol.mjs";

const args = process.argv.slice(2);
const flag = (name, def) => {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};
const tag = flag("--tag", "stdio");
const tools = makeToolSet(tag);
const log = createLog(flag("--log", ""));

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});
rl.on("line", (line) => {
  if (!line.trim()) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }
  const out = handleMessage(msg, tools, log);
  if (out === null || out === undefined) return;
  const items = Array.isArray(msg) ? out : [out];
  for (const item of items)
    process.stdout.write(JSON.stringify(item) + "\n");
});
