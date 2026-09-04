// Test observer extension (W-484): dumps pi's tool surface so the
// acceptance test can assert MCP tools are visible inside real pi without
// a model. Runs in real pi as a global extension
// (`<agentdir>/extensions/observe/index.ts`).
//
// Two sinks (both assertion-friendly, neither touches the MCP gate):
// - `$PI_ACP_OBSERVE_LOG` (JSONL append): the Rust test reads this file.
// - `PI_ACP_MCP:observe-tools:*` stderr lines: pi routes extension output
//   to stderr and pi-acp retains every PI_ACP_MCP:-prefixed line; the
//   registrar parser ignores the `observe-tools:` shape, so the gate is
//   unaffected (verified: markers still settle on `registered:` only).
import fs from "node:fs";

export default function piAcpChainObserve(pi: any) {
  const dump = (phase: string) => {
    let names: unknown = [];
    try {
      names = pi
        .getAllTools()
        .map((t: any) => t.name)
        .sort();
    } catch (e: any) {
      names = [`error:${String(e?.message ?? e)}`];
    }
    const line = JSON.stringify({ phase, tools: names });
    const logPath =
      typeof process !== "undefined"
        ? process.env?.PI_ACP_OBSERVE_LOG
        : undefined;
    if (typeof logPath === "string" && logPath !== "") {
      try {
        fs.appendFileSync(logPath, line + "\n");
      } catch {
        // File sink is best-effort; the stderr marker below still lands.
      }
    }
    console.log(`PI_ACP_MCP:observe-tools:${line}`);
  };
  pi.on("session_start", () => {
    dump("start");
    setTimeout(() => dump("t3s"), 3000);
    setTimeout(() => dump("t10s"), 10000);
  });
}
