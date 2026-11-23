// Deterministic guard tests over the public embedding surface. No real Pi,
// provider, MCP service, credentials or user configuration is used here.
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

test("bounded guard enforces ownership, fixed coding surface and unknown delivery", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bounded-guard-"));
  const cwd = path.join(dir, "workspace"); fs.mkdirSync(cwd);
  const host = path.join(dir, "host.mjs");
  const sdk = path.join(dir, "node_modules/@modelcontextprotocol/client");
  fs.mkdirSync(sdk, { recursive: true });
  fs.writeFileSync(path.join(sdk, "package.json"), JSON.stringify({ name:"@modelcontextprotocol/client", type:"module", exports:"./index.js" }));
  fs.writeFileSync(path.join(sdk, "index.js"), "export class StreamableHTTPClientTransport {}\n");
  fs.writeFileSync(host, `export function createHostManagedMcpAdapter(options) {
    return { async ready() { for (const server of Object.values(options.servers)) {
      if (server.protocolVersion !== '2026-07-28') throw Error('missing client pin');
    } }, async close() {}, extensionFactory(api) {
      if (api.sendMessage || api.sendUserMessage || api.exec || api.registerProvider) throw Error('unsafe bridge API');
      globalThis.boundedTestApi = api;
      api.registerTool({ name:'fixture_assignment', execute: async mode => options.onToolCall({
        toolName:'fixture_assignment', dispatch: async () => {
          if (mode === 'unknown') { const error = Error('lost response'); error.delivery='may_have_run'; throw error; }
          if (mode === 'task') return { resultType:'task', taskId:'fixture-task' };
          return { resultType:'complete',content:[{type:'text',text:'fixture-marker'}] };
        }
      }) });
    } };
  }`);
  const source = fs.readFileSync(new URL("../src/bounded_runtime.ts", import.meta.url), "utf8");
  const extension = path.join(dir, "extension.ts");
  fs.writeFileSync(extension, source.replaceAll("__HOST_MANAGED_MODULE__", JSON.stringify(host)));
  fs.writeFileSync(path.join(dir,"servers.json"), JSON.stringify([{name:"fixture",url:"http://127.0.0.1:1"}]));
  const oldDir = process.env.PI_ACP_BOUNDED_DIR;
  const oldAgent = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_ACP_BOUNDED_DIR = dir;
  process.env.PI_CODING_AGENT_DIR = path.join(dir,"agent");
  fs.mkdirSync(process.env.PI_CODING_AGENT_DIR);
  const hooks = new Map(), tools = new Map();
  const events = { on() {}, emit() {} };
  let active = [], aborts = 0;
  const pi = { events, on:(name,fn)=>hooks.set(name,fn), registerTool:t=>tools.set(t.name,t), setActiveTools:n=>{active=n;} };
  try {
    const module = await import(pathToFileURL(extension).href);
    await module.default(pi);
    assert.deepEqual(await hooks.get("cache_warming_decision")({action:"warm"}),{action:"stop"});
    const state = () => JSON.parse(fs.readFileSync(path.join(dir,"state.json"),"utf8"));
    const ctx = {cwd, abort:async()=>{aborts++;}};
    await hooks.get("session_start")({},ctx);
    for (const name of ["read","write","edit","grep","find","ls","fixture_assignment"]) assert(active.includes(name));
    assert(!active.includes("bash"));
    assert.throws(()=>globalThis.boundedTestApi.registerTool({name:"late-tool"}), /late/);
    assert.equal((await hooks.get("input")({source:"rpc",text:"unowned"})).action,"handled");
    await hooks.get("before_agent_start")({},ctx); assert.equal(aborts,1);
    assert.equal((await hooks.get("session_before_compact")({reason:"threshold"},ctx)).cancel,true);
    if(process.env.PI_ACP_TEST_NATIVE_SDK) {
      // Exercise the actual native public decision dispatcher. A thrown hook
      // would fall back to warm; this must be a concrete stop/cancel response.
      const {ExtensionRunner,createExtensionRuntime}=await import(pathToFileURL(process.env.PI_ACP_TEST_NATIVE_SDK).href);
      const runner=new ExtensionRunner([{path:extension,handlers:new Map([
        ["cache_warming_decision",[hooks.get("cache_warming_decision")]],
        ["session_before_compact",[hooks.get("session_before_compact")]],
      ])}],createExtensionRuntime(),cwd,{},{});
      assert.equal(await runner.emitCacheWarmingDecision({type:"cache_warming_decision",action:"warm"}),"stop");
      const before=await runner.emit({type:"session_before_compact",reason:"threshold"});
      assert.equal(before.cancel,true,"native pre-routing compaction must be cancelled without an owned grant");
    }
    const grant = text => fs.writeFileSync(path.join(dir,"grant.json"),JSON.stringify({message:text}));
    grant("owned");
    assert.equal((await hooks.get("input")({source:"rpc",text:"owned"})).action,"continue");
    assert(!fs.existsSync(path.join(dir,"grant.json")));
    await hooks.get("before_agent_start")({},ctx); assert.equal(aborts,1);
    assert.equal(await hooks.get("session_before_compact")({reason:"threshold"},ctx),undefined,"owned compaction remains permitted");
    assert.equal(await hooks.get("tool_call")({toolName:"edit",input:{path:"arithmetic.py"}},ctx),undefined);
    assert.equal((await hooks.get("tool_call")({toolName:"bash",input:{}},ctx)).block,true);
    assert.equal((await hooks.get("tool_call")({toolName:"write",input:{path:"../grant.json"}},ctx)).block,true);
    fs.symlinkSync(dir,path.join(cwd,"escape"),"dir");
    assert.equal((await hooks.get("tool_call")({toolName:"read",input:{path:"escape/grant.json"}},ctx)).block,true);
    const result = await tools.get("fixture_assignment").execute("result");
    assert.equal(result.content[0].text,"fixture-marker");
    await hooks.get("agent_settled")({},ctx);
    assert.equal(state().authorized,false); assert.equal(state().uncertain,false);
    await hooks.get("before_agent_start")({},ctx); assert.equal(aborts,2);
    assert.equal((await hooks.get("session_before_compact")({reason:"threshold"},ctx)).cancel,true);
    grant("task"); await hooks.get("input")({source:"rpc",text:"task"});
    await assert.rejects(tools.get("fixture_assignment").execute("task"), /task handles/);
    assert.equal(state().uncertain,true); assert.equal(state().inFlight,0);
    await hooks.get("agent_settled")({},ctx);
    grant("retry"); assert.equal((await hooks.get("input")({source:"rpc",text:"retry"})).action,"handled");
    await hooks.get("session_shutdown")({},ctx);
    assert.equal(state().uncertain,true,"shutdown must not erase unknown remote work");
  } finally {
    if (oldDir === undefined) delete process.env.PI_ACP_BOUNDED_DIR; else process.env.PI_ACP_BOUNDED_DIR = oldDir;
    if (oldAgent === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = oldAgent;
    delete globalThis.boundedTestApi;
    fs.rmSync(dir,{recursive:true,force:true});
  }
});
