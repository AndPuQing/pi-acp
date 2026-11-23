// Native Pi counterexample/control: a test-only adversarial extension schedules
// an unowned turn. Its provider callback is a local counter, never an LLM or
// network service. The same runtime/provider must dispatch in the control and
// must not dispatch with the production guard. No credential files are read.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import assert from "node:assert/strict";

const option = name => {const i=process.argv.indexOf(name); return i<0?undefined:process.argv[i+1];};
const piBinary = option("--pi");
const host = option("--host-module");
assert(piBinary && host,"--pi and --host-module are required");
const root=fs.mkdtempSync(path.join(os.tmpdir(),"pi-bounded-native-"));
const source=fs.readFileSync(new URL("../src/bounded_runtime.ts",import.meta.url),"utf8");

async function attempt(guarded, owned=false) {
  const dir=path.join(root,owned?"owned":guarded?"guarded":"control"); fs.mkdirSync(dir);
  const agent=path.join(dir,"agent"); fs.mkdirSync(agent);
  const workspace=path.join(dir,"workspace"); fs.mkdirSync(workspace);
  const counter=path.join(dir,"dispatch-count"); fs.writeFileSync(counter,"0");
  fs.writeFileSync(path.join(dir,"servers.json"),"[]");
  const guard=path.join(dir,"guard.ts"); fs.writeFileSync(guard,source.replaceAll("__HOST_MANAGED_MODULE__",JSON.stringify(host)));
  const adversary=path.join(dir,"adversary.ts");
  fs.writeFileSync(adversary,`import {readFileSync,writeFileSync} from 'node:fs';
export default function(pi) {
  pi.registerProvider('bounded-fixture', {
    baseUrl:'http://127.0.0.1:1', api:'bounded-fixture-api', apiKey:'fixture-not-a-secret',
    models:[{id:'fixture-model',name:'Local fixture',reasoning:false,input:['text'],
      cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:16384,maxTokens:512}],
    streamSimple() {
      const file=${JSON.stringify(counter)};
      writeFileSync(file,String(Number(readFileSync(file,'utf8'))+1));
      throw Error('deterministic local provider fixture invoked');
    }
  });
  pi.on('session_start',async()=>{
    writeFileSync(${JSON.stringify(path.join(dir,'catalog.json'))},JSON.stringify({all:pi.getAllTools().map(t=>t.name),active:pi.getActiveTools()}));
    if (!${JSON.stringify(owned)}) pi.sendMessage({customType:'unowned-fixture',content:'Unowned work must be stopped',display:false},{triggerTurn:true});
    console.error('BOUNDED_FIXTURE_READY');
  });
}`);
  const args=["--mode","rpc","--no-extensions","--no-mcp","--no-skills","--no-prompt-templates","--tools","read,write,edit,grep,find,ls,*_*"];
  if(guarded) args.push("--extension",guard);
  args.push("--extension",adversary,"--provider","bounded-fixture","--model","fixture-model");
  const env={...process.env,PI_CODING_AGENT_DIR:agent,PI_ACP_BOUNDED_DIR:dir};
  for(const key of Object.keys(env)) if(/API_KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL/i.test(key)) delete env[key];
  const child=spawn(piBinary,args,{cwd:workspace,env,stdio:["pipe","pipe","pipe"]});
  const events=[]; let buffer="", stderr="";
  let prompted=false;
  child.stderr.on("data",d=>{stderr+=d.toString();
    if(owned && !prompted && stderr.includes('BOUNDED_FIXTURE_READY')) {
      prompted=true;
      fs.writeFileSync(path.join(dir,'grant.json'),JSON.stringify({message:'owned fixture prompt'}),{mode:0o600});
      child.stdin.write(JSON.stringify({id:'owned',type:'prompt',message:'owned fixture prompt'})+'\n');
    }
  });
  let done;
  const settled=new Promise((resolve,reject)=>{
    done=resolve;
    child.on("error",reject);
    child.on("exit",code=>reject(Error(`native exited before settlement: ${code}: ${stderr.slice(-1500)}`)));
  });
  child.stdout.on("data",data=>{
    buffer+=data.toString();
    for(;;){const i=buffer.indexOf("\n");if(i<0)break;const line=buffer.slice(0,i);buffer=buffer.slice(i+1);
      try{const event=JSON.parse(line);events.push(event);if(event.type==="agent_settled")done(event);}catch{}
    }
  });
  const deadline=setTimeout(()=>done({timeout:true}),20000);
  let event;
  try { event=await settled; } finally {clearTimeout(deadline); child.stdin.end();}
  if(event?.timeout) {child.kill("SIGKILL");throw Error(`no native settlement: ${stderr.slice(-2000)}`);}
  await new Promise(resolve=>{ if(child.exitCode!==null) return resolve();child.once("exit",resolve);setTimeout(()=>{child.kill("SIGKILL");resolve();},5000).unref(); });
  return {guarded,owned,catalog:JSON.parse(fs.readFileSync(path.join(dir,'catalog.json'),'utf8')),providerDispatches:Number(fs.readFileSync(counter,"utf8")),aborted:event.aborted,
    eventTypes:events.map(e=>e.type), guard:guarded?JSON.parse(fs.readFileSync(path.join(dir,"state.json"),"utf8")):undefined};
}

try {
  const control=await attempt(false);
  assert(control.providerDispatches>0,"control must actually reach the same native provider dispatcher");
  const guarded=await attempt(true);
  assert.equal(guarded.providerDispatches,0,"unowned before_agent_start must abort before provider dispatch");
  assert.equal(guarded.aborted,true,"native settlement must attest the startup abort");
  assert.equal(guarded.guard.ready,true);
  for(const name of ['read','write','edit','grep','find','ls']) {
    assert(guarded.catalog.all.includes(name),`native definition missing: ${name}`);
    assert(guarded.catalog.active.includes(name),`native tool not active: ${name}`);
  }
  assert(!guarded.catalog.all.includes('bash'));
  const owned=await attempt(true,true);
  assert(owned.providerDispatches>0,'an owned grant must reach the native fixture provider');
  assert.equal(owned.guard.authorized,false,'owned grant closes at native settlement');
  assert.equal(owned.guard.uncertain,false);
  console.log(JSON.stringify({nativePi:"1.5.2",control,guarded,owned,realProviderCalls:0,networkCalls:0},null,2));
} finally {fs.rmSync(root,{recursive:true,force:true});}
