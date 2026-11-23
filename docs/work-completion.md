# Work completion and lifecycle guarantees

The endpoint uses released Rust SDK 5.5.1 and schema 1.21.2, whose published
source is ACP v2 beta.3 at `94824f11ec78f7a6699323a9342e47fa5edf2530`.
Wire version 2 alone does not identify a draft. Older v2 clients must validate
compatibility; this version does not emit the former alpha.4 empty prompt response.

The unrestricted profile remains the default. Its background inventory is
omitted, meaning unknown coverage. The opt-in bounded coding profile below
restricts managed AI activity and reports the standard background snapshot.
Neither profile promises termination of arbitrary detached host processes or
settlement of arbitrary remote services.

## Insertion and foreground completion

`session/prompt` first validates the session and admits the work. It inserts an
ACP user message with a globally fresh ID, echoes `user_message` with that ID,
and acknowledges insertion with the same required `PromptResponse.messageId`.
Execution proceeds independently of the acknowledgement. An invalid session or
rejected admission gets no insertion echo. Provider errors after insertion do
not undo the message or turn its acknowledgement into a completion signal.

Valid `idempotencyKey` retries return the recorded insertion ID and start no
second turn or echo. Key claiming is atomic, including concurrent retries.
Keys are retained with the ACP conversation across endpoint restarts. This is
insertion deduplication, not proof of an interrupted turn's outcome. Never
resubmit a prompt to reconstruct missing usage or uncertain execution.

Pi's early native RPC response confirms command handling. Ordinary foreground
work ends at native `agent_settled`, after retries, compaction and continuations;
intermediate `turn_end` and `agent_end` do not establish completion. ACP
`state_update` reports running and terminal state separately from insertion.
The unrestricted engine can queue prompts; its aggregate foreground stays
running until the queued workflow drains. The bounded profile
admits one foreground owner at a time and refuses overlapping new insertions.

A small endpoint-owned ACP conversation cache preserves canonical user and
assistant message IDs, tool updates and insertion keys. Resume replays those
same snapshots rather than minting new IDs from native history. Message patches
preserve omitted fields and apply metadata patch rules. New assistant messages
use a fresh lifetime prefix, so replacement subprocesses cannot overwrite old
messages. Tip fork copies retained snapshots into the child's session namespace;
delete clears retained content and keys and leaves only an empty tombstone to
reject late frames. This cache is not an accounting or task-authorization journal.

## Opt-in bounded coding profile

Set these operator variables in the environment that launches the endpoint:

| Variable | Value |
| --- | --- |
| `PI_ACP_EXECUTION_MODE` | `bounded` |
| `PI_ACP_BOUNDED_MCP_MODULE` | Absolute public `pi-mcp-adapter/host-managed` TypeScript module entry from an audited, pinned package supporting per-server `protocolVersion` |
| `PI_ACP_ENABLE_MCP` | `true` when providing ACP HTTP MCP servers |

This profile currently validates native Pi **1.5.2** only. The public
host-managed bridge API must support `protocolVersion: "2026-07-28"` as an
actual SDK Client era pin. The original published 7.0.1 host-managed API did not
expose that option; an older module must not be substituted for the corrected
package used to validate a deployment. Package/lock/source identity belongs in
the deployment's evidence. No JavaScript build shim or private Client patch is
used: Pi's documented TS extension loader loads the explicit wrapper and public
host-managed module.

The profile enforces the following controls:

- Native automatic extensions, built-in MCP discovery, skills and prompt-template
  discovery are disabled. Exactly one endpoint-owned guard extension is loaded.
  Slash commands, additional workspace roots and fork are unavailable in this
  profile. The unrestricted profile retains its existing tool/extension behavior.
- Supported `--tools` patterns retain native read, write, edit, grep, find and ls
  definitions plus namespaced bridge tools. The active set and execution gate
  contain only those coding tools and the exact initial MCP catalog. There is no
  Bash tool. Late tool registration/collisions are refused; the host-managed API
  retires tools when their definitions change instead of adopting new tools.
- Coding paths must resolve inside the primary workspace, including through
  existing symlinks. The bridge, native configuration/lifecycle directory and
  temporary guard controls must be outside that writable workspace. This is a
  tool restriction, not a system sandbox or general host isolation mechanism.
- The bridge receives only a frozen tool-registration/events surface, with no
  model, shell, provider-registration or message/follow-up API. It reads no ambient
  MCP configuration, exposes no proxy/install/command surface, and has no
  authProvider, sampling, elicitation, task backgrounding or automatic reconnect.
  Each direct MCP dispatch is attempted at most once by this API.
- An endpoint-issued one-use input grant admits exactly the owned native prompt.
  Unsupported input is handled without starting a model turn. An unowned native
  `before_agent_start` uses the supported `ctx.abort()` control; throwing a hook
  is insufficient because native Pi catches hook exceptions.
- Native cache warming bypasses ordinary turn hooks, so
  `cache_warming_decision` unconditionally returns `stop`. Native pre-start
  compaction precedes `before_agent_start`, so `session_before_compact` cancels
  compaction without an owned grant or after uncertainty. Owned compaction is
  permitted within the foreground workflow.

The selected MCP path supports HTTP servers and synchronous complete results.
It pins each host-managed Client to MCP 2026-07-28, with no legacy or SSE
fallback. Modern complete results are accepted; remote task handles,
input-required results, invalid results and uncertain delivery cannot be treated
as successful completion. A failure known to be `not_sent` does not invent remote
work. Other uncertain delivery latches the session's unresolved state and blocks
further tool dispatch and automatic prompt retry.

## State, close and recovery

The standard background field is emitted on every bounded state snapshot when
coverage is established. `background: []` means the enforced profile has no
independent managed AI workflow. Foreground model/tool activity is still running
work, not a background item. This claim excludes arbitrary hidden server behavior
and deterministic host processes; a synchronous tool response is not an invoice
or proof of every remote side effect.

Unknown delivery or lifecycle information is represented by standard unknown
state with background coverage omitted. It is never converted to an empty list.
The unrestricted profile never claims empty background merely because a prompt
settled. Tool completion, turn outcome, process exit and billing completeness
remain distinct observations.

`session/cancel` requests native abort and clears queued native work. Sending the
notification is not termination. Only observed foreground settlement can report
cancelled idle; incomplete tool delivery can instead leave unknown state. Close
interrupts active work with errors, disposes the owned subprocess and waits for
exit observation. Unix teardown uses the process group; Windows uses the process
tree. Those cleanup mechanisms do not promise arbitrary detached descendant or
remote-task termination.

A minimal endpoint lifecycle record tracks active, idle, explicitly closed and
uncertain states, plus an exclusive ownership directory. A clean close records
quiet recovery only after the owned subprocess has exited. A hard endpoint death
leaves unresolved ownership: replacement/resume is rejected until safely
reconciled, even if a newly started native process looks quiet. Closing unknown
work does not settle it. No automatic stale-lock removal or blind resubmission is
performed, and no token/charge values are reconstructed.

Resume of a still-live bounded session preserves its subprocess and frozen
catalog. Completed, cleanly closed sessions may restore in a fresh subprocess.
For `replayFrom: {"type": "start"}`, v2 sends exactly one `replay_complete`
after the last retained history update, including an empty replay. The current
state snapshot follows that marker and precedes the response. Omitted/null
`replayFrom` sends no history or marker. A replay read/send failure is an error,
never a successful replay-completion boundary. Quiet snapshots have no invented historical stop reason; repeated
snapshots are idempotent observations, not new work or usage. An unresolved prior
record remains unknown or prevents resume. Configuration/history alone is never
used to infer a previous generation's outcome.

## Verification

Rust tests cover insertion validation, concurrent key deduplication, stable replay
and fork/delete identities, foreground settlement/cancellation, state synchronization
and interrupted ownership. Guard tests exercise fixed tools, workspace containment,
late mutations, task refusal and uncertainty. With the installed native SDK,
`bounded_runtime.mjs` also tests the actual public warming/compaction decision
dispatcher, including its fallback behavior.

`bounded_native.mjs` runs native Pi 1.5.2 in isolated temporary agent directories.
A test-only adversarial extension triggers an unowned turn with a local provider
counter. The control reaches that callback; the production guard must yield an
aborted native settlement with zero provider dispatches. A granted prompt must
reach the callback. The test also checks actual native tool definitions and active
names, not activation calls alone. There is no external model/network request or
reading/copying of credentials in these tests.

Run the guards with Node's supported type stripping, and the native check with
explicit installed paths:

```sh
node --experimental-strip-types --test crates/pi-acp/tests/bounded_runtime.mjs
node crates/pi-acp/tests/bounded_native.mjs --pi /path/to/pi --host-module /path/to/pi-mcp-adapter/host-managed.ts
```

Real consumer coding, modern-only error traffic, cancellation, clean restart/resume
and hard-interruption safety still require evidence against the exact combined
endpoint/bridge/package candidate. Unit tests and configuration labels do not
substitute for that proof. Effective routing, optional subscription usage/tokens
and actual charges remain unknown when no authoritative evidence is available.
