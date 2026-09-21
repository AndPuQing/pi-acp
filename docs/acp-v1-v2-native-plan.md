# ACP v1 / v2 native support — remaining work

## Why this exists

ACP v1 and v2 are **separate protocols**, not two spellings of one. Several
concepts changed shape between them, and a bash tool call is the sharpest
example:

- **v1** embeds a **client-owned** terminal: `ToolCallContent::Terminal` plus
  `terminal_info` / `terminal_output` / `terminal_exit` `_meta`, and the client
  owns `terminal/create|output|release`.
- **v2** has no such tool content and no client-owned terminal at all. A
  terminal is **agent-owned** and streamed through `terminal_update` /
  `terminal_output_chunk`; a tool call is a patch-style `tool_call_update`.

The old design had a **v1-only session core** and converted every frame to v2 at
the outbound boundary. That conversion is lossy in ways that cannot be repaired:
the schema **refuses** v1 `Terminal` content on the v2 path, the boundary
silently dropped the whole frame, and every replayed bash call reached the client
as `tool: "other"` with no result. See
`crates/pi-acp/src/render.rs` for the full account.

### The decision

**C-protocol**: the engine is shared, but the **protocol layer is two native
implementations with no conversion**. The pump states the *facts* of an update
(protocol-neutral); each protocol's renderer builds its **own** frame. v1 and v2
each get their native shapes, and `initialize` negotiation picks which
implementation owns the connection (the SDK's `AgentProtocolRouter`).

The workflow is incremental and one-way: **implement one v2 endpoint natively,
then delete the v1↔v2 conversion it replaced.** The `protocol-v2` cargo feature
is temporary scaffolding and is removed at the end.

## Status

- [x] Slice 1 — bash tool call, outbound native (`render.rs`).
- [x] Slice 2 — text chunks, `agent_message` patch, `state_update`, outbound
      native. Pump's `protocol` field removed (the pump is protocol-agnostic);
      dead `send_agent_message` deleted.
- [ ] Slice 3 — non-bash tool frames + `OutboundMessage::Notify` fallback.
- [ ] Slice 4 — notice-class updates (`session_info`, `available_commands`,
      `config_option`, `current_mode`).
- [ ] Slice 5 — `session/request_permission`.
- [ ] Slice 6 — inbound endpoints, one at a time.
- [ ] Slice 7 — delete the conversion layer, drop the `protocol-v2` feature.

The neutral-fact pattern to follow is in `crates/pi-acp/src/render.rs`
(`BashToolCall`, `TextChunk`, `MessagePatch`, `Foreground`) and the connector arm
in `crates/pi-acp/src/session/session.rs` (`spawn_outbound_connector`).

---

## Slice 3 — non-bash tool frames and the `Notify` fallback

Everything a non-bash tool produces still builds a **v1** frame and converts it.

- `crates/pi-acp/src/agent.rs:2596` — replayed `ToolCall` (the non-bash half of
  `replay_history`; the bash half is already native).
- `crates/pi-acp/src/agent.rs:2619` — replayed `ToolCallUpdate`.
- `crates/pi-acp/src/session/session.rs:1579` / `:2777` — the
  `OutboundMessage::Notify` fallback and its connector arm. Once every producer
  has a native renderer this variant and its conversion arm disappear.

Non-bash tool kinds to render natively: `read`, `edit`/`write` (structured
`Diff`), `search`, `fetch`, `think`, and the generic tool call. The v2 forms are
`tool_call_update` with `kind`, `title`, `rawInput`, `content` (including a v2
`Diff`), and `rawOutput`.

**Done when:** no `send_session_update` call site remains in `agent.rs`, and the
`OutboundMessage::Notify` variant is gone.

## Slice 4 — notice-class updates

Still v1 frames converted at the boundary:

- `crates/pi-acp/src/agent.rs:239`, `:1087`, `:1724` — `SessionInfoUpdate`
  (session title, three sites).
- `crates/pi-acp/src/agent.rs:1913` — a resource-link `AgentMessageChunk` (the
  session export). This is the **last** producer that relies on
  `with_message_id`; rendering it natively makes that helper dead.
- `crates/pi-acp/src/agent.rs:2209` — `AvailableCommandsUpdate`.
- `crates/pi-acp/src/agent.rs:2234` — `ConfigOptionUpdate`.
- `crates/pi-acp/src/agent.rs:2253` — `CurrentModeUpdate`. v2 **removed session
  modes**; this must stay skipped on v2 (v2 expresses modes through
  `ConfigOptionUpdate`). Make the skip explicit in the renderer rather than in
  the converter.

**Done when:** `crates/pi-acp/src/protocol.rs`'s `send_session_update`,
`convert_session_update_to_v2`, and `with_message_id` are all dead.

## Slice 5 — `session/request_permission`

`crates/pi-acp/src/protocol.rs`'s `request_permission` converts the request to
v2 and the response back to v1. Build both natively:

- v2 request/response types are constructed directly (no `try_v1_to_v2` /
  `try_v2_to_v1`).
- The extension-ui bridge that consumes the answer is in
  `crates/pi-acp/src/session/session.rs` (`handle_extension_ui_request` and the
  `OutboundMessage::RequestPermission` arm).

## Slice 6 — inbound endpoints

`crates/pi-acp/src/v2.rs` maps each v2 request to a v1 request, calls the shared
v1 handler, and converts the response back. Replace each with a **native v2
handler that calls the shared engine directly**, deleting the `to_v1` / `to_v2`
pair for that endpoint as it goes. One at a time:

- [ ] `initialize` — `crates/pi-acp/src/agent.rs:630` (`try_v2_to_v1`) and
      `crates/pi-acp/src/protocol.rs:242` (`initialize_response_to_v2`, which
      also hand-clears `authMethods`, drops `mcpCapabilities.sse`, and adds the
      v2 session capabilities). Build the v2 response from the engine's facts.
- [ ] `session/new` — `v2.rs:113`, `:120`.
- [ ] `session/prompt` — `v2.rs:142`.
- [ ] `session/resume` — `v2.rs:173`, `:185` (and `plan_resume`).
- [ ] `session/close` — `v2.rs:216`, `:218`.
- [ ] `session/list` — `v2.rs:224`, `:228`.
- [ ] `session/delete` — `v2.rs:239`, `:246`.
- [ ] `session/set_config_option` — `v2.rs:257`, `:264`.
- [ ] `session/cancel` — `crates/pi-acp/src/agent.rs:655` (`try_v2_to_v1`).
- [ ] `session/set_model` — **not registered on v2 at all today** (gap). The v1
      path claims it in an `on_receive_dispatch` handler
      (`crates/pi-acp/src/agent.rs`, `SESSION_SET_MODEL_METHOD`); give v2 its own
      native handler.
- [ ] `session/set_mode` — stays `methodNotFound` on v2 (v2 removed modes); keep
      the explicit rejection.

## Slice 7 — cleanup

- [ ] Delete `crates/pi-acp/src/protocol.rs`'s conversion layer:
      `send_session_update`, `convert_session_update_to_v2`, `with_message_id`,
      `SYNTHETIC_MESSAGE_SEQ`, `initialize_response_to_v2`.
- [ ] Delete `crates/pi-acp/src/v2.rs`'s `to_v1` / `to_v2` helpers.
- [ ] Remove the `protocol-v2` cargo feature
      (`crates/pi-acp/Cargo.toml`), make both implementations always compiled,
      and rely solely on `initialize` negotiation. Update the `#[cfg(feature =
      "protocol-v2")]` gates throughout `render.rs`, `session/session.rs`,
      `agent.rs`, and the tests.
- [ ] Update the module docs that still describe the "v1 core + boundary
      conversion" design: `crates/pi-acp/src/lib.rs`, `src/protocol.rs`,
      `src/v2.rs`, `src/session/session.rs`.

---

## Definition of done

- No `agent_client_protocol_schema::v2::conversion` call remains anywhere in
  `crates/pi-acp/src`.
- No v1 protocol type appears in the v2 outbound path or in a v2 handler.
- `protocol-v2` is not a cargo feature; one binary serves both, negotiated in
  `initialize`.
- The v1 path is byte-for-byte unchanged (the existing v1 tests are the guard).

## Testing notes

- The v1 path's guard is the existing `tests/session.rs` suite (30 tests,
  including `tool_statuses_are_monotonic_and_bash_terminals_stream`).
- The v2 e2e suite is `tests/acp_v2.rs`.
- `crates/pi-acp/src/render.rs` has unit tests per renderer, including a
  regression that pins the original bug: the legacy v1 bash frame **cannot**
  convert to v2, while the native v2 frame needs no conversion.
- When a test harness records outbound frames (`tests/session.rs`
  `run_recorder`), it must render them for the fixture's protocol, exactly as
  the real connector does.
