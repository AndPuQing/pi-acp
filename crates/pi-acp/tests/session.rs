//! S5 (W-452) acceptance tests for the session state machine.
//!
//! These drive [`PiAcpSession`] against the **mock pi** (`--mock-rpc` fixture
//! in the `pi-acp` binary) with **scripted event sequences**
//! (`--mock-scenario <dir>`: `<dir>/<n>.jsonl` replayed for the n-th prompt).
//! The outbound ACP traffic is recorded on the session's outbound channel
//! (the same channel [`spawn_outbound_connector`] bridges to the SDK in
//! production), so the state machine's notifications, tool statuses, diffs and
//! permission requests are asserted directly.
//!
//! Acceptance per W-452: turn queueing / cancel / monotonic tool status / diff
//! generation pass on mock event sequences.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    ContentBlock, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SessionUpdate, ToolCall, ToolCallContent, ToolCallStatus, ToolCallUpdate,
};
use pi_acp::error::AcpxError;
use pi_acp::pi::rpc::ImageContent;
use pi_acp::session::{OutboundMessage, PiAcpSession, SessionManager, SessionParams, StopReason};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::{mpsc, Mutex};

/// Path to the `pi-acp` binary under test (the mock fixture lives in it).
const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");
/// Test deadline; the mock responds instantly, so 10s is generous.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The interesting parts of [`OutboundMessage`] recorded by the harness.
#[derive(Debug, Clone)]
enum Recorded {
    Notify(SessionUpdate),
    Permission(RequestPermissionRequest),
}

struct Fixture {
    _tmp: TempDir,
    session: Arc<PiAcpSession>,
    recorded: Arc<Mutex<Vec<Recorded>>>,
    permission_answers: mpsc::Sender<RequestPermissionResponse>,
    scenarios: PathBuf,
    command_log: PathBuf,
    extension_log: PathBuf,
}

/// Spawn a session against the mock pi with a recording outbound sink.
async fn fixture(extra_args: &[&str]) -> Fixture {
    fixture_with_settle_timeout(extra_args, Duration::ZERO).await
}

async fn fixture_with_settle_timeout(extra_args: &[&str], settle_timeout: Duration) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let scenarios = tmp.path().join("scenarios");
    let command_log = tmp.path().join("commands.log");
    let extension_log = tmp.path().join("extensions.log");
    fs::create_dir_all(&scenarios).unwrap();

    let mut args = vec![
        "--mock-rpc".to_string(),
        "--mock-scenario".to_string(),
        scenarios.to_str().unwrap().to_string(),
        "--mock-command-log".to_string(),
        command_log.to_str().unwrap().to_string(),
        "--mock-extension-log".to_string(),
        extension_log.to_str().unwrap().to_string(),
    ];
    args.extend(extra_args.iter().map(|s| s.to_string()));

    let (outbound_tx, outbound_rx) = mpsc::channel(512);
    let (answer_tx, answer_rx) = mpsc::channel(16);
    let recorded: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(Vec::new()));
    let rec = recorded.clone();
    tokio::spawn(run_recorder(outbound_rx, rec, answer_rx));

    let session = PiAcpSession::spawn(SessionParams {
        pi_command: BIN.to_string(),
        extra_args: args,
        timeout: TIMEOUT,
        settle_timeout,
        cwd: tmp.path().to_path_buf(),
        outbound: outbound_tx,
        session_path: None,
        session_id_override: None,
        file_commands: vec![],
    })
    .await
    .expect("session spawn");

    Fixture {
        _tmp: tmp,
        session,
        recorded,
        permission_answers: answer_tx,
        scenarios,
        command_log,
        extension_log,
    }
}

/// Consume the outbound channel: record notifications, answer permission
/// requests from the test's `permission_answers` queue (Cancelled fallback).
async fn run_recorder(
    mut rx: mpsc::Receiver<OutboundMessage>,
    recorded: Arc<Mutex<Vec<Recorded>>>,
    mut permission_answers: mpsc::Receiver<RequestPermissionResponse>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            OutboundMessage::Notify(notif) => {
                recorded.lock().await.push(Recorded::Notify(notif.update));
            }
            OutboundMessage::RequestPermission(request, respond) => {
                recorded
                    .lock()
                    .await
                    .push(Recorded::Permission(request.clone()));
                let answer = permission_answers.recv().await.unwrap_or_else(|| {
                    RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
                });
                let _ = respond.send(Ok(answer));
            }
            OutboundMessage::Flush(ack) => {
                // Ordering barrier: everything before it is already recorded.
                let _ = ack.send(());
            }
        }
    }
}

/// Write the n-th prompt's scenario file (one JSON event per line).
fn write_scenario(dir: &Path, n: usize, events: &[Value]) {
    let lines: Vec<String> = events.iter().map(|e| e.to_string()).collect();
    fs::write(dir.join(format!("{n}.jsonl")), lines.join("\n")).unwrap();
}

/// Poll until `predicate` sees a recorded message (bounded; fails the test on
/// timeout so a stuck pump surfaces as a failure rather than a hang).
async fn wait_until<F: Fn(&[Recorded]) -> bool>(fx: &Fixture, predicate: F) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let recorded = fx.recorded.lock().await.clone();
        if predicate(&recorded) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for recorded messages; got: {recorded:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_command(fx: &Fixture, expected: &str) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if read_log(&fx.command_log)
            .iter()
            .any(|command| command == expected)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for command {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// All streamed agent text chunks in order.
fn text_chunks(recorded: &[Recorded]) -> Vec<String> {
    recorded
        .iter()
        .filter_map(|r| match r {
            Recorded::Notify(SessionUpdate::AgentMessageChunk(c)) => match &c.content {
                ContentBlock::Text(t) => Some(t.text.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn queue_depths(recorded: &[Recorded]) -> Vec<(u64, bool)> {
    recorded
        .iter()
        .filter_map(|r| match r {
            Recorded::Notify(SessionUpdate::SessionInfoUpdate(upd)) => {
                let meta = upd.meta.as_ref()?;
                let pi_acp = meta.get("piAcp")?;
                let depth = pi_acp.get("queueDepth")?.as_u64()?;
                let running = pi_acp.get("running")?.as_bool()?;
                Some((depth, running))
            }
            _ => None,
        })
        .collect()
}

/// Tool call status updates for one tool call id, in order.
fn tool_statuses(recorded: &[Recorded], id: &str) -> Vec<ToolCallStatus> {
    recorded
        .iter()
        .filter_map(|r| match r {
            Recorded::Notify(SessionUpdate::ToolCall(t)) if t.tool_call_id.0.as_ref() == id => {
                Some(t.status)
            }
            Recorded::Notify(SessionUpdate::ToolCallUpdate(u))
                if u.tool_call_id.0.as_ref() == id =>
            {
                u.fields.status
            }
            _ => None,
        })
        .collect()
}

async fn prompt_turn(fx: &Fixture, text: &str) -> Result<StopReason, AcpxError> {
    tokio::time::timeout(
        TIMEOUT,
        fx.session
            .prompt(text.to_string(), Vec::<ImageContent>::new()),
    )
    .await
    .expect("prompt timed out")
}

fn read_log(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .map(|s| s.lines().map(str::to_string).collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Turn queueing
// ---------------------------------------------------------------------------

/// Two prompts: the second is queued while the first streams; the queue drains
/// one-at-a-time, each turn completing on `agent_settled` (never `agent_end`).
#[tokio::test]
async fn prompts_queue_and_drain_serially() {
    let fx = fixture(&["--mock-event-delay-ms", "150"]).await;
    // Turn 1 streams one chunk then emits agent_end (a low-level run end) —
    // the ACP turn must NOT complete there. Turn 2 is a plain text turn.
    write_scenario(
        &fx.scenarios,
        1,
        &[
            json!({"type":"message_update","usage":{},"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"first"}}),
            json!({"type":"agent_end","messages":[],"willRetry":false}),
        ],
    );
    write_scenario(
        &fx.scenarios,
        2,
        &[
            json!({"type":"message_update","usage":{},"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"second"}}),
        ],
    );

    // Prompt 1 first; wait for its streamed chunk so prompt 2 is guaranteed to
    // arrive while turn 1 is still running (the queueing branch).
    let s = fx.session.clone();
    let turn1 = tokio::spawn(async move { s.prompt("hello one".into(), vec![]).await });
    wait_until(&fx, |r| text_chunks(r).contains(&"first".to_string())).await;

    let s = fx.session.clone();
    let turn2 = tokio::spawn(async move { s.prompt("hello two".into(), vec![]).await });

    assert_eq!(turn1.await.unwrap().unwrap(), StopReason::EndTurn);
    assert_eq!(turn2.await.unwrap().unwrap(), StopReason::EndTurn);

    let recorded = fx.recorded.lock().await.clone();

    // Text streamed in order, with the queueing notices around them.
    let chunks = text_chunks(&recorded);
    assert!(
        chunks.contains(&"Queued message (position 1).".to_string()),
        "queue notice missing: {chunks:?}"
    );
    assert!(
        chunks.contains(&"Starting queued message. (0 remaining)".to_string()),
        "start notice missing: {chunks:?}"
    );
    let streamed: Vec<&str> = chunks
        .iter()
        .map(String::as_str)
        .filter(|t| *t == "first" || *t == "second")
        .collect();
    assert_eq!(streamed, vec!["first", "second"]);

    // Queue depth: turn1 starts -> prompt2 queued -> turn2 starts -> idle.
    let depths = queue_depths(&recorded);
    assert_eq!(depths, vec![(0, true), (1, true), (0, true), (0, false)]);
}

/// Responses and events are delivered through separate channels. A late prompt
/// response must not arm the settle deadline for the queued turn when pi emits
/// `agent_settled` before that response is observed by the pump.
#[tokio::test]
async fn prompt_response_order_cannot_poison_the_next_turn() {
    let fx = fixture_with_settle_timeout(
        &[
            "--mock-no-settle",
            "--mock-event-delay-ms",
            "150",
            "--mock-prompt-response-after-events-ms",
            "50",
        ],
        Duration::from_millis(100),
    )
    .await;
    write_scenario(
        &fx.scenarios,
        1,
        &[
            json!({
                "type": "message_update",
                "usage": {},
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": "first"
                }
            }),
            json!({"type": "agent_settled"}),
        ],
    );
    write_scenario(
        &fx.scenarios,
        2,
        &[
            json!({
                "type": "message_update",
                "usage": {},
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": "second"
                }
            }),
            json!({"type": "agent_settled"}),
        ],
    );

    let first_session = fx.session.clone();
    let first = tokio::spawn(async move { first_session.prompt("first".into(), vec![]).await });
    wait_for_command(&fx, "prompt").await;

    // Queue the next turn before the first prompt response is emitted. Its
    // response is intentionally delayed until after its own settle event too.
    let second_session = fx.session.clone();
    let second = tokio::spawn(async move { second_session.prompt("second".into(), vec![]).await });
    wait_until(&fx, |recorded| {
        text_chunks(recorded).contains(&"Queued message (position 1).".to_string())
    })
    .await;

    assert_eq!(first.await.unwrap().unwrap(), StopReason::EndTurn);
    assert_eq!(second.await.unwrap().unwrap(), StopReason::EndTurn);
}

/// A rejected prompt must fail the queued turns too, and a later prompt must
/// not reuse an unhealthy pi session.
#[tokio::test]
async fn prompt_failure_fails_queue_and_poison_session() {
    let fx = fixture(&[
        "--mock-delay-ms",
        "100",
        "--mock-prompt-error",
        "prompt rejected",
    ])
    .await;

    let s = fx.session.clone();
    let first = tokio::spawn(async move { s.prompt("first".into(), vec![]).await });
    wait_for_command(&fx, "prompt").await;

    let s = fx.session.clone();
    let second = tokio::spawn(async move { s.prompt("second".into(), vec![]).await });
    wait_until(&fx, |recorded| {
        text_chunks(recorded).contains(&"Queued message (position 1).".to_string())
    })
    .await;

    let first_result = tokio::time::timeout(TIMEOUT, first)
        .await
        .expect("rejected prompt must resolve")
        .unwrap()
        .unwrap_err();
    assert!(matches!(first_result, AcpxError::RpcFailed { .. }));

    let second_result = tokio::time::timeout(TIMEOUT, second)
        .await
        .expect("queued prompt must resolve after rejection")
        .unwrap()
        .unwrap_err();
    assert!(matches!(second_result, AcpxError::SessionClosed(_)));

    let later = fx.session.prompt("later".into(), vec![]).await.unwrap_err();
    assert!(matches!(later, AcpxError::SessionClosed(_)));
}

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

/// `cancel()` clears the queued turn (immediately resolves `Cancelled`) and
/// aborts the in-flight turn, which settles as `Cancelled` too.
#[tokio::test]
async fn cancel_clears_queue_and_aborts_running_turn() {
    // No auto-settle: turn 1 stays in flight until the abort settles it.
    let fx = fixture(&["--mock-no-settle"]).await;
    write_scenario(
        &fx.scenarios,
        1,
        &[json!({
            "type": "tool_execution_start",
            "toolCallId": "t1",
            "toolName": "read",
            "args": { "path": "a.txt" }
        })],
    );

    let s = fx.session.clone();
    let turn1 = tokio::spawn(async move { s.prompt("one".into(), vec![]).await });
    wait_until(&fx, |r| {
        tool_statuses(r, "t1").contains(&ToolCallStatus::InProgress)
    })
    .await;

    let s = fx.session.clone();
    let turn2 = tokio::spawn(async move { s.prompt("two".into(), vec![]).await });
    // Let the pump observe the queued turn.
    wait_until(&fx, |r| {
        text_chunks(r).contains(&"Queued message (position 1).".to_string())
    })
    .await;

    fx.session.cancel().await.expect("cancel");

    // The queued turn resolves immediately with Cancelled; the in-flight turn
    // resolves Cancelled once the abort settles it.
    assert_eq!(turn2.await.unwrap().unwrap(), StopReason::Cancelled);
    assert_eq!(turn1.await.unwrap().unwrap(), StopReason::Cancelled);

    let recorded = fx.recorded.lock().await.clone();
    let chunks = text_chunks(&recorded);
    assert!(
        chunks.contains(&"Cleared queued prompts.".to_string()),
        "clear notice missing: {chunks:?}"
    );
    // abort was sent to pi (mock command log).
    let commands = read_log(&fx.command_log);
    assert!(
        commands.iter().any(|c| c == "abort"),
        "abort missing: {commands:?}"
    );

    // Tool t1 was surfaced in_progress and never completed.
    assert_eq!(
        tool_statuses(&recorded, "t1"),
        vec![ToolCallStatus::InProgress]
    );
}

/// Cancellation must be able to reach pi while the prompt RPC is still
/// waiting for its early response. The abort request must not wait for the
/// session's process ownership mutex, or cancellation can hang until timeout.
#[tokio::test]
async fn cancel_interrupts_prompt_waiting_for_early_response() {
    let fx = fixture(&["--mock-prompt-hang"]).await;

    let s = fx.session.clone();
    let turn = tokio::spawn(async move { s.prompt("stuck".into(), vec![]).await });
    wait_for_command(&fx, "prompt").await;

    tokio::time::timeout(Duration::from_secs(1), fx.session.cancel())
        .await
        .expect("cancel must not wait for the prompt RPC timeout")
        .expect("abort must be accepted by pi");
    assert_eq!(turn.await.unwrap().unwrap(), StopReason::Cancelled);

    let commands = read_log(&fx.command_log);
    assert!(
        commands.iter().any(|command| command == "abort"),
        "abort missing: {commands:?}"
    );
}

// ---------------------------------------------------------------------------
// Monotonic tool status
// ---------------------------------------------------------------------------

/// Statuses only ever move forward: an out-of-order `toolcall_end` after
/// `tool_execution_start` must not downgrade `in_progress` back to `pending`;
/// a bash tool streams through the terminal with a monotonic lifecycle.
#[tokio::test]
async fn tool_statuses_are_monotonic_and_bash_terminals_stream() {
    let fx = fixture(&[]).await;
    write_scenario(
        &fx.scenarios,
        1,
        &[
            // Out-of-order: execution starts before the streaming toolcall_end
            // arrives. The later event must NOT downgrade the status.
            json!({
                "type": "tool_execution_start",
                "toolCallId": "t1",
                "toolName": "read",
                "args": { "path": "a.txt" }
            }),
            json!({
                "type": "message_update",
                "usage": {},
                "assistantMessageEvent": {
                    "type": "toolcall_end",
                    "contentIndex": 0,
                    "toolCall": { "id": "t1", "name": "read", "arguments": { "path": "a.txt" } }
                }
            }),
            json!({
                "type": "tool_execution_end",
                "toolCallId": "t1",
                "toolName": "read",
                "result": { "content": [{ "type": "text", "text": "file contents" }] },
                "isError": false
            }),
            // A bash tool: streamed while pending, executed, output deltas,
            // then the terminal exits.
            json!({
                "type": "message_update",
                "usage": {},
                "assistantMessageEvent": { "type": "toolcall_start", "contentIndex": 0, "id": "t2", "toolName": "bash" }
            }),
            json!({
                "type": "tool_execution_start",
                "toolCallId": "t2",
                "toolName": "bash",
                "args": { "command": "ls" }
            }),
            json!({
                "type": "tool_execution_update",
                "toolCallId": "t2",
                "toolName": "bash",
                "args": {},
                "partialResult": { "details": { "stdout": "hello" } }
            }),
            json!({
                "type": "bash_execution_update",
                "id": "t2",
                "delta": " world"
            }),
            json!({
                "type": "tool_execution_end",
                "toolCallId": "t2",
                "toolName": "bash",
                "result": { "details": { "stdout": "hello world", "exitCode": 0 } },
                "isError": false
            }),
        ],
    );

    assert_eq!(
        prompt_turn(&fx, "run tools").await.unwrap(),
        StopReason::EndTurn
    );
    let recorded = fx.recorded.lock().await.clone();

    // t1: in_progress (start) -> in_progress (late toolcall_end — no downgrade)
    //     -> completed (end).
    assert_eq!(
        tool_statuses(&recorded, "t1"),
        vec![
            ToolCallStatus::InProgress,
            ToolCallStatus::InProgress,
            ToolCallStatus::Completed,
        ]
    );

    // t2: pending (toolcall_start) -> in_progress (execution start) ->
    //     in_progress (output deltas) -> completed (terminal exit).
    assert_eq!(
        tool_statuses(&recorded, "t2"),
        vec![
            ToolCallStatus::Pending,
            ToolCallStatus::InProgress,
            ToolCallStatus::InProgress,
            ToolCallStatus::InProgress,
            ToolCallStatus::Completed,
        ]
    );

    // The first t2 emission is a tool_call carrying the terminal (content +
    // terminal_info meta); later ones are updates with terminal_output/exit.
    let mut seen_tool_call = false;
    let mut terminal_deltas: Vec<String> = Vec::new();
    let mut exit_codes: Vec<i64> = Vec::new();
    for r in &recorded {
        match r {
            Recorded::Notify(SessionUpdate::ToolCall(c)) if c.tool_call_id.0.as_ref() == "t2" => {
                seen_tool_call = true;
                assert_eq!(c.kind, agent_client_protocol::schema::v1::ToolKind::Execute);
                assert!(
                    matches!(c.content.first(), Some(ToolCallContent::Terminal(_))),
                    "initial bash tool_call must embed a terminal: {c:?}"
                );
                let meta = c.meta.as_ref().expect("terminal_info meta");
                assert_eq!(meta["terminal_info"]["terminal_id"], "t2");
            }
            Recorded::Notify(SessionUpdate::ToolCallUpdate(u))
                if u.tool_call_id.0.as_ref() == "t2" =>
            {
                if let Some(meta) = &u.meta {
                    if let Some(data) = meta.get("terminal_output").and_then(|m| m.get("data")) {
                        terminal_deltas.push(data.as_str().unwrap().to_string());
                    }
                    if let Some(code) = meta.get("terminal_exit").and_then(|m| m.get("exit_code")) {
                        exit_codes.push(code.as_i64().unwrap());
                    }
                }
            }
            _ => {}
        }
    }
    assert!(seen_tool_call, "bash tool_call never surfaced");
    assert_eq!(
        terminal_deltas,
        vec!["hello".to_string(), " world".to_string()]
    );
    assert_eq!(exit_codes, vec![0]);
}

// ---------------------------------------------------------------------------
// Structured diffs
// ---------------------------------------------------------------------------

/// `edit`: snapshot before mutation, unique oldText locates the 1-based line,
/// and the completed tool call carries the old/new text diff (no rawOutput).
#[tokio::test]
async fn edit_emits_structured_diff_with_line_location() {
    let fx = fixture(&[]).await;
    let abs = fx._tmp.path().join("a.txt");
    write_scenario(
        &fx.scenarios,
        1,
        &[
            json!({
                "__directive__": "write_file",
                "path": abs.to_str().unwrap(),
                "content": "one\ntwo\nthree"
            }),
            json!({
                "type": "tool_execution_start",
                "toolCallId": "t-edit",
                "toolName": "edit",
                "args": {
                    "path": "a.txt",
                    "edits": [{ "oldText": "two", "newText": "TWO" }]
                }
            }),
            json!({
                "__directive__": "wait_ms",
                "ms": 400
            }),
            json!({
                "__directive__": "write_file",
                "path": abs.to_str().unwrap(),
                "content": "one\nTWO\nthree"
            }),
            json!({
                "type": "tool_execution_end",
                "toolCallId": "t-edit",
                "toolName": "edit",
                "result": {
                    "content": [{ "type": "text", "text": "Successfully replaced 1 block(s) in a.txt." }],
                    "details": {}
                },
                "isError": false
            }),
        ],
    );

    assert_eq!(
        prompt_turn(&fx, "edit it").await.unwrap(),
        StopReason::EndTurn
    );
    let recorded = fx.recorded.lock().await.clone();

    // The tool_call carries the absolute location with the 1-based line of the
    // uniquely-located oldText ("two" is on line 2).
    let calls: Vec<&ToolCall> = recorded
        .iter()
        .filter_map(|r| match r {
            Recorded::Notify(SessionUpdate::ToolCall(t))
                if t.tool_call_id.0.as_ref() == "t-edit" =>
            {
                Some(t)
            }
            _ => None,
        })
        .collect();
    assert_eq!(calls.len(), 1, "tool calls: {calls:?}");
    assert_eq!(
        calls[0].locations.len(),
        1,
        "locations: {:?}",
        calls[0].locations
    );
    assert_eq!(calls[0].locations[0].path, abs);
    assert_eq!(calls[0].locations[0].line, Some(2));

    // The completed update carries the structured diff (old/new text, raw path
    // as given — TS parity) and no rawOutput.
    let updates: Vec<&ToolCallUpdate> = recorded
        .iter()
        .filter_map(|r| match r {
            Recorded::Notify(SessionUpdate::ToolCallUpdate(u))
                if u.tool_call_id.0.as_ref() == "t-edit" =>
            {
                Some(u)
            }
            _ => None,
        })
        .collect();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].fields.status, Some(ToolCallStatus::Completed));
    assert_eq!(updates[0].fields.raw_output, None);
    match updates[0].fields.content.as_deref() {
        Some([ToolCallContent::Diff(diff)]) => {
            assert_eq!(diff.path, PathBuf::from("a.txt"));
            assert_eq!(diff.old_text.as_deref(), Some("one\ntwo\nthree"));
            assert_eq!(diff.new_text, "one\nTWO\nthree");
        }
        other => panic!("expected a single Diff content, got {other:?}"),
    }
}

/// `write` of a new file: oldText is `None` (new file), diff still emitted.
#[tokio::test]
async fn write_new_file_emits_diff_without_old_text() {
    let fx = fixture(&[]).await;
    let abs = fx._tmp.path().join("new.txt");
    write_scenario(
        &fx.scenarios,
        1,
        &[
            json!({
                "type": "tool_execution_start",
                "toolCallId": "t-w",
                "toolName": "write",
                "args": { "path": "new.txt" }
            }),
            json!({
                "__directive__": "wait_ms",
                "ms": 400
            }),
            json!({
                "__directive__": "write_file",
                "path": abs.to_str().unwrap(),
                "content": "hello"
            }),
            json!({
                "type": "tool_execution_end",
                "toolCallId": "t-w",
                "toolName": "write",
                "result": { "content": [{ "type": "text", "text": "ok" }], "details": {} },
                "isError": false
            }),
        ],
    );

    assert_eq!(
        prompt_turn(&fx, "write it").await.unwrap(),
        StopReason::EndTurn
    );
    let recorded = fx.recorded.lock().await.clone();

    let updates: Vec<&ToolCallUpdate> = recorded
        .iter()
        .filter_map(|r| match r {
            Recorded::Notify(SessionUpdate::ToolCallUpdate(u))
                if u.tool_call_id.0.as_ref() == "t-w" =>
            {
                Some(u)
            }
            _ => None,
        })
        .collect();
    assert_eq!(updates.len(), 1);
    match updates[0].fields.content.as_deref() {
        Some([ToolCallContent::Diff(diff)]) => {
            assert_eq!(diff.path, PathBuf::from("new.txt"));
            assert_eq!(diff.old_text, None);
            assert_eq!(diff.new_text, "hello");
        }
        other => panic!("expected a single Diff content, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Extension UI bridge
// ---------------------------------------------------------------------------

/// `select` bridges to `session/request_permission`; the chosen option maps
/// back to the value pi receives.
#[tokio::test]
async fn extension_select_bridges_to_permission_and_answers_value() {
    let fx = fixture(&[]).await;
    write_scenario(
        &fx.scenarios,
        1,
        &[json!({
            "type": "extension_ui_request",
            "id": "ui-1",
            "method": "select",
            "title": "Pick",
            "options": ["alpha", "beta"]
        })],
    );

    let s = fx.session.clone();
    let turn = tokio::spawn(async move { s.prompt("choose".into(), vec![]).await });

    // The permission request arrives on the outbound channel.
    wait_until(&fx, |r| {
        r.iter().any(|m| matches!(m, Recorded::Permission(_)))
    })
    .await;
    let recorded = fx.recorded.lock().await.clone();
    let permission = match recorded.iter().find_map(|m| match m {
        Recorded::Permission(p) => Some(p.clone()),
        _ => None,
    }) {
        Some(p) => p,
        None => panic!("no permission request recorded"),
    };
    assert_eq!(
        permission
            .options
            .iter()
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    assert_eq!(
        permission
            .options
            .iter()
            .map(|o| o.option_id.0.as_ref())
            .collect::<Vec<_>>(),
        vec!["choice-0", "choice-1"]
    );
    assert_eq!(permission.tool_call.fields.title.as_deref(), Some("Pick"));

    // The user picks "beta" (choice-1).
    fx.permission_answers
        .send(RequestPermissionResponse::new(
            RequestPermissionOutcome::Selected(
                agent_client_protocol::schema::v1::SelectedPermissionOutcome::new("choice-1"),
            ),
        ))
        .await
        .unwrap();

    assert_eq!(turn.await.unwrap().unwrap(), StopReason::EndTurn);

    // pi received the chosen value.
    let answers = read_log(&fx.extension_log);
    assert!(
        answers
            .iter()
            .any(|l| l.contains("\"value\":\"beta\"") && l.contains("ui-1")),
        "extension answers: {answers:?}"
    );
}

/// `confirm` -> permission with Yes/No; selecting Yes answers `confirmed: true`.
#[tokio::test]
async fn extension_confirm_yes_answers_confirmed() {
    let fx = fixture(&[]).await;
    write_scenario(
        &fx.scenarios,
        1,
        &[json!({
            "type": "extension_ui_request",
            "id": "ui-2",
            "method": "confirm",
            "title": "Run?",
            "message": "proceed?"
        })],
    );

    let s = fx.session.clone();
    let turn = tokio::spawn(async move { s.prompt("go".into(), vec![]).await });
    wait_until(&fx, |r| {
        r.iter().any(|m| matches!(m, Recorded::Permission(_)))
    })
    .await;

    fx.permission_answers
        .send(RequestPermissionResponse::new(
            RequestPermissionOutcome::Selected(
                agent_client_protocol::schema::v1::SelectedPermissionOutcome::new("yes"),
            ),
        ))
        .await
        .unwrap();

    assert_eq!(turn.await.unwrap().unwrap(), StopReason::EndTurn);
    let answers = read_log(&fx.extension_log);
    assert!(
        answers
            .iter()
            .any(|l| l.contains("\"confirmed\":true") && l.contains("ui-2")),
        "extension answers: {answers:?}"
    );
}

/// `input` is not supported in ACP v1: the session says so and cancels it.
#[tokio::test]
async fn extension_input_is_notified_and_cancelled() {
    let fx = fixture(&[]).await;
    write_scenario(
        &fx.scenarios,
        1,
        &[json!({
            "type": "extension_ui_request",
            "id": "ui-3",
            "method": "input",
            "title": "Type",
            "placeholder": "hint"
        })],
    );

    assert_eq!(
        prompt_turn(&fx, "ask me").await.unwrap(),
        StopReason::EndTurn
    );
    let recorded = fx.recorded.lock().await.clone();
    assert!(
        text_chunks(&recorded)
            .iter()
            .any(|t| t.contains("not supported in ACP yet")),
        "no unsupported notice: {:?}",
        text_chunks(&recorded)
    );
    let answers = read_log(&fx.extension_log);
    assert!(
        answers
            .iter()
            .any(|l| l.contains("\"cancelled\":true") && l.contains("ui-3")),
        "extension answers: {answers:?}"
    );
}

// ---------------------------------------------------------------------------
// Robustness
// ---------------------------------------------------------------------------

/// Unknown / not-yet-handled pi events never break the pump or the turn.
#[tokio::test]
async fn unknown_events_do_not_break_the_turn() {
    let fx = fixture(&[]).await;
    write_scenario(
        &fx.scenarios,
        1,
        &[
            json!({"type":"queue_update","steering":["s"],"followUp":[]}),
            json!({"type":"session_info_changed","name":"Renamed"}),
            json!({"type":"brand_new_event","some":"field"}),
            json!({"type":"message_update","usage":{},"assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"still works"}}),
        ],
    );
    assert_eq!(prompt_turn(&fx, "hi").await.unwrap(), StopReason::EndTurn);
    let recorded = fx.recorded.lock().await.clone();
    assert_eq!(text_chunks(&recorded), vec!["still works".to_string()]);
}

/// A dead pi fails the in-flight turn with `PiExited` instead of a silent
/// empty end_turn (fixes #82). `--mock-exit-after 2` lets the mock answer the
/// session handshake `get_state` (command 1) and die on the `prompt` (2).
#[tokio::test]
async fn pi_exit_fails_the_running_turn() {
    let fx = fixture(&["--mock-exit-after", "2"]).await;
    write_scenario(&fx.scenarios, 1, &[json!({"type":"turn_start"})]);
    let result = prompt_turn(&fx, "hello").await;
    match result {
        Err(AcpxError::PiExited { code, .. }) => assert_eq!(code, Some(42)),
        other => panic!("expected PiExited, got {other:?}"),
    }
}

/// After pi dies, **later** prompts on the same session also fail loudly with
/// `PiExited` (code/signal + hint), never a silent empty end_turn and never a
/// generic "session closed" that hides what happened (S8 / fixes #82).
#[tokio::test]
async fn dead_session_reports_pi_exit_on_subsequent_prompts() {
    let fx = fixture(&["--mock-exit-after", "2"]).await;
    write_scenario(&fx.scenarios, 1, &[json!({"type":"turn_start"})]);

    // First prompt dies mid-flight (command 2 = the prompt).
    match prompt_turn(&fx, "hello").await {
        Err(AcpxError::PiExited { code, .. }) => assert_eq!(code, Some(42)),
        other => panic!("expected PiExited, got {other:?}"),
    }

    // The pump has torn down; the session remembers the exit and surfaces it.
    match prompt_turn(&fx, "again").await {
        Err(AcpxError::PiExited { code, signal }) => {
            assert_eq!(code, Some(42), "exit code must survive the teardown");
            assert_eq!(signal, None);
        }
        other => panic!("expected PiExited on subsequent prompt, got {other:?}"),
    }

    // Non-prompt commands fail the same loud way.
    match fx.session.get_state().await {
        Err(AcpxError::PiExited { code, .. }) => assert_eq!(code, Some(42)),
        other => panic!("expected PiExited from get_state, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Settle fallback (design §11 risk #84)
// ---------------------------------------------------------------------------

/// pi accepts the prompt but never emits `agent_settled` (the mock's
/// `--mock-no-settle`): the settle deadline must resolve the turn with an
/// explicit `SettleTimeout` instead of hanging `session/prompt` forever — the
/// per-request RPC timeout only bounds the early response, not the settle
/// wait.
#[tokio::test]
async fn missing_agent_settled_resolves_with_settle_timeout() {
    let tmp = tempfile::tempdir().unwrap();
    let scenarios = tmp.path().join("scenarios");
    fs::create_dir_all(&scenarios).unwrap();
    let (outbound_tx, outbound_rx) = mpsc::channel(512);
    let (answer_tx, answer_rx) = mpsc::channel(16);
    let recorded: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(Vec::new()));
    let rec = recorded.clone();
    tokio::spawn(run_recorder(outbound_rx, rec, answer_rx));
    let _ = answer_tx;

    let session = PiAcpSession::spawn(SessionParams {
        pi_command: BIN.to_string(),
        extra_args: vec!["--mock-rpc".to_string(), "--mock-no-settle".to_string()],
        timeout: TIMEOUT,
        // Short deadline so the test runs fast; 0 would disable the fallback.
        settle_timeout: Duration::from_millis(500),
        cwd: tmp.path().to_path_buf(),
        outbound: outbound_tx,
        session_path: None,
        session_id_override: None,
        file_commands: vec![],
    })
    .await
    .expect("session spawn");

    // The early prompt response is accepted (Ok) but no settle follows; the
    // fallback must resolve the turn rather than hang.
    let result = tokio::time::timeout(
        TIMEOUT,
        session.prompt("hello".to_string(), Vec::<ImageContent>::new()),
    )
    .await
    .expect("prompt must resolve (settle fallback)");
    match result {
        Err(AcpxError::SettleTimeout { .. }) => {}
        other => panic!("expected SettleTimeout, got {other:?}"),
    }

    // The session is still alive and disposable after the fallback fired.
    session.dispose().await;
}

/// A settle timeout must resolve and discard prompts queued behind the stuck
/// turn; later prompts are rejected until the session is recreated.
#[tokio::test]
async fn settle_timeout_fails_queue_and_poison_session() {
    let fx = fixture_with_settle_timeout(&["--mock-no-settle"], Duration::from_millis(200)).await;

    let s = fx.session.clone();
    let first = tokio::spawn(async move { s.prompt("first".into(), vec![]).await });
    wait_for_command(&fx, "prompt").await;

    let s = fx.session.clone();
    let second = tokio::spawn(async move { s.prompt("second".into(), vec![]).await });
    wait_until(&fx, |recorded| {
        text_chunks(recorded).contains(&"Queued message (position 1).".to_string())
    })
    .await;

    let first_result = tokio::time::timeout(TIMEOUT, first)
        .await
        .expect("stuck prompt must resolve")
        .unwrap()
        .unwrap_err();
    assert!(matches!(first_result, AcpxError::SettleTimeout { .. }));

    let second_result = tokio::time::timeout(TIMEOUT, second)
        .await
        .expect("queued prompt must resolve after settle timeout")
        .unwrap()
        .unwrap_err();
    assert!(matches!(second_result, AcpxError::SessionClosed(_)));

    let later = fx.session.prompt("later".into(), vec![]).await.unwrap_err();
    assert!(matches!(later, AcpxError::SessionClosed(_)));
    fx.session.dispose().await;
}

/// Restoring a file under one ACP id must fail when pi reports a different
/// native id, and the rejected spawn must not leave its mock child running.
#[tokio::test]
async fn session_id_override_mismatch_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let session_file = tmp.path().join("native.jsonl");
    let header = json!({
        "type": "session",
        "id": "native-session-id",
        "cwd": tmp.path().to_string_lossy(),
    });
    fs::write(&session_file, format!("{header}\n")).unwrap();
    let (outbound_tx, _outbound_rx) = mpsc::channel(16);

    let result = PiAcpSession::spawn(SessionParams {
        pi_command: BIN.to_string(),
        extra_args: vec!["--mock-rpc".to_string()],
        timeout: TIMEOUT,
        settle_timeout: Duration::ZERO,
        cwd: tmp.path().to_path_buf(),
        outbound: outbound_tx,
        session_path: Some(session_file),
        session_id_override: Some("requested-session-id".into()),
        file_commands: vec![],
    })
    .await;

    match result {
        Err(AcpxError::SessionIdMismatch { expected, actual }) => {
            assert_eq!(expected, "requested-session-id");
            assert_eq!(actual, "native-session-id");
        }
        Ok(_) => panic!("mismatched native session id must be rejected"),
        Err(other) => panic!("expected SessionIdMismatch, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SessionManager
// ---------------------------------------------------------------------------

/// The manager registers sessions and `dispose_all` tears them down (further
/// prompts fail with an explicit error instead of hanging).
#[tokio::test]
async fn session_manager_registers_and_disposes_sessions() {
    let tmp = tempfile::tempdir().unwrap();
    let (outbound_tx, outbound_rx) = mpsc::channel(64);
    let _ = outbound_rx; // no consumer needed for this test
    let manager = SessionManager::new();
    let session = PiAcpSession::spawn(SessionParams {
        pi_command: BIN.to_string(),
        extra_args: vec!["--mock-rpc".to_string()],
        timeout: TIMEOUT,
        settle_timeout: Duration::ZERO,
        cwd: tmp.path().to_path_buf(),
        outbound: outbound_tx,
        session_path: None,
        session_id_override: None,
        file_commands: vec![],
    })
    .await
    .unwrap();
    let sid = session.session_id().clone();
    manager.insert(session.clone()).await;

    assert!(manager.maybe_get(&sid).await.is_some());
    assert!(manager.get(&sid).await.is_ok());

    manager.dispose_all().await;
    assert!(manager.maybe_get(&sid).await.is_none());

    // The session is gone: a prompt fails explicitly instead of hanging.
    let err = session.prompt("hi".into(), vec![]).await.unwrap_err();
    assert!(matches!(err, AcpxError::SessionClosed(_)), "got {err:?}");
}

/// Replacing an existing session id must dispose the old process without
/// leaving the manager pointing at a dead instance. The mock intentionally
/// returns the same id for every spawn, matching the collision this guards.
#[tokio::test]
async fn session_manager_replacement_disposes_previous_instance() {
    let tmp = tempfile::tempdir().unwrap();
    let (first_outbound, _first_receiver) = mpsc::channel(64);
    let (second_outbound, _second_receiver) = mpsc::channel(64);
    let manager = SessionManager::new();

    let params = |outbound| SessionParams {
        pi_command: BIN.to_string(),
        extra_args: vec!["--mock-rpc".to_string()],
        timeout: TIMEOUT,
        settle_timeout: Duration::ZERO,
        cwd: tmp.path().to_path_buf(),
        outbound,
        session_path: None,
        session_id_override: None,
        file_commands: vec![],
    };
    let first = PiAcpSession::spawn(params(first_outbound)).await.unwrap();
    let session_id = first.session_id().clone();
    manager.insert(first.clone()).await;

    let second = PiAcpSession::spawn(params(second_outbound)).await.unwrap();
    manager.insert(second.clone()).await;

    let current = manager.maybe_get(&session_id).await.unwrap();
    assert!(Arc::ptr_eq(&current, &second));
    assert!(matches!(
        first.prompt("old".into(), vec![]).await,
        Err(AcpxError::SessionClosed(_))
    ));
    assert_eq!(
        second.get_state().await.unwrap().session_id,
        "mock-session-id"
    );

    manager.dispose_all().await;
}
