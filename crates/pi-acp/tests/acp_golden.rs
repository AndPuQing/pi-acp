//! S8 (W-455) ACP frame golden-file regression (design §9.5).
//!
//! Records the raw ACP frames the agent writes to stdout — in wire order — for
//! a fixed `initialize → session/new → prompt` flow against the mock pi, and
//! compares them against a checked-in golden file (`tests/golden/acp-frames.golden`).
//!
//! This guards the two #70/D4 ordering properties from regressing:
//! 1. the startup-info prelude (which carries the version-check notice) is
//!    written **before** the `session/new` response frame, not after the turn;
//! 2. all outbound frames leave through the single ordered connection in the
//!    exact order the handlers produced them.
//!
//! Nondeterministic bits (temp cwd paths, timestamps, pi version, the
//! client-side queue-depth notifications that race the turn response) are
//! normalized/filtered so the golden is byte-stable on all CI platforms.
//!
//! Regenerate with `UPDATE_GOLDEN=1 cargo test --test acp_golden` after an
//! intentional frame-sequence change, and review the diff.

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, NewSessionRequest, PromptRequest, SessionNotification,
    TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{
    on_receive_notification, AcpAgent, AcpAgentConfig, Client, LineDirection,
};
use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_pi-acp");
const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/acp-frames.golden"
);

/// Raw ACP stdout frames captured in wire order.
type Frames = Arc<Mutex<Vec<String>>>;

/// `session_info_update` notifications carrying the client-side queue-depth
/// meta are emitted by the pump right at turn settle, racing the prompt
/// response frame — excluded from the golden (the ordering they'd guard is
/// not part of the #70 contract).
fn is_queue_depth_update(v: &Value) -> bool {
    if v.get("method").and_then(Value::as_str) != Some("session/update") {
        return false;
    }
    let Some(update) = v.get("params").and_then(|p| p.get("update")) else {
        return false;
    };
    update.get("sessionUpdate").and_then(Value::as_str) == Some("session_info_update")
        && update
            .get("_meta")
            .and_then(|m| m.get("piAcp"))
            .and_then(|p| p.get("queueDepth"))
            .is_some()
}

/// Normalize one raw stdout line into its canonical golden form.
fn normalize_frame(raw: &str, cwd: &str) -> Option<String> {
    let mut v: Value = serde_json::from_str(raw).ok()?;
    if is_queue_depth_update(&v) {
        return None;
    }
    // JSON-RPC request ids are per-connection UUIDs — replace with a marker.
    if v.get("id").is_some() {
        v["id"] = Value::String("<ID>".to_string());
    }
    normalize_value(&mut v, cwd);
    serde_json::to_string(&v).ok()
}

fn normalize_value(v: &mut Value, cwd: &str) {
    match v {
        Value::String(s) => {
            // Keep the checked-in golden independent of both path separators
            // and the temp directory representation used by the host OS.
            *s = s.replace(cwd, "<CWD>").replace('\\', "/");
            *s = normalize_pi_version(s);
        }
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                if key == "updatedAt" && value.is_string() {
                    *value = Value::String("<TS>".to_string());
                } else {
                    normalize_value(value, cwd);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_value(item, cwd);
            }
        }
        _ => {}
    }
}

/// `pi v<semver>` header → `pi v<VER>` (the golden guards ordering, not the
/// adapter's own version).
fn normalize_pi_version(s: &str) -> String {
    const PREFIX: &str = "pi v";
    let Some(idx) = s.find(PREFIX) else {
        return s.to_string();
    };
    let rest = &s[idx + PREFIX.len()..];
    let ver_len = rest
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-'))
        .unwrap_or(rest.len());
    let ver = &rest[..ver_len];
    if ver.is_empty() || !ver.as_bytes()[0].is_ascii_digit() {
        return s.to_string();
    }
    format!("{}{}{}", &s[..idx], "pi v<VER>", &rest[ver_len..])
}

/// Drive the fixed flow and return the normalized golden lines.
async fn capture_frames() -> (Vec<String>, String) {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    let agent_dir = tmp.path().join("agent");
    let home = tmp.path().join("home");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(&agent_dir).unwrap();
    fs::create_dir_all(&home).unwrap();
    fs::write(cwd.join("AGENTS.md"), "test context").unwrap();

    let frames: Frames = Arc::new(Mutex::new(Vec::new()));
    let frames_for_cb = frames.clone();
    let agent = AcpAgent::new(
        AcpAgentConfig::new(BIN)
            .env("PI_ACP_MOCK", "1")
            .env("PI_ACP_PI_COMMAND", BIN)
            .env("PI_CODING_AGENT_DIR", agent_dir.to_str().unwrap())
            // Isolate ~/.agents/skills etc. so the prelude is deterministic.
            .env("HOME", home.to_str().unwrap())
            .env("USERPROFILE", home.to_str().unwrap()),
    )
    .with_debug(move |line, direction| {
        if direction == LineDirection::Stdout {
            frames_for_cb.lock().unwrap().push(line.to_string());
        }
    });

    let result = Client
        .builder()
        .name("s8-golden-client")
        .on_receive_notification(
            async move |_notif: SessionNotification, _cx| Ok(()),
            on_receive_notification!(),
        )
        .connect_with(agent, async |cx| {
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let new_session = cx
                .send_request(NewSessionRequest::new(cwd.clone()))
                .block_task()
                .await?;
            let sid = new_session.session_id;
            cx.send_request(PromptRequest::new(
                sid,
                vec![ContentBlock::Text(TextContent::new("hello".to_string()))],
            ))
            .block_task()
            .await?;
            Ok(())
        })
        .await;
    result.expect("ACP golden flow should complete");

    let cwd_str = cwd.to_str().unwrap();
    let lines: Vec<String> = frames
        .lock()
        .unwrap()
        .iter()
        .filter_map(|raw| normalize_frame(raw, cwd_str))
        .collect();
    let body = format!("{}\n", lines.join("\n"));
    (lines, body)
}

#[tokio::test]
async fn acp_frame_sequence_matches_golden() {
    let (_lines, body) = capture_frames().await;

    let golden_path = PathBuf::from(GOLDEN);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        fs::create_dir_all(golden_path.parent().unwrap()).unwrap();
        fs::write(&golden_path, &body).unwrap();
        return;
    }

    let golden = fs::read_to_string(&golden_path).unwrap_or_else(|_| {
        panic!("golden file missing at {GOLDEN}; run with UPDATE_GOLDEN=1 to create it")
    });
    let golden = golden.replace("\r\n", "\n");
    assert_eq!(body, golden, "ACP frame sequence drifted from the golden file\n--- recorded ---\n{body}\n--- golden ---\n{golden}");

    // The ordering property the golden guards, stated explicitly: the startup
    // prelude (with the version-check notice) precedes the session/new
    // response, and the prompt response is last.
    let lines: Vec<&str> = body.lines().collect();
    let find = |needle: &str| lines.iter().position(|l| l.contains(needle));
    let startup = find("pi v<VER>").expect("startup prelude frame");
    // JSON-RPC responses do not echo the method name; identify them by payload.
    let new_resp = find("configOptions").expect("session/new response frame");
    let prompt_resp = find("stopReason").expect("session/prompt response frame");
    assert!(
        startup < new_resp,
        "startup prelude must precede session/new response (D4 / #70)"
    );
    assert!(
        new_resp < prompt_resp,
        "session/new response must precede the prompt"
    );
}

/// The golden's shape is checked too: it must contain exactly the expected
/// frame kinds (guards against silently empty captures).
#[tokio::test]
async fn golden_contains_expected_frame_kinds() {
    let (_lines, body) = capture_frames().await;
    for needle in [
        "authMethods",
        "startupInfo",
        "configOptions",
        "available_commands_update",
        "stopReason",
        "usage_update",
        "pi v<VER>",
    ] {
        assert!(
            body.contains(needle),
            "golden flow missing {needle}:\n{body}"
        );
    }
    // The racy queue-depth frames are filtered out of the golden.
    assert!(
        !body.contains("queueDepth"),
        "queue-depth frames must be filtered:\n{body}"
    );
    // The cwd path is normalized, and no raw timestamps leak through.
    assert!(
        body.contains("<CWD>/AGENTS.md"),
        "cwd must be normalized:\n{body}"
    );
    assert!(
        !body.contains("updatedAt\":\"202"),
        "timestamps must be normalized:\n{body}"
    );
}

/// Placeholder sanity (kept small): normalization replaces paths and versions.
#[test]
fn normalization_is_stable() {
    let mut v = serde_json::json!({
        "params": { "update": { "agentMessageChunk": { "content": { "type": "text", "text": "pi v1.2.3\n- /tmp/x/AGENTS.md" } } } },
        "x": { "updatedAt": "2026-08-31T00:00:00.000Z" }
    });
    normalize_value(&mut v, "/tmp/x");
    let out = serde_json::to_string(&v).unwrap();
    assert!(out.contains("pi v<VER>"), "{out}");
    assert!(out.contains("<CWD>/AGENTS.md"), "{out}");
    assert!(out.contains("\"updatedAt\":\"<TS>\""), "{out}");
    assert!(!out.contains("/tmp/x"), "{out}");

    let mut windows = serde_json::json!({ "path": "C:\\tmp\\x\\AGENTS.md" });
    normalize_value(&mut windows, "C:\\tmp\\x");
    assert_eq!(windows["path"], "<CWD>/AGENTS.md");
}
