use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::{json, Value};

const PROTOCOL: &str = "dsh.runtime.v1";
static NEXT_STATE_DIR: AtomicU64 = AtomicU64::new(1);

struct BridgeProcess {
    child: Child,
    state_dir: Option<PathBuf>,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    notifications: Vec<Value>,
}

impl BridgeProcess {
    fn spawn() -> Self {
        let state_dir = std::env::temp_dir().join(format!(
            "dsh-rs-bridge-process-test-{}-{}",
            std::process::id(),
            NEXT_STATE_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&state_dir).expect("create isolated bridge state directory");
        Self::spawn_in(state_dir)
    }

    fn spawn_in(state_dir: PathBuf) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_dsh-rs-bridge"))
            .arg("--stdio")
            .env("DSH_RS_BRIDGE_MOCK_DELAY_MS", "60")
            .env("DSH_RS_BRIDGE_STATE_DIR", &state_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn real dsh-rs-bridge process");
        let stdin = child.stdin.take().expect("child stdin");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout"));
        Self {
            child,
            state_dir: Some(state_dir),
            stdin: Some(stdin),
            stdout,
            next_id: 0,
            notifications: Vec::new(),
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = format!("request-{}", self.next_id);
        let request = json!({
            "protocol": PROTOCOL,
            "id": id,
            "method": method,
            "params": params
        });
        let stdin = self.stdin.as_mut().expect("bridge stdin still open");
        writeln!(stdin, "{request}").expect("write JSONL request");
        stdin.flush().expect("flush JSONL request");

        loop {
            let mut line = String::new();
            let bytes = self
                .stdout
                .read_line(&mut line)
                .expect("read bridge stdout line");
            assert!(bytes > 0, "bridge exited before responding to {method}");

            let message: Value =
                serde_json::from_str(&line).expect("bridge stdout must contain valid JSON");
            if message["protocol"] != PROTOCOL {
                panic!("unexpected protocol envelope: {message}");
            }
            if message["method"] == "runtime.event" {
                self.notifications.push(message);
                continue;
            }
            if message["id"] == id {
                if let Some(error) = message.get("error") {
                    panic!("bridge method {method} failed: {error}");
                }
                return message["result"].clone();
            }
        }
    }

    fn shutdown_and_wait(&mut self) {
        let result = self.call("bridge.shutdown", json!({}));
        assert_eq!(result["stopping"], true);
        self.stdin.take();
        let status = self.child.wait().expect("wait for bridge exit");
        assert!(status.success(), "bridge exited unsuccessfully: {status}");
        if let Some(state_dir) = self.state_dir.take() {
            let _ = std::fs::remove_dir_all(state_dir);
        }
    }

    /// Shut down but keep the state directory for a successor bridge.
    fn shutdown_and_keep(&mut self) {
        let result = self.call("bridge.shutdown", json!({}));
        assert_eq!(result["stopping"], true);
        self.stdin.take();
        let status = self.child.wait().expect("wait for bridge exit");
        assert!(status.success(), "bridge exited unsuccessfully: {status}");
        self.state_dir = None;
    }
}

impl Drop for BridgeProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(state_dir) = self.state_dir.take() {
            let _ = std::fs::remove_dir_all(state_dir);
        }
    }
}

#[test]
fn jsonl_process_supports_runtime_discovery_start_wait_continue_cancel_and_shutdown() {
    let mut bridge = BridgeProcess::spawn();

    let runtimes = bridge.call("runtime.list", json!({}));
    assert_eq!(runtimes.as_array().map(Vec::len), Some(1));
    assert_eq!(runtimes[0]["id"], "mock");
    assert_eq!(runtimes[0]["capabilities"]["continuation"], true);
    assert_eq!(runtimes[0]["capabilities"]["live_input"], false);

    let first = bridge.call(
        "session.start",
        json!({
            "dsh_session_id": "process-session",
            "runtime_id": "mock",
            "prompt": "first prompt",
            "cwd": null
        }),
    );
    let external_session_id = first["external_session_id"].clone();
    assert_eq!(first["status"], "running");

    let first_terminal = bridge.call(
        "session.wait",
        json!({ "dsh_session_id": "process-session" }),
    );
    assert_eq!(first_terminal["status"], "completed");

    let sent = bridge.call(
        "session.send",
        json!({
            "dsh_session_id": "process-session",
            "text": "second prompt"
        }),
    );
    assert_eq!(sent["accepted"], true);

    let second_terminal = bridge.call(
        "session.wait",
        json!({ "dsh_session_id": "process-session" }),
    );
    assert_eq!(second_terminal["status"], "completed");
    assert_eq!(
        second_terminal["external_session_id"], external_session_id,
        "continuation must retain the runtime-owned session identity"
    );

    let cancel_start = bridge.call(
        "session.start",
        json!({
            "dsh_session_id": "cancel-session",
            "runtime_id": "mock",
            // The hang marker keeps the turn from self-completing, so the
            // cancel below deterministically interrupts a live turn instead
            // of racing the mock delay window.
            "prompt": "hang: cancel this",
            "cwd": null
        }),
    );
    assert_eq!(cancel_start["status"], "running");
    let cancelled = bridge.call(
        "session.cancel",
        json!({
            "dsh_session_id": "cancel-session",
            "reason": "process integration test"
        }),
    );
    assert_eq!(cancelled["status"], "cancelled");

    let mut last_seq_by_session = HashMap::<String, u64>::new();
    let mut saw_started = false;
    let mut saw_continued = false;
    let mut saw_assistant_delta = false;
    let mut saw_cancelled = false;
    for notification in &bridge.notifications {
        let event = &notification["params"]["event"];
        let session_id = event["dsh_session_id"]
            .as_str()
            .expect("event carries DSH session id")
            .to_owned();
        let seq = notification["params"]["host_seq"]
            .as_u64()
            .expect("event carries host sequence");
        if let Some(previous) = last_seq_by_session.insert(session_id, seq) {
            assert!(seq > previous, "host sequence must increase per session");
        }
        match event["event_type"].as_str() {
            Some("runtime.started") => saw_started = true,
            Some("runtime.continued") => saw_continued = true,
            Some("assistant.delta") if event["payload"]["text"] == "continued: second prompt" => {
                saw_assistant_delta = true
            }
            Some("turn.cancelled") => saw_cancelled = true,
            _ => {}
        }
    }
    assert!(saw_started, "must observe runtime.started notification");
    assert!(saw_continued, "must observe continuation notification");
    assert!(
        saw_assistant_delta,
        "must observe output from the continued turn"
    );
    assert!(
        saw_cancelled,
        "must observe the cancellation terminal event"
    );

    bridge.shutdown_and_wait();
}

#[test]
fn jsonl_process_replays_events_across_restart_with_continuous_sequence() {
    let state_dir = std::env::temp_dir().join(format!(
        "dsh-rs-bridge-process-restart-{}-{}",
        std::process::id(),
        NEXT_STATE_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&state_dir).expect("create shared bridge state directory");

    let mut first = BridgeProcess::spawn_in(state_dir.clone());
    let started = first.call(
        "session.start",
        json!({
            "dsh_session_id": "events-session",
            "runtime_id": "mock",
            "prompt": "before restart",
            "cwd": null
        }),
    );
    assert_eq!(started["status"], "running");
    let terminal = first.call(
        "session.wait",
        json!({ "dsh_session_id": "events-session" }),
    );
    assert_eq!(terminal["status"], "completed");
    first.shutdown_and_keep();
    drop(first);

    // The successor bridge replays the durable event journal: the full
    // pre-restart history is available and host_seq does not restart at 1.
    let mut second = BridgeProcess::spawn_in(state_dir);
    let replayed = second.call(
        "session.events",
        json!({ "dsh_session_id": "events-session" }),
    );
    let entries = replayed.as_array().expect("replayed events array");
    assert!(!entries.is_empty(), "pre-restart events must survive");
    assert!(entries
        .iter()
        .any(|entry| entry["event"]["event_type"] == "assistant.delta"));
    assert!(entries
        .iter()
        .any(|entry| entry["event"]["event_type"] == "turn.completed"));
    let before_max = entries
        .iter()
        .map(|entry| entry["host_seq"].as_u64().expect("host seq"))
        .max()
        .expect("max host seq");

    let sent = second.call(
        "session.send",
        json!({
            "dsh_session_id": "events-session",
            "text": "after restart"
        }),
    );
    assert_eq!(sent["accepted"], true);
    let terminal = second.call(
        "session.wait",
        json!({ "dsh_session_id": "events-session" }),
    );
    assert_eq!(terminal["status"], "completed");

    let tail = second.call(
        "session.events",
        json!({
            "dsh_session_id": "events-session",
            "after_host_seq": before_max
        }),
    );
    let tail_entries = tail.as_array().expect("tail events array");
    assert!(!tail_entries.is_empty(), "post-restart events must replay");
    assert_eq!(
        tail_entries[0]["host_seq"].as_u64(),
        Some(before_max + 1),
        "host_seq must continue across the restart, not restart at 1"
    );
    assert!(tail_entries
        .iter()
        .all(|entry| entry["host_seq"].as_u64().unwrap_or(0) > before_max));

    // The stdout runtime.event notifications and the journal must share ONE
    // numbering space: the after_host_seq catch-up protocol only works if a
    // notification's host_seq is the same number the journal replays.
    let notified_seqs: Vec<u64> = second
        .notifications
        .iter()
        .filter(|notification| {
            notification["params"]["event"]["dsh_session_id"] == "events-session"
        })
        .map(|notification| {
            notification["params"]["host_seq"]
                .as_u64()
                .expect("notification host seq")
        })
        .collect();
    assert!(
        !notified_seqs.is_empty(),
        "post-restart turn must notify events"
    );
    let full_replay = second.call(
        "session.events",
        json!({ "dsh_session_id": "events-session" }),
    );
    let replay_seqs: std::collections::HashSet<u64> = full_replay
        .as_array()
        .expect("full replay array")
        .iter()
        .map(|entry| entry["host_seq"].as_u64().expect("replay host seq"))
        .collect();
    for seq in &notified_seqs {
        assert!(
            replay_seqs.contains(seq),
            "stdout host_seq {seq} must exist in the journal replay"
        );
    }
    assert_eq!(
        notified_seqs.iter().max(),
        replay_seqs.iter().max(),
        "stdout notifications and journal replay must share the numbering space"
    );

    second.shutdown_and_wait();
}

#[test]
fn second_bridge_refuses_to_share_the_state_directory() {
    let state_dir = std::env::temp_dir().join(format!(
        "dsh-rs-bridge-process-exclusive-{}-{}",
        std::process::id(),
        NEXT_STATE_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&state_dir).expect("create shared bridge state directory");
    let first = BridgeProcess::spawn_in(state_dir.clone());

    // A second bridge on the same state directory must refuse to start:
    // silent dual writers would duplicate host_seq values in the journals.
    let mut second = BridgeProcess::spawn_in(state_dir.clone());
    let mut exited = None;
    for _ in 0..100 {
        if let Some(status) = second.child.try_wait().expect("poll second bridge") {
            exited = Some(status);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let status = exited.expect("second bridge must exit when the state dir is locked");
    assert!(
        !status.success(),
        "the second bridge must fail loudly instead of sharing the journals"
    );
    drop(second);
    drop(first);
    let _ = std::fs::remove_dir_all(state_dir);
}
