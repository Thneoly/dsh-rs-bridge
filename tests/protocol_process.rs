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
    state_dir: PathBuf,
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
            state_dir,
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
        let _ = std::fs::remove_dir_all(&self.state_dir);
    }
}

impl Drop for BridgeProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.state_dir);
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
            "prompt": "cancel this",
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
