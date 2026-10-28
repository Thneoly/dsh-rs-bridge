use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::{json, Value};

static NEXT_STATE_DIR: AtomicU64 = AtomicU64::new(1);

struct BridgeProcess {
    child: Child,
    state_dir: PathBuf,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    notifications: Vec<Value>,
}

impl BridgeProcess {
    fn spawn() -> Self {
        let state_dir = std::env::temp_dir().join(format!(
            "dsh-rs-bridge-stdio-test-{}-{}",
            std::process::id(),
            NEXT_STATE_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&state_dir).expect("create isolated bridge state directory");
        let mut child = Command::new(env!("CARGO_BIN_EXE_dsh-rs-bridge"))
            .arg("--stdio")
            .env("DSH_RS_BRIDGE_MOCK_DELAY_MS", "1")
            .env("DSH_RS_BRIDGE_STATE_DIR", &state_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn bridge process");

        let stdin = child.stdin.take().expect("bridge stdin");
        let stdout = BufReader::new(child.stdout.take().expect("bridge stdout"));
        Self {
            child,
            state_dir,
            stdin,
            stdout,
            notifications: Vec::new(),
        }
    }

    fn send(&mut self, id: &str, method: &str, params: Value) {
        let request = json!({
            "protocol": "dsh.runtime.v1",
            "id": id,
            "method": method,
            "params": params,
        });
        serde_json::to_writer(&mut self.stdin, &request).expect("serialize request");
        self.stdin.write_all(b"\n").expect("write request newline");
        self.stdin.flush().expect("flush request");
    }

    fn response(&mut self, id: &str) -> Value {
        loop {
            let mut line = String::new();
            let count = self
                .stdout
                .read_line(&mut line)
                .expect("read bridge stdout");
            assert_ne!(count, 0, "bridge exited before response id={id}");

            let message: Value = serde_json::from_str(&line)
                .unwrap_or_else(|error| panic!("invalid JSONL output {line:?}: {error}"));
            assert_eq!(message["protocol"], "dsh.runtime.v1");

            if message.get("id").and_then(Value::as_str) == Some(id) {
                return message;
            }
            assert_eq!(
                message["method"], "runtime.event",
                "unexpected uncorrelated output: {message}"
            );
            self.notifications.push(message);
        }
    }

    fn shutdown(mut self) {
        self.send("shutdown", "bridge.shutdown", json!({}));
        let response = self.response("shutdown");
        assert_eq!(response["result"]["stopping"], true);
        drop(self.stdin);
        let status = self.child.wait().expect("wait for bridge process");
        assert!(status.success(), "bridge process exited with {status}");
    }
}

#[test]
fn stdio_protocol_lists_starts_streams_waits_and_shuts_down() {
    let mut bridge = BridgeProcess::spawn();

    bridge.send("list", "runtime.list", json!({}));
    let list = bridge.response("list");
    let runtimes = list["result"].as_array().expect("runtime list");
    assert_eq!(runtimes.len(), 1);
    assert_eq!(runtimes[0]["id"], "mock");
    assert_eq!(runtimes[0]["health"]["status"], "ready");

    bridge.send(
        "start",
        "session.start",
        json!({
            "dsh_session_id": "process-session",
            "runtime_id": "mock",
            "prompt": "exercise real process boundary",
            "cwd": null
        }),
    );
    let started = bridge.response("start");
    assert_eq!(started["result"]["status"], "running");
    assert_eq!(started["result"]["dsh_session_id"], "process-session");
    assert!(started["result"]["external_session_id"].is_string());

    bridge.send(
        "wait",
        "session.wait",
        json!({ "dsh_session_id": "process-session" }),
    );
    let finished = bridge.response("wait");
    assert_eq!(finished["result"]["status"], "completed");

    assert!(
        bridge.notifications.iter().any(|event| {
            event["params"]["event"]["event_type"] == "runtime.started"
                && event["params"]["event"]["dsh_session_id"] == "process-session"
        }),
        "expected runtime.started notification"
    );
    assert!(
        bridge.notifications.iter().any(|event| {
            event["params"]["event"]["event_type"] == "assistant.delta"
                && event["params"]["event"]["dsh_session_id"] == "process-session"
        }),
        "expected assistant.delta notification"
    );
    assert!(
        bridge.notifications.iter().any(|event| {
            event["params"]["event"]["event_type"] == "runtime.ended"
                && event["params"]["event"]["dsh_session_id"] == "process-session"
        }),
        "expected runtime.ended notification"
    );

    let event_sequences: Vec<u64> = bridge
        .notifications
        .iter()
        .filter(|event| event["params"]["event"]["dsh_session_id"] == "process-session")
        .filter_map(|event| event["params"]["host_seq"].as_u64())
        .collect();
    assert!(
        event_sequences.windows(2).all(|pair| pair[0] < pair[1]),
        "host event sequence must be strictly increasing: {event_sequences:?}"
    );

    bridge.shutdown();
}

#[test]
fn stdio_protocol_returns_correlated_errors_for_unknown_methods() {
    let mut bridge = BridgeProcess::spawn();
    bridge.send("unknown", "does.not.exist", json!({}));
    let response = bridge.response("unknown");
    assert_eq!(response["error"]["code"], "configuration");
    assert!(response["error"]["message"]
        .as_str()
        .expect("error message")
        .contains("unknown method"));

    bridge.shutdown();
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
