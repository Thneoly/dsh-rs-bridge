use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use dsh_rs_bridge::{
    AcpRuntime, AcpRuntimeConfig, ExecutionEventEnvelope, ExecutionEventSink, FileEventJournal,
    FileSessionStore, MockRuntime, PersistedExecutionEvent, ResumeExecutionRequest, RuntimeError,
    RuntimeSupervisor, StartExecutionRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

const PROTOCOL: &str = "dsh.runtime.v1";

#[derive(Clone)]
struct JsonlWriter {
    stdout: Arc<Mutex<tokio::io::Stdout>>,
}

impl JsonlWriter {
    fn new() -> Self {
        Self {
            stdout: Arc::new(Mutex::new(tokio::io::stdout())),
        }
    }

    async fn write_value(&self, value: &Value) -> Result<(), RuntimeError> {
        let mut stdout = self.stdout.lock().await;
        let mut line =
            serde_json::to_vec(value).map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        line.push(b'\n');
        stdout
            .write_all(&line)
            .await
            .map_err(|error| RuntimeError::Transport(error.to_string()))?;
        stdout
            .flush()
            .await
            .map_err(|error| RuntimeError::Transport(error.to_string()))
    }
}

/// Durable event sink: every event is journaled first (which allocates the
/// restart-continuous host_seq), then forwarded to the host on stdout. The
/// stdout notification carries the same sequence number the journal holds.
struct DurableEventSink {
    journal: std::sync::Arc<FileEventJournal>,
    writer: JsonlWriter,
}

#[async_trait::async_trait]
impl ExecutionEventSink for DurableEventSink {
    async fn append(&self, event: ExecutionEventEnvelope) -> Result<u64, RuntimeError> {
        let host_seq = self.journal.append(event.clone()).await?;
        let persisted = PersistedExecutionEvent { host_seq, event };
        self.writer
            .write_value(&json!({
                "protocol": PROTOCOL,
                "method": "runtime.event",
                "params": persisted
            }))
            .await?;
        Ok(host_seq)
    }
}

#[derive(Debug, Deserialize)]
struct RpcRequest {
    protocol: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct RpcError {
    code: &'static str,
    message: String,
}

#[derive(Debug, Deserialize)]
struct StartParams {
    dsh_session_id: String,
    runtime_id: String,
    prompt: String,
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SessionParams {
    dsh_session_id: String,
}

#[derive(Debug, Deserialize)]
struct CancelParams {
    dsh_session_id: String,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SendParams {
    dsh_session_id: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct SessionEventsParams {
    dsh_session_id: String,
    #[serde(default)]
    after_host_seq: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PermissionRespondParams {
    dsh_session_id: String,
    request_id: String,
    option_id: String,
}

#[derive(Debug, Deserialize)]
struct ProbeParams {
    runtime_id: String,
}

async fn dispatch(
    supervisor: &RuntimeSupervisor,
    journal: &FileEventJournal,
    request: &RpcRequest,
) -> Result<Value, RuntimeError> {
    match request.method.as_str() {
        "runtime.list" => serde_json::to_value(supervisor.list_runtimes().await?)
            .map_err(|error| RuntimeError::Protocol(error.to_string())),
        "runtime.probe" => {
            let params: ProbeParams = parse_params(&request.params)?;
            let runtimes = supervisor.list_runtimes().await?;
            runtimes
                .into_iter()
                .find(|runtime| runtime.id == params.runtime_id)
                .map(|runtime| serde_json::to_value(runtime).unwrap_or(Value::Null))
                .ok_or_else(|| RuntimeError::NotFound(format!("runtime: {}", params.runtime_id)))
        }
        "session.start" => {
            let params: StartParams = parse_params(&request.params)?;
            let summary = supervisor
                .start(StartExecutionRequest {
                    dsh_session_id: params.dsh_session_id,
                    runtime_id: params.runtime_id,
                    prompt: params.prompt,
                    cwd: params.cwd,
                    cancellation: CancellationToken::new(),
                })
                .await?;
            serde_json::to_value(summary).map_err(|error| RuntimeError::Protocol(error.to_string()))
        }
        "session.resume" => {
            let params: SessionParams = parse_params(&request.params)?;
            let summary = supervisor
                .resume(ResumeExecutionRequest {
                    dsh_session_id: params.dsh_session_id,
                    cancellation: CancellationToken::new(),
                })
                .await?;
            serde_json::to_value(summary).map_err(|error| RuntimeError::Protocol(error.to_string()))
        }
        "session.restore" => {
            let params: SessionParams = parse_params(&request.params)?;
            let summary = supervisor.restore_session(&params.dsh_session_id).await?;
            serde_json::to_value(summary).map_err(|error| RuntimeError::Protocol(error.to_string()))
        }
        "session.cancel" => {
            let params: CancelParams = parse_params(&request.params)?;
            let summary = supervisor
                .interrupt(
                    &params.dsh_session_id,
                    params.reason.as_deref().unwrap_or("cancelled by DSH host"),
                )
                .await?;
            serde_json::to_value(summary).map_err(|error| RuntimeError::Protocol(error.to_string()))
        }
        "session.wait" => {
            let params: SessionParams = parse_params(&request.params)?;
            let summary = supervisor.wait_for_terminal(&params.dsh_session_id).await?;
            serde_json::to_value(summary).map_err(|error| RuntimeError::Protocol(error.to_string()))
        }
        "session.send" => {
            let params: SendParams = parse_params(&request.params)?;
            supervisor.send(&params.dsh_session_id, params.text).await?;
            Ok(json!({ "accepted": true }))
        }
        "permission.respond" => {
            let params: PermissionRespondParams = parse_params(&request.params)?;
            supervisor
                .respond_permission(
                    &params.dsh_session_id,
                    &params.request_id,
                    &params.option_id,
                )
                .await?;
            Ok(json!({ "delivered": true }))
        }
        "session.events" => {
            let params: SessionEventsParams = parse_params(&request.params)?;
            let events = journal
                .replay(&params.dsh_session_id, params.after_host_seq)
                .await?;
            serde_json::to_value(events).map_err(|error| RuntimeError::Protocol(error.to_string()))
        }
        "bridge.shutdown" => {
            supervisor.shutdown().await?;
            Ok(json!({ "stopping": true }))
        }
        method => Err(RuntimeError::Configuration(format!(
            "unknown method: {method}"
        ))),
    }
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: &Value) -> Result<T, RuntimeError> {
    serde_json::from_value(params.clone())
        .map_err(|error| RuntimeError::Protocol(format!("invalid params: {error}")))
}

async fn write_response(
    writer: &JsonlWriter,
    id: &Value,
    result: Result<Value, RuntimeError>,
) -> Result<(), RuntimeError> {
    let response = match result {
        Ok(result) => json!({
            "protocol": PROTOCOL,
            "id": id,
            "result": result
        }),
        Err(error) => json!({
            "protocol": PROTOCOL,
            "id": id,
            "error": RpcError {
                code: error.code(),
                message: error.to_string()
            }
        }),
    };
    writer.write_value(&response).await
}

/// Exclusive ownership of the state directory. The journals assume a single
/// writer: two bridge processes sharing `DSH_RS_BRIDGE_STATE_DIR` would
/// allocate duplicate `host_seq` values silently. The guard is an OS-level
/// exclusive lock on `bridge.lock` held for the process lifetime: the OS
/// releases it when the process dies, so a crashed bridge never leaves a
/// stale lock behind and a respawned successor takes over cleanly.
struct StateDirGuard {
    _file: std::fs::File,
}

fn acquire_state_dir(state_dir: &std::path::Path) -> Result<StateDirGuard, RuntimeError> {
    use fs4::fs_std::FileExt;

    std::fs::create_dir_all(state_dir).map_err(|error| {
        RuntimeError::Persistence(format!(
            "create state directory {}: {error}",
            state_dir.display()
        ))
    })?;
    let lock_path = state_dir.join("bridge.lock");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| {
            RuntimeError::Persistence(format!("open lock file {}: {error}", lock_path.display()))
        })?;
    // fs4's try_lock_exclusive returns Ok(false) — not Err — when another
    // process holds the lock; only Ok(true) means the directory is ours.
    match file.try_lock_exclusive() {
        Ok(true) => Ok(StateDirGuard { _file: file }),
        Ok(false) => Err(RuntimeError::Persistence(format!(
            "another dsh-rs-bridge owns the state directory {}. \
             Stop the other bridge before starting a replacement.",
            state_dir.display()
        ))),
        Err(error) => Err(RuntimeError::Persistence(format!(
            "lock the state directory {}: {error}",
            state_dir.display()
        ))),
    }
}

async fn run_stdio() -> Result<(), RuntimeError> {
    let writer = JsonlWriter::new();
    let state_dir = std::env::var_os("DSH_RS_BRIDGE_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".dsh-rs-bridge"));
    let _state_guard = acquire_state_dir(&state_dir)?;
    let sessions = Arc::new(FileSessionStore::open(state_dir.join("sessions.jsonl")).await?);
    let journal = Arc::new(FileEventJournal::open(state_dir.join("events.jsonl")).await?);
    let event_sink = Arc::new(DurableEventSink {
        journal: journal.clone(),
        writer: writer.clone(),
    });
    let supervisor = RuntimeSupervisor::new(sessions, event_sink);
    let delay_ms = std::env::var("DSH_RS_BRIDGE_MOCK_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5);
    supervisor.register(Arc::new(
        MockRuntime::new("mock")
            .with_continuation(true)
            .with_tool_events(std::env::var("DSH_RS_BRIDGE_MOCK_TOOL_EVENTS").is_ok())
            .with_delay(Duration::from_millis(delay_ms)),
    ))?;

    // ACP is opt-in so the bridge remains runnable without an installed agent.
    if let Ok(command) = std::env::var("DSH_RS_BRIDGE_ACP_COMMAND") {
        if !command.trim().is_empty() {
            let mut config = AcpRuntimeConfig::new(command);
            if let Ok(args) = std::env::var("DSH_RS_BRIDGE_ACP_ARGS") {
                config.args = serde_json::from_str::<Vec<String>>(&args).map_err(|error| {
                    RuntimeError::Configuration(format!(
                        "DSH_RS_BRIDGE_ACP_ARGS must be a JSON string array: {error}"
                    ))
                })?;
            }
            if let Ok(cwd) = std::env::var("DSH_RS_BRIDGE_ACP_CWD") {
                if !cwd.trim().is_empty() {
                    config.cwd = Some(cwd.into());
                }
            }
            if let Ok(env) = std::env::var("DSH_RS_BRIDGE_ACP_ENV") {
                config.env =
                    serde_json::from_str::<HashMap<String, String>>(&env).map_err(|error| {
                        RuntimeError::Configuration(format!(
                            "DSH_RS_BRIDGE_ACP_ENV must be a JSON string map: {error}"
                        ))
                    })?;
            }
            supervisor.register(Arc::new(AcpRuntime::new(config)))?;
        }
    }

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut pending = JoinSet::new();
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|error| RuntimeError::Transport(error.to_string()))?
    {
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                write_response(
                    &writer,
                    &Value::Null,
                    Err(RuntimeError::Protocol(format!(
                        "invalid JSONL request: {error}"
                    ))),
                )
                .await?;
                continue;
            }
        };
        let id = request.id.clone();
        if request.protocol != PROTOCOL {
            write_response(
                &writer,
                &id,
                Err(RuntimeError::Protocol(format!(
                    "unsupported protocol: {}",
                    request.protocol
                ))),
            )
            .await?;
            continue;
        }

        let shutdown = request.method == "bridge.shutdown";
        let task_writer = writer.clone();
        let task_supervisor = supervisor.clone();
        let task_journal = journal.clone();
        pending.spawn(async move {
            let id = request.id.clone();
            let result = dispatch(&task_supervisor, &task_journal, &request).await;
            if let Err(error) = write_response(&task_writer, &id, result).await {
                eprintln!("dsh-rs-bridge: response write failed: {error}");
            }
        });
        if shutdown {
            break;
        }
    }

    while let Some(result) = pending.join_next().await {
        if let Err(error) = result {
            eprintln!("dsh-rs-bridge: request task failed: {error}");
        }
    }

    // The OS-level state directory lock releases when the process exits,
    // orderly or not: a successor (or respawned) bridge takes over cleanly.
    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--version") => println!("{}", env!("CARGO_PKG_VERSION")),
        Some("--probe") => println!(
            "{}",
            json!({
                "protocol": PROTOCOL,
                "runtime_id": "dsh-rs-bridge",
                "version": env!("CARGO_PKG_VERSION"),
                "capabilities": [
                    "runtime.list",
                    "runtime.probe",
                    "session.start",
                    "session.resume",
                    "session.cancel",
                    "session.wait",
                    "session.send",
                    "session.events",
                    "permission.respond"
                ]
            })
        ),
        Some("--stdio") => {
            if let Err(error) = run_stdio().await {
                eprintln!("dsh-rs-bridge: {error}");
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("Usage: dsh-rs-bridge --probe | --stdio | --version");
            std::process::exit(2);
        }
    }
}
