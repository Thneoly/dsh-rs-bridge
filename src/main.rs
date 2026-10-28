use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use dsh_rs_bridge::{
    AcpRuntime, AcpRuntimeConfig, ExecutionEventEnvelope, ExecutionEventSink, FileSessionStore,
    MockRuntime, PersistedExecutionEvent, ResumeExecutionRequest, RuntimeError, RuntimeSupervisor,
    StartExecutionRequest,
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

struct JsonlEventSink {
    writer: JsonlWriter,
    sequences: Mutex<HashMap<String, u64>>,
}

impl JsonlEventSink {
    fn new(writer: JsonlWriter) -> Self {
        Self {
            writer,
            sequences: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl ExecutionEventSink for JsonlEventSink {
    async fn append(&self, event: ExecutionEventEnvelope) -> Result<u64, RuntimeError> {
        let host_seq = {
            let mut sequences = self.sequences.lock().await;
            let sequence = sequences.entry(event.dsh_session_id.clone()).or_insert(0);
            *sequence += 1;
            *sequence
        };
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

async fn run_stdio() -> Result<(), RuntimeError> {
    let writer = JsonlWriter::new();
    let event_sink = Arc::new(JsonlEventSink::new(writer.clone()));
    let state_dir = std::env::var_os("DSH_RS_BRIDGE_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".dsh-rs-bridge"));
    let sessions = Arc::new(FileSessionStore::open(state_dir.join("sessions.jsonl")).await?);
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
        pending.spawn(async move {
            let id = request.id.clone();
            let result = dispatch(&task_supervisor, &request).await;
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
