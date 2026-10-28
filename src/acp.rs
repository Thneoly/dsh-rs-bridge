//! ACP v1 subprocess adapter.
//!
//! The adapter speaks ACP JSON-RPC over a child process's stdin/stdout. It
//! deliberately does not translate ACP's session identity into a DSH session
//! ID: the opaque external ID binds the DSH session to the ACP-owned session.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot, Mutex},
    time::timeout,
};

use crate::contracts::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContinueRequest, RuntimeError, RuntimeEvent,
    RuntimeEventType, RuntimeHealth, RuntimeHealthStatus, RuntimeResumeRequest, RuntimeSendRequest,
    RuntimeSessionControl, RuntimeSessionHandle, RuntimeStartRequest,
};

const ACP_PROTOCOL_VERSION: u64 = 1;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const PROMPT_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
static NEXT_EXTERNAL_SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct AcpRuntimeConfig {
    pub id: String,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
}

impl AcpRuntimeConfig {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            id: "acp".to_owned(),
            command: command.into(),
            args: Vec::new(),
            cwd: None,
            env: HashMap::new(),
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn with_env(mut self, env: HashMap<String, String>) -> Self {
        self.env = env;
        self
    }
}

pub struct AcpRuntime {
    config: AcpRuntimeConfig,
    sessions: Mutex<HashMap<String, Arc<AcpSession>>>,
}

impl AcpRuntime {
    pub fn new(config: AcpRuntimeConfig) -> Self {
        Self {
            config,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    fn ensure_configured(&self) -> Result<(), RuntimeError> {
        if self.config.id.trim().is_empty() {
            return Err(RuntimeError::Configuration(
                "ACP runtime id must not be empty".to_owned(),
            ));
        }
        if self.config.command.trim().is_empty() {
            return Err(RuntimeError::Configuration(
                "ACP runtime command must not be empty".to_owned(),
            ));
        }
        Ok(())
    }

    async fn open_session(
        &self,
        request: &RuntimeStartRequest,
    ) -> Result<Arc<AcpSession>, RuntimeError> {
        let cwd = resolve_cwd(request.cwd.as_deref(), self.config.cwd.as_deref())?;
        let process = AcpProcess::spawn(&self.config, &cwd).await?;

        let initialize = process
            .request(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": {
                        "name": "dsh-rs-bridge",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "clientCapabilities": {}
                }),
            )
            .await;

        let initialized = match initialize {
            Ok(result) => result,
            Err(error) => {
                process.stop().await;
                return Err(error);
            }
        };

        if initialized.get("protocolVersion").and_then(Value::as_u64) != Some(ACP_PROTOCOL_VERSION)
        {
            process.stop().await;
            return Err(RuntimeError::Protocol(format!(
                "ACP protocol version mismatch: expected {ACP_PROTOCOL_VERSION}, received {}",
                initialized
                    .get("protocolVersion")
                    .map(Value::to_string)
                    .unwrap_or_else(|| "missing".to_owned())
            )));
        }

        let created = process
            .request(
                "session/new",
                json!({
                    "cwd": cwd.to_string_lossy(),
                    "mcpServers": []
                }),
            )
            .await;

        let result = match created {
            Ok(result) => result,
            Err(error) => {
                process.stop().await;
                return Err(error);
            }
        };
        let acp_session_id = match result.get("sessionId").and_then(Value::as_str) {
            Some(id) if !id.trim().is_empty() => id.to_owned(),
            _ => {
                process.stop().await;
                return Err(RuntimeError::Protocol(
                    "ACP session/new response is missing sessionId".to_owned(),
                ));
            }
        };

        let nonce = NEXT_EXTERNAL_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        let external_session_id = format!("acp:{nonce}:{acp_session_id}");
        Ok(Arc::new(AcpSession {
            external_session_id,
            acp_session_id,
            process,
            turn_lock: Arc::new(Mutex::new(())),
            next_turn_id: AtomicU64::new(1),
        }))
    }

    async fn attach_prompt(
        &self,
        session: Arc<AcpSession>,
        prompt: String,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        session.begin_prompt(prompt).await
    }
}

#[async_trait]
impl RuntimeAdapter for AcpRuntime {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming_events: true,
            // A new bridge process cannot yet reconstruct the ACP process/session
            // binding. Keep this false until durable binding storage is implemented.
            resume: false,
            interrupt: true,
            // ACP v1 prompt is a request/turn, not an in-flight steering channel.
            live_input: false,
            // A completed turn can be followed by another prompt on the same ACP session.
            continuation: true,
        }
    }

    async fn health_check(&self) -> RuntimeHealth {
        match self.ensure_configured() {
            Ok(()) => RuntimeHealth {
                status: RuntimeHealthStatus::Ready,
                detail: Some(
                    "ACP v1 subprocess configured; availability is checked on launch".to_owned(),
                ),
            },
            Err(error) => RuntimeHealth {
                status: RuntimeHealthStatus::Unavailable,
                detail: Some(error.to_string()),
            },
        }
    }

    async fn start(
        &self,
        request: RuntimeStartRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        self.ensure_configured()?;
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Lifecycle(
                "ACP start cancelled before process launch".to_owned(),
            ));
        }

        let session = self.open_session(&request).await?;
        if request.cancellation.is_cancelled() {
            session.shutdown().await;
            return Err(RuntimeError::Lifecycle(
                "ACP start cancelled after session creation".to_owned(),
            ));
        }

        let handle = match self.attach_prompt(session.clone(), request.prompt).await {
            Ok(handle) => handle,
            Err(error) => {
                session.shutdown().await;
                return Err(error);
            }
        };
        self.sessions
            .lock()
            .await
            .insert(session.external_session_id.clone(), session);
        Ok(handle)
    }

    async fn resume(
        &self,
        _request: RuntimeResumeRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        Err(RuntimeError::Capability(
            "ACP resume is disabled until external session bindings can be restored across bridge restarts"
                .to_owned(),
        ))
    }

    async fn continue_session(
        &self,
        request: RuntimeContinueRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Lifecycle(
                "ACP continuation cancelled before prompt dispatch".to_owned(),
            ));
        }
        let session = self
            .sessions
            .lock()
            .await
            .get(&request.external_session_id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::NotFound(format!(
                    "in-process ACP session binding: {}",
                    request.external_session_id
                ))
            })?;
        self.attach_prompt(session, request.prompt).await
    }

    async fn shutdown(&self) -> Result<(), RuntimeError> {
        let sessions: Vec<Arc<AcpSession>> =
            self.sessions.lock().await.drain().map(|(_, s)| s).collect();
        for session in sessions {
            session.shutdown().await;
        }
        Ok(())
    }
}

fn resolve_cwd(
    request_cwd: Option<&str>,
    config_cwd: Option<&Path>,
) -> Result<PathBuf, RuntimeError> {
    let cwd = request_cwd
        .map(PathBuf::from)
        .or_else(|| config_cwd.map(Path::to_path_buf))
        .unwrap_or(std::env::current_dir().map_err(|error| {
            RuntimeError::Configuration(format!("cannot determine current directory: {error}"))
        })?);
    let absolute = if cwd.is_absolute() {
        cwd
    } else {
        std::env::current_dir()
            .map_err(|error| RuntimeError::Configuration(error.to_string()))?
            .join(cwd)
    };
    Ok(absolute)
}

struct TurnRoute {
    turn_id: u64,
    acp_session_id: String,
    tx: mpsc::Sender<RuntimeEvent>,
    seq: AtomicU64,
}

impl TurnRoute {
    async fn emit(&self, event_type: RuntimeEventType, payload: Value) -> Result<(), RuntimeError> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        self.tx
            .send(RuntimeEvent {
                seq,
                event_type,
                payload,
            })
            .await
            .map_err(|_| RuntimeError::Transport("DSH detached from ACP event stream".to_owned()))
    }

    async fn finish(&self, status: RuntimeStatusForAcp, payload: Value) {
        let event_type = match status {
            RuntimeStatusForAcp::Completed => RuntimeEventType::TurnCompleted,
            RuntimeStatusForAcp::Cancelled => RuntimeEventType::TurnCancelled,
            RuntimeStatusForAcp::Failed => RuntimeEventType::TurnFailed,
        };
        let _ = self.emit(event_type, payload).await;
    }
}

#[derive(Clone, Copy)]
enum RuntimeStatusForAcp {
    Completed,
    Cancelled,
    Failed,
}

struct AcpSession {
    external_session_id: String,
    acp_session_id: String,
    process: Arc<AcpProcess>,
    turn_lock: Arc<Mutex<()>>,
    next_turn_id: AtomicU64,
}

impl AcpSession {
    async fn begin_prompt(
        self: &Arc<Self>,
        prompt: String,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        let turn_guard = self.turn_lock.clone().lock_owned().await;
        if self.process.stopped.load(Ordering::SeqCst) {
            return Err(RuntimeError::Transport(
                "ACP agent process is no longer running".to_owned(),
            ));
        }

        let turn_id = self.next_turn_id.fetch_add(1, Ordering::Relaxed);
        let (tx, events) = mpsc::channel(128);
        let route = Arc::new(TurnRoute {
            turn_id,
            acp_session_id: self.acp_session_id.clone(),
            tx,
            seq: AtomicU64::new(0),
        });
        *self.process.route.lock().await = Some(route.clone());

        let process = self.process.clone();
        let session_id = self.acp_session_id.clone();
        let params = json!({
            "sessionId": session_id,
            "prompt": [{ "type": "text", "text": prompt }]
        });
        let (request_id, response_rx) = match process.send_request("session/prompt", params).await {
            Ok(pending) => pending,
            Err(error) => {
                let mut current = process.route.lock().await;
                if current.as_ref().map(|current| current.turn_id) == Some(route.turn_id) {
                    *current = None;
                }
                return Err(error);
            }
        };

        let task_route = route.clone();
        tokio::spawn(async move {
            let result = process
                .wait_response(request_id, "session/prompt", response_rx)
                .await;

            match result {
                Ok(result) => {
                    let stop_reason = result
                        .get("stopReason")
                        .and_then(Value::as_str)
                        .unwrap_or("end_turn");
                    let (status, payload) = match stop_reason {
                        "cancelled" => (
                            RuntimeStatusForAcp::Cancelled,
                            json!({ "status": "cancelled", "stop_reason": stop_reason }),
                        ),
                        "end_turn" | "max_tokens" | "refusal" => (
                            RuntimeStatusForAcp::Completed,
                            json!({ "status": "completed", "stop_reason": stop_reason }),
                        ),
                        other => (
                            RuntimeStatusForAcp::Failed,
                            json!({
                                "status": "failed",
                                "stop_reason": other,
                                "message": "ACP agent returned an unknown stopReason"
                            }),
                        ),
                    };
                    task_route.finish(status, payload).await;
                }
                Err(error) => {
                    task_route
                        .finish(
                            RuntimeStatusForAcp::Failed,
                            json!({ "status": "failed", "message": error.to_string() }),
                        )
                        .await;
                }
            }

            let mut current = process.route.lock().await;
            if current.as_ref().map(|current| current.turn_id) == Some(task_route.turn_id) {
                *current = None;
            }
            drop(turn_guard);
        });

        let control: Arc<dyn RuntimeSessionControl> = Arc::new(AcpSessionControl {
            process: self.process.clone(),
            acp_session_id: self.acp_session_id.clone(),
            route,
            closed: AtomicBool::new(false),
        });
        Ok(RuntimeSessionHandle {
            external_session_id: self.external_session_id.clone(),
            events,
            control,
        })
    }

    async fn shutdown(&self) {
        // Close is best effort: some ACP agents exit on EOF and others respond to
        // session/close. Process termination remains the final cleanup guarantee.
        let _ = timeout(
            Duration::from_secs(2),
            self.process
                .request("session/close", json!({ "sessionId": self.acp_session_id })),
        )
        .await;
        self.process.stop().await;
    }
}

struct AcpSessionControl {
    process: Arc<AcpProcess>,
    acp_session_id: String,
    route: Arc<TurnRoute>,
    closed: AtomicBool,
}

#[async_trait]
impl RuntimeSessionControl for AcpSessionControl {
    async fn send(&self, _request: RuntimeSendRequest) -> Result<(), RuntimeError> {
        Err(RuntimeError::Capability(
            "ACP v1 adapter does not support live input during an active prompt".to_owned(),
        ))
    }

    async fn interrupt(&self, _reason: &str) -> Result<(), RuntimeError> {
        self.process
            .notify(
                "session/cancel",
                json!({ "sessionId": self.acp_session_id }),
            )
            .await
    }

    async fn close(&self) -> Result<(), RuntimeError> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut current = self.process.route.lock().await;
        if current.as_ref().map(|current| current.turn_id) == Some(self.route.turn_id) {
            *current = None;
        }
        Ok(())
    }
}

struct AcpProcess {
    child: Mutex<Child>,
    stdin: Mutex<Option<ChildStdin>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, RuntimeError>>>>,
    route: Mutex<Option<Arc<TurnRoute>>>,
    next_request_id: AtomicU64,
    stopped: AtomicBool,
}

impl AcpProcess {
    async fn spawn(config: &AcpRuntimeConfig, cwd: &Path) -> Result<Arc<Self>, RuntimeError> {
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .current_dir(cwd)
            .envs(&config.env)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        let mut child = command.spawn().map_err(|error| {
            RuntimeError::Transport(format!(
                "failed to launch ACP command {:?}: {error}",
                config.command
            ))
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::Transport("ACP child stdin was not piped".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::Transport("ACP child stdout was not piped".to_owned()))?;

        let process = Arc::new(Self {
            child: Mutex::new(child),
            stdin: Mutex::new(Some(stdin)),
            pending: Mutex::new(HashMap::new()),
            route: Mutex::new(None),
            next_request_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&process);
        tokio::spawn(async move {
            Self::read_loop(weak, stdout).await;
        });
        Ok(process)
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, RuntimeError> {
        let (id, response_rx) = self.send_request(method, params).await?;
        self.wait_response(id, method, response_rx).await
    }

    async fn send_request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(u64, oneshot::Receiver<Result<Value, RuntimeError>>), RuntimeError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(RuntimeError::Transport(
                "ACP process has stopped".to_owned(),
            ));
        }
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        if let Err(error) = self
            .write_message(json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            }))
            .await
        {
            self.pending.lock().await.remove(&id);
            return Err(error);
        }
        Ok((id, rx))
    }

    async fn wait_response(
        &self,
        id: u64,
        method: &str,
        response_rx: oneshot::Receiver<Result<Value, RuntimeError>>,
    ) -> Result<Value, RuntimeError> {
        let limit = if method == "session/prompt" {
            PROMPT_TIMEOUT
        } else {
            REQUEST_TIMEOUT
        };
        match timeout(limit, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(RuntimeError::Transport(format!(
                "ACP response channel closed for {method}"
            ))),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(RuntimeError::Transport(format!(
                    "ACP request timed out: {method}"
                )))
            }
        }
    }

    async fn notify(&self, method: &str, params: Value) -> Result<(), RuntimeError> {
        self.write_message(json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }))
        .await
    }

    async fn write_message(&self, message: Value) -> Result<(), RuntimeError> {
        let mut line = serde_json::to_vec(&message)
            .map_err(|error| RuntimeError::Protocol(error.to_string()))?;
        line.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        let writer = stdin
            .as_mut()
            .ok_or_else(|| RuntimeError::Transport("ACP child stdin is closed".to_owned()))?;
        writer
            .write_all(&line)
            .await
            .map_err(|error| RuntimeError::Transport(format!("write ACP request: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| RuntimeError::Transport(format!("flush ACP request: {error}")))
    }

    async fn read_loop(weak: Weak<Self>, stdout: ChildStdout) {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => break,
                Err(error) => {
                    if let Some(process) = weak.upgrade() {
                        process
                            .fail_pending(RuntimeError::Transport(format!(
                                "read ACP stdout: {error}"
                            )))
                            .await;
                    }
                    return;
                }
            };
            let Some(process) = weak.upgrade() else {
                return;
            };
            let message: Value = match serde_json::from_str(&line) {
                Ok(message) => message,
                Err(error) => {
                    process
                        .fail_pending(RuntimeError::Protocol(format!(
                            "invalid ACP JSON-RPC line: {error}"
                        )))
                        .await;
                    continue;
                }
            };
            if let Err(error) = process.handle_message(message).await {
                eprintln!("dsh-rs-bridge: ACP message handling failed: {error}");
            }
        }

        if let Some(process) = weak.upgrade() {
            if !process.stopped.load(Ordering::SeqCst) {
                process
                    .fail_pending(RuntimeError::Transport(
                        "ACP agent closed stdout unexpectedly".to_owned(),
                    ))
                    .await;
            }
        }
    }

    async fn handle_message(&self, message: Value) -> Result<(), RuntimeError> {
        if let Some(method) = message.get("method").and_then(Value::as_str) {
            if let Some(id) = message.get("id").cloned() {
                let response = if method == "session/request_permission" {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": { "outcome": "cancelled" }
                    })
                } else {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": format!("unsupported ACP client request: {method}")
                        }
                    })
                };
                return self.write_message(response).await;
            }
            if method == "session/update" {
                self.handle_session_update(message.get("params").unwrap_or(&Value::Null))
                    .await?;
            }
            return Ok(());
        }

        let Some(id) = message.get("id").and_then(Value::as_u64) else {
            return Ok(());
        };
        let pending = self.pending.lock().await.remove(&id);
        if let Some(sender) = pending {
            let result = if let Some(error) = message.get("error") {
                Err(RuntimeError::Transport(format!(
                    "ACP JSON-RPC error {}: {}",
                    error
                        .get("code")
                        .map(Value::to_string)
                        .unwrap_or_else(|| "unknown".to_owned()),
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unspecified error")
                )))
            } else {
                Ok(message.get("result").cloned().unwrap_or(Value::Null))
            };
            let _ = sender.send(result);
        }
        Ok(())
    }

    async fn handle_session_update(&self, params: &Value) -> Result<(), RuntimeError> {
        let session_id = params.get("sessionId").and_then(Value::as_str);
        let update = params.get("update").unwrap_or(&Value::Null);
        let Some(route) = self.route.lock().await.clone() else {
            return Ok(());
        };
        if session_id != Some(route.acp_session_id.as_str()) {
            return Ok(());
        }

        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        match kind {
            "agent_message_chunk" => {
                let content = update.get("content").unwrap_or(&Value::Null);
                let text = content
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| content.as_str().map(str::to_owned));
                if let Some(text) = text {
                    route
                        .emit(RuntimeEventType::AssistantDelta, json!({ "text": text }))
                        .await?;
                }
            }
            "agent_message" => {
                let content = update.get("content").unwrap_or(&Value::Null);
                let text = content
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| content.as_str().map(str::to_owned));
                if let Some(text) = text {
                    route
                        .emit(RuntimeEventType::AssistantMessage, json!({ "text": text }))
                        .await?;
                }
            }
            "tool_call" => {
                route
                    .emit(RuntimeEventType::ToolStarted, update.clone())
                    .await?;
            }
            "tool_call_update" => {
                let state = update.get("status").and_then(Value::as_str).unwrap_or("");
                let event_type = if matches!(state, "completed" | "failed") {
                    RuntimeEventType::ToolCompleted
                } else {
                    RuntimeEventType::ToolStarted
                };
                route.emit(event_type, update.clone()).await?;
            }
            _ => {
                route
                    .emit(
                        RuntimeEventType::RuntimeStatus,
                        json!({ "acp_session_update": kind, "update": update }),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn fail_pending(&self, error: RuntimeError) {
        let mut pending = self.pending.lock().await;
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(RuntimeError::Transport(error.to_string())));
        }
    }

    async fn stop(&self) {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        self.stdin.lock().await.take();
        let mut child = self.child.lock().await;
        let _ = child.start_kill();
        let _ = timeout(SHUTDOWN_TIMEOUT, child.wait()).await;
        self.fail_pending(RuntimeError::Transport("ACP process stopped".to_owned()))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_cwd;
    use std::path::Path;

    #[test]
    fn cwd_is_normalized_to_absolute_path() {
        let cwd = resolve_cwd(Some("."), None).expect("resolve cwd");
        assert!(cwd.is_absolute());
    }

    #[test]
    fn config_cwd_is_used_when_request_cwd_is_missing() {
        let cwd = resolve_cwd(None, Some(Path::new("."))).expect("resolve cwd");
        assert!(cwd.is_absolute());
    }
}
