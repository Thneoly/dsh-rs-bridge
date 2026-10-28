use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatus {
    Starting,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl RuntimeStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeEventType {
    #[serde(rename = "assistant.delta")]
    AssistantDelta,
    #[serde(rename = "assistant.message")]
    AssistantMessage,
    #[serde(rename = "tool.started")]
    ToolStarted,
    #[serde(rename = "tool.completed")]
    ToolCompleted,
    #[serde(rename = "turn.completed")]
    TurnCompleted,
    #[serde(rename = "turn.cancelled")]
    TurnCancelled,
    #[serde(rename = "turn.failed")]
    TurnFailed,
    #[serde(rename = "runtime.status")]
    RuntimeStatus,
}

impl RuntimeEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AssistantDelta => "assistant.delta",
            Self::AssistantMessage => "assistant.message",
            Self::ToolStarted => "tool.started",
            Self::ToolCompleted => "tool.completed",
            Self::TurnCompleted => "turn.completed",
            Self::TurnCancelled => "turn.cancelled",
            Self::TurnFailed => "turn.failed",
            Self::RuntimeStatus => "runtime.status",
        }
    }

    pub fn terminal_status(self) -> Option<RuntimeStatus> {
        match self {
            Self::TurnCompleted => Some(RuntimeStatus::Completed),
            Self::TurnCancelled => Some(RuntimeStatus::Cancelled),
            Self::TurnFailed => Some(RuntimeStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeEvent {
    /// Strictly increasing within one attached event stream.
    pub seq: u64,
    #[serde(rename = "type")]
    pub event_type: RuntimeEventType,
    /// Event payload must be a JSON object.
    pub payload: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilities {
    pub streaming_events: bool,
    /// Resume an interrupted execution rather than only reattaching its session.
    pub resume: bool,
    /// Reattach an existing runtime-owned conversation after the bridge restarts.
    pub session_restore: bool,
    pub interrupt: bool,
    /// Accept steering/follow-up input while an execution is still active.
    pub live_input: bool,
    /// Start another turn in the same external session after the previous turn ended.
    pub continuation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeHealthStatus {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeHealth {
    pub status: RuntimeHealthStatus,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeDescriptor {
    pub id: String,
    pub capabilities: RuntimeCapabilities,
    pub health: RuntimeHealth,
}

#[derive(Debug, Clone)]
pub struct RuntimeStartRequest {
    pub dsh_session_id: String,
    pub prompt: String,
    pub cwd: Option<String>,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct RuntimeResumeRequest {
    pub dsh_session_id: String,
    /// Runtime-owned identity, persisted separately from the DSH session ID.
    pub external_session_id: String,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct RuntimeRestoreRequest {
    pub dsh_session_id: String,
    /// Runtime-owned identity persisted by the bridge session store.
    pub external_session_id: String,
    /// Absolute working directory used to reconstruct the external conversation.
    pub cwd: Option<String>,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct RuntimeSendRequest {
    pub text: String,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct RuntimeContinueRequest {
    pub dsh_session_id: String,
    /// Runtime-owned identity of the existing conversation.
    pub external_session_id: String,
    pub prompt: String,
    pub cancellation: CancellationToken,
}

pub struct RuntimeSessionHandle {
    pub external_session_id: String,
    pub events: mpsc::Receiver<RuntimeEvent>,
    pub control: Arc<dyn RuntimeSessionControl>,
}

#[async_trait]
pub trait RuntimeSessionControl: Send + Sync {
    async fn send(&self, request: RuntimeSendRequest) -> Result<(), RuntimeError>;
    async fn interrupt(&self, reason: &str) -> Result<(), RuntimeError>;
    /// Must be idempotent. It releases resources owned by this attachment.
    async fn close(&self) -> Result<(), RuntimeError>;
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> RuntimeCapabilities;
    async fn health_check(&self) -> RuntimeHealth;
    async fn start(
        &self,
        request: RuntimeStartRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError>;

    async fn resume(
        &self,
        _request: RuntimeResumeRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        Err(RuntimeError::Capability(format!(
            "runtime {} does not implement execution resume",
            self.id()
        )))
    }

    /// Reattach an idle or completed runtime-owned conversation after bridge restart.
    async fn restore_session(&self, _request: RuntimeRestoreRequest) -> Result<(), RuntimeError> {
        Err(RuntimeError::Capability(format!(
            "runtime {} does not support session restoration",
            self.id()
        )))
    }

    async fn continue_session(
        &self,
        _request: RuntimeContinueRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        Err(RuntimeError::Capability(format!(
            "runtime {} does not support continuation",
            self.id()
        )))
    }

    /// Stop adapter-owned resources after active executions have drained.
    async fn shutdown(&self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSessionRecord {
    pub dsh_session_id: String,
    pub runtime_id: String,
    pub external_session_id: Option<String>,
    /// Working directory captured at session creation when supplied by the host.
    #[serde(default)]
    pub cwd: Option<String>,
    pub status: RuntimeStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEventEnvelope {
    pub dsh_session_id: String,
    pub runtime_id: String,
    pub external_session_id: String,
    pub event_type: String,
    pub runtime_seq: Option<u64>,
    pub payload: Value,
    pub at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedExecutionEvent {
    pub host_seq: u64,
    pub event: ExecutionEventEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionSummary {
    pub dsh_session_id: String,
    pub runtime_id: String,
    pub external_session_id: Option<String>,
    pub status: RuntimeStatus,
}

#[derive(Debug, Clone)]
pub struct StartExecutionRequest {
    pub dsh_session_id: String,
    pub runtime_id: String,
    pub prompt: String,
    pub cwd: Option<String>,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct ResumeExecutionRequest {
    pub dsh_session_id: String,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("capability error: {0}")]
    Capability(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("lifecycle error: {0}")]
    Lifecycle(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("persistence error: {0}")]
    Persistence(String),
}

impl RuntimeError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Configuration(_) => "configuration",
            Self::Capability(_) => "capability",
            Self::Protocol(_) => "protocol",
            Self::Lifecycle(_) => "lifecycle",
            Self::NotFound(_) => "not_found",
            Self::Transport(_) => "transport",
            Self::Persistence(_) => "persistence",
        }
    }
}
