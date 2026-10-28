use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::{mpsc, Mutex};

use crate::contracts::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContinueRequest, RuntimeError, RuntimeEvent,
    RuntimeEventType, RuntimeHealth, RuntimeHealthStatus, RuntimeResumeRequest, RuntimeSendRequest,
    RuntimeSessionControl, RuntimeSessionHandle, RuntimeStartRequest, RuntimeStatus,
};

#[derive(Clone)]
pub struct MockRuntime {
    id: String,
    output: String,
    delay: Duration,
    continuation: bool,
    tool_events: bool,
}

impl MockRuntime {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            output: "mock runtime completed the task".to_owned(),
            delay: Duration::from_millis(5),
            continuation: false,
            tool_events: false,
        }
    }

    pub fn with_output(mut self, output: impl Into<String>) -> Self {
        self.output = output.into();
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn with_continuation(mut self, enabled: bool) -> Self {
        self.continuation = enabled;
        self
    }

    /// Emit deterministic tool lifecycle events for adapter integration tests.
    pub fn with_tool_events(mut self, enabled: bool) -> Self {
        self.tool_events = enabled;
        self
    }

    fn capabilities_value(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            streaming_events: true,
            resume: true,
            interrupt: true,
            live_input: false,
            continuation: self.continuation,
        }
    }

    fn spawn_turn(
        &self,
        external_session_id: String,
        output: String,
        delay: Duration,
        resumed: bool,
        tool_events: bool,
    ) -> RuntimeSessionHandle {
        let (tx, rx) = mpsc::channel(32);
        let control = Arc::new(MockControl::new(tx));
        let task_control = control.clone();
        tokio::spawn(async move {
            let emitted = task_control
                .emit(
                    RuntimeEventType::RuntimeStatus,
                    json!({ "status": "running", "resumed": resumed }),
                )
                .await
                .unwrap_or(false);
            if !emitted {
                return;
            }

            if tool_events {
                let emitted = task_control
                    .emit(
                        RuntimeEventType::ToolStarted,
                        json!({
                            "toolCallId": "mock-call-1",
                            "title": "mock lookup",
                            "status": "in_progress",
                            "rawInput": { "query": "integration-test" }
                        }),
                    )
                    .await
                    .unwrap_or(false);
                if !emitted {
                    return;
                }
                let emitted = task_control
                    .emit(
                        RuntimeEventType::ToolCompleted,
                        json!({
                            "toolCallId": "mock-call-1",
                            "title": "mock lookup",
                            "status": "completed",
                            "content": [{ "type": "text", "text": "mock tool result" }]
                        }),
                    )
                    .await
                    .unwrap_or(false);
                if !emitted {
                    return;
                }
            }

            tokio::time::sleep(delay).await;
            let message = if resumed {
                format!("resumed: {output}")
            } else {
                output
            };
            let emitted = task_control
                .emit(RuntimeEventType::AssistantDelta, json!({ "text": message }))
                .await
                .unwrap_or(false);
            if !emitted {
                return;
            }

            tokio::time::sleep(delay).await;
            let _ = task_control
                .finish(
                    RuntimeEventType::TurnCompleted,
                    json!({ "status": RuntimeStatus::Completed }),
                )
                .await;
        });

        RuntimeSessionHandle {
            external_session_id,
            events: rx,
            control,
        }
    }
}

#[async_trait]
impl RuntimeAdapter for MockRuntime {
    fn id(&self) -> &str {
        &self.id
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.capabilities_value()
    }

    async fn health_check(&self) -> RuntimeHealth {
        RuntimeHealth {
            status: RuntimeHealthStatus::Ready,
            detail: Some("deterministic in-process mock".to_owned()),
        }
    }

    async fn start(
        &self,
        request: RuntimeStartRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Lifecycle(
                "start cancelled before mock runtime launch".to_owned(),
            ));
        }
        let external_session_id = format!("{}::{}", self.id, request.dsh_session_id);
        Ok(self.spawn_turn(
            external_session_id,
            self.output.clone(),
            self.delay,
            false,
            self.tool_events,
        ))
    }

    async fn resume(
        &self,
        request: RuntimeResumeRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Lifecycle(
                "resume cancelled before mock runtime launch".to_owned(),
            ));
        }
        Ok(self.spawn_turn(
            request.external_session_id,
            self.output.clone(),
            self.delay,
            true,
            self.tool_events,
        ))
    }

    async fn continue_session(
        &self,
        request: RuntimeContinueRequest,
    ) -> Result<RuntimeSessionHandle, RuntimeError> {
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Lifecycle(
                "continuation cancelled before mock runtime launch".to_owned(),
            ));
        }
        if !self.continuation {
            return Err(RuntimeError::Capability(
                "mock runtime does not support continuation".to_owned(),
            ));
        }
        Ok(self.spawn_turn(
            request.external_session_id,
            format!("continued: {}", request.prompt),
            self.delay,
            false,
            self.tool_events,
        ))
    }
}

struct MockControlState {
    next_seq: u64,
    terminal: bool,
    closed: bool,
}

struct MockControl {
    tx: mpsc::Sender<RuntimeEvent>,
    state: Mutex<MockControlState>,
}

impl MockControl {
    fn new(tx: mpsc::Sender<RuntimeEvent>) -> Self {
        Self {
            tx,
            state: Mutex::new(MockControlState {
                next_seq: 0,
                terminal: false,
                closed: false,
            }),
        }
    }

    async fn emit(
        &self,
        event_type: RuntimeEventType,
        payload: serde_json::Value,
    ) -> Result<bool, RuntimeError> {
        let mut state = self.state.lock().await;
        if state.closed || state.terminal {
            return Ok(false);
        }
        state.next_seq += 1;
        let event = RuntimeEvent {
            seq: state.next_seq,
            event_type,
            payload,
        };
        self.tx
            .send(event)
            .await
            .map_err(|_| RuntimeError::Transport("mock event receiver closed".to_owned()))?;
        Ok(true)
    }

    async fn finish(
        &self,
        event_type: RuntimeEventType,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        if event_type.terminal_status().is_none() {
            return Err(RuntimeError::Protocol(
                "finish requires a terminal event type".to_owned(),
            ));
        }
        let mut state = self.state.lock().await;
        if state.closed || state.terminal {
            return Ok(());
        }
        state.terminal = true;
        state.next_seq += 1;
        let event = RuntimeEvent {
            seq: state.next_seq,
            event_type,
            payload,
        };
        self.tx
            .send(event)
            .await
            .map_err(|_| RuntimeError::Transport("mock event receiver closed".to_owned()))
    }
}

#[async_trait]
impl RuntimeSessionControl for MockControl {
    async fn send(&self, _request: RuntimeSendRequest) -> Result<(), RuntimeError> {
        Err(RuntimeError::Capability(
            "mock runtime does not support follow-up input".to_owned(),
        ))
    }

    async fn interrupt(&self, reason: &str) -> Result<(), RuntimeError> {
        self.finish(
            RuntimeEventType::TurnCancelled,
            json!({ "reason": reason, "status": RuntimeStatus::Cancelled }),
        )
        .await
    }

    async fn close(&self) -> Result<(), RuntimeError> {
        self.state.lock().await.closed = true;
        Ok(())
    }
}
