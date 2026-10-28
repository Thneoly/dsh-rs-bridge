use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::{mpsc, Mutex};

use crate::contracts::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContinueRequest, RuntimeError, RuntimeEvent,
    RuntimeEventType, RuntimeHealth, RuntimeHealthStatus, RuntimeResumeRequest, RuntimeSendRequest,
    RuntimeSessionControl, RuntimeSessionHandle, RuntimeStartRequest, RuntimeStatus,
};

/// Prompts starting with this marker never reach a terminal state on their own;
/// only an interrupt (or adapter shutdown) ends the turn. Tests that verify
/// cancellation semantics use this to stay deterministic instead of racing a
/// delay window, mirroring the `cancel-me` prompt of the ACP test fixture.
const HANG_PROMPT_PREFIX: &str = "hang:";

fn is_hang_prompt(prompt: &str) -> bool {
    prompt.starts_with(HANG_PROMPT_PREFIX)
}

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
            session_restore: false,
            interrupt: true,
            live_input: false,
            continuation: self.continuation,
            permission_requests: false,
        }
    }

    fn spawn_turn(
        &self,
        external_session_id: String,
        output: String,
        delay: Duration,
        resumed: bool,
        tool_events: bool,
        hang: bool,
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

            if hang {
                // Park until the turn is interrupted or the control side closes;
                // polling lets the task terminate instead of leaking a timer.
                loop {
                    if task_control.is_done().await {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
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
            is_hang_prompt(&request.prompt),
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
            false,
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
            is_hang_prompt(&request.prompt),
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

    /// Whether the turn already reached a terminal state or was closed.
    async fn is_done(&self) -> bool {
        let state = self.state.lock().await;
        state.closed || state.terminal
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
