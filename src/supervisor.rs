use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::contracts::{
    ExecutionEventEnvelope, ResumeExecutionRequest, RuntimeAdapter, RuntimeCapabilities,
    RuntimeDescriptor, RuntimeError, RuntimeEvent, RuntimeExecutionSummary, RuntimeHealthStatus,
    RuntimeResumeRequest, RuntimeSessionControl, RuntimeSessionHandle, RuntimeSessionRecord,
    RuntimeStartRequest, RuntimeStatus, StartExecutionRequest,
};
use crate::store::{ExecutionEventSink, SessionStore};

struct ActiveExecution {
    control: Arc<dyn RuntimeSessionControl>,
    capabilities: RuntimeCapabilities,
    status_tx: watch::Sender<RuntimeStatus>,
}

struct SupervisorInner {
    adapters: RwLock<HashMap<String, Arc<dyn RuntimeAdapter>>>,
    sessions: Arc<dyn SessionStore>,
    events: Arc<dyn ExecutionEventSink>,
    active: Mutex<HashMap<String, Arc<ActiveExecution>>>,
    reservations: Mutex<HashSet<String>>,
}

#[derive(Clone)]
pub struct RuntimeSupervisor {
    inner: Arc<SupervisorInner>,
}

impl RuntimeSupervisor {
    pub fn new(sessions: Arc<dyn SessionStore>, events: Arc<dyn ExecutionEventSink>) -> Self {
        Self {
            inner: Arc::new(SupervisorInner {
                adapters: RwLock::new(HashMap::new()),
                sessions,
                events,
                active: Mutex::new(HashMap::new()),
                reservations: Mutex::new(HashSet::new()),
            }),
        }
    }

    pub fn register(&self, adapter: Arc<dyn RuntimeAdapter>) -> Result<(), RuntimeError> {
        let id = adapter.id().trim();
        if id.is_empty() {
            return Err(RuntimeError::Configuration(
                "runtime adapter id must not be empty".to_owned(),
            ));
        }
        if !adapter.capabilities().streaming_events {
            return Err(RuntimeError::Capability(format!(
                "runtime adapter must support streaming events: {id}"
            )));
        }
        let mut adapters =
            self.inner.adapters.write().map_err(|_| {
                RuntimeError::Lifecycle("runtime registry lock poisoned".to_owned())
            })?;
        if adapters.contains_key(id) {
            return Err(RuntimeError::Configuration(format!(
                "runtime adapter already registered: {id}"
            )));
        }
        adapters.insert(id.to_owned(), adapter);
        Ok(())
    }

    pub async fn list_runtimes(&self) -> Result<Vec<RuntimeDescriptor>, RuntimeError> {
        let adapters: Vec<Arc<dyn RuntimeAdapter>> = self
            .inner
            .adapters
            .read()
            .map_err(|_| RuntimeError::Lifecycle("runtime registry lock poisoned".to_owned()))?
            .values()
            .cloned()
            .collect();
        let mut descriptors = Vec::with_capacity(adapters.len());
        for adapter in adapters {
            descriptors.push(RuntimeDescriptor {
                id: adapter.id().to_owned(),
                capabilities: adapter.capabilities(),
                health: adapter.health_check().await,
            });
        }
        descriptors.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(descriptors)
    }

    pub async fn start(
        &self,
        request: StartExecutionRequest,
    ) -> Result<RuntimeExecutionSummary, RuntimeError> {
        self.reserve(&request.dsh_session_id).await?;
        let result = self.start_reserved(request.clone()).await;
        self.release_reservation(&request.dsh_session_id).await;
        result
    }

    async fn start_reserved(
        &self,
        request: StartExecutionRequest,
    ) -> Result<RuntimeExecutionSummary, RuntimeError> {
        let adapter = self.require_adapter(&request.runtime_id)?;
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Lifecycle(
                "start cancelled before adapter launch".to_owned(),
            ));
        }
        let health = adapter.health_check().await;
        if health.status != RuntimeHealthStatus::Ready {
            return Err(RuntimeError::Transport(format!(
                "runtime {} is not ready: {:?}",
                adapter.id(),
                health
            )));
        }

        let stamp = now_ms();
        let mut record = RuntimeSessionRecord {
            dsh_session_id: request.dsh_session_id.clone(),
            runtime_id: adapter.id().to_owned(),
            external_session_id: None,
            status: RuntimeStatus::Starting,
            created_at_ms: stamp,
            updated_at_ms: stamp,
        };
        self.inner.sessions.create(record.clone()).await?;

        let handle = match adapter
            .start(RuntimeStartRequest {
                dsh_session_id: request.dsh_session_id.clone(),
                prompt: request.prompt,
                cwd: request.cwd,
                cancellation: request.cancellation.clone(),
            })
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                record.status = if request.cancellation.is_cancelled() {
                    RuntimeStatus::Cancelled
                } else {
                    RuntimeStatus::Failed
                };
                record.updated_at_ms = now_ms();
                let _ = self.inner.sessions.update(record).await;
                return Err(error);
            }
        };

        let external_session_id = handle.external_session_id.clone();
        if request.cancellation.is_cancelled() {
            let _ = handle
                .control
                .interrupt("start cancelled during launch")
                .await;
            let _ = handle.control.close().await;
            record.status = RuntimeStatus::Cancelled;
            record.external_session_id = Some(external_session_id);
            record.updated_at_ms = now_ms();
            let _ = self.inner.sessions.update(record).await;
            return Err(RuntimeError::Lifecycle(
                "start cancelled during adapter launch".to_owned(),
            ));
        }

        record.external_session_id = Some(external_session_id.clone());
        record.status = RuntimeStatus::Running;
        record.updated_at_ms = now_ms();
        if let Err(error) = self.inner.sessions.update(record.clone()).await {
            let _ = handle.control.close().await;
            return Err(error);
        }

        if let Err(error) = self
            .inner
            .events
            .append(envelope(
                &record,
                "runtime.started",
                None,
                json!({ "external_session_id": external_session_id }),
            ))
            .await
        {
            let _ = handle.control.close().await;
            record.status = RuntimeStatus::Failed;
            record.updated_at_ms = now_ms();
            let _ = self.inner.sessions.update(record).await;
            return Err(error);
        }

        let RuntimeSessionHandle {
            events,
            control,
            external_session_id: _,
        } = handle;
        let (status_tx, _) = watch::channel(RuntimeStatus::Running);
        let active = Arc::new(ActiveExecution {
            control: control.clone(),
            capabilities: adapter.capabilities(),
            status_tx,
        });
        self.inner
            .active
            .lock()
            .await
            .insert(request.dsh_session_id.clone(), active.clone());

        let inner = self.inner.clone();
        tokio::spawn(async move {
            consume_events(inner, record, events, control, active).await;
        });

        Ok(RuntimeExecutionSummary {
            dsh_session_id: request.dsh_session_id,
            runtime_id: adapter.id().to_owned(),
            external_session_id: Some(external_session_id),
            status: RuntimeStatus::Running,
        })
    }

    pub async fn resume(
        &self,
        request: ResumeExecutionRequest,
    ) -> Result<RuntimeExecutionSummary, RuntimeError> {
        self.reserve(&request.dsh_session_id).await?;
        let result = self.resume_reserved(request.clone()).await;
        self.release_reservation(&request.dsh_session_id).await;
        result
    }

    async fn resume_reserved(
        &self,
        request: ResumeExecutionRequest,
    ) -> Result<RuntimeExecutionSummary, RuntimeError> {
        let mut record = self
            .inner
            .sessions
            .get(&request.dsh_session_id)
            .await?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("DSH session: {}", request.dsh_session_id))
            })?;
        let external_id = record.external_session_id.clone().ok_or_else(|| {
            RuntimeError::Lifecycle(format!(
                "external session reference missing: {}",
                request.dsh_session_id
            ))
        })?;
        let adapter = self.require_adapter(&record.runtime_id)?;
        if !adapter.capabilities().resume {
            return Err(RuntimeError::Capability(format!(
                "runtime {} does not support resume",
                adapter.id()
            )));
        }
        if request.cancellation.is_cancelled() {
            return Err(RuntimeError::Lifecycle(
                "resume cancelled before adapter launch".to_owned(),
            ));
        }

        record.status = RuntimeStatus::Starting;
        record.updated_at_ms = now_ms();
        self.inner.sessions.update(record.clone()).await?;

        let handle = match adapter
            .resume(RuntimeResumeRequest {
                dsh_session_id: request.dsh_session_id.clone(),
                external_session_id: external_id.clone(),
                cancellation: request.cancellation.clone(),
            })
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                record.status = RuntimeStatus::Failed;
                record.updated_at_ms = now_ms();
                let _ = self.inner.sessions.update(record).await;
                return Err(error);
            }
        };
        if handle.external_session_id != external_id {
            let _ = handle.control.close().await;
            record.status = RuntimeStatus::Failed;
            record.updated_at_ms = now_ms();
            let _ = self.inner.sessions.update(record).await;
            return Err(RuntimeError::Protocol(
                "adapter changed external session identity during resume".to_owned(),
            ));
        }

        record.status = RuntimeStatus::Running;
        record.updated_at_ms = now_ms();
        if let Err(error) = self.inner.sessions.update(record.clone()).await {
            let _ = handle.control.close().await;
            return Err(error);
        }
        if let Err(error) = self
            .inner
            .events
            .append(envelope(
                &record,
                "runtime.resumed",
                None,
                json!({ "external_session_id": external_id }),
            ))
            .await
        {
            let _ = handle.control.close().await;
            record.status = RuntimeStatus::Failed;
            record.updated_at_ms = now_ms();
            let _ = self.inner.sessions.update(record).await;
            return Err(error);
        }

        let RuntimeSessionHandle {
            events,
            control,
            external_session_id: _,
        } = handle;
        let (status_tx, _) = watch::channel(RuntimeStatus::Running);
        let active = Arc::new(ActiveExecution {
            control: control.clone(),
            capabilities: adapter.capabilities(),
            status_tx,
        });
        self.inner
            .active
            .lock()
            .await
            .insert(request.dsh_session_id.clone(), active.clone());

        let inner = self.inner.clone();
        tokio::spawn(async move {
            consume_events(inner, record, events, control, active).await;
        });

        Ok(RuntimeExecutionSummary {
            dsh_session_id: request.dsh_session_id,
            runtime_id: adapter.id().to_owned(),
            external_session_id: Some(external_id),
            status: RuntimeStatus::Running,
        })
    }

    pub async fn send(&self, dsh_session_id: &str, text: String) -> Result<(), RuntimeError> {
        let active = self.active_execution(dsh_session_id).await?;
        if !active.capabilities.continuation {
            return Err(RuntimeError::Capability(format!(
                "runtime does not support follow-up input: {dsh_session_id}"
            )));
        }
        active
            .control
            .send(crate::contracts::RuntimeSendRequest {
                text,
                cancellation: CancellationToken::new(),
            })
            .await
    }

    pub async fn interrupt(
        &self,
        dsh_session_id: &str,
        reason: &str,
    ) -> Result<RuntimeExecutionSummary, RuntimeError> {
        if let Ok(active) = self.active_execution(dsh_session_id).await {
            if !active.capabilities.interrupt {
                return Err(RuntimeError::Capability(format!(
                    "runtime does not support interrupt: {dsh_session_id}"
                )));
            }
            active.control.interrupt(reason).await?;
        }
        self.wait_for_terminal(dsh_session_id).await
    }

    pub async fn wait_for_terminal(
        &self,
        dsh_session_id: &str,
    ) -> Result<RuntimeExecutionSummary, RuntimeError> {
        let active = self.inner.active.lock().await.get(dsh_session_id).cloned();
        if let Some(active) = active {
            let mut receiver = active.status_tx.subscribe();
            loop {
                if receiver.borrow().is_terminal() {
                    break;
                }
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        }

        let record = self
            .inner
            .sessions
            .get(dsh_session_id)
            .await?
            .ok_or_else(|| RuntimeError::NotFound(format!("DSH session: {dsh_session_id}")))?;
        if !record.status.is_terminal() {
            return Err(RuntimeError::Lifecycle(format!(
                "session has not reached a terminal state: {dsh_session_id}"
            )));
        }
        Ok(summary(&record))
    }

    async fn active_execution(
        &self,
        dsh_session_id: &str,
    ) -> Result<Arc<ActiveExecution>, RuntimeError> {
        self.inner
            .active
            .lock()
            .await
            .get(dsh_session_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("active execution: {dsh_session_id}")))
    }

    async fn reserve(&self, dsh_session_id: &str) -> Result<(), RuntimeError> {
        let mut reservations = self.inner.reservations.lock().await;
        if reservations.contains(dsh_session_id) {
            return Err(RuntimeError::Lifecycle(format!(
                "session is already starting or resuming: {dsh_session_id}"
            )));
        }
        if self.inner.active.lock().await.contains_key(dsh_session_id) {
            return Err(RuntimeError::Lifecycle(format!(
                "session is already active: {dsh_session_id}"
            )));
        }
        reservations.insert(dsh_session_id.to_owned());
        Ok(())
    }

    async fn release_reservation(&self, dsh_session_id: &str) {
        self.inner.reservations.lock().await.remove(dsh_session_id);
    }

    fn require_adapter(&self, id: &str) -> Result<Arc<dyn RuntimeAdapter>, RuntimeError> {
        self.inner
            .adapters
            .read()
            .map_err(|_| RuntimeError::Lifecycle("runtime registry lock poisoned".to_owned()))?
            .get(id)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::Configuration(format!("runtime adapter not registered: {id}"))
            })
    }
}

async fn consume_events(
    inner: Arc<SupervisorInner>,
    mut record: RuntimeSessionRecord,
    mut events: tokio::sync::mpsc::Receiver<RuntimeEvent>,
    control: Arc<dyn RuntimeSessionControl>,
    active: Arc<ActiveExecution>,
) {
    let mut last_runtime_seq = 0;
    let mut terminal_status = None;
    let mut failure = None;

    while let Some(event) = events.recv().await {
        if event.seq <= last_runtime_seq {
            failure = Some(format!(
                "runtime event sequence must increase: received {}, previous {}",
                event.seq, last_runtime_seq
            ));
            break;
        }
        if !event.payload.is_object() {
            failure = Some("runtime event payload must be a JSON object".to_owned());
            break;
        }
        last_runtime_seq = event.seq;
        if let Err(error) = inner
            .events
            .append(envelope(
                &record,
                event.event_type.as_str(),
                Some(event.seq),
                event.payload.clone(),
            ))
            .await
        {
            failure = Some(error.to_string());
            break;
        }
        if let Some(status) = event.event_type.terminal_status() {
            terminal_status = Some(status);
            break;
        }
    }

    let mut final_status = terminal_status.unwrap_or(RuntimeStatus::Failed);
    if terminal_status.is_none() && failure.is_none() {
        failure = Some("event stream closed without a terminal event".to_owned());
    }

    record.status = final_status;
    record.updated_at_ms = now_ms();
    if let Err(error) = inner.sessions.update(record.clone()).await {
        failure = Some(format!("failed to persist terminal status: {error}"));
        final_status = RuntimeStatus::Failed;
    }

    if let Some(message) = failure {
        let _ = inner
            .events
            .append(envelope(
                &record,
                "runtime.failed",
                None,
                json!({ "category": "protocol_or_persistence", "message": message }),
            ))
            .await;
        record.status = RuntimeStatus::Failed;
        record.updated_at_ms = now_ms();
        let _ = inner.sessions.update(record.clone()).await;
        active.status_tx.send_replace(RuntimeStatus::Failed);
    } else {
        let _ = inner
            .events
            .append(envelope(
                &record,
                "runtime.ended",
                None,
                json!({ "status": final_status }),
            ))
            .await;
        active.status_tx.send_replace(final_status);
    }

    if let Err(error) = control.close().await {
        let _ = inner
            .events
            .append(envelope(
                &record,
                "runtime.cleanup_failed",
                None,
                json!({ "message": error.to_string() }),
            ))
            .await;
    }

    inner.active.lock().await.remove(&record.dsh_session_id);
}

fn envelope(
    record: &RuntimeSessionRecord,
    event_type: &str,
    runtime_seq: Option<u64>,
    payload: serde_json::Value,
) -> ExecutionEventEnvelope {
    ExecutionEventEnvelope {
        dsh_session_id: record.dsh_session_id.clone(),
        runtime_id: record.runtime_id.clone(),
        external_session_id: record.external_session_id.clone().unwrap_or_default(),
        event_type: event_type.to_owned(),
        runtime_seq,
        payload,
        at_unix_ms: now_ms(),
    }
}

fn summary(record: &RuntimeSessionRecord) -> RuntimeExecutionSummary {
    RuntimeExecutionSummary {
        dsh_session_id: record.dsh_session_id.clone(),
        runtime_id: record.runtime_id.clone(),
        external_session_id: record.external_session_id.clone(),
        status: record.status,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
