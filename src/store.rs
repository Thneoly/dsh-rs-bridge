use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::contracts::{
    ExecutionEventEnvelope, PersistedExecutionEvent, RuntimeError, RuntimeSessionRecord,
};

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn create(&self, record: RuntimeSessionRecord) -> Result<(), RuntimeError>;
    async fn get(&self, dsh_session_id: &str) -> Result<Option<RuntimeSessionRecord>, RuntimeError>;
    async fn update(&self, record: RuntimeSessionRecord) -> Result<(), RuntimeError>;
}

#[async_trait]
pub trait ExecutionEventSink: Send + Sync {
    /// Append an event and return the host-owned sequence number for its DSH session.
    async fn append(&self, event: ExecutionEventEnvelope) -> Result<u64, RuntimeError>;
}

#[derive(Default)]
pub struct MemorySessionStore {
    records: Mutex<HashMap<String, RuntimeSessionRecord>>,
}

#[async_trait]
impl SessionStore for MemorySessionStore {
    async fn create(&self, record: RuntimeSessionRecord) -> Result<(), RuntimeError> {
        let mut records = self.records.lock().await;
        if records.contains_key(&record.dsh_session_id) {
            return Err(RuntimeError::Lifecycle(format!(
                "session already exists: {}",
                record.dsh_session_id
            )));
        }
        records.insert(record.dsh_session_id.clone(), record);
        Ok(())
    }

    async fn get(&self, dsh_session_id: &str) -> Result<Option<RuntimeSessionRecord>, RuntimeError> {
        Ok(self.records.lock().await.get(dsh_session_id).cloned())
    }

    async fn update(&self, record: RuntimeSessionRecord) -> Result<(), RuntimeError> {
        let mut records = self.records.lock().await;
        if !records.contains_key(&record.dsh_session_id) {
            return Err(RuntimeError::NotFound(format!(
                "session record: {}",
                record.dsh_session_id
            )));
        }
        records.insert(record.dsh_session_id.clone(), record);
        Ok(())
    }
}

#[derive(Default)]
struct EventSinkState {
    sequences: HashMap<String, u64>,
    events: Vec<PersistedExecutionEvent>,
}

#[derive(Default)]
pub struct MemoryEventSink {
    state: Mutex<EventSinkState>,
}

impl MemoryEventSink {
    pub async fn all_events(&self) -> Vec<PersistedExecutionEvent> {
        self.state.lock().await.events.clone()
    }

    pub async fn events_for(&self, dsh_session_id: &str) -> Vec<PersistedExecutionEvent> {
        self.state
            .lock()
            .await
            .events
            .iter()
            .filter(|item| item.event.dsh_session_id == dsh_session_id)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl ExecutionEventSink for MemoryEventSink {
    async fn append(&self, event: ExecutionEventEnvelope) -> Result<u64, RuntimeError> {
        let mut state = self.state.lock().await;
        let sequence = state.sequences.entry(event.dsh_session_id.clone()).or_insert(0);
        *sequence += 1;
        let host_seq = *sequence;
        state.events.push(PersistedExecutionEvent { host_seq, event });
        Ok(host_seq)
    }
}
