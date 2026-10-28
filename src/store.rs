use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::Mutex};

use crate::contracts::{
    ExecutionEventEnvelope, PersistedExecutionEvent, RuntimeError, RuntimeSessionRecord,
};

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn create(&self, record: RuntimeSessionRecord) -> Result<(), RuntimeError>;
    async fn get(
        &self,
        dsh_session_id: &str,
    ) -> Result<Option<RuntimeSessionRecord>, RuntimeError>;
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

    async fn get(
        &self,
        dsh_session_id: &str,
    ) -> Result<Option<RuntimeSessionRecord>, RuntimeError> {
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

#[derive(Debug, Serialize, Deserialize)]
struct SessionJournalEntry {
    version: u8,
    record: RuntimeSessionRecord,
}

/// Append-only JSONL journal for DSH-to-runtime session bindings.
///
/// Every create/update writes a complete versioned record and calls sync_data.
/// Startup replays the last record for each DSH session. A torn final line is
/// truncated before the store is returned; malformed complete lines fail closed.
pub struct FileSessionStore {
    path: PathBuf,
    records: Mutex<HashMap<String, RuntimeSessionRecord>>,
}

impl FileSessionStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self, RuntimeError> {
        let path = path.into();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                RuntimeError::Persistence(format!(
                    "create session-store directory {}: {error}",
                    parent.display()
                ))
            })?;
        }

        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(RuntimeError::Persistence(format!(
                    "read session journal {}: {error}",
                    path.display()
                )));
            }
        };

        let mut valid_len = bytes.len();
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            let tail_start = bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map(|index| index + 1)
                .unwrap_or(0);
            let tail_is_valid = serde_json::from_slice::<SessionJournalEntry>(&bytes[tail_start..])
                .map(|entry| entry.version == 1)
                .unwrap_or(false);
            if !tail_is_valid {
                valid_len = tail_start;
                let file = OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .await
                    .map_err(|error| {
                        RuntimeError::Persistence(format!(
                            "open torn session journal {} for repair: {error}",
                            path.display()
                        ))
                    })?;
                file.set_len(valid_len as u64).await.map_err(|error| {
                    RuntimeError::Persistence(format!(
                        "truncate torn session journal {}: {error}",
                        path.display()
                    ))
                })?;
                file.sync_data().await.map_err(|error| {
                    RuntimeError::Persistence(format!(
                        "sync repaired session journal {}: {error}",
                        path.display()
                    ))
                })?;
            } else {
                // The JSON object itself survived but the final newline did not.
                // Restore the record delimiter before any later append.
                let mut file = OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .await
                    .map_err(|error| {
                        RuntimeError::Persistence(format!(
                            "open complete session journal tail {}: {error}",
                            path.display()
                        ))
                    })?;
                file.write_all(b"\n").await.map_err(|error| {
                    RuntimeError::Persistence(format!(
                        "repair session journal delimiter {}: {error}",
                        path.display()
                    ))
                })?;
                file.sync_data().await.map_err(|error| {
                    RuntimeError::Persistence(format!(
                        "sync session journal delimiter {}: {error}",
                        path.display()
                    ))
                })?;
            }
        }

        let content = std::str::from_utf8(&bytes[..valid_len]).map_err(|error| {
            RuntimeError::Persistence(format!(
                "session journal {} is not UTF-8: {error}",
                path.display()
            ))
        })?;
        let mut records = HashMap::new();
        for (index, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let entry: SessionJournalEntry = serde_json::from_str(line).map_err(|error| {
                RuntimeError::Persistence(format!(
                    "invalid session journal entry at {}:{}: {error}",
                    path.display(),
                    index + 1
                ))
            })?;
            if entry.version != 1 {
                return Err(RuntimeError::Persistence(format!(
                    "unsupported session journal version {} at {}:{}",
                    entry.version,
                    path.display(),
                    index + 1
                )));
            }
            records.insert(entry.record.dsh_session_id.clone(), entry.record);
        }

        Ok(Self {
            path,
            records: Mutex::new(records),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    async fn append_record(&self, record: &RuntimeSessionRecord) -> Result<(), RuntimeError> {
        if let Some(parent) = self.path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                RuntimeError::Persistence(format!(
                    "create session-store directory {}: {error}",
                    parent.display()
                ))
            })?;
        }

        let entry = SessionJournalEntry {
            version: 1,
            record: record.clone(),
        };
        let mut line = serde_json::to_vec(&entry)
            .map_err(|error| RuntimeError::Persistence(error.to_string()))?;
        line.push(b'\n');

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|error| {
                RuntimeError::Persistence(format!(
                    "open session journal {}: {error}",
                    self.path.display()
                ))
            })?;
        file.write_all(&line).await.map_err(|error| {
            RuntimeError::Persistence(format!(
                "append session journal {}: {error}",
                self.path.display()
            ))
        })?;
        file.sync_data().await.map_err(|error| {
            RuntimeError::Persistence(format!(
                "sync session journal {}: {error}",
                self.path.display()
            ))
        })
    }
}

#[async_trait]
impl SessionStore for FileSessionStore {
    async fn create(&self, record: RuntimeSessionRecord) -> Result<(), RuntimeError> {
        let mut records = self.records.lock().await;
        if records.contains_key(&record.dsh_session_id) {
            return Err(RuntimeError::Lifecycle(format!(
                "session already exists: {}",
                record.dsh_session_id
            )));
        }
        self.append_record(&record).await?;
        records.insert(record.dsh_session_id.clone(), record);
        Ok(())
    }

    async fn get(
        &self,
        dsh_session_id: &str,
    ) -> Result<Option<RuntimeSessionRecord>, RuntimeError> {
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
        self.append_record(&record).await?;
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
        let sequence = state
            .sequences
            .entry(event.dsh_session_id.clone())
            .or_insert(0);
        *sequence += 1;
        let host_seq = *sequence;
        state
            .events
            .push(PersistedExecutionEvent { host_seq, event });
        Ok(host_seq)
    }
}
