use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use dsh_rs_bridge::{
    AcpRuntime, AcpRuntimeConfig, FileSessionStore, MemoryEventSink, MemorySessionStore,
    RuntimeStatus, RuntimeSupervisor, SessionStore, StartExecutionRequest,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn request(session_id: &str, prompt: &str) -> StartExecutionRequest {
    StartExecutionRequest {
        dsh_session_id: session_id.to_owned(),
        runtime_id: "acp-test".to_owned(),
        prompt: prompt.to_owned(),
        cwd: None,
        cancellation: CancellationToken::new(),
    }
}

#[tokio::test]
async fn acp_subprocess_streams_output_continues_and_cancels() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions.clone(), events.clone());
    let config = AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new());
    supervisor
        .register(Arc::new(AcpRuntime::new(config)))
        .expect("register ACP runtime");

    let first = supervisor
        .start(request("acp-session-1", "hello"))
        .await
        .expect("start ACP session");
    assert_eq!(first.status, RuntimeStatus::Running);
    let first_external_id = first
        .external_session_id
        .clone()
        .expect("external session id");
    let finished = timeout(
        Duration::from_secs(10),
        supervisor.wait_for_terminal("acp-session-1"),
    )
    .await
    .expect("first turn timed out")
    .expect("wait for first turn");
    assert_eq!(finished.status, RuntimeStatus::Completed);

    let first_events = events.events_for("acp-session-1").await;
    assert!(
        first_events.iter().any(|event| {
            event.event.event_type == "assistant.delta"
                && event.event.payload["text"] == "ACP says: hello"
        }),
        "ACP session/update must become an assistant delta"
    );

    supervisor
        .send("acp-session-1", "second prompt".to_owned())
        .await
        .expect("continue existing ACP session");
    let continued = timeout(
        Duration::from_secs(10),
        supervisor.wait_for_terminal("acp-session-1"),
    )
    .await
    .expect("continuation timed out")
    .expect("wait for continuation");
    assert_eq!(continued.status, RuntimeStatus::Completed);
    let record = sessions
        .get("acp-session-1")
        .await
        .expect("session store")
        .expect("session record");
    assert_eq!(
        record.external_session_id.as_deref(),
        Some(first_external_id.as_str())
    );
    assert!(events
        .events_for("acp-session-1")
        .await
        .iter()
        .any(|event| {
            event.event.event_type == "assistant.delta"
                && event.event.payload["text"] == "ACP says: second prompt"
        }));

    supervisor
        .start(request("acp-session-cancel", "cancel-me"))
        .await
        .expect("start cancellable ACP session");
    let cancelled = timeout(
        Duration::from_secs(10),
        supervisor.interrupt("acp-session-cancel", "integration test"),
    )
    .await
    .expect("cancellation timed out")
    .expect("cancel ACP prompt");
    assert_eq!(cancelled.status, RuntimeStatus::Cancelled);
    assert!(events
        .events_for("acp-session-cancel")
        .await
        .iter()
        .any(|event| event.event.event_type == "turn.cancelled"));

    supervisor
        .shutdown()
        .await
        .expect("shutdown ACP subprocesses");
}

static NEXT_STORE_PATH: AtomicU64 = AtomicU64::new(1);

fn persistent_store_path() -> PathBuf {
    std::env::temp_dir()
        .join(format!(
            "dsh-rs-bridge-acp-restore-{}-{}",
            std::process::id(),
            NEXT_STORE_PATH.fetch_add(1, Ordering::Relaxed)
        ))
        .join("sessions.jsonl")
}

#[tokio::test]
async fn acp_session_binding_survives_bridge_restart_and_restores_conversation() {
    let path = persistent_store_path();
    let sessions = Arc::new(
        FileSessionStore::open(&path)
            .await
            .expect("open file store"),
    );
    let first_events = Arc::new(MemoryEventSink::default());
    let first_supervisor = RuntimeSupervisor::new(sessions.clone(), first_events);

    let config = AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new());
    first_supervisor
        .register(Arc::new(AcpRuntime::new(config)))
        .expect("register first ACP runtime");

    let started = first_supervisor
        .start(request("acp-persistent-session", "before restart"))
        .await
        .expect("start first ACP session");
    let external_id = started
        .external_session_id
        .clone()
        .expect("external session id");
    let finished = timeout(
        Duration::from_secs(10),
        first_supervisor.wait_for_terminal("acp-persistent-session"),
    )
    .await
    .expect("first turn timed out")
    .expect("wait for first turn");
    assert_eq!(finished.status, RuntimeStatus::Completed);
    first_supervisor
        .shutdown()
        .await
        .expect("shutdown first bridge");
    drop(first_supervisor);
    drop(sessions);

    let restored_store = Arc::new(
        FileSessionStore::open(&path)
            .await
            .expect("reopen persisted session store"),
    );
    let restored_events = Arc::new(MemoryEventSink::default());
    let restored_supervisor =
        RuntimeSupervisor::new(restored_store.clone(), restored_events.clone());
    let config = AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new());
    restored_supervisor
        .register(Arc::new(AcpRuntime::new(config)))
        .expect("register replacement ACP runtime");

    let restored = restored_supervisor
        .restore_session("acp-persistent-session")
        .await
        .expect("restore ACP conversation after bridge restart");
    assert_eq!(
        restored.external_session_id.as_deref(),
        Some(external_id.as_str())
    );
    assert_eq!(restored.status, RuntimeStatus::Completed);

    restored_supervisor
        .send("acp-persistent-session", "after restart".to_owned())
        .await
        .expect("continue restored ACP conversation");
    let continued = timeout(
        Duration::from_secs(10),
        restored_supervisor.wait_for_terminal("acp-persistent-session"),
    )
    .await
    .expect("restored continuation timed out")
    .expect("wait for restored continuation");
    assert_eq!(continued.status, RuntimeStatus::Completed);
    assert_eq!(
        continued.external_session_id.as_deref(),
        Some(external_id.as_str())
    );
    assert!(restored_events
        .events_for("acp-persistent-session")
        .await
        .iter()
        .any(|event| {
            event.event.event_type == "assistant.delta"
                && event.event.payload["text"] == "ACP says: after restart"
        }));

    restored_supervisor
        .shutdown()
        .await
        .expect("shutdown restored bridge");
    drop(restored_store);
    tokio::fs::remove_dir_all(path.parent().expect("store directory"))
        .await
        .expect("remove temporary store");
}
