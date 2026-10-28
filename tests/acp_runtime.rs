use std::{collections::HashMap, sync::Arc, time::Duration};

use dsh_rs_bridge::{
    AcpRuntime, AcpRuntimeConfig, MemoryEventSink, MemorySessionStore, RuntimeStatus,
    RuntimeSupervisor, SessionStore, StartExecutionRequest,
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
