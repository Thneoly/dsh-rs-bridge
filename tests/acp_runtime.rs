use std::{collections::HashMap, sync::Arc};

use dsh_rs_bridge::{
    AcpRuntime, AcpRuntimeConfig, MemoryEventSink, MemorySessionStore, RuntimeStatus,
    RuntimeSupervisor, StartExecutionRequest,
};
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
    let first_external_id = first.external_session_id.clone().expect("external session id");
    let finished = supervisor
        .wait_for_terminal("acp-session-1")
        .await
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
    let continued = supervisor
        .wait_for_terminal("acp-session-1")
        .await
        .expect("wait for continuation");
    assert_eq!(continued.status, RuntimeStatus::Completed);
    let record = sessions
        .get("acp-session-1")
        .await
        .expect("session store")
        .expect("session record");
    assert_eq!(record.external_session_id.as_deref(), Some(first_external_id.as_str()));
    assert!(events.events_for("acp-session-1").await.iter().any(|event| {
        event.event.event_type == "assistant.delta"
            && event.event.payload["text"] == "ACP says: second prompt"
    }));

    supervisor
        .start(request("acp-session-cancel", "cancel-me"))
        .await
        .expect("start cancellable ACP session");
    let cancelled = supervisor
        .interrupt("acp-session-cancel", "integration test")
        .await
        .expect("cancel ACP prompt");
    assert_eq!(cancelled.status, RuntimeStatus::Cancelled);
    assert!(
        events
            .events_for("acp-session-cancel")
            .await
            .iter()
            .any(|event| event.event.event_type == "turn.cancelled")
    );

    supervisor.shutdown().await.expect("shutdown ACP subprocesses");
}
