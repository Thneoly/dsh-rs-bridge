use std::{sync::Arc, time::Duration};

use dsh_rs_bridge::{
    MemoryEventSink, MemorySessionStore, MockRuntime, RuntimeError, RuntimeStatus,
    RuntimeSupervisor, SessionStore, StartExecutionRequest,
};
use tokio_util::sync::CancellationToken;

fn setup_with_continuation(
    delay: Duration,
    continuation: bool,
) -> (
    RuntimeSupervisor,
    Arc<MemorySessionStore>,
    Arc<MemoryEventSink>,
) {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions.clone(), events.clone());
    supervisor
        .register(Arc::new(
            MockRuntime::new("mock")
                .with_output("hello from mock")
                .with_delay(delay)
                .with_continuation(continuation),
        ))
        .expect("register mock runtime");
    (supervisor, sessions, events)
}

fn setup(
    delay: Duration,
) -> (
    RuntimeSupervisor,
    Arc<MemorySessionStore>,
    Arc<MemoryEventSink>,
) {
    setup_with_continuation(delay, false)
}

fn request(session_id: &str) -> StartExecutionRequest {
    StartExecutionRequest {
        dsh_session_id: session_id.to_owned(),
        runtime_id: "mock".to_owned(),
        prompt: "test prompt".to_owned(),
        cwd: None,
        cancellation: CancellationToken::new(),
    }
}

#[tokio::test]
async fn start_streams_events_and_reaches_completed() {
    let (supervisor, sessions, events) = setup(Duration::from_millis(1));
    let started = supervisor.start(request("session-a")).await.expect("start");
    assert_eq!(started.status, RuntimeStatus::Running);
    assert!(started.external_session_id.is_some());

    let finished = supervisor
        .wait_for_terminal("session-a")
        .await
        .expect("wait");
    assert_eq!(finished.status, RuntimeStatus::Completed);

    let record = sessions
        .get("session-a")
        .await
        .expect("store")
        .expect("record");
    assert_eq!(record.status, RuntimeStatus::Completed);
    let stream = events.events_for("session-a").await;
    assert!(stream
        .iter()
        .any(|item| item.event.event_type == "assistant.delta"));
    assert!(stream
        .iter()
        .any(|item| item.event.event_type == "runtime.ended"));
    assert!(stream
        .windows(2)
        .all(|pair| pair[0].host_seq < pair[1].host_seq));
}

#[tokio::test]
async fn interrupt_converges_to_cancelled() {
    let (supervisor, _, _) = setup(Duration::from_millis(100));
    supervisor
        .start(request("session-cancel"))
        .await
        .expect("start");
    let finished = supervisor
        .interrupt("session-cancel", "test cancellation")
        .await
        .expect("interrupt");
    assert_eq!(finished.status, RuntimeStatus::Cancelled);
}

#[tokio::test]
async fn resume_keeps_external_session_identity() {
    let (supervisor, _, events) = setup(Duration::from_millis(1));
    let first = supervisor
        .start(request("session-resume"))
        .await
        .expect("start");
    supervisor
        .wait_for_terminal("session-resume")
        .await
        .expect("first terminal");

    let second = supervisor
        .resume(dsh_rs_bridge::ResumeExecutionRequest {
            dsh_session_id: "session-resume".to_owned(),
            cancellation: CancellationToken::new(),
        })
        .await
        .expect("resume");
    assert_eq!(first.external_session_id, second.external_session_id);

    let finished = supervisor
        .wait_for_terminal("session-resume")
        .await
        .expect("resumed terminal");
    assert_eq!(finished.status, RuntimeStatus::Completed);
    assert!(events
        .events_for("session-resume")
        .await
        .iter()
        .any(|item| item.event.event_type == "runtime.resumed"));
}

#[tokio::test]
async fn concurrent_sessions_are_isolated() {
    let (supervisor, _, events) = setup(Duration::from_millis(5));
    let a = supervisor
        .start(request("session-a"))
        .await
        .expect("start a");
    let b = supervisor
        .start(request("session-b"))
        .await
        .expect("start b");
    assert_ne!(a.external_session_id, b.external_session_id);

    let (a, b) = tokio::join!(
        supervisor.wait_for_terminal("session-a"),
        supervisor.wait_for_terminal("session-b")
    );
    assert_eq!(a.expect("a terminal").status, RuntimeStatus::Completed);
    assert_eq!(b.expect("b terminal").status, RuntimeStatus::Completed);
    assert!(events
        .events_for("session-a")
        .await
        .iter()
        .all(|e| e.event.dsh_session_id == "session-a"));
    assert!(events
        .events_for("session-b")
        .await
        .iter()
        .all(|e| e.event.dsh_session_id == "session-b"));
}

#[tokio::test]
async fn continuation_starts_a_new_turn_with_the_same_external_session() {
    let (supervisor, sessions, events) =
        setup_with_continuation(Duration::from_millis(1), true);
    let first = supervisor
        .start(request("session-continue"))
        .await
        .expect("start");
    supervisor
        .wait_for_terminal("session-continue")
        .await
        .expect("first terminal");

    supervisor
        .send("session-continue", "second prompt".to_owned())
        .await
        .expect("continue session");
    let second = supervisor
        .wait_for_terminal("session-continue")
        .await
        .expect("second terminal");

    assert_eq!(second.status, RuntimeStatus::Completed);
    assert_eq!(first.external_session_id, second.external_session_id);
    let record = sessions
        .get("session-continue")
        .await
        .expect("store")
        .expect("record");
    assert_eq!(record.status, RuntimeStatus::Completed);
    let stream = events.events_for("session-continue").await;
    assert!(stream
        .iter()
        .any(|item| item.event.event_type == "runtime.continued"));
    assert!(stream.iter().any(|item| {
        item.event.event_type == "assistant.delta"
            && item.event.payload["text"] == "continued: second prompt"
    }));
}

#[tokio::test]
async fn one_shot_runtime_rejects_follow_up_input() {
    let (supervisor, _, _) = setup(Duration::from_millis(100));
    supervisor
        .start(request("session-send"))
        .await
        .expect("start");
    let result = supervisor
        .send("session-send", "follow-up".to_owned())
        .await;
    assert!(matches!(result, Err(RuntimeError::Capability(_))));
    supervisor
        .interrupt("session-send", "test cleanup")
        .await
        .expect("cancel active mock");
}

#[tokio::test]
async fn duplicate_session_start_is_rejected() {
    let (supervisor, _, _) = setup(Duration::from_millis(100));
    supervisor
        .start(request("session-duplicate"))
        .await
        .expect("start");
    let result = supervisor.start(request("session-duplicate")).await;
    assert!(matches!(result, Err(RuntimeError::Lifecycle(_))));
    supervisor
        .interrupt("session-duplicate", "test cleanup")
        .await
        .expect("cancel active mock");
}

#[tokio::test]
async fn shutdown_interrupts_active_sessions_and_rejects_new_work() {
    let (supervisor, _, _) = setup(Duration::from_millis(100));
    supervisor
        .start(request("session-shutdown"))
        .await
        .expect("start");
    supervisor.shutdown().await.expect("shutdown");

    let finished = supervisor
        .wait_for_terminal("session-shutdown")
        .await
        .expect("terminal after shutdown");
    assert_eq!(finished.status, RuntimeStatus::Cancelled);

    let result = supervisor.start(request("session-after-shutdown")).await;
    assert!(matches!(result, Err(RuntimeError::Lifecycle(_))));
}
