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

fn acp_supervisor(
    sessions: Arc<MemorySessionStore>,
    events: Arc<MemoryEventSink>,
) -> RuntimeSupervisor {
    let supervisor = RuntimeSupervisor::new(sessions, events);
    let config = AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new());
    supervisor
        .register(Arc::new(AcpRuntime::new(config)))
        .expect("register ACP runtime");
    supervisor
}

async fn wait_for_terminal_status(
    supervisor: &RuntimeSupervisor,
    session_id: &str,
) -> dsh_rs_bridge::RuntimeExecutionSummary {
    timeout(
        Duration::from_secs(10),
        supervisor.wait_for_terminal(session_id),
    )
    .await
    .expect("terminal state timed out")
    .expect("wait for terminal state")
}

async fn eventually_delta(events: &Arc<MemoryEventSink>, session_id: &str, text: &str) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let observed = events.events_for(session_id).await.iter().any(|item| {
            item.event.event_type == "assistant.delta"
                && item.event.payload["text"].as_str() == Some(text)
        });
        if observed {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn acp_agent_death_mid_turn_fails_and_restore_reattaches() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = acp_supervisor(sessions.clone(), events.clone());

    let started = supervisor
        .start(request("acp-crash-session", "crash"))
        .await
        .expect("start session that will crash");
    let external_id = started
        .external_session_id
        .clone()
        .expect("external session id");

    let finished = wait_for_terminal_status(&supervisor, "acp-crash-session").await;
    assert_eq!(finished.status, RuntimeStatus::Failed);
    let stream = events.events_for("acp-crash-session").await;
    let failure = stream
        .iter()
        .find(|item| item.event.event_type == "turn.failed")
        .expect("agent death must surface as turn.failed");
    assert!(
        !failure.event.payload["message"]
            .as_str()
            .unwrap_or_default()
            .is_empty(),
        "turn.failed must carry the death cause"
    );

    // The dead session entry must be replaceable: restore spawns a fresh
    // agent process bound to the same external conversation.
    let restored = supervisor
        .restore_session("acp-crash-session")
        .await
        .expect("restore after agent death");
    assert_eq!(
        restored.external_session_id.as_deref(),
        Some(external_id.as_str())
    );

    supervisor
        .send("acp-crash-session", "after crash".to_owned())
        .await
        .expect("continue restored conversation");
    let continued = wait_for_terminal_status(&supervisor, "acp-crash-session").await;
    assert_eq!(continued.status, RuntimeStatus::Completed);
    assert_eq!(
        continued.external_session_id.as_deref(),
        Some(external_id.as_str())
    );
    assert!(
        eventually_delta(&events, "acp-crash-session", "ACP says: after crash").await,
        "restored conversation must stream new output"
    );

    supervisor
        .shutdown()
        .await
        .expect("shutdown after recovery");
}

#[tokio::test]
async fn acp_malformed_line_fails_inflight_turn_and_session_stays_usable() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = acp_supervisor(sessions, events.clone());

    supervisor
        .start(request("acp-garbage-session", "garbage"))
        .await
        .expect("start session that emits a corrupt line");

    let finished = wait_for_terminal_status(&supervisor, "acp-garbage-session").await;
    assert_eq!(finished.status, RuntimeStatus::Failed);
    assert!(events
        .events_for("acp-garbage-session")
        .await
        .iter()
        .any(|item| item.event.event_type == "turn.failed"));

    // The process itself is still healthy: the next turn on the same session
    // must complete normally.
    supervisor
        .send("acp-garbage-session", "still alive".to_owned())
        .await
        .expect("continue after corrupt line");
    let continued = wait_for_terminal_status(&supervisor, "acp-garbage-session").await;
    assert_eq!(continued.status, RuntimeStatus::Completed);
    assert!(
        eventually_delta(&events, "acp-garbage-session", "ACP says: still alive").await,
        "session must remain usable after a corrupt line"
    );

    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn acp_prompt_timeout_kills_agent_and_restore_recovers() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions, events);
    let config = AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new())
        .with_prompt_timeout(Duration::from_millis(500));
    supervisor
        .register(Arc::new(AcpRuntime::new(config)))
        .expect("register ACP runtime");

    supervisor
        .start(request("acp-timeout-session", "ignore-cancel"))
        .await
        .expect("start session whose prompt never answers");

    let finished = wait_for_terminal_status(&supervisor, "acp-timeout-session").await;
    assert_eq!(finished.status, RuntimeStatus::Failed);

    // The timed-out agent must be KILLED, not merely abandoned: ACP cannot
    // attribute its late session/update notifications to a turn, so a live
    // orphan would contaminate the next turn's event stream. A prompt without
    // a preceding restore must therefore hit the dead-process guard.
    let refused = supervisor
        .send("acp-timeout-session", "probe".to_owned())
        .await;
    assert!(
        matches!(&refused, Err(dsh_rs_bridge::RuntimeError::Transport(message))
            if message.contains("no longer running")),
        "prompt after timeout kill must be refused, got: {refused:?}"
    );

    let restored = supervisor
        .restore_session("acp-timeout-session")
        .await
        .expect("restore after prompt timeout");
    assert!(restored.external_session_id.is_some());
    supervisor
        .send("acp-timeout-session", "after timeout".to_owned())
        .await
        .expect("continue restored conversation");
    let continued = wait_for_terminal_status(&supervisor, "acp-timeout-session").await;
    assert_eq!(continued.status, RuntimeStatus::Completed);

    supervisor
        .shutdown()
        .await
        .expect("shutdown after timeout kill");
}

#[tokio::test]
async fn acp_stdin_wedge_write_times_out_and_kills_agent() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions, events);
    let config = AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new())
        .with_stdin_write_timeout(Duration::from_millis(500));
    supervisor
        .register(Arc::new(AcpRuntime::new(config)))
        .expect("register ACP runtime");

    // First turn completes; the agent then stops draining stdin forever.
    supervisor
        .start(request("acp-wedge-session", "wedge-now"))
        .await
        .expect("start session that will wedge");
    let finished = wait_for_terminal_status(&supervisor, "acp-wedge-session").await;
    assert_eq!(finished.status, RuntimeStatus::Completed);

    // A prompt larger than any pipe buffer cannot be written to the wedged
    // agent: the bounded write must fail the request and kill the child
    // instead of holding the stdin mutex (and the turn lock) forever.
    let oversized = "x".repeat(256 * 1024);
    let wedged = timeout(
        Duration::from_secs(10),
        supervisor.send("acp-wedge-session", oversized),
    )
    .await
    .expect("wedged write must be bounded")
    .expect_err("wedged write must fail");
    assert!(
        wedged.to_string().contains("stdin write timed out"),
        "unexpected wedge error: {wedged}"
    );

    // The wedged agent is dead now: a restore must replace it and the same
    // session must work again.
    supervisor
        .restore_session("acp-wedge-session")
        .await
        .expect("restore after stdin wedge");
    supervisor
        .send("acp-wedge-session", "after wedge".to_owned())
        .await
        .expect("continue after stdin wedge");
    let continued = wait_for_terminal_status(&supervisor, "acp-wedge-session").await;
    assert_eq!(continued.status, RuntimeStatus::Completed);

    supervisor.shutdown().await.expect("shutdown after wedge");
}

/// Poll until a permission.requested event lands for the session; returns the
/// event payload.
async fn eventually_permission(
    events: &Arc<MemoryEventSink>,
    session_id: &str,
) -> Option<serde_json::Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let observed = events.events_for(session_id).await.iter().find_map(|item| {
            if item.event.event_type == "permission.requested" {
                Some(item.event.payload.clone())
            } else {
                None
            }
        });
        if let Some(payload) = observed {
            return Some(payload);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn acp_config() -> AcpRuntimeConfig {
    AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new())
}

#[tokio::test]
async fn acp_permission_allow_enables_tool_run() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions, events.clone());
    supervisor
        .register(Arc::new(AcpRuntime::new(acp_config())))
        .expect("register ACP runtime");

    supervisor
        .start(request("acp-perm-allow", "needs-permission"))
        .await
        .expect("start permission session");
    let payload = eventually_permission(&events, "acp-perm-allow")
        .await
        .expect("permission request must be forwarded to the host");
    let request_id = payload["request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    supervisor
        .respond_permission("acp-perm-allow", &request_id, "allow_once")
        .await
        .expect("deliver allow decision");

    let finished = wait_for_terminal_status(&supervisor, "acp-perm-allow").await;
    assert_eq!(finished.status, RuntimeStatus::Completed);
    let stream = events.events_for("acp-perm-allow").await;
    assert!(
        stream
            .iter()
            .any(|item| item.event.event_type == "tool.started"),
        "allowed permission must surface the tool lifecycle"
    );
    assert!(
        stream
            .iter()
            .any(|item| item.event.event_type == "tool.completed"),
        "allowed permission must surface tool completion"
    );
    assert!(
        stream.iter().any(|item| {
            item.event.event_type == "permission.responded"
                && item.event.payload["source"] == "host"
                && item.event.payload["option_id"] == "allow_once"
        }),
        "host decision must be reported as permission.responded"
    );
    assert!(
        stream.iter().any(|item| {
            item.event.event_type == "assistant.delta"
                && item.event.payload["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("tool ran"))
        }),
        "allowed tool must feed the assistant output"
    );

    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn acp_permission_denied_skips_tool() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions, events.clone());
    supervisor
        .register(Arc::new(AcpRuntime::new(acp_config())))
        .expect("register ACP runtime");

    supervisor
        .start(request("acp-perm-deny", "needs-permission"))
        .await
        .expect("start permission session");
    let payload = eventually_permission(&events, "acp-perm-deny")
        .await
        .expect("permission request must be forwarded");
    let request_id = payload["request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    supervisor
        .respond_permission("acp-perm-deny", &request_id, "reject_once")
        .await
        .expect("deliver deny decision");

    let finished = wait_for_terminal_status(&supervisor, "acp-perm-deny").await;
    assert_eq!(finished.status, RuntimeStatus::Completed);
    let stream = events.events_for("acp-perm-deny").await;
    assert!(
        !stream
            .iter()
            .any(|item| item.event.event_type == "tool.started"),
        "denied permission must not run the tool"
    );
    assert!(
        stream.iter().any(|item| {
            item.event.event_type == "assistant.delta"
                && item.event.payload["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("permission denied"))
        }),
        "denied permission must be visible in the assistant output"
    );

    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn acp_permission_timeout_auto_denies() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions, events.clone());
    let config = acp_config().with_permission_timeout(Duration::from_millis(300));
    supervisor
        .register(Arc::new(AcpRuntime::new(config)))
        .expect("register ACP runtime");

    supervisor
        .start(request("acp-perm-timeout", "needs-permission"))
        .await
        .expect("start permission session");
    assert!(
        eventually_permission(&events, "acp-perm-timeout")
            .await
            .is_some(),
        "permission request must be forwarded before the deadline"
    );

    // The host never answers: the adapter must auto-deny and finish the turn.
    let finished = wait_for_terminal_status(&supervisor, "acp-perm-timeout").await;
    assert_eq!(finished.status, RuntimeStatus::Completed);
    let stream = events.events_for("acp-perm-timeout").await;
    assert!(
        stream.iter().any(|item| {
            item.event.event_type == "permission.responded"
                && item.event.payload["source"] == "timeout"
        }),
        "timeout auto-deny must be reported as permission.responded"
    );
    assert!(
        !stream
            .iter()
            .any(|item| item.event.event_type == "tool.started"),
        "timed-out permission must not run the tool"
    );

    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn acp_permission_pending_cancel_settles_turn() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::new(sessions, events.clone());
    supervisor
        .register(Arc::new(AcpRuntime::new(acp_config())))
        .expect("register ACP runtime");

    supervisor
        .start(request("acp-perm-cancel", "needs-permission"))
        .await
        .expect("start permission session");
    let payload = eventually_permission(&events, "acp-perm-cancel")
        .await
        .expect("permission request must be forwarded");
    let request_id = payload["request_id"]
        .as_str()
        .expect("request id")
        .to_owned();

    // Cancelling while a permission is pending must settle BOTH the pending
    // request (cancelled outcome, as the ACP spec requires) and the turn,
    // within the interrupt deadline — not the 300 s permission deadline.
    let cancelled = timeout(
        Duration::from_secs(10),
        supervisor.interrupt("acp-perm-cancel", "cancel while permission pending"),
    )
    .await
    .expect("interrupt must be bounded while a permission is pending")
    .expect("interrupt must converge");
    assert_eq!(cancelled.status, RuntimeStatus::Cancelled);
    let stream = events.events_for("acp-perm-cancel").await;
    assert!(
        stream.iter().any(|item| {
            item.event.event_type == "permission.responded"
                && item.event.payload["source"] == "cancelled"
        }),
        "cancelling must resolve the pending permission with source=cancelled"
    );
    assert!(
        stream
            .iter()
            .any(|item| item.event.event_type == "turn.cancelled"),
        "the turn itself must settle as cancelled"
    );

    // The settled request must not be answerable a second time.
    let refused = supervisor
        .respond_permission("acp-perm-cancel", &request_id, "allow_once")
        .await;
    assert!(
        refused.is_err(),
        "responding to an already-settled permission must fail, got: {refused:?}"
    );

    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn interrupt_is_bounded_when_agent_ignores_cancellation() {
    let sessions = Arc::new(MemorySessionStore::default());
    let events = Arc::new(MemoryEventSink::default());
    let supervisor = RuntimeSupervisor::with_interrupt_timeout(
        sessions,
        events.clone(),
        Duration::from_millis(500),
    );
    let config = AcpRuntimeConfig::new(env!("CARGO_BIN_EXE_acp-test-agent"))
        .with_id("acp-test")
        .with_env(HashMap::new());
    let runtime = Arc::new(AcpRuntime::new(config));
    supervisor
        .register(runtime.clone())
        .expect("register ACP runtime");

    supervisor
        .start(request("acp-stuck-session", "ignore-cancel"))
        .await
        .expect("start stuck session");
    assert!(
        eventually_delta(&events, "acp-stuck-session", "ACP is stuck").await,
        "stuck agent must stream its first chunk"
    );

    // An agent that ignores session/cancel must not hold the host hostage.
    let interrupted = timeout(
        Duration::from_secs(5),
        supervisor.interrupt("acp-stuck-session", "test"),
    )
    .await
    .expect("interrupt must be bounded");
    assert!(
        interrupted.is_err(),
        "unsettled cancel must surface an error"
    );

    // Shutdown must still run the adapter cleanup hooks (draining the
    // adapter's session map and killing the stuck agent) even though the
    // session never converges; the failure is reported afterwards.
    let shutdown = supervisor.shutdown().await;
    assert!(
        shutdown.is_err(),
        "a never-settling session must be reported, not silently dropped"
    );
    assert_eq!(
        runtime.session_count().await,
        0,
        "adapter shutdown hooks must drain every session even when one is stuck"
    );
}

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
