use dsh_rs_bridge::{
    contracts::ExecutionEventEnvelope, ExecutionEventSink, FileEventJournal, RuntimeError,
};
use serde_json::json;

static NEXT_PATH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn journal_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "dsh-rs-bridge-event-journal-{}-{}",
        std::process::id(),
        NEXT_PATH.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

fn envelope(session: &str, event_type: &str) -> ExecutionEventEnvelope {
    ExecutionEventEnvelope {
        dsh_session_id: session.to_owned(),
        runtime_id: "mock".to_owned(),
        external_session_id: format!("{session}::external"),
        event_type: event_type.to_owned(),
        runtime_seq: None,
        payload: json!({ "event_type": event_type }),
        at_unix_ms: 42,
    }
}

#[tokio::test]
async fn host_seq_continues_across_restarts_and_replay_filters() {
    let path = journal_path();
    let journal = FileEventJournal::open(&path).await.expect("open journal");
    for index in 0..3 {
        journal
            .append(envelope("session-a", &format!("event-{index}")))
            .await
            .expect("append session-a");
    }
    journal
        .append(envelope("session-b", "other"))
        .await
        .expect("append session-b");
    drop(journal);

    // Reopen: the counters must be seeded from the journal, not restarted.
    let journal = FileEventJournal::open(&path).await.expect("reopen journal");
    let seq = journal
        .append(envelope("session-a", "after-restart"))
        .await
        .expect("append after restart");
    assert_eq!(seq, 4, "host_seq must continue across a journal reopen");

    let all = journal.replay("session-a", None).await.expect("replay");
    assert_eq!(
        all.iter().map(|item| item.host_seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "replay must return the session's full durable history in order"
    );
    assert!(all
        .iter()
        .any(|item| item.event.event_type == "after-restart"));

    let tail = journal
        .replay("session-a", Some(2))
        .await
        .expect("replay after seq");
    assert_eq!(
        tail.iter().map(|item| item.host_seq).collect::<Vec<_>>(),
        vec![3, 4],
        "after_host_seq must filter strictly"
    );

    let other = journal.replay("session-b", None).await.expect("replay b");
    assert_eq!(other.len(), 1, "sessions must be isolated in the journal");
    drop(journal);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn torn_tail_is_truncated_and_valid_tail_delimiter_is_restored() {
    let path = journal_path();
    {
        let journal = FileEventJournal::open(&path).await.expect("open journal");
        journal
            .append(envelope("session-a", "first"))
            .await
            .expect("append");
        drop(journal);
    }
    // Torn write: half a JSON entry without the trailing newline.
    let bytes = std::fs::read(&path).expect("read journal");
    let mut torn = bytes.clone();
    torn.extend_from_slice(b"{\"version\":1,\"host_seq\":2,\"eve");
    std::fs::write(&path, &torn).expect("write torn tail");

    let journal = FileEventJournal::open(&path)
        .await
        .expect("repair torn tail");
    let events = journal.replay("session-a", None).await.expect("replay");
    assert_eq!(events.len(), 1, "torn tail must be dropped");
    let seq = journal
        .append(envelope("session-a", "after-repair"))
        .await
        .expect("append after repair");
    assert_eq!(seq, 2, "counter must not count the torn entry");
    drop(journal);

    // Complete JSON that only lost its newline: delimiter must be restored.
    let mut bytes = std::fs::read(&path).expect("read journal");
    assert!(bytes.ends_with(b"\n"));
    bytes.truncate(bytes.len() - 1);
    std::fs::write(&path, &bytes).expect("strip delimiter");

    let journal = FileEventJournal::open(&path)
        .await
        .expect("repair delimiter");
    let events = journal.replay("session-a", None).await.expect("replay");
    assert_eq!(events.len(), 2, "valid tail entry must survive");
    let seq = journal
        .append(envelope("session-a", "after-delimiter-repair"))
        .await
        .expect("append after delimiter repair");
    assert_eq!(seq, 3);
    drop(journal);

    // The delimiter repair is load-bearing: without it the append above would
    // have glued two JSON entries onto one line and the NEXT open would fail
    // closed. Pin the on-disk shape, not just the in-memory counter.
    let content = std::fs::read_to_string(&path).expect("read journal after repair append");
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3, "each entry must stay on its own line");
    assert!(content.ends_with('\n'), "journal must end with a delimiter");
    for (index, line) in lines.iter().enumerate() {
        serde_json::from_str::<serde_json::Value>(line).unwrap_or_else(|error| {
            panic!(
                "journal line {} must stay standalone JSON: {error}",
                index + 1
            )
        });
    }
    FileEventJournal::open(&path)
        .await
        .expect("journal must reopen cleanly after the repair append");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn malformed_complete_line_fails_closed() {
    let path = journal_path();
    std::fs::write(&path, b"{\"version\":1,\"host_seq\":1}\n").expect("seed journal");

    let error = FileEventJournal::open(&path)
        .await
        .err()
        .expect("malformed journal must fail closed");
    assert!(matches!(error, RuntimeError::Persistence(_)));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn failed_append_consumes_no_sequence_and_leaves_no_fragment() {
    // A failed append must neither consume a sequence number nor leave a
    // partial line behind: the counter commits only after a durable write,
    // and the write is rolled back on failure. Injected by making the
    // journal read-only for one append.
    let path = journal_path();
    let journal = FileEventJournal::open(&path).await.expect("open journal");
    journal
        .append(envelope("session-a", "first"))
        .await
        .expect("append");

    let mut permissions = std::fs::metadata(&path)
        .expect("journal metadata")
        .permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&path, permissions).expect("make journal read-only");
    let failed = journal.append(envelope("session-a", "lost")).await;
    assert!(
        failed.is_err(),
        "append to a read-only journal must fail loudly"
    );
    // Metadata re-read gives writable permissions without set_readonly(false)
    // (which clippy rightly flags: it would make the file world-writable on Unix).
    assert!(
        std::fs::metadata(&path)
            .expect("journal metadata")
            .permissions()
            .readonly(),
        "journal must still be read-only here"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("restore writability");
    }
    #[cfg(windows)]
    #[allow(clippy::permissions_set_readonly_false)]
    {
        let mut permissions = std::fs::metadata(&path)
            .expect("journal metadata")
            .permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(&path, permissions).expect("restore writability");
    }

    let seq = journal
        .append(envelope("session-a", "second"))
        .await
        .expect("append after failure");
    assert_eq!(seq, 2, "a failed append must not consume a sequence number");

    let content = std::fs::read_to_string(&path).expect("read journal");
    for (index, line) in content.lines().enumerate() {
        serde_json::from_str::<serde_json::Value>(line).unwrap_or_else(|error| {
            panic!("line {} must stay standalone JSON: {error}", index + 1)
        });
    }
    let _ = std::fs::remove_file(&path);
}
