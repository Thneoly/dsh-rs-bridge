use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use dsh_rs_bridge::{
    FileSessionStore, RuntimeError, RuntimeSessionRecord, RuntimeStatus, SessionStore,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn journal_path() -> PathBuf {
    std::env::temp_dir()
        .join(format!(
            "dsh-rs-bridge-file-store-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ))
        .join("sessions.jsonl")
}

fn record(session_id: &str) -> RuntimeSessionRecord {
    RuntimeSessionRecord {
        dsh_session_id: session_id.to_owned(),
        runtime_id: "acp".to_owned(),
        external_session_id: None,
        cwd: Some("/workspace/project".to_owned()),
        status: RuntimeStatus::Starting,
        created_at_ms: 10,
        updated_at_ms: 10,
    }
}

#[tokio::test]
async fn file_store_replays_latest_binding_and_repairs_torn_tail() {
    let path = journal_path();
    let store = FileSessionStore::open(&path)
        .await
        .expect("open new journal");
    let initial = record("session-persisted");
    store.create(initial.clone()).await.expect("create record");

    let mut updated = initial;
    updated.external_session_id = Some("acp:42:external-session".to_owned());
    updated.status = RuntimeStatus::Completed;
    updated.updated_at_ms = 20;
    store.update(updated.clone()).await.expect("update record");
    drop(store);

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .expect("open journal for torn-tail simulation");
    use tokio::io::AsyncWriteExt;
    file.write_all(b"{\"version\":1,\"record\":")
        .await
        .expect("append partial JSON line");
    file.sync_data().await.expect("sync partial line");
    drop(file);

    let reopened = FileSessionStore::open(&path)
        .await
        .expect("reopen and repair journal");
    assert_eq!(
        reopened
            .get("session-persisted")
            .await
            .expect("lookup")
            .expect("record"),
        updated
    );

    let bytes = tokio::fs::read(&path).await.expect("read repaired journal");
    assert!(
        bytes.ends_with(b"\n"),
        "journal must end at a complete record boundary"
    );
    let text = String::from_utf8(bytes).expect("journal is UTF-8");
    assert_eq!(text.lines().count(), 2, "torn tail must be removed");
    tokio::fs::remove_dir_all(path.parent().expect("journal parent"))
        .await
        .expect("remove temporary journal directory");
}

#[tokio::test]
async fn file_store_rejects_duplicate_create_and_missing_update() {
    let path = journal_path();
    let store = FileSessionStore::open(&path).await.expect("open journal");
    let initial = record("session-one");
    store.create(initial.clone()).await.expect("create record");

    assert!(matches!(
        store.create(initial.clone()).await,
        Err(RuntimeError::Lifecycle(_))
    ));
    let mut missing = initial;
    missing.dsh_session_id = "session-missing".to_owned();
    assert!(matches!(
        store.update(missing).await,
        Err(RuntimeError::NotFound(_))
    ));

    drop(store);
    tokio::fs::remove_dir_all(path.parent().expect("journal parent"))
        .await
        .expect("remove temporary journal directory");
}

#[tokio::test]
async fn file_store_repairs_missing_delimiter_before_appending_next_record() {
    let path = journal_path();
    let store = FileSessionStore::open(&path).await.expect("open journal");
    store
        .create(record("session-delimiter"))
        .await
        .expect("create record");
    drop(store);

    let mut bytes = tokio::fs::read(&path).await.expect("read journal");
    assert_eq!(bytes.pop(), Some(b'\n'));
    tokio::fs::write(&path, bytes)
        .await
        .expect("remove final delimiter");

    let reopened = FileSessionStore::open(&path)
        .await
        .expect("repair complete JSON tail");
    let mut updated = reopened
        .get("session-delimiter")
        .await
        .expect("lookup")
        .expect("record");
    updated.status = RuntimeStatus::Completed;
    updated.updated_at_ms = 30;
    reopened
        .update(updated.clone())
        .await
        .expect("append update");
    drop(reopened);

    let content = tokio::fs::read_to_string(&path)
        .await
        .expect("read final journal");
    assert_eq!(content.lines().count(), 2);
    let last: serde_json::Value = serde_json::from_str(content.lines().last().expect("last line"))
        .expect("last journal entry is valid JSON");
    assert_eq!(last["record"]["status"], "completed");

    tokio::fs::remove_dir_all(path.parent().expect("journal parent"))
        .await
        .expect("remove temporary journal directory");
}
