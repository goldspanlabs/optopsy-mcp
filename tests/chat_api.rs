//! Integration tests for the `SqliteChatStore` (via `ChatStore` trait).

use optopsy_mcp::data::database::Database;
use optopsy_mcp::data::traits::{ChatStore, MessageRow, ResultInput};

fn test_db() -> impl ChatStore {
    let db = Database::open_in_memory().expect("open_in_memory");
    db.chat()
}

// ── 1. create_and_list_threads ──────────────────────────────────────────────

#[test]
fn create_and_list_threads() {
    let store = test_db();
    let thread = store.create_thread("t1").unwrap();

    assert_eq!(thread.id, "t1");
    assert_eq!(thread.status, "regular");

    let threads = store.list_threads().unwrap();
    assert_eq!(threads.len(), 1);
    assert_eq!(threads[0].id, "t1");
}

// ── 2. get_thread_not_found ─────────────────────────────────────────────────

#[test]
fn get_thread_not_found() {
    let store = test_db();
    let result = store.get_thread("nonexistent").unwrap();
    assert!(result.is_none());
}

// ── 3. update_thread_title_and_status ───────────────────────────────────────

#[test]
fn update_thread_title_and_status() {
    let store = test_db();
    store.create_thread("t1").unwrap();

    let updated = store
        .update_thread("t1", Some("My Title"), Some("archived"))
        .unwrap();
    assert!(updated);

    let thread = store.get_thread("t1").unwrap().expect("thread should exist");
    assert_eq!(thread.title.as_deref(), Some("My Title"));
    assert_eq!(thread.status, "archived");
}

// ── 4. delete_thread_cascades ───────────────────────────────────────────────

#[test]
fn delete_thread_cascades() {
    let store = test_db();
    store.create_thread("t1").unwrap();

    store
        .upsert_message(&MessageRow {
            id: "m1".into(),
            thread_id: "t1".into(),
            parent_id: None,
            format: "aui/v0".into(),
            content: r#"{"text":"hello"}"#.into(),
            created_at: String::new(),
        })
        .unwrap();

    // Verify message exists before delete
    assert_eq!(store.get_messages("t1", 100, 0).unwrap().len(), 1);

    assert!(store.delete_thread("t1").unwrap());
    assert!(store.get_thread("t1").unwrap().is_none());
    assert!(store.get_messages("t1", 100, 0).unwrap().is_empty());
}

// ── 5. message_pagination ───────────────────────────────────────────────────

#[test]
fn message_pagination() {
    let store = test_db();
    store.create_thread("t1").unwrap();

    for i in 0..5 {
        store
            .upsert_message(&MessageRow {
                id: format!("m{i}"),
                thread_id: "t1".into(),
                parent_id: None,
                format: "aui/v0".into(),
                content: format!("msg {i}"),
                created_at: String::new(),
            })
            .unwrap();
    }

    // First page: limit 2, offset 0
    let page1 = store.get_messages("t1", 2, 0).unwrap();
    assert_eq!(page1.len(), 2);

    // Second page: limit 2, offset 2
    let page2 = store.get_messages("t1", 2, 2).unwrap();
    assert_eq!(page2.len(), 2);

    // Third page: limit 2, offset 4 — only 1 remaining
    let page3 = store.get_messages("t1", 2, 4).unwrap();
    assert_eq!(page3.len(), 1);

    // All messages
    let all = store.get_messages("t1", 100, 0).unwrap();
    assert_eq!(all.len(), 5);
}

// ── 6. replace_all_results_deduplicates ─────────────────────────────────────

#[test]
fn replace_all_results_deduplicates() {
    let store = test_db();
    store.create_thread("t1").unwrap();

    let inputs = vec![
        ResultInput {
            key: "k1".into(),
            result_type: "chart".into(),
            label: "First".into(),
            tool_call_id: None,
            params: "{}".into(),
            data: Some("a".into()),
        },
        ResultInput {
            key: "k1".into(),
            result_type: "chart".into(),
            label: "Second".into(),
            tool_call_id: None,
            params: "{}".into(),
            data: Some("b".into()),
        },
    ];
    store.replace_all_results("t1", &inputs).unwrap();

    let results = store.get_results("t1").unwrap();
    assert_eq!(results.len(), 1);
    // Last entry wins deduplication
    assert_eq!(results[0].label, "Second");
}

// ── 7. delete_single_result ─────────────────────────────────────────────────

#[test]
fn delete_single_result() {
    let store = test_db();
    store.create_thread("t1").unwrap();

    let inputs = vec![
        ResultInput {
            key: "k1".into(),
            result_type: "chart".into(),
            label: "Chart A".into(),
            tool_call_id: None,
            params: "{}".into(),
            data: None,
        },
        ResultInput {
            key: "k2".into(),
            result_type: "table".into(),
            label: "Table B".into(),
            tool_call_id: None,
            params: "{}".into(),
            data: None,
        },
    ];
    store.replace_all_results("t1", &inputs).unwrap();

    // Delete k1, verify k2 remains
    assert!(store.delete_result("t1", "k1").unwrap());
    let remaining = store.get_results("t1").unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, "k2");

    // Deleting again returns false
    assert!(!store.delete_result("t1", "k1").unwrap());
}
