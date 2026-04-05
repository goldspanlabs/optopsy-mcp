//! Integration tests for the `/tasks/*` REST endpoints.
//!
//! Uses `tower::ServiceExt::oneshot()` to drive the router in-process.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use optopsy_mcp::server::router::build_api_router;
use tower::ServiceExt;

// ──────────────────────────────────────────────────────────────────────────────
// Local helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Send a request through the router and collect the full body as a `String`.
async fn send(app: axum::Router, req: Request<Body>) -> (StatusCode, String) {
    let resp = app.oneshot(req).await.expect("oneshot failed");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let body = String::from_utf8_lossy(&bytes).to_string();
    (status, body)
}

/// Build a POST request with JSON body.
fn post_json(path: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("build request")
}

/// Minimal backtest request body: a stock strategy with no options legs.
fn backtest_body(strategy: &str) -> String {
    serde_json::json!({
        "strategy": strategy,
        "params": {
            "symbol": "SPY"
        }
    })
    .to_string()
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 1: POST /tasks/backtest returns task_id
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_backtest_returns_task_id() {
    let (state, _tmp) = common::test_app_state();
    let app = build_api_router(state);

    let req = post_json("/tasks/backtest", &backtest_body("some_strategy"));
    let (status, body) = send(app, req).await;

    assert_eq!(status, StatusCode::OK, "expected 200, body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    let task_id = json.get("task_id").and_then(|v| v.as_str());
    assert!(
        task_id.is_some(),
        "response should contain task_id string, got: {body}"
    );
    assert!(!task_id.unwrap().is_empty(), "task_id should not be empty");
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 2: GET /tasks shows submitted task (or it already completed/failed)
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_active_tasks_shows_submitted_task() {
    let (state, _tmp) = common::test_app_state();
    let app = build_api_router(state);

    // Submit
    let req = post_json("/tasks/backtest", &backtest_body("some_strategy"));
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let task_id = json["task_id"].as_str().unwrap().to_string();

    // Give the spawned task a moment to register
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // GET /tasks — task may be active or already terminal (fast failure)
    let req = Request::builder()
        .method("GET")
        .uri("/tasks")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, StatusCode::OK);
    let active: serde_json::Value = serde_json::from_str(&body).unwrap();

    // Either it's still active in the list, or it has already finished (fast fail with no data).
    // Verify via GET /tasks/{id} that the task exists at all.
    let task_in_list = active
        .as_array()
        .is_some_and(|arr| arr.iter().any(|t| t["id"].as_str() == Some(&task_id)));

    if !task_in_list {
        // Task finished fast — verify it's retrievable
        let req = Request::builder()
            .method("GET")
            .uri(format!("/tasks/{task_id}"))
            .body(Body::empty())
            .unwrap();
        let (status, _body) = send(app, req).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "task {task_id} should be retrievable even if not in active list"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 3: GET /tasks/{id} returns a snapshot with expected fields
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_task_returns_snapshot() {
    let (state, _tmp) = common::test_app_state();
    let app = build_api_router(state);

    // Submit
    let req = post_json("/tasks/backtest", &backtest_body("my_strategy"));
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, StatusCode::OK);
    let submit_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let task_id = submit_json["task_id"].as_str().unwrap().to_string();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // GET /tasks/{id}
    let req = Request::builder()
        .method("GET")
        .uri(format!("/tasks/{task_id}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(app, req).await;
    assert_eq!(status, StatusCode::OK, "expected 200, body: {body}");

    let snap: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    assert_eq!(snap["id"].as_str(), Some(task_id.as_str()), "id mismatch");
    assert_eq!(
        snap["kind"].as_str(),
        Some("single"),
        "kind should be single"
    );
    assert_eq!(
        snap["symbol"].as_str(),
        Some("SPY"),
        "symbol should be SPY from params"
    );
    assert_eq!(
        snap["strategy"].as_str(),
        Some("my_strategy"),
        "strategy should match submitted name"
    );
    assert!(
        snap["created_at"].as_str().is_some(),
        "created_at should be present"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 4: GET /tasks/{nonexistent} returns 404
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_nonexistent_task_returns_404() {
    let (state, _tmp) = common::test_app_state();
    let app = build_api_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/tasks/nonexistent-id-that-does-not-exist")
        .body(Body::empty())
        .unwrap();
    let (status, _body) = send(app, req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 5: DELETE /tasks/{id} returns 204; subsequent GET shows cancelled
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_task_returns_204() {
    let (state, _tmp) = common::test_app_state();
    let app = build_api_router(state);

    // Submit
    let req = post_json("/tasks/backtest", &backtest_body("cancel_strategy"));
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, StatusCode::OK);
    let submit_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let task_id = submit_json["task_id"].as_str().unwrap().to_string();

    // DELETE /tasks/{id} — immediately after submit (task likely queued)
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/tasks/{task_id}"))
        .body(Body::empty())
        .unwrap();
    let (status, _body) = send(app.clone(), req).await;
    // May be 204 (cancelled) or 404 (completed/failed before cancel arrived)
    assert!(
        status == StatusCode::NO_CONTENT || status == StatusCode::NOT_FOUND,
        "expected 204 or 404, got {status}"
    );

    if status == StatusCode::NO_CONTENT {
        // Verify status is cancelled
        let req = Request::builder()
            .method("GET")
            .uri(format!("/tasks/{task_id}"))
            .body(Body::empty())
            .unwrap();
        let (get_status, body) = send(app, req).await;
        assert_eq!(get_status, StatusCode::OK);
        let snap: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            snap["status"].as_str(),
            Some("cancelled"),
            "status should be cancelled after DELETE"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 6: SSE stream delivers events including "done"
// ──────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────────
// Test 7: E2E — real OHLCV data through the full task manager pipeline
// ──────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_backtest_runs_through_task_manager() {
    let (state, _tmp, strategy_id) = common::test_app_state_with_ohlcv();
    let app = build_api_router(state.clone());

    // 1. Submit backtest
    let (status, body) = send(
        app.clone(),
        post_json(
            "/tasks/backtest",
            &format!(
                r#"{{"strategy":"{strategy_id}","params":{{"symbol":"NVDA","CAPITAL":100000}}}}"#
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "submit failed: {body}");
    let task_id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();

    // 2. Connect to SSE stream and collect all events (stream closes after "done")
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/tasks/{task_id}/stream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        response.into_body().collect(),
    )
    .await
    .expect("backtest should complete within 60s")
    .unwrap()
    .to_bytes();
    let sse_text = String::from_utf8_lossy(&body_bytes);

    // 3. Verify SSE stream had events (progress heartbeat or state updates)
    assert!(
        sse_text.contains("event:") || sse_text.contains("event: "),
        "Should have SSE events in stream, got:\n{sse_text}"
    );

    // 4. Verify SSE stream ended with a "done" event
    assert!(
        sse_text.contains("done"),
        "Should have done event in SSE stream, got:\n{sse_text}"
    );

    // 4b. Verify the stream contained a result event (backtest completed)
    assert!(
        sse_text.contains("event: result") || sse_text.contains("event:result"),
        "Should have result event in SSE stream, got:\n{sse_text}"
    );

    // 5. Verify result was persisted — get task to find the result_id
    let (_, task_body) = send(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/tasks/{task_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let task_snap: serde_json::Value = serde_json::from_str(&task_body).unwrap();
    assert_eq!(
        task_snap["status"].as_str().unwrap(),
        "completed",
        "Task should be completed, got: {task_snap}"
    );

    let result_id = task_snap["result_id"]
        .as_str()
        .expect("completed task should have result_id");

    // 6. Verify the run was persisted to the database via GET /runs/{id}
    let (run_status, run_body) = send(
        app,
        Request::builder()
            .method("GET")
            .uri(format!("/runs/{result_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        run_status,
        StatusCode::OK,
        "Run should be fetchable from DB, body: {run_body}"
    );
    let run_detail: serde_json::Value = serde_json::from_str(&run_body).unwrap();
    assert_eq!(
        run_detail["symbol"].as_str().unwrap(),
        "NVDA",
        "symbol mismatch"
    );
    assert!(
        run_detail["trade_count"].as_i64().unwrap_or(0) > 0,
        "Should have at least one trade, got: {run_detail}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_stream_delivers_events() {
    let (state, _tmp, strategy_id) = common::test_app_state_with_strategy();
    let app = build_api_router(state);

    // Submit backtest using the seeded strategy
    let body = serde_json::json!({
        "strategy": strategy_id,
        "params": {
            "symbol": "SPY"
        }
    })
    .to_string();
    let req = post_json("/tasks/backtest", &body);
    let (status, resp_body) = send(app.clone(), req).await;
    assert_eq!(status, StatusCode::OK);
    let submit_json: serde_json::Value = serde_json::from_str(&resp_body).unwrap();
    let task_id = submit_json["task_id"].as_str().unwrap().to_string();

    // GET /tasks/{id}/stream
    let req = Request::builder()
        .method("GET")
        .uri(format!("/tasks/{task_id}/stream"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.expect("oneshot failed");
    assert_eq!(resp.status(), StatusCode::OK);

    // Read the SSE body with a 30s timeout
    let collect_fut = resp.into_body().collect();
    let bytes = tokio::time::timeout(std::time::Duration::from_secs(30), collect_fut)
        .await
        .expect("SSE stream timed out after 30s")
        .expect("collect body bytes");

    let sse_text = String::from_utf8_lossy(&bytes.to_bytes()).to_string();

    // SSE events must be present
    assert!(
        sse_text.contains("event:") || sse_text.contains("event: "),
        "SSE body should contain event: lines, got: {sse_text}"
    );

    // The stream must end with a "done" event
    assert!(
        sse_text.contains("done"),
        "SSE body should contain 'done' event, got: {sse_text}"
    );
}
