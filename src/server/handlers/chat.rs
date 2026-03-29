//! REST API handlers for chat thread, message, and result CRUD operations.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::server::state::AppState;

// ──────────────────────────────────────────────────────────────────────────────
// Request / query types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateThreadBody {
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateThreadBody {
    pub title: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MessagesQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    100
}

#[derive(Debug, Deserialize)]
pub struct UpsertMessageBody {
    pub id: String,
    pub parent_id: Option<String>,
    #[serde(default = "default_format")]
    pub format: String,
    pub content: Value,
}

fn default_format() -> String {
    "aui/v0".to_string()
}

#[derive(Debug, Deserialize)]
pub struct ResultPayload {
    pub key: String,
    #[serde(rename = "type")]
    pub result_type: String,
    pub label: String,
    pub tool_call_id: Option<String>,
    #[serde(default = "default_params")]
    pub params: Value,
    pub data: Option<Value>,
}

fn default_params() -> Value {
    json!({})
}

#[derive(Debug, Deserialize)]
pub struct ReplaceResultsBody {
    pub results: Vec<ResultPayload>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Thread handlers
// ──────────────────────────────────────────────────────────────────────────────

/// Query parameters for `GET /threads`.
#[derive(Debug, Deserialize, Default)]
pub struct ListThreadsQuery {
    pub strategy_id: Option<String>,
}

/// `GET /threads` — List all threads, optionally filtered by `strategy_id`.
pub async fn list_threads(
    State(state): State<AppState>,
    Query(query): Query<ListThreadsQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    let threads = tokio::task::spawn_blocking(move || {
        if let Some(ref sid) = query.strategy_id {
            store.list_threads_for_strategy(sid)
        } else {
            store.list_threads()
        }
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "threads": threads })))
}

/// `POST /threads` — Create a new thread.
pub async fn create_thread(
    State(state): State<AppState>,
    Json(body): Json<CreateThreadBody>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, String)> {
    let store = state.chat_store.clone();
    let thread = tokio::task::spawn_blocking(move || store.create_thread(&body.id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(thread).unwrap_or_default()),
    ))
}

/// `GET /threads/{id}` — Get a single thread.
pub async fn get_thread(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    let thread = tokio::task::spawn_blocking(move || store.get_thread(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Thread not found".to_string()))?;

    Ok(Json(serde_json::to_value(thread).unwrap_or_default()))
}

/// `PATCH /threads/{id}` — Update a thread's title and/or status.
pub async fn update_thread(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateThreadBody>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    tokio::task::spawn_blocking(move || {
        store.update_thread(&id, body.title.as_deref(), body.status.as_deref())
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "ok": true })))
}

/// `DELETE /threads/{id}` — Delete a thread (cascades).
pub async fn delete_thread(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    tokio::task::spawn_blocking(move || store.delete_thread(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "ok": true })))
}

// ──────────────────────────────────────────────────────────────────────────────
// Message handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `GET /threads/{id}/messages` — Get messages for a thread.
pub async fn get_messages(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    let messages =
        tokio::task::spawn_blocking(move || store.get_messages(&id, query.limit, query.offset))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Parse content from JSON string to Value in response
    let messages: Vec<Value> = messages
        .into_iter()
        .map(|m| {
            let content: Value = serde_json::from_str(&m.content)
                .unwrap_or_else(|_| Value::String(m.content.clone()));
            json!({
                "id": m.id,
                "thread_id": m.thread_id,
                "parent_id": m.parent_id,
                "format": m.format,
                "content": content,
                "created_at": m.created_at,
            })
        })
        .collect();

    Ok(Json(json!({ "messages": messages })))
}

/// `POST /threads/{id}/messages` — Upsert a message.
pub async fn upsert_message(
    State(state): State<AppState>,
    Path(thread_id): Path<String>,
    Json(body): Json<UpsertMessageBody>,
) -> Result<Json<Value>, (StatusCode, String)> {
    // Serialize content Value to JSON string for storage
    let content_str = serde_json::to_string(&body.content)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let msg = crate::data::traits::MessageRow {
        id: body.id,
        thread_id,
        parent_id: body.parent_id,
        format: body.format,
        content: content_str,
        created_at: String::new(), // DB sets this
    };

    let store = state.chat_store.clone();
    tokio::task::spawn_blocking(move || store.upsert_message(&msg))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "ok": true })))
}

/// `DELETE /threads/{id}/messages` — Delete all messages for a thread.
pub async fn delete_messages(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    tokio::task::spawn_blocking(move || store.delete_messages(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "ok": true })))
}

// ──────────────────────────────────────────────────────────────────────────────
// Result handlers
// ──────────────────────────────────────────────────────────────────────────────

/// `GET /threads/{id}/results` — Get all results for a thread.
pub async fn get_results(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    let results = tokio::task::spawn_blocking(move || store.get_results(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Parse params/data from JSON strings to Values
    let results: Vec<Value> = results
        .into_iter()
        .map(|r| {
            let params: Value = serde_json::from_str(&r.params).unwrap_or_else(|_| json!({}));
            let data: Option<Value> = r.data.as_ref().and_then(|d| serde_json::from_str(d).ok());
            json!({
                "id": r.id,
                "thread_id": r.thread_id,
                "key": r.key,
                "type": r.result_type,
                "label": r.label,
                "tool_call_id": r.tool_call_id,
                "params": params,
                "data": data,
                "created_at": r.created_at,
            })
        })
        .collect();

    Ok(Json(json!({ "results": results })))
}

/// `PUT /threads/{id}/results` and `POST /threads/{id}/results` — Replace all results.
pub async fn replace_results(
    State(state): State<AppState>,
    Path(thread_id): Path<String>,
    Json(body): Json<ReplaceResultsBody>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let inputs: Vec<crate::data::traits::ResultInput> = body
        .results
        .into_iter()
        .map(|r| {
            let params_str = serde_json::to_string(&r.params).unwrap_or_else(|_| "{}".to_string());
            let data_str = r
                .data
                .map(|d| serde_json::to_string(&d).unwrap_or_else(|_| "null".to_string()));
            crate::data::traits::ResultInput {
                key: r.key,
                result_type: r.result_type,
                label: r.label,
                tool_call_id: r.tool_call_id,
                params: params_str,
                data: data_str,
            }
        })
        .collect();

    let store = state.chat_store.clone();
    tokio::task::spawn_blocking(move || store.replace_all_results(&thread_id, &inputs))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "ok": true })))
}

/// `DELETE /threads/{id}/results/{key}` — Delete a single result.
pub async fn delete_result(
    State(state): State<AppState>,
    Path((thread_id, key)): Path<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = state.chat_store.clone();
    tokio::task::spawn_blocking(move || store.delete_result(&thread_id, &key))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({ "ok": true })))
}
