//! HTTP routes that follow Flue's v1 snapshot shape, plus a demo console.

#![forbid(unsafe_code)]

pub mod agui;
pub mod hooks;
pub mod mcp;
pub mod protocol;
pub mod ui;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use oah_channels::ChannelSecrets;
use oah_core::{
    AgentName, ConversationId, FoldState, InstanceId, Principal, RecordBatch, StreamOffset,
    UnixMillis,
};
use oah_mcp::InProcessMcp;
use oah_runtime::{list_agent_names, Runtime};
use oah_schedules::ScheduleBook;
use oah_store::StoreError;
use tokio::sync::Mutex;
use crate::protocol::{
    AbortBody, AdmitBody, AdmitResponse, ApprovalAnswer, ErrorEnvelope, HistorySnapshot,
    UpdatesQuery,
};
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<Runtime>,
    pub public_base: String,
    pub channels: ChannelSecrets,
    pub schedules: Arc<Mutex<ScheduleBook>>,
    pub mcp: Arc<InProcessMcp>,
}

pub fn agent_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui::index))
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/agents", get(list_agents))
        .route("/agents/{agent}/{id}", get(get_conversation).post(post_message).head(head_conversation))
        .route("/agents/{agent}/{id}/abort", post(post_abort))
        .route("/agents/{agent}/{id}/approvals", get(list_approvals))
        .route(
            "/agents/{agent}/{id}/approvals/{call}",
            post(answer_approval),
        )
        .route("/hooks/slack/{agent}/{id}", post(hooks::slack_hook))
        .route("/hooks/github/{agent}/{id}", post(hooks::github_hook))
        .route("/hooks/bearer/{agent}/{id}", post(hooks::bearer_hook))
        .route("/schedules", get(list_schedules).post(upsert_schedule))
        .route("/schedules/{id}", axum::routing::delete(delete_schedule))
        .route("/ag-ui", post(post_agui))
        .route("/mcp", post(mcp::post_mcp))
        .with_state(state)
}

async fn live() -> impl IntoResponse {
    Json(json!({"ok": true}))
}

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    match state.runtime.store.migrate().await {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true, "store": "ready"}))),
        Err(err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"ok": false, "store": err.to_string()})),
        ),
    }
}

async fn list_agents(State(state): State<AppState>) -> impl IntoResponse {
    let agents: Vec<_> = list_agent_names(&state.runtime)
        .into_iter()
        .filter_map(|n| {
            state.runtime.agents.get(&n).map(|a| {
                json!({
                    "name": n.as_str(),
                    "description": a.description(),
                })
            })
        })
        .collect();
    Json(json!({ "agents": agents }))
}

async fn post_message(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
    Query(q): Query<UpdatesQuery>,
    headers: HeaderMap,
    Json(body): Json<AdmitBody>,
) -> Response {
    if q.wait.is_some() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "?wait is not supported; poll GET view=updates instead",
        );
    }
    let parsed = match parse_ids(&agent, &id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let principal = principal_from(&headers);
    match state
        .runtime
        .dispatch(
            &parsed.0,
            &parsed.1,
            body.message_text(),
            principal,
            body.idempotency_key.clone(),
            body.uid.clone(),
        )
        .await
    {
        Ok(receipt) => {
            let cid = ConversationId::new(&parsed.0, &parsed.1);
            let stream_url = format!(
                "{}/agents/{}/{}?view=updates&offset=-1&live=sse",
                state.public_base.trim_end_matches('/'),
                parsed.0,
                parsed.1
            );
            let mut res = Json(AdmitResponse {
                stream_url: stream_url.clone(),
                offset: receipt.offset.to_string(),
                submission_id: receipt.submission_id.to_string(),
                uid: receipt.uid,
                deduplicated: if receipt.deduplicated {
                    Some(true)
                } else {
                    None
                },
            })
            .into_response();
            *res.status_mut() = StatusCode::ACCEPTED;
            if let Ok(loc) = HeaderValue::from_str(&format!("/agents/{}/{}", parsed.0, parsed.1)) {
                res.headers_mut().insert(header::LOCATION, loc);
            }
            if let Ok(off) = HeaderValue::from_str(&receipt.offset.to_string()) {
                res.headers_mut()
                    .insert("Stream-Next-Offset", off);
            }
            let _ = cid;
            res
        }
        Err(err) => map_runtime_err(err),
    }
}

async fn get_conversation(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
    Query(q): Query<UpdatesQuery>,
) -> Response {
    let parsed = match parse_ids(&agent, &id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let cid = ConversationId::new(&parsed.0, &parsed.1);
    if q.view.as_deref() == Some("updates") {
        return stream_updates(state, cid, q).await;
    }
    match history_snapshot(&state, &cid).await {
        Ok(snap) => Json(snap).into_response(),
        Err(StoreError::NotFound(_)) => error_response(
            StatusCode::NOT_FOUND,
            "stream_not_found",
            "conversation does not exist yet",
        ),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", err.to_string()),
    }
}

async fn head_conversation(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
) -> Response {
    let parsed = match parse_ids(&agent, &id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let cid = ConversationId::new(&parsed.0, &parsed.1);
    match state.runtime.store.stream_head(cid.as_str()).await {
        Ok((offset, _, _)) => {
            let mut res = StatusCode::OK.into_response();
            if let Ok(v) = HeaderValue::from_str(&offset.to_string()) {
                res.headers_mut().insert("Stream-Next-Offset", v);
            }
            res
        }
        Err(StoreError::NotFound(_)) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn post_abort(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
) -> Response {
    let parsed = match parse_ids(&agent, &id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match state.runtime.abort(&parsed.0, &parsed.1).await {
        Ok(n) => Json(AbortBody { aborted: n > 0, count: n }).into_response(),
        Err(err) => map_runtime_err(err),
    }
}

async fn list_approvals(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
) -> Response {
    let parsed = match parse_ids(&agent, &id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let cid = ConversationId::new(&parsed.0, &parsed.1);
    match state.runtime.store.list_pending_approvals(&cid).await {
        Ok(items) => Json(json!({
            "approvals": items.iter().map(|s| json!({
                "submissionId": s.submission_id.to_string(),
                "toolCallId": s.tool_call_id,
                "tool": s.tool,
                "reason": s.reason,
                "kind": s.kind,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", err.to_string()),
    }
}

async fn answer_approval(
    State(state): State<AppState>,
    Path((agent, id, call)): Path<(String, String, String)>,
    Json(body): Json<ApprovalAnswer>,
) -> Response {
    let parsed = match parse_ids(&agent, &id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let cid = ConversationId::new(&parsed.0, &parsed.1);
    let pending = match state.runtime.store.list_pending_approvals(&cid).await {
        Ok(p) => p,
        Err(err) => {
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", err.to_string())
        }
    };
    let Some(item) = pending.iter().find(|s| s.tool_call_id == call) else {
        return error_response(StatusCode::NOT_FOUND, "route_not_found", "approval not found");
    };
    match state
        .runtime
        .answer(&item.submission_id, &call, body.approved, "console")
        .await
    {
        Ok(()) => Json(json!({"ok": true, "approved": body.approved})).into_response(),
        Err(oah_runtime::RuntimeError::Store(StoreError::AlreadyAnswered)) => {
            error_response(StatusCode::CONFLICT, "already_answered", "already answered")
        }
        Err(oah_runtime::RuntimeError::Store(StoreError::SubmissionSettled)) => error_response(
            StatusCode::GONE,
            "submission_settled",
            "submission already settled",
        ),
        Err(err) => map_runtime_err(err),
    }
}

async fn history_snapshot(state: &AppState, cid: &ConversationId) -> Result<HistorySnapshot, StoreError> {
    let (offset, incarnation, _) = state.runtime.store.stream_head(cid.as_str()).await?;
    let records = state.runtime.store.read_all(cid.as_str()).await?;
    let mut fold = FoldState::default();
    let _ = oah_core::reduce_batch(
        &mut fold,
        &RecordBatch {
            path: cid.to_string(),
            seq: 1,
            records,
            submission_id: None,
            attempt_id: None,
        },
    );
    Ok(HistorySnapshot {
        v: 1,
        conversation_id: cid.to_string(),
        offset: offset.to_string(),
        messages: fold.messages,
        settlements: fold.settlements,
        incarnation,
    })
}

async fn stream_updates(state: AppState, cid: ConversationId, q: UpdatesQuery) -> Response {
    let after = match StreamOffset::parse(q.offset.as_deref().unwrap_or("-1")) {
        Ok(o) => o,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_request", "malformed offset")
        }
    };
    let live = q.live.as_deref().unwrap_or("");
    if live == "sse" {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(64);
        tokio::spawn(async move {
            let mut cursor = after;
            let mut notify = state.runtime.store.notify();
            let started = UnixMillis::now_system();
            loop {
                match state
                    .runtime
                    .store
                    .read_after(cid.as_str(), cursor, 100)
                    .await
                {
                    Ok(batches) => {
                        if batches.is_empty() {
                            let ev = Event::default()
                                .event("control")
                                .data(json!({"streamNextOffset": cursor.to_string(), "upToDate": true}).to_string());
                            if tx.send(Ok(ev)).await.is_err() {
                                break;
                            }
                        } else {
                            for batch in batches {
                                cursor = StreamOffset::new(batch.seq, 0);
                                for rec in batch.records {
                                    let ev = Event::default()
                                        .event("data")
                                        .data(serde_json::to_string(&rec).unwrap_or_else(|_| "{}".into()));
                                    if tx.send(Ok(ev)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(StoreError::NotFound(_)) => {
                        let ev = Event::default().event("control").data(
                            json!({"streamNextOffset": "-1", "upToDate": true}).to_string(),
                        );
                        let _ = tx.send(Ok(ev)).await;
                    }
                    Err(err) => {
                        warn!(error = %err, "sse read failed");
                    }
                }
                if started.as_millis() + 30_000 < UnixMillis::now_system().as_millis() {
                    break;
                }
                tokio::select! {
                    _ = notify.changed() => {}
                    _ = sleep(Duration::from_millis(250)) => {}
                }
            }
        });
        return Sse::new(ReceiverStream::new(rx))
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("heartbeat"))
            .into_response();
    }

    // long-poll / snapshot of new batches
    let deadline = UnixMillis::now_system().as_millis() + 30_000;
    let mut notify = state.runtime.store.notify();
    loop {
        match state.runtime.store.read_after(cid.as_str(), after, 100).await {
            Ok(batches) if !batches.is_empty() => {
                return Json(json!({
                    "v": 1,
                    "chunks": batches,
                }))
                .into_response();
            }
            Ok(_) => {}
            Err(StoreError::NotFound(_)) => {
                return error_response(
                    StatusCode::NOT_FOUND,
                    "stream_not_found",
                    "conversation does not exist yet",
                );
            }
            Err(err) => {
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", err.to_string());
            }
        }
        if UnixMillis::now_system().as_millis() >= deadline {
            return Json(json!([{"type": "stream-checkpoint", "incarnation": 0}])).into_response();
        }
        tokio::select! {
            _ = notify.changed() => {}
            _ = sleep(Duration::from_millis(250)) => {}
        }
    }
}

async fn list_schedules(State(state): State<AppState>) -> impl IntoResponse {
    let book = state.schedules.lock().await;
    let items: Vec<_> = book
        .items
        .values()
        .map(|s| {
            json!({
                "id": s.id.to_string(),
                "agent": s.agent.to_string(),
                "instance": s.instance.to_string(),
                "cron": s.cron.0,
                "nextDue": s.next_due.as_millis(),
                "enabled": s.enabled,
            })
        })
        .collect();
    Json(json!({ "schedules": items }))
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScheduleBody {
    agent: String,
    instance: String,
    cron: String,
    #[serde(default)]
    payload: serde_json::Value,
    #[serde(default = "default_true")]
    enabled: bool,
}

fn default_true() -> bool {
    true
}

async fn upsert_schedule(
    State(state): State<AppState>,
    Json(body): Json<ScheduleBody>,
) -> Response {
    let agent = match AgentName::parse(&body.agent) {
        Ok(a) => a,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_request", "invalid agent")
        }
    };
    let instance = match InstanceId::parse(&body.instance) {
        Ok(i) => i,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_request", "invalid instance")
        }
    };
    let cron = match oah_schedules::CronExpr::parse(&body.cron) {
        Ok(c) => c,
        Err(err) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_request", err.to_string())
        }
    };
    let now = UnixMillis::now_system();
    let next_due = match cron.next_after(now) {
        Ok(n) => n,
        Err(err) => {
            return error_response(StatusCode::BAD_REQUEST, "invalid_request", err.to_string())
        }
    };
    let schedule = oah_schedules::Schedule {
        id: oah_core::ScheduleId::new(),
        agent,
        instance,
        cron,
        payload: body.payload,
        next_due,
        enabled: body.enabled,
    };
    let id = schedule.id.to_string();
    state.schedules.lock().await.insert(schedule);
    (StatusCode::CREATED, Json(json!({ "id": id, "nextDue": next_due.as_millis() }))).into_response()
}

async fn delete_schedule(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    let parsed = match oah_core::ScheduleId::parse(&id) {
        Ok(i) => i,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid_request", "invalid id"),
    };
    match state.schedules.lock().await.remove(&parsed) {
        Some(_) => Json(json!({"ok": true})).into_response(),
        None => error_response(StatusCode::NOT_FOUND, "route_not_found", "schedule not found"),
    }
}

async fn post_agui(State(state): State<AppState>, Json(body): Json<agui::AguiRunRequest>) -> Response {
    let (agent_s, id_s) = match body.agent_instance() {
        Some(v) => v,
        None => {
            let agent = body.agent.clone().unwrap_or_else(|| "support-desk".into());
            (agent, "agui".into())
        }
    };
    let parsed = match parse_ids(&agent_s, &id_s) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let text = body.text();
    match state
        .runtime
        .dispatch(
            &parsed.0,
            &parsed.1,
            text,
            Principal::anonymous(),
            None,
            body.run_id.clone(),
        )
        .await
    {
        Ok(receipt) => {
            let thread = format!("{}/{}", parsed.0, parsed.1);
            Json(json!({
                "runId": receipt.submission_id.to_string(),
                "threadId": thread,
                "events": [agui::run_started(receipt.submission_id.as_str(), &thread)],
                "streamUrl": format!(
                    "{}/agents/{}/{}?view=updates&offset=-1&live=sse",
                    state.public_base.trim_end_matches('/'),
                    parsed.0,
                    parsed.1
                )
            }))
            .into_response()
        }
        Err(err) => map_runtime_err(err),
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn parse_ids(agent: &str, id: &str) -> Result<(AgentName, InstanceId), Response> {
    let agent = AgentName::parse(agent).map_err(|_| {
        error_response(StatusCode::BAD_REQUEST, "invalid_request", "invalid agent name")
    })?;
    let id = InstanceId::parse(id).map_err(|_| {
        error_response(StatusCode::BAD_REQUEST, "invalid_request", "invalid instance id")
    })?;
    Ok((agent, id))
}

fn principal_from(headers: &HeaderMap) -> Principal {
    if let Some(v) = headers.get("x-oah-principal") {
        if let Ok(s) = v.to_str() {
            return Principal {
                kind: oah_core::principal::PrincipalKind::User,
                id: s.to_string(),
                display: None,
            };
        }
    }
    Principal::anonymous()
}

pub(crate) fn map_runtime_err(err: oah_runtime::RuntimeError) -> Response {
    match err {
        oah_runtime::RuntimeError::UnknownAgent(_) => {
            error_response(StatusCode::NOT_FOUND, "route_not_found", err.to_string())
        }
        oah_runtime::RuntimeError::Store(StoreError::Conflict(msg)) if msg == "submission_conflict" => {
            error_response(StatusCode::CONFLICT, "submission_conflict", msg)
        }
        oah_runtime::RuntimeError::Store(StoreError::Conflict(msg)) if msg == "agent_instance_exists" => {
            error_response(StatusCode::CONFLICT, "agent_instance_exists", msg)
        }
        oah_runtime::RuntimeError::Store(StoreError::NotFound(msg)) => {
            error_response(StatusCode::NOT_FOUND, "agent_instance_not_found", msg)
        }
        other => error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", other.to_string()),
    }
}

pub(crate) fn error_response(status: StatusCode, ty: &str, message: impl Into<String>) -> Response {
    let env = ErrorEnvelope::new(ty, message);
    let mut res = Json(env.clone()).into_response();
    *res.status_mut() = status;
    if let Ok(v) = HeaderValue::from_str(&env.error.ref_) {
        res.headers_mut().insert("flue-error-ref", v);
    }
    res.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    res
}

// tokio-stream is not in workspace deps. Provide a tiny Stream wrapper.
mod tokio_stream {
    pub mod wrappers {
        use futures::Stream;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::sync::mpsc::Receiver;

        pub struct ReceiverStream<T>(pub Receiver<T>);

        impl<T> ReceiverStream<T> {
            pub fn new(rx: Receiver<T>) -> Self {
                Self(rx)
            }
        }

        impl<T> Stream for ReceiverStream<T> {
            type Item = T;
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
                self.0.poll_recv(cx)
            }
        }
    }
}
