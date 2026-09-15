//! Channel webhook mounts.

use crate::{error_response, parse_ids, AppState};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use oah_channels::{
    reject_oversize, verify_bearer, verify_github, verify_slack_v0, webhook_signal, ChannelError,
};
use oah_core::{ConversationId, Principal, UnixMillis};
use serde_json::{json, Value};

pub async fn slack_hook(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(secret) = state.channels.slack.as_deref() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel_disabled",
            "Slack signing secret is not configured",
        );
    };
    if let Err(resp) = map_size(&body) {
        return resp;
    }
    let ts = header_str(&headers, "x-slack-request-timestamp").unwrap_or("");
    let sig = header_str(&headers, "x-slack-signature").unwrap_or("");
    let now = UnixMillis::now_system().as_millis() / 1000;
    if let Err(err) = verify_slack_v0(secret, ts, &body, sig, now) {
        return map_channel_err(err);
    }
    admit_webhook(state, &agent, &id, "slack", &body).await
}

pub async fn github_hook(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(secret) = state.channels.github.as_deref() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel_disabled",
            "GitHub webhook secret is not configured",
        );
    };
    if let Err(resp) = map_size(&body) {
        return resp;
    }
    let sig = header_str(&headers, "x-hub-signature-256").unwrap_or("");
    if let Err(err) = verify_github(secret, &body, sig) {
        return map_channel_err(err);
    }
    admit_webhook(state, &agent, &id, "github", &body).await
}

pub async fn bearer_hook(
    State(state): State<AppState>,
    Path((agent, id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(token) = state.channels.bearer.as_deref() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel_disabled",
            "Bearer channel token is not configured",
        );
    };
    if let Err(resp) = map_size(&body) {
        return resp;
    }
    let auth = header_str(&headers, "authorization").unwrap_or("");
    if let Err(err) = verify_bearer(token, auth) {
        return map_channel_err(err);
    }
    admit_webhook(state, &agent, &id, "bearer", &body).await
}

#[allow(clippy::result_large_err)]
fn map_size(body: &Bytes) -> Result<(), Response> {
    reject_oversize(body).map_err(map_channel_err)
}

fn map_channel_err(err: ChannelError) -> Response {
    match err {
        ChannelError::TooLarge => error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            err.to_string(),
        ),
        ChannelError::Unauthorized | ChannelError::Skew => {
            error_response(StatusCode::UNAUTHORIZED, "unauthorized", err.to_string())
        }
        ChannelError::Other(msg) => {
            error_response(StatusCode::BAD_REQUEST, "invalid_request", msg)
        }
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

async fn admit_webhook(
    state: AppState,
    agent: &str,
    id: &str,
    channel: &str,
    body: &Bytes,
) -> Response {
    let parsed = match parse_ids(agent, id) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let payload: Value = serde_json::from_slice(body).unwrap_or_else(|_| {
        json!({ "raw": String::from_utf8_lossy(body) })
    });
    let cid = ConversationId::new(&parsed.0, &parsed.1);
    let req = webhook_signal(cid, channel, payload, Principal::anonymous());
    match state.runtime.admit(req).await {
        Ok(receipt) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "submissionId": receipt.submission_id.to_string(),
                "uid": receipt.uid,
                "channel": channel
            })),
        )
            .into_response(),
        Err(err) => crate::map_runtime_err(err),
    }
}
