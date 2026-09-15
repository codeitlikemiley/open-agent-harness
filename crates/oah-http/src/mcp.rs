//! MCP JSON-RPC door: `POST /mcp`.

use crate::{error_response, AppState};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use oah_core::{AgentName, InstanceId, Principal};
use oah_mcp::rpc::{handle_sync, rpc_error, rpc_ok, JsonRpcRequest};
use serde_json::{json, Value};

pub async fn post_mcp(State(state): State<AppState>, Json(req): Json<JsonRpcRequest>) -> Response {
    if req.jsonrpc != "2.0" {
        return Json(rpc_error(req.id, -32600, "jsonrpc must be 2.0")).into_response();
    }
    if let Some(ready) = handle_sync(&state.mcp, &req) {
        return Json(ready).into_response();
    }
    if req.method == "notifications/initialized" || req.method == "notifications/cancelled" {
        return StatusCode::NO_CONTENT.into_response();
    }
    if req.method == "tools/call" {
        let name = req
            .params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("");
        if name == "oah_dispatch" {
            return dispatch_agent(&state, &req).await;
        }
    }
    Json(rpc_error(
        req.id,
        -32601,
        format!("method not found: {}", req.method),
    ))
    .into_response()
}

async fn dispatch_agent(state: &AppState, req: &JsonRpcRequest) -> Response {
    let args = req
        .params
        .get("arguments")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    let agent = args.get("agent").and_then(Value::as_str).unwrap_or("");
    let id = args.get("id").and_then(Value::as_str).unwrap_or("");
    let message = args
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let agent = match AgentName::parse(agent) {
        Ok(a) => a,
        Err(_) => {
            return Json(rpc_error(req.id.clone(), -32602, "invalid agent")).into_response()
        }
    };
    let instance = match InstanceId::parse(id) {
        Ok(i) => i,
        Err(_) => {
            return Json(rpc_error(req.id.clone(), -32602, "invalid instance")).into_response()
        }
    };
    match state
        .runtime
        .dispatch(
            &agent,
            &instance,
            message,
            Principal::anonymous(),
            None,
            None,
        )
        .await
    {
        Ok(receipt) => Json(rpc_ok(
            req.id.clone(),
            json!({
                "content": [{
                    "type": "text",
                    "text": format!(
                        "admitted {} uid={}",
                        receipt.submission_id, receipt.uid
                    )
                }],
                "isError": false
            }),
        ))
        .into_response(),
        Err(err) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            err.to_string(),
        ),
    }
}
