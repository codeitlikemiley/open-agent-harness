//! Streaming client for `hexuria/open-ai-gateway` `POST /v1/messages`.
//!
//! The harness never holds provider credentials. A key is an `oag_live_…`
//! bearer that the gateway maps to a principal, route and spend quota.

#![forbid(unsafe_code)]

use futures::Stream;
use futures::StreamExt;
use oah_model::{
    Catalog, ContentBlock, MessageRole, ModelCapabilities, ModelClient, ModelError, ModelErrorKind,
    ModelEvent, ModelInfo, ModelRequest, ModelStream, StopReason, Usage,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct OagConfig {
    pub base_url: String,
    pub api_key: String,
}

impl OagConfig {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum OagBuildError {
    #[error("gateway URL is empty")]
    EmptyUrl,
    #[error("gateway key is empty or a placeholder")]
    EmptyKey,
    #[error("http client: {0}")]
    Client(String),
}

pub struct OagClient {
    http: reqwest::Client,
    cfg: Arc<OagConfig>,
}

impl OagClient {
    pub fn new(cfg: OagConfig) -> Result<Self, OagBuildError> {
        if cfg.base_url.is_empty() {
            return Err(OagBuildError::EmptyUrl);
        }
        if cfg.api_key.is_empty() || cfg.api_key.contains("changeme") || cfg.api_key.len() < 16 {
            return Err(OagBuildError::EmptyKey);
        }
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| OagBuildError::Client(e.to_string()))?;
        Ok(Self {
            http,
            cfg: Arc::new(cfg),
        })
    }
}

#[async_trait::async_trait]
impl ModelClient for OagClient {
    async fn stream(
        &self,
        req: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        let body = encode_messages_request(&req);
        let mut headers = HeaderMap::new();
        let bearer = format!("Bearer {}", self.cfg.api_key);
        let auth = HeaderValue::from_str(&bearer).map_err(|_| {
            ModelError::new(ModelErrorKind::Authentication, "invalid gateway key bytes")
        })?;
        headers.insert(AUTHORIZATION, auth);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let url = format!("{}/v1/messages", self.cfg.base_url);
        let request = self.http.post(url).headers(headers).json(&body);
        let response = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(ModelError::new(ModelErrorKind::Cancelled, "cancelled before response"));
            }
            res = request.send() => res.map_err(|e| ModelError::new(ModelErrorKind::Upstream, e.to_string()))?,
        };

        if !response.status().is_success() {
            return Err(map_http_error(response).await);
        }

        let served = response
            .headers()
            .get("x-oag-model")
            .and_then(|v| v.to_str().ok())
            .map(ToOwned::to_owned);
        Ok(sse_to_stream(response.bytes_stream(), served, cancel))
    }

    async fn catalog(&self) -> Result<Catalog, ModelError> {
        let url = format!("{}/v1/models", self.cfg.base_url);
        let resp = self
            .http
            .get(url)
            .bearer_auth(&self.cfg.api_key)
            .send()
            .await
            .map_err(|e| ModelError::new(ModelErrorKind::Upstream, e.to_string()))?;
        if !resp.status().is_success() {
            return Err(map_http_error(resp).await);
        }
        let value: Value = resp
            .json()
            .await
            .map_err(|e| ModelError::new(ModelErrorKind::Internal, e.to_string()))?;
        Ok(parse_catalog(value))
    }
}

fn encode_messages_request(req: &ModelRequest) -> Value {
    let mut messages = Vec::new();
    for msg in &req.messages {
        let role = match msg.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
        };
        let mut content = Vec::new();
        for block in &msg.content {
            match block {
                ContentBlock::Text { text } => {
                    content.push(json!({"type": "text", "text": text}));
                }
                ContentBlock::Thinking { text, signature } => {
                    let mut obj = json!({"type": "thinking", "thinking": text});
                    if let Some(sig) = signature {
                        obj["signature"] = json!(sig);
                    }
                    content.push(obj);
                }
                ContentBlock::ToolUse { id, name, input } => {
                    content.push(json!({
                        "type": "tool_use",
                        "id": id,
                        "name": name,
                        "input": input
                    }));
                }
                ContentBlock::ToolResult {
                    tool_use_id,
                    content: c,
                    is_error,
                } => {
                    content.push(json!({
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": c,
                        "is_error": is_error
                    }));
                }
                ContentBlock::Image { media_type, data } => {
                    content.push(json!({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": data
                        }
                    }));
                }
            }
        }
        messages.push(json!({"role": role, "content": content}));
    }

    let tools: Vec<Value> = req
        .tools
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema
            })
        })
        .collect();

    let mut body = json!({
        "model": req.model,
        "max_tokens": req.max_tokens,
        "stream": true,
        "system": req.system,
        "messages": messages,
    });
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools);
    }
    if let Some(user) = &req.affinity_user_id {
        body["metadata"] = json!({"user_id": user});
    }
    if let Some(level) = req.thinking {
        if !matches!(
            level,
            oah_model::ThinkingLevel::Off | oah_model::ThinkingLevel::Minimal
        ) {
            body["thinking"] = json!({"type": "enabled"});
            body["output_config"] = json!({"effort": level.as_str()});
        }
    }
    body
}

async fn map_http_error(response: reqwest::Response) -> ModelError {
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let error_type = parsed
        .pointer("/error/type")
        .and_then(Value::as_str)
        .unwrap_or(match status {
            401 => "authentication_error",
            402 => "budget_exhausted",
            400 => "invalid_request",
            429 => "rate_limit_error",
            503 => "overloaded",
            504 => "upstream_timeout",
            502 => "upstream_error",
            _ => "internal_error",
        });
    let message = parsed
        .pointer("/error/message")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or(body);
    let mut err = ModelError::from_gateway_type(error_type, message);
    if err.kind == ModelErrorKind::Upstream && (400..=402).contains(&status) {
        err.retryable = false;
    }
    err
}

fn parse_catalog(value: Value) -> Catalog {
    let mut models = Vec::new();
    if let Some(arr) = value.get("data").and_then(Value::as_array) {
        for item in arr {
            let id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let oag = item.get("oag").cloned().unwrap_or(Value::Null);
            models.push(ModelInfo {
                id,
                context_window: oag
                    .get("context_window")
                    .and_then(Value::as_u64)
                    .unwrap_or(128_000) as u32,
                max_output_tokens: oag
                    .get("max_output_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(4_096) as u32,
                capabilities: ModelCapabilities {
                    vision: oag
                        .pointer("/capabilities/vision")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    tools: oag
                        .pointer("/capabilities/tools")
                        .and_then(Value::as_bool)
                        .unwrap_or(true),
                    reasoning: oag
                        .pointer("/capabilities/reasoning")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    prompt_cache: oag
                        .pointer("/capabilities/prompt_cache")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                },
                input_price_per_mtok: None,
                output_price_per_mtok: None,
            });
        }
    }
    Catalog { models }
}

struct RxStream(mpsc::Receiver<Result<ModelEvent, ModelError>>);

impl Stream for RxStream {
    type Item = Result<ModelEvent, ModelError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

fn sse_to_stream<S>(inner: S, served: Option<String>, cancel: CancellationToken) -> ModelStream
where
    S: Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + Unpin + 'static,
{
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        let mut pending = String::new();
        let mut started = false;
        let mut saw_done = false;
        let mut stream = inner;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = tx.send(Err(ModelError::new(ModelErrorKind::Cancelled, "cancelled"))).await;
                    break;
                }
                next = stream.next() => {
                    match next {
                        None => {
                            if !saw_done {
                                let _ = tx.send(Err(ModelError::new(
                                    ModelErrorKind::Interrupted,
                                    "missing terminal event",
                                ))).await;
                            }
                            break;
                        }
                        Some(Err(err)) => {
                            let _ = tx.send(Err(ModelError::new(ModelErrorKind::Upstream, err.to_string()))).await;
                            break;
                        }
                        Some(Ok(bytes)) => {
                            let Ok(chunk) = std::str::from_utf8(&bytes) else {
                                let _ = tx.send(Err(ModelError::new(
                                    ModelErrorKind::Internal,
                                    "non-utf8 sse chunk",
                                ))).await;
                                break;
                            };
                            for ev in parse_sse_chunk(&mut pending, chunk, &mut started, &served) {
                                if matches!(&ev, Ok(ModelEvent::Done(_))) {
                                    saw_done = true;
                                }
                                if tx.send(ev).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    });
    Box::pin(RxStream(rx))
}

/// Parse an Anthropic-dialect SSE stream into `ModelEvent`s.
pub fn parse_sse_chunk(
    pending: &mut String,
    chunk: &str,
    started: &mut bool,
    served: &Option<String>,
) -> Vec<Result<ModelEvent, ModelError>> {
    pending.push_str(chunk);
    let mut out = Vec::new();
    while let Some(idx) = pending.find("\n\n") {
        let frame = pending[..idx].to_string();
        pending.replace_range(..idx + 2, "");
        if let Some(ev) = parse_sse_frame(&frame, started, served) {
            out.push(ev);
        }
    }
    out
}

fn parse_sse_frame(
    frame: &str,
    started: &mut bool,
    served: &Option<String>,
) -> Option<Result<ModelEvent, ModelError>> {
    let mut event = "message";
    let mut data = String::new();
    for line in frame.lines() {
        if line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = rest.trim();
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        }
    }
    if data.is_empty() {
        return None;
    }
    let value: Value = match serde_json::from_str(&data) {
        Ok(v) => v,
        Err(err) => {
            return Some(Err(ModelError::new(
                ModelErrorKind::Internal,
                format!("malformed sse json: {err}"),
            )))
        }
    };
    match event {
        "message_start" => {
            *started = true;
            Some(Ok(ModelEvent::Start {
                served_model: served.clone(),
            }))
        }
        "content_block_start" => {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as u32;
            let block = value.get("content_block").cloned().unwrap_or(Value::Null);
            match block.get("type").and_then(Value::as_str) {
                Some("tool_use") => Some(Ok(ModelEvent::ToolCallStart {
                    index,
                    id: block.get("id").and_then(Value::as_str).unwrap_or("").to_string(),
                    name: block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                })),
                _ => None,
            }
        }
        "content_block_delta" => {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as u32;
            let delta = value.get("delta").cloned().unwrap_or(Value::Null);
            match delta.get("type").and_then(Value::as_str) {
                Some("text_delta") => Some(Ok(ModelEvent::TextDelta(
                    delta.get("text").and_then(Value::as_str).unwrap_or("").to_string(),
                ))),
                Some("thinking_delta") => Some(Ok(ModelEvent::ThinkingDelta(
                    delta
                        .get("thinking")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                ))),
                Some("signature_delta") => Some(Ok(ModelEvent::ThinkingSignature(
                    delta
                        .get("signature")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                ))),
                Some("input_json_delta") => Some(Ok(ModelEvent::ToolCallArgsDelta {
                    index,
                    json: delta
                        .get("partial_json")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                })),
                _ => None,
            }
        }
        "content_block_stop" => {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0) as u32;
            Some(Ok(ModelEvent::ToolCallEnd { index }))
        }
        "message_delta" => {
            if let Some(reason) = value.pointer("/delta/stop_reason").and_then(Value::as_str) {
                return Some(Ok(ModelEvent::Done(StopReason::parse(reason))));
            }
            let usage = value.get("usage").cloned().unwrap_or(Value::Null);
            if usage.is_object() {
                Some(Ok(ModelEvent::Usage(Usage {
                    input_tokens: usage.get("input_tokens").and_then(Value::as_u64).unwrap_or(0) as u32,
                    output_tokens: usage.get("output_tokens").and_then(Value::as_u64).unwrap_or(0)
                        as u32,
                    cache_read_tokens: usage
                        .get("cache_read_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as u32,
                    cache_write_tokens: usage
                        .get("cache_creation_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as u32,
                })))
            } else {
                None
            }
        }
        "message_stop" => Some(Ok(ModelEvent::Done(StopReason::Stop))),
        "error" => {
            let t = value
                .pointer("/error/type")
                .and_then(Value::as_str)
                .unwrap_or("api_error");
            let m = value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("in-stream error");
            Some(Err(ModelError::from_gateway_type(t, m)))
        }
        "ping" => None,
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn parses_text_and_done() {
        let mut pending = String::new();
        let mut started = false;
        let served = Some("anthropic/claude".into());
        let chunk = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\"}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}\n\n",
        );
        let evs = parse_sse_chunk(&mut pending, chunk, &mut started, &served);
        assert!(evs
            .iter()
            .any(|e| matches!(e, Ok(ModelEvent::TextDelta(t)) if t == "hi")));
        assert!(evs.iter().any(|e| matches!(e, Ok(ModelEvent::Done(_)))));
    }
}
