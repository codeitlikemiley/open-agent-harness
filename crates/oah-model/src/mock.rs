use crate::catalog::Catalog;
use crate::client::ModelClient;
use crate::error::{ModelError, ModelErrorKind};
use crate::request::{ContentBlock, MessageRole, ModelRequest};
use crate::stream::{ModelEvent, ModelStream, StopReason, Usage};
use futures::stream;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Deterministic, scripted model. Compiled into every build; the host refuses
/// to start a *release* binary in mock mode (MDL-12).
pub struct MockModel {
    turns: Mutex<Vec<ScriptedTurn>>,
    cursor: AtomicUsize,
    /// When the script is exhausted, synthesize a reply from the last user text.
    fallback_echo: bool,
}

#[derive(Debug, Clone)]
pub enum ScriptedTurn {
    Text(String),
    Tool {
        name: String,
        arguments: Value,
        id: String,
    },
    ThenText(String),
    Fail(ModelError),
}

impl MockModel {
    pub fn new(turns: Vec<ScriptedTurn>) -> Self {
        Self {
            turns: Mutex::new(turns),
            cursor: AtomicUsize::new(0),
            fallback_echo: true,
        }
    }

    pub fn scripted(turns: Vec<ScriptedTurn>) -> Self {
        Self {
            fallback_echo: false,
            ..Self::new(turns)
        }
    }

    pub fn support_desk() -> Self {
        Self::new(vec![
            ScriptedTurn::Tool {
                name: "lookup_ticket".into(),
                arguments: serde_json::json!({"id": "{{id}}"}),
                id: "call_mock_lookup".into(),
            },
            ScriptedTurn::ThenText(String::new()),
        ])
    }

    fn next_turn(&self, req: &ModelRequest) -> Result<Vec<ModelEvent>, ModelError> {
        if let Ok(guard) = self.turns.lock() {
            let i = self.cursor.load(Ordering::SeqCst);
            if let Some(turn) = guard.get(i) {
                self.cursor.store(i.saturating_add(1), Ordering::SeqCst);
                return Ok(events_for_turn(turn, req));
            }
        }
        if self.fallback_echo {
            return Ok(fallback_events(req));
        }
        Err(ModelError::new(
            ModelErrorKind::InvalidRequest,
            "mock script exhausted",
        ))
    }
}

fn events_for_turn(turn: &ScriptedTurn, req: &ModelRequest) -> Vec<ModelEvent> {
    match turn {
        ScriptedTurn::Text(text) => vec![
            ModelEvent::Start {
                served_model: Some("mock/scripted".into()),
            },
            ModelEvent::TextDelta(text.clone()),
            ModelEvent::Usage(Usage {
                input_tokens: 16,
                output_tokens: 16,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            }),
            ModelEvent::Done(StopReason::Stop),
        ],
        ScriptedTurn::Tool { name, arguments, id } => {
            let args = resolve_placeholders(arguments, req);
            vec![
                ModelEvent::Start {
                    served_model: Some("mock/scripted".into()),
                },
                ModelEvent::ToolCallStart {
                    index: 0,
                    id: id.clone(),
                    name: name.clone(),
                },
                ModelEvent::ToolCallArgsDelta {
                    index: 0,
                    json: args.to_string(),
                },
                ModelEvent::ToolCallEnd { index: 0 },
                ModelEvent::Done(StopReason::ToolUse),
            ]
        }
        ScriptedTurn::ThenText(preset) => fallback_or_preset(req, preset),
        ScriptedTurn::Fail(err) => {
            let _ = err;
            vec![
                ModelEvent::Start {
                    served_model: Some("mock/scripted".into()),
                },
                ModelEvent::Done(StopReason::Stop),
            ]
        }
    }
}

fn fallback_or_preset(req: &ModelRequest, preset: &str) -> Vec<ModelEvent> {
    if !preset.is_empty() {
        return vec![
            ModelEvent::Start {
                served_model: Some("mock/scripted".into()),
            },
            ModelEvent::TextDelta(preset.to_string()),
            ModelEvent::Done(StopReason::Stop),
        ];
    }
    fallback_events(req)
}

fn last_user_text(req: &ModelRequest) -> String {
    for msg in req.messages.iter().rev() {
        if msg.role != MessageRole::User {
            continue;
        }
        for block in msg.content.iter().rev() {
            if let ContentBlock::Text { text } = block {
                return text.clone();
            }
            if let ContentBlock::ToolResult { content, .. } = block {
                return match content {
                    Some(v) => v.to_string(),
                    None => String::new(),
                };
            }
        }
    }
    String::new()
}

fn fallback_events(req: &ModelRequest) -> Vec<ModelEvent> {
    let last = last_user_text(req);
    let reply = if last.starts_with('{') {
        compose_after_tool(&last)
    } else if last.is_empty() {
        "I am ready. Send a ticket or a question and I will triage it.".to_string()
    } else {
        format!(
            "Thanks. I have the ticket. Next step: confirm the customer impact and proposed reply.\n\nYou wrote:\n{}",
            truncate(&last, 400)
        )
    };
    vec![
        ModelEvent::Start {
            served_model: Some("mock/scripted".into()),
        },
        ModelEvent::TextDelta(reply),
        ModelEvent::Usage(Usage {
            input_tokens: 24,
            output_tokens: 48,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        }),
        ModelEvent::Done(StopReason::Stop),
    ]
}

fn compose_after_tool(blob: &str) -> String {
    format!(
        "I looked the ticket up. {blob}\n\nSuggested reply: thank the customer, restate the issue in one sentence, and give a concrete next step with an owner and a time window."
    )
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

fn extract_ticket_id(text: &str) -> &str {
    for word in text.split(|c: char| !c.is_ascii_alphanumeric() && c != '-') {
        if word.is_empty() {
            continue;
        }
        if word.chars().all(|c| c.is_ascii_digit())
            || word.starts_with("ticket-")
            || (word.contains('-') && word.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'))
        {
            return word;
        }
    }
    "unknown"
}

fn resolve_placeholders(value: &Value, req: &ModelRequest) -> Value {
    match value {
        Value::String(s) if s == "{{id}}" => {
            let text = last_user_text(req);
            let id = extract_ticket_id(&text);
            Value::String(id.to_string())
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k.clone(), resolve_placeholders(v, req));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

#[async_trait::async_trait]
impl ModelClient for MockModel {
    async fn stream(
        &self,
        req: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        if cancel.is_cancelled() {
            return Err(ModelError::new(ModelErrorKind::Cancelled, "cancelled"));
        }
        if let ScriptedTurn::Fail(err) = {
            let i = self.cursor.load(Ordering::SeqCst);
            if let Ok(guard) = self.turns.lock() {
                if let Some(ScriptedTurn::Fail(err)) = guard.get(i) {
                    self.cursor.store(i.saturating_add(1), Ordering::SeqCst);
                    return Err(err.clone());
                }
            }
            ScriptedTurn::Text(String::new())
        } {
            let _ = err;
        }
        let events = self.next_turn(&req)?;
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }

    async fn catalog(&self) -> Result<Catalog, ModelError> {
        Ok(Catalog::mock())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::request::Message;
    use futures::StreamExt;

    #[tokio::test]
    async fn scripted_tool_then_text() {
        let mock = MockModel::scripted(vec![
            ScriptedTurn::Tool {
                name: "lookup_ticket".into(),
                arguments: serde_json::json!({"id": "7"}),
                id: "call_1".into(),
            },
            ScriptedTurn::Text("done".into()),
        ]);
        let req = ModelRequest {
            model: "mock/scripted".into(),
            max_tokens: 64,
            stream: true,
            system: "sys".into(),
            messages: vec![Message::user_text("ticket 7")],
            tools: vec![],
            thinking: None,
            affinity_user_id: None,
        };
        let mut s = mock
            .stream(req.clone(), CancellationToken::new())
            .await
            .unwrap();
        let mut saw_tool = false;
        while let Some(ev) = s.next().await {
            if matches!(ev.unwrap(), ModelEvent::ToolCallStart { .. }) {
                saw_tool = true;
            }
        }
        assert!(saw_tool);
        let mut s2 = mock.stream(req, CancellationToken::new()).await.unwrap();
        let mut text = String::new();
        while let Some(ev) = s2.next().await {
            if let ModelEvent::TextDelta(t) = ev.unwrap() {
                text.push_str(&t);
            }
        }
        assert_eq!(text, "done");
    }
}
