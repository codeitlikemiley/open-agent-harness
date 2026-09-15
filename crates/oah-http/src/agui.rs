//! AG-UI event mapping (`RUN_*`, `TOOL_CALL_*`, `run-awaiting-approval`).

use oah_core::{Record, RecordBody};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AguiRunRequest {
    pub thread_id: Option<String>,
    pub run_id: Option<String>,
    pub agent: Option<String>,
    pub messages: Option<Vec<AguiMessage>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AguiMessage {
    pub role: Option<String>,
    pub content: Option<Value>,
}

impl AguiRunRequest {
    pub fn text(&self) -> String {
        let Some(messages) = &self.messages else {
            return String::new();
        };
        for msg in messages.iter().rev() {
            if msg.role.as_deref() == Some("assistant") {
                continue;
            }
            match &msg.content {
                Some(Value::String(s)) => return s.clone(),
                Some(Value::Array(arr)) => {
                    for part in arr {
                        if let Some(s) = part.get("text").and_then(Value::as_str) {
                            return s.to_string();
                        }
                    }
                }
                _ => {}
            }
        }
        String::new()
    }

    pub fn agent_instance(&self) -> Option<(String, String)> {
        if let Some(thread) = &self.thread_id {
            if let Some((a, i)) = thread.split_once('/') {
                return Some((a.to_string(), i.to_string()));
            }
        }
        None
    }
}

pub fn record_to_agui(record: &Record) -> Option<Value> {
    match &record.body {
        RecordBody::AssistantMessageStarted { .. } => Some(json!({
            "type": "TEXT_MESSAGE_START",
            "messageId": record.id.to_string(),
            "role": "assistant"
        })),
        RecordBody::AssistantTextDelta { text } => Some(json!({
            "type": "TEXT_MESSAGE_CONTENT",
            "delta": text
        })),
        RecordBody::AssistantTextCompleted => Some(json!({
            "type": "TEXT_MESSAGE_END"
        })),
        RecordBody::AssistantToolCall {
            tool_call_id,
            name,
            arguments,
            ..
        } => Some(json!({
            "type": "TOOL_CALL_START",
            "toolCallId": tool_call_id.to_string(),
            "toolCallName": name,
            "args": arguments
        })),
        RecordBody::ToolOutcome {
            tool_call_id,
            output,
            ..
        } => Some(json!({
            "type": "TOOL_CALL_END",
            "toolCallId": tool_call_id.to_string(),
            "result": output
        })),
        RecordBody::ToolSuspended {
            tool_call_id,
            name,
            reason,
            ..
        } => Some(json!({
            "type": "run-awaiting-approval",
            "toolCallId": tool_call_id.to_string(),
            "tool": name,
            "reason": reason
        })),
        RecordBody::SubmissionSettled { outcome, error, .. } => {
            if error.is_some() {
                Some(json!({
                    "type": "RUN_ERROR",
                    "outcome": format!("{outcome:?}")
                }))
            } else {
                Some(json!({
                    "type": "RUN_FINISHED",
                    "outcome": format!("{outcome:?}")
                }))
            }
        }
        _ => None,
    }
}

pub fn run_started(run_id: &str, thread_id: &str) -> Value {
    json!({
        "type": "RUN_STARTED",
        "runId": run_id,
        "threadId": thread_id
    })
}
