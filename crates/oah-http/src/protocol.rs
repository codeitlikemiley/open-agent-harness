use oah_core::{ProjectedMessage, ProjectedSettlement};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmitBody {
    /// Bare Flue message (`{ type, content | text | body }`) plus siblings.
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub content: Option<serde_json::Value>,
    #[serde(default)]
    pub message: Option<serde_json::Value>,
    #[serde(default)]
    pub initial_data: Option<serde_json::Value>,
    #[serde(default)]
    pub uid: Option<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

impl AdmitBody {
    pub fn message_text(&self) -> String {
        if let Some(t) = &self.text {
            return t.clone();
        }
        if let Some(t) = &self.body {
            return t.clone();
        }
        if let Some(serde_json::Value::String(s)) = &self.message {
            return s.clone();
        }
        if let Some(obj) = self.message.as_ref().and_then(|m| m.get("text")) {
            if let Some(s) = obj.as_str() {
                return s.to_string();
            }
        }
        if let Some(serde_json::Value::String(s)) = &self.content {
            return s.clone();
        }
        if let Some(arr) = self.content.as_ref().and_then(|c| c.as_array()) {
            for part in arr {
                if let Some(s) = part.get("text").and_then(|t| t.as_str()) {
                    return s.to_string();
                }
            }
        }
        String::new()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmitResponse {
    pub stream_url: String,
    pub offset: String,
    pub submission_id: String,
    pub uid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deduplicated: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdatesQuery {
    pub view: Option<String>,
    pub offset: Option<String>,
    pub live: Option<String>,
    #[serde(default)]
    pub wait: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistorySnapshot {
    pub v: u32,
    pub conversation_id: String,
    pub offset: String,
    pub messages: Vec<ProjectedMessage>,
    pub settlements: Vec<ProjectedSettlement>,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AbortBody {
    pub aborted: bool,
    pub count: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApprovalAnswer {
    pub approved: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    #[serde(rename = "type")]
    pub ty: String,
    pub message: String,
    #[serde(rename = "ref")]
    pub ref_: String,
}

impl ErrorEnvelope {
    pub fn new(ty: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                ty: ty.into(),
                message: message.into(),
                ref_: format!("err_{}", ulid::Ulid::new()),
            },
        }
    }
}
