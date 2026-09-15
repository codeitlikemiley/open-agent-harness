use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub model: String,
    pub max_tokens: u32,
    pub stream: bool,
    pub system: String,
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingLevel>,
    /// Stable affinity id: `aff_` + hash of agent, instance, session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_user_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: MessageRole,
    pub content: Vec<ContentBlock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Thinking {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<Value>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
    },
    Image {
        media_type: String,
        data: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingLevel {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ThinkingLevel {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "off" => Some(Self::Off),
            "minimal" => Some(Self::Minimal),
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "xhigh" => Some(Self::Xhigh),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

impl Default for ThinkingLevel {
    fn default() -> Self {
        Self::Medium
    }
}

impl Message {
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: MessageRole::User,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    pub fn assistant_text(text: impl Into<String>) -> Self {
        Self {
            role: MessageRole::Assistant,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }
}
