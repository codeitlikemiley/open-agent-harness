use crate::error::ModelError;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    Stop,
    Length,
    ToolUse,
    Refusal,
}

impl StopReason {
    pub fn parse(raw: &str) -> Self {
        match raw {
            "max_tokens" | "length" => Self::Length,
            "tool_use" | "tool_calls" => Self::ToolUse,
            "refusal" => Self::Refusal,
            _ => Self::Stop,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Length => "length",
            Self::ToolUse => "tool_use",
            Self::Refusal => "refusal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_read_tokens: u32,
    pub cache_write_tokens: u32,
}

impl Usage {
    pub fn total(self) -> u32 {
        self.input_tokens
            .saturating_add(self.output_tokens)
            .saturating_add(self.cache_read_tokens)
            .saturating_add(self.cache_write_tokens)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModelEvent {
    Start { served_model: Option<String> },
    TextDelta(String),
    ThinkingDelta(String),
    ThinkingSignature(String),
    ToolCallStart { index: u32, id: String, name: String },
    ToolCallArgsDelta { index: u32, json: String },
    ToolCallEnd { index: u32 },
    Usage(Usage),
    Done(StopReason),
}

pub type ModelStream = BoxStream<'static, Result<ModelEvent, ModelError>>;
