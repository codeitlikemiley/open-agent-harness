use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    pub models: Vec<ModelInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub context_window: u32,
    pub max_output_tokens: u32,
    pub capabilities: ModelCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_price_per_mtok: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_price_per_mtok: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelCapabilities {
    pub vision: bool,
    pub tools: bool,
    pub reasoning: bool,
    pub prompt_cache: bool,
}

impl Catalog {
    pub fn lookup(&self, id: &str) -> Option<&ModelInfo> {
        self.models.iter().find(|m| m.id == id)
    }

    pub fn mock() -> Self {
        Self {
            models: vec![ModelInfo {
                id: "mock/scripted".into(),
                context_window: 128_000,
                max_output_tokens: 4_096,
                capabilities: ModelCapabilities {
                    vision: false,
                    tools: true,
                    reasoning: false,
                    prompt_cache: false,
                },
                input_price_per_mtok: None,
                output_price_per_mtok: None,
            }],
        }
    }
}

impl Default for ModelInfo {
    fn default() -> Self {
        Self {
            id: "mock/scripted".into(),
            context_window: 128_000,
            max_output_tokens: 4_096,
            capabilities: ModelCapabilities {
                vision: false,
                tools: true,
                reasoning: false,
                prompt_cache: false,
            },
            input_price_per_mtok: None,
            output_price_per_mtok: None,
        }
    }
}
