use crate::catalog::Catalog;
use crate::error::ModelError;
use crate::request::ModelRequest;
use crate::stream::{ModelStream, Usage};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// One production implementation: `oah-oag` against `/v1/messages`.
#[async_trait]
pub trait ModelClient: Send + Sync {
    async fn stream(
        &self,
        req: ModelRequest,
        cancel: CancellationToken,
    ) -> Result<ModelStream, ModelError>;

    async fn catalog(&self) -> Result<Catalog, ModelError> {
        Ok(Catalog::mock())
    }

    async fn count_tokens(&self, req: &ModelRequest) -> Result<Usage, ModelError> {
        let mut chars = req.system.len();
        for msg in &req.messages {
            for block in &msg.content {
                if let crate::request::ContentBlock::Text { text } = block {
                    chars = chars.saturating_add(text.len());
                }
            }
        }
        Ok(Usage {
            input_tokens: u32::try_from(chars / 4).unwrap_or(u32::MAX),
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        })
    }
}
