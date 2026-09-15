//! Canonical model messages and the `ModelClient` trait.
//!
//! Production inference goes through `oah-oag` → `open-ai-gateway` `/v1/messages`.
//! This crate has no provider SDKs.

#![forbid(unsafe_code)]

pub mod catalog;
pub mod client;
pub mod error;
pub mod mock;
pub mod request;
pub mod schema;
pub mod stream;

pub use catalog::{Catalog, ModelCapabilities, ModelInfo};
pub use client::ModelClient;
pub use error::{ModelError, ModelErrorKind};
pub use mock::{MockModel, ScriptedTurn};
pub use request::{ContentBlock, Message, MessageRole, ModelRequest, ThinkingLevel, ToolSpec};
pub use schema::{empty_object_schema, strip_schema_meta, tool_schema_bytes};
pub use stream::{ModelEvent, ModelStream, StopReason, Usage};
