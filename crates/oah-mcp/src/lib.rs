//! MCP tool naming (`mcp__<server>__<tool>`), allowlists, and result flatten.

#![forbid(unsafe_code)]

pub mod rpc;

use serde_json::{json, Value};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpError {
    #[error("tool {0} is not on the MCP allowlist")]
    Denied(String),
    #[error("{0}")]
    Transport(String),
}

pub fn mcp_tool_name(server: &str, tool: &str) -> String {
    format!("mcp__{server}__{tool}")
}

pub fn parse_mcp_tool_name(raw: &str) -> Option<(String, String)> {
    let rest = raw.strip_prefix("mcp__")?;
    let (server, tool) = rest.split_once("__")?;
    if server.is_empty() || tool.is_empty() {
        return None;
    }
    Some((server.to_string(), tool.to_string()))
}

/// Flatten MCP `content` arrays / `structuredContent` into a single JSON value
/// the model sees (PRD: one tool result, no envelope leakage).
pub fn flatten_mcp_result(value: &Value) -> Value {
    if let Some(structured) = value.get("structuredContent") {
        return structured.clone();
    }
    if let Some(arr) = value.get("content").and_then(Value::as_array) {
        let mut texts = Vec::new();
        for part in arr {
            if let Some(t) = part.get("text").and_then(Value::as_str) {
                texts.push(t.to_string());
            } else if part.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(t) = part.get("text").and_then(Value::as_str) {
                    texts.push(t.to_string());
                }
            }
        }
        if texts.len() == 1 {
            return Value::String(texts.remove(0));
        }
        if !texts.is_empty() {
            return Value::String(texts.join("\n"));
        }
    }
    if let Some(is_error) = value.get("isError").and_then(Value::as_bool) {
        if is_error {
            return json!({
                "error": value.get("message").cloned().unwrap_or(Value::String("mcp error".into()))
            });
        }
    }
    value.clone()
}

#[derive(Debug, Clone, Default)]
pub struct McpAllowlist {
    /// Empty means allow all names that parse as `mcp__…`.
    allowed: BTreeSet<String>,
}

impl McpAllowlist {
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            allowed: names.into_iter().map(Into::into).collect(),
        }
    }

    pub fn allows(&self, tool_name: &str) -> bool {
        if tool_name.starts_with("oah_") {
            return true;
        }
        if parse_mcp_tool_name(tool_name).is_none() {
            return false;
        }
        self.allowed.is_empty() || self.allowed.contains(tool_name)
    }

    pub fn check(&self, tool_name: &str) -> Result<(), McpError> {
        if self.allows(tool_name) {
            Ok(())
        } else {
            Err(McpError::Denied(tool_name.to_string()))
        }
    }
}

/// In-process MCP server door: register handlers by `mcp__server__tool`.
pub struct InProcessMcp {
    allow: McpAllowlist,
    handlers: std::collections::HashMap<
        String,
        Box<dyn Fn(Value) -> Result<Value, McpError> + Send + Sync>,
    >,
}

impl InProcessMcp {
    pub fn new(allow: McpAllowlist) -> Self {
        Self {
            allow,
            handlers: std::collections::HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        server: &str,
        tool: &str,
        handler: impl Fn(Value) -> Result<Value, McpError> + Send + Sync + 'static,
    ) {
        self.handlers
            .insert(mcp_tool_name(server, tool), Box::new(handler));
    }

    pub fn tool_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.handlers.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn call(&self, name: &str, args: Value) -> Result<Value, McpError> {
        self.allow.check(name)?;
        let handler = self
            .handlers
            .get(name)
            .ok_or_else(|| McpError::Transport(format!("no handler for {name}")))?;
        let raw = handler(args)?;
        Ok(flatten_mcp_result(&raw))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn naming_roundtrip() {
        let n = mcp_tool_name("slack", "post_message");
        assert_eq!(n, "mcp__slack__post_message");
        assert_eq!(
            parse_mcp_tool_name(&n),
            Some(("slack".into(), "post_message".into()))
        );
    }

    #[test]
    fn flatten_text_and_structured() {
        let v = json!({"content":[{"type":"text","text":"hi"}]});
        assert_eq!(flatten_mcp_result(&v), Value::String("hi".into()));
        let s = json!({"structuredContent":{"ok":true}});
        assert_eq!(flatten_mcp_result(&s), json!({"ok":true}));
    }

    #[test]
    fn allowlist_and_call() {
        let name = mcp_tool_name("echo", "ping");
        let allow = McpAllowlist::new([name.clone()]);
        let mut mcp = InProcessMcp::new(allow);
        mcp.register("echo", "ping", |args| Ok(json!({"content":[{"type":"text","text": args.to_string()}]})));
        let out = mcp.call(&name, json!({"x":1})).unwrap();
        assert!(out.as_str().unwrap().contains("x"));
        assert!(mcp.call("mcp__echo__other", json!({})).is_err());
    }
}
