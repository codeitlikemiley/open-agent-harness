//! JSON-RPC 2.0 MCP door (initialize / tools/list / tools/call).

use crate::{flatten_mcp_result, InProcessMcp, McpError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const PROTOCOL_VERSION: &str = "2025-03-26";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolSpec {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

pub fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": "oah", "version": env!("CARGO_PKG_VERSION") }
    })
}

pub fn oah_dispatch_spec() -> McpToolSpec {
    McpToolSpec {
        name: "oah_dispatch".into(),
        description: "Admit a user message to an open-agent-harness agent instance.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "agent": { "type": "string" },
                "id": { "type": "string" },
                "message": { "type": "string" }
            },
            "required": ["agent", "id", "message"]
        }),
    }
}

pub fn rpc_error(id: Option<Value>, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() }
    })
}

pub fn rpc_ok(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Handle initialize / ping / tools/list for the in-process registry.
/// `tools/call` for `mcp__*` names is handled here; `oah_dispatch` is left
/// to the HTTP host (it needs the runtime).
pub fn handle_sync(mcp: &InProcessMcp, req: &JsonRpcRequest) -> Option<Value> {
    match req.method.as_str() {
        "initialize" => Some(rpc_ok(req.id.clone(), initialize_result())),
        "notifications/initialized" | "notifications/cancelled" => None,
        "ping" => Some(rpc_ok(req.id.clone(), json!({}))),
        "tools/list" => {
            let mut tools: Vec<McpToolSpec> = mcp
                .tool_names()
                .into_iter()
                .map(|name| McpToolSpec {
                    name,
                    description: "In-process MCP tool".into(),
                    input_schema: json!({"type":"object"}),
                })
                .collect();
            tools.insert(0, oah_dispatch_spec());
            Some(rpc_ok(req.id.clone(), json!({ "tools": tools })))
        }
        "tools/call" => {
            let name = req
                .params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("");
            if name == "oah_dispatch" {
                return None;
            }
            let args = req
                .params
                .get("arguments")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            match mcp.call(name, args) {
                Ok(v) => Some(rpc_ok(
                    req.id.clone(),
                    json!({
                        "content": [{ "type": "text", "text": flatten_mcp_result(&v).to_string() }],
                        "isError": false
                    }),
                )),
                Err(McpError::Denied(n)) => Some(rpc_error(req.id.clone(), -32602, format!("denied {n}"))),
                Err(err) => Some(rpc_error(req.id.clone(), -32000, err.to_string())),
            }
        }
        _ => Some(rpc_error(
            req.id.clone(),
            -32601,
            format!("method not found: {}", req.method),
        )),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::{mcp_tool_name, McpAllowlist};

    #[test]
    fn initialize_and_list() {
        let allow = McpAllowlist::new([mcp_tool_name("echo", "ping")]);
        let mut mcp = InProcessMcp::new(allow);
        mcp.register("echo", "ping", |_| Ok(json!({"ok":true})));
        let init = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(1)),
            method: "initialize".into(),
            params: json!({}),
        };
        let res = handle_sync(&mcp, &init).unwrap();
        assert_eq!(res["result"]["serverInfo"]["name"], "oah");
        let list = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Some(json!(2)),
            method: "tools/list".into(),
            params: json!({}),
        };
        let res = handle_sync(&mcp, &list).unwrap();
        let tools = res["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|t| t["name"] == "oah_dispatch"));
        assert!(tools.iter().any(|t| t["name"] == "mcp__echo__ping"));
    }
}
