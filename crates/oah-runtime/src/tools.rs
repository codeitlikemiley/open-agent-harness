use oah_core::{ConversationId, Principal, SessionKey};
use oah_loop::{PreparedCall, ToolExecutor, ToolResult};
use oah_model::{empty_object_schema, strip_schema_meta, ToolSpec};
use oah_store::{AdmitReceipt, AdmitRequest};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub type SpawnFut = Pin<Box<dyn Future<Output = Result<AdmitReceipt, String>> + Send>>;
pub type SpawnFn = Arc<dyn Fn(AdmitRequest) -> SpawnFut + Send + Sync>;

const OUTPUT_CAP: usize = 256 * 1024;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub output: Value,
    pub terminate: bool,
}

impl ToolOutput {
    pub fn json(value: impl Into<Value>) -> Self {
        Self {
            output: value.into(),
            terminate: false,
        }
    }

    pub fn text(s: impl Into<String>) -> Self {
        Self {
            output: Value::String(s.into()),
            terminate: false,
        }
    }
}

pub struct ToolCtx {
    pub tool_call_id: String,
    pub cancel: CancellationToken,
    pub principal: Principal,
    pub spawn: Option<SpawnFn>,
    pub session: Option<SessionKey>,
    pub conversation: Option<ConversationId>,
}

pub type ToolFn = Arc<
    dyn Fn(ToolCtx, Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ToolOutput, ToolError>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub run: ToolFn,
    /// Idempotent tools may re-run after a crash with no outcome.
    /// Non-durable tools must not re-execute; resume writes `interrupted`.
    pub durable: bool,
}

impl ToolDef {
    pub fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: strip_schema_meta(self.input_schema.clone()),
        }
    }
}

pub fn validate_tool_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 96 {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':' )
}

/// Built-in ticket lookup used by the support-desk sample.
pub fn lookup_ticket_tool() -> ToolDef {
    ToolDef {
        name: "lookup_ticket".into(),
        description: "Look up a support ticket by id and return status, customer, and last note.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "id": { "type": "string", "description": "Ticket id" }
            },
            "required": ["id"],
            "additionalProperties": false
        }),
        durable: true,
        run: Arc::new(|_cx, input| {
            Box::pin(async move {
                #[derive(Deserialize)]
                struct In {
                    id: String,
                }
                let parsed: In = serde_json::from_value(input).map_err(|e| {
                    ToolError::Message(format!("invalid arguments: {e}"))
                })?;
                let (customer, status, note) = match parsed.id.as_str() {
                    "42" | "ticket-42" => (
                        "Amina Cole",
                        "open",
                        "Customer cannot export a CSV of last month's invoices.",
                    ),
                    "7" | "ticket-7" => (
                        "Noah Patel",
                        "waiting_on_customer",
                        "Asked for the failing request id; no reply in 2 days.",
                    ),
                    _ => (
                        "Unknown",
                        "open",
                        "No row in the sample store; treat as a new ticket.",
                    ),
                };
                let _ = customer;
                Ok(ToolOutput::json(json!({
                    "id": parsed.id,
                    "customer": customer,
                    "status": status,
                    "last_note": note,
                    "priority": if parsed.id == "42" { "high" } else { "normal" }
                })))
            })
        }),
    }
}

pub struct RegistryExecutor {
    pub tools: HashMap<String, ToolDef>,
    pub principal: Principal,
    pub spawn: Option<SpawnFn>,
    pub session: Option<SessionKey>,
    pub conversation: Option<ConversationId>,
}

impl ToolExecutor for RegistryExecutor {
    fn prepare(&self, call: PreparedCall) -> Result<PreparedCall, ToolResult> {
        if !self.tools.contains_key(&call.name) {
            return Err(oah_loop::tool_not_found(&call.name, &call.id));
        }
        if call.arguments.is_null() {
            return Err(ToolResult {
                id: call.id,
                name: call.name,
                output: Value::String(
                    "invalid arguments: expected a JSON object (failing paths: $)".into(),
                ),
                is_error: true,
                terminate: false,
            });
        }
        Ok(call)
    }

    async fn execute(&self, call: PreparedCall, cancel: CancellationToken) -> ToolResult {
        let Some(def) = self.tools.get(&call.name) else {
            return oah_loop::tool_not_found(&call.name, &call.id);
        };
        let ctx = ToolCtx {
            tool_call_id: call.id.clone(),
            cancel,
            principal: self.principal.clone(),
            spawn: self.spawn.clone(),
            session: self.session.clone(),
            conversation: self.conversation.clone(),
        };
        match (def.run)(ctx, call.arguments).await {
            Ok(out) => {
                let output = cap_output(out.output);
                ToolResult {
                    id: call.id,
                    name: call.name,
                    output,
                    is_error: false,
                    terminate: out.terminate,
                }
            }
            Err(err) => ToolResult {
                id: call.id,
                name: call.name,
                output: Value::String(err.to_string()),
                is_error: true,
                terminate: false,
            },
        }
    }
}

fn cap_output(value: Value) -> Value {
    let raw = value.to_string();
    if raw.len() <= OUTPUT_CAP {
        return value;
    }
    Value::String(format!(
        "{}...\n[truncated at {OUTPUT_CAP} bytes]",
        &raw[..OUTPUT_CAP.min(raw.len())]
    ))
}

pub fn empty_schema() -> Value {
    empty_object_schema()
}

pub fn refund_tool() -> ToolDef {
    ToolDef {
        name: "refund".into(),
        description: "Issue a refund. Always gated for human approval.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "ticket_id": { "type": "string" },
                "amount": { "type": "number" }
            },
            "required": ["ticket_id", "amount"]
        }),
        durable: true,
        run: Arc::new(|_cx, input| {
            Box::pin(async move { Ok(ToolOutput::json(json!({"refunded": input}))) })
        }),
    }
}

pub fn activate_skill_tool(skills: Vec<oah_skills::Skill>) -> ToolDef {
    ToolDef {
        name: "activate_skill".into(),
        description: "Load the full body of a registered skill by name.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "name": { "type": "string" } },
            "required": ["name"]
        }),
        durable: true,
        run: Arc::new(move |cx, input| {
            let skills = skills.clone();
            Box::pin(async move {
                let name = input
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let Some(skill) = skills.iter().find(|s| s.name == name) else {
                    return Err(ToolError::Message(format!("unknown skill {name}")));
                };
                Ok(ToolOutput::text(skill.activate(cx.tool_call_id.as_str())))
            })
        }),
    }
}

pub fn task_tool() -> ToolDef {
    ToolDef {
        name: "task".into(),
        description: "Spawn a child session (subagent) with a prompt.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "prompt": { "type": "string" },
                "label": { "type": "string" }
            },
            "required": ["prompt"]
        }),
        durable: true,
        run: Arc::new(|cx, input| {
            Box::pin(async move {
                let prompt = input
                    .get("prompt")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let parent = cx.session.ok_or_else(|| {
                    ToolError::Message("task requires a parent session".into())
                })?;
                let conversation = cx.conversation.ok_or_else(|| {
                    ToolError::Message("task requires a conversation".into())
                })?;
                let depth = parent.as_str().matches("task:").count();
                if depth >= oah_core::MAX_DELEGATION_DEPTH as usize {
                    return Err(ToolError::Message(format!(
                        "delegation depth {depth} exceeds MAX_DELEGATION_DEPTH"
                    )));
                }
                let spawn = cx.spawn.ok_or_else(|| {
                    ToolError::Message("task spawn is not configured".into())
                })?;
                let child = SessionKey::task_child(&parent);
                let req = AdmitRequest {
                    conversation_id: conversation,
                    session_key: child.clone(),
                    kind: oah_store::DeliveryKind::User,
                    payload: json!({ "body": prompt }),
                    principal: cx.principal,
                    idempotency_key: None,
                    uid: None,
                    max_attempts: 10,
                };
                let receipt = spawn(req)
                    .await
                    .map_err(ToolError::Message)?;
                Ok(ToolOutput::json(json!({
                    "childSession": child.to_string(),
                    "submissionId": receipt.submission_id.to_string(),
                    "status": "queued"
                })))
            })
        }),
    }
}

pub fn mcp_bridge_tool(mcp: Arc<oah_mcp::InProcessMcp>, name: String) -> ToolDef {
    ToolDef {
        name: name.clone(),
        description: format!("MCP tool {name}"),
        input_schema: json!({"type": "object"}),
        durable: true,
        run: Arc::new(move |_cx, input| {
            let mcp = mcp.clone();
            let name = name.clone();
            Box::pin(async move {
                let v = mcp
                    .call(&name, input)
                    .map_err(|e| ToolError::Message(e.to_string()))?;
                Ok(ToolOutput::json(v))
            })
        }),
    }
}

pub fn step_run_tool() -> ToolDef {
    ToolDef {
        name: "step".into(),
        description: "Record a durable step.run result (idempotent by tool_call_id + step).".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "step": { "type": "string" },
                "result": {}
            },
            "required": ["step"]
        }),
        durable: true,
        run: Arc::new(|cx, input| {
            Box::pin(async move {
                Ok(ToolOutput::json(json!({
                    "step": input.get("step"),
                    "result": input.get("result"),
                    "toolCallId": cx.tool_call_id,
                })))
            })
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use oah_core::Principal;

    #[tokio::test]
    async fn task_requires_parent_session() {
        let tool = task_tool();
        let cx = ToolCtx {
            tool_call_id: "call_t".into(),
            cancel: CancellationToken::new(),
            principal: Principal::anonymous(),
            spawn: None,
            session: None,
            conversation: None,
        };
        let err = (tool.run)(cx, json!({"prompt": "x"})).await.unwrap_err();
        assert!(err.to_string().contains("parent session"));
    }
}
