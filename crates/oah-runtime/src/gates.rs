use async_trait::async_trait;
use oah_core::Principal;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct GatedCall {
    pub name: String,
    pub tool_call_id: String,
    pub arguments: Value,
}

#[derive(Debug, Clone)]
pub struct GateCx<'a> {
    pub principal: &'a Principal,
}

#[derive(Debug, Clone)]
pub enum Verdict {
    Allow,
    Deny { reason: String },
    Ask { reason: String, kind: AskKind },
}

#[derive(Debug, Clone)]
pub enum AskKind {
    Approval,
    AutoReview,
}

#[async_trait]
pub trait ToolGate: Send + Sync {
    fn name(&self) -> &str;
    async fn check(&self, call: &mut GatedCall, cx: &GateCx<'_>) -> Verdict;
}

/// Denies (or asks) for tools whose names match a deny list.
pub struct NamedDenyGate {
    pub name: String,
    pub deny: Vec<String>,
    pub ask: Vec<String>,
}

#[async_trait]
impl ToolGate for NamedDenyGate {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self, call: &mut GatedCall, _cx: &GateCx<'_>) -> Verdict {
        if self.deny.iter().any(|n| n == &call.name) {
            return Verdict::Deny {
                reason: format!("tool {} is denied by policy", call.name),
            };
        }
        if self.ask.iter().any(|n| n == &call.name) {
            return Verdict::Ask {
                reason: format!("tool {} needs approval", call.name),
                kind: AskKind::Approval,
            };
        }
        Verdict::Allow
    }
}
