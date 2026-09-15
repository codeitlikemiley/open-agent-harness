//! Registry, render, submissions, tool gates, and the native coordinator.

#![forbid(unsafe_code)]

pub mod agent;
pub mod coordinator;
pub mod gates;
pub mod markdown;
pub mod sandbox_tools;
pub mod session;
pub mod tools;

pub use agent::{Agent, AgentBox, Durability, Instructions, PrepareCx, RenderCx, RenderError};
pub use coordinator::Coordinator;
pub use gates::{AskKind, GateCx, GatedCall, ToolGate, Verdict};
pub use markdown::MarkdownAgent;
pub use session::{drain_pending_settlements, process_claim, ProcessOutcome};
pub use tools::{
    activate_skill_tool, lookup_ticket_tool, mcp_bridge_tool, refund_tool, step_run_tool, task_tool,
    ToolCtx, ToolDef, ToolError, ToolFn, ToolOutput,
};

use oah_core::{
    AgentName, ConversationId, InstanceId, OwnerId, Principal, SessionKey, SubmissionId, UnixMillis,
};
use oah_model::ModelClient;
use oah_store::{AdmitReceipt, AdmitRequest, DeliveryKind, Store, StoreError};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("unknown agent {0}")]
    UnknownAgent(String),
    #[error("{0}")]
    Store(#[from] StoreError),
    #[error("{0}")]
    Core(#[from] oah_core::CoreError),
    #[error("{0}")]
    Render(#[from] RenderError),
    #[error("{0}")]
    Other(String),
}

pub type SharedStore = Arc<dyn Store>;
pub type SharedModel = Arc<dyn ModelClient>;

pub struct Runtime {
    pub agents: HashMap<AgentName, AgentBox>,
    pub store: SharedStore,
    pub model: SharedModel,
    pub owner: OwnerId,
    pub default_model: String,
    pub max_output_tokens: u32,
    pub demo: bool,
    pub sandboxes: Arc<oah_sandbox::VirtualProvider>,
    pub mcp: Option<Arc<oah_mcp::InProcessMcp>>,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    pub fn agent(&self, name: &AgentName) -> Result<&AgentBox, RuntimeError> {
        self.agents
            .get(name)
            .ok_or_else(|| RuntimeError::UnknownAgent(name.to_string()))
    }

    pub async fn dispatch(
        &self,
        agent: &AgentName,
        instance: &InstanceId,
        body: impl Into<String>,
        principal: Principal,
        idempotency_key: Option<String>,
        uid: Option<String>,
    ) -> Result<AdmitReceipt, RuntimeError> {
        let _ = self.agent(agent)?;
        let cid = ConversationId::new(agent, instance);
        let now = UnixMillis::now_system();
        let req = AdmitRequest {
            conversation_id: cid.clone(),
            session_key: SessionKey::root(&cid),
            kind: DeliveryKind::User,
            payload: json!({ "body": body.into() }),
            principal,
            idempotency_key,
            uid,
            max_attempts: 10,
        };
        Ok(self.store.admit(req, now).await?)
    }

    pub async fn admit(
        &self,
        req: AdmitRequest,
    ) -> Result<AdmitReceipt, RuntimeError> {
        Ok(self.store.admit(req, UnixMillis::now_system()).await?)
    }

    pub async fn abort(&self, agent: &AgentName, instance: &InstanceId) -> Result<u32, RuntimeError> {
        let cid = ConversationId::new(agent, instance);
        Ok(self
            .store
            .request_abort(&SessionKey::root(&cid), UnixMillis::now_system())
            .await?)
    }

    pub async fn answer(
        &self,
        submission: &SubmissionId,
        tool_call_id: &str,
        approved: bool,
        by: &str,
    ) -> Result<(), RuntimeError> {
        let now = UnixMillis::now_system();
        let row = self.store.get_submission(submission).await?;
        self.store
            .answer_suspension(submission, tool_call_id, approved, by, now)
            .await?;
        let call = oah_core::ToolCallId::from_model_id(tool_call_id.to_string());
        let rec = oah_core::Record::new(
            row.conversation_id.clone(),
            row.session_key.to_string(),
            now,
            oah_core::RecordBody::ToolApprovalAnswered {
                tool_call_id: call,
                approved,
                answered_by: by.to_string(),
                reason: None,
            },
        )
        .with_submission(submission.clone());
        let rec = match &row.attempt_id {
            Some(att) => rec.with_attempt(att.clone()),
            None => rec,
        };
        self.store
            .append(
                row.conversation_id.as_str(),
                vec![rec],
                Some(submission),
                row.attempt_id.as_ref(),
            )
            .await?;
        Ok(())
    }
}

#[derive(Default)]
pub struct RuntimeBuilder {
    agents: HashMap<AgentName, AgentBox>,
    store: Option<SharedStore>,
    model: Option<SharedModel>,
    default_model: String,
    max_output_tokens: u32,
    demo: bool,
    mcp: Option<Arc<oah_mcp::InProcessMcp>>,
}

impl RuntimeBuilder {
    pub fn agent(mut self, agent: AgentBox) -> Self {
        self.agents.insert(agent.name().clone(), agent);
        self
    }

    pub fn store(mut self, store: SharedStore) -> Self {
        self.store = Some(store);
        self
    }

    pub fn model(mut self, model: SharedModel) -> Self {
        self.model = Some(model);
        self
    }

    pub fn default_model(mut self, spec: impl Into<String>) -> Self {
        self.default_model = spec.into();
        self
    }

    pub fn max_output_tokens(mut self, n: u32) -> Self {
        self.max_output_tokens = n;
        self
    }

    pub fn demo(mut self, demo: bool) -> Self {
        self.demo = demo;
        self
    }

    pub fn mcp(mut self, mcp: Arc<oah_mcp::InProcessMcp>) -> Self {
        self.mcp = Some(mcp);
        self
    }

    pub fn build(self) -> Result<Runtime, RuntimeError> {
        let store = self
            .store
            .ok_or_else(|| RuntimeError::Other("store is required".into()))?;
        let model = self
            .model
            .ok_or_else(|| RuntimeError::Other("model is required".into()))?;
        Ok(Runtime {
            agents: self.agents,
            store,
            model,
            owner: OwnerId::new(),
            default_model: if self.default_model.is_empty() {
                "mock/scripted".into()
            } else {
                self.default_model
            },
            max_output_tokens: if self.max_output_tokens == 0 {
                4096
            } else {
                self.max_output_tokens
            },
            demo: self.demo,
            sandboxes: Arc::new(oah_sandbox::VirtualProvider::new()),
            mcp: self.mcp,
        })
    }
}

pub fn list_agent_names(rt: &Runtime) -> Vec<AgentName> {
    let mut names: Vec<_> = rt.agents.keys().cloned().collect();
    names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    names
}

pub fn cancel_pair() -> (CancellationToken, CancellationToken) {
    let parent = CancellationToken::new();
    let child = parent.child_token();
    (parent, child)
}

/// Props visible to every render in an attempt (output of `prepare`).
#[derive(Debug, Clone, Default)]
pub struct Props {
    pub values: HashMap<String, Value>,
}
