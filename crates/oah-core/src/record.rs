use crate::id::{
    AttemptId, ConversationId, OperationId, RecordId, SubmissionId, ToolCallId, TurnId,
};
use crate::time::UnixMillis;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const RECORD_FORMAT_VERSION: u32 = 1;
pub const STORE_FORMAT_VERSION: u32 = 1;

pub fn harness_info() -> HarnessInfo {
    HarnessInfo {
        name: "oah".into(),
        version: crate::VERSION.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessInfo {
    pub name: String,
    pub version: String,
}

/// An atomic batch of records written under one producer fence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordBatch {
    pub path: String,
    pub seq: i64,
    pub records: Vec<Record>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<SubmissionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<AttemptId>,
}

/// Append-only conversation record (`v:1` envelope + typed body).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub v: u32,
    pub id: RecordId,
    pub conversation_id: ConversationId,
    pub harness: HarnessInfo,
    pub session: String,
    pub timestamp: UnixMillis,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<SubmissionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<OperationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<TurnId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<AttemptId>,
    #[serde(flatten)]
    pub body: RecordBody,
}

impl Record {
    pub fn new(
        conversation_id: ConversationId,
        session: impl Into<String>,
        timestamp: UnixMillis,
        body: RecordBody,
    ) -> Self {
        Self {
            v: RECORD_FORMAT_VERSION,
            id: RecordId::new(),
            conversation_id,
            harness: harness_info(),
            session: session.into(),
            timestamp,
            submission_id: None,
            operation_id: None,
            turn_id: None,
            attempt_id: None,
            body,
        }
    }

    pub fn with_submission(mut self, id: SubmissionId) -> Self {
        self.submission_id = Some(id);
        self
    }

    pub fn with_attempt(mut self, id: AttemptId) -> Self {
        self.attempt_id = Some(id);
        self
    }

    pub fn with_turn(mut self, id: TurnId) -> Self {
        self.turn_id = Some(id);
        self
    }

    pub fn kind(&self) -> &'static str {
        self.body.kind()
    }
}

/// The 24 Flue record types plus three harness additions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RecordBody {
    ConversationCreated {
        uid: String,
    },
    UserMessage {
        body: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        joined: Option<bool>,
    },
    Signal {
        #[serde(rename = "signalType")]
        signal_type: String,
        body: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attributes: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tag_name: Option<String>,
    },
    AssistantMessageStarted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    AssistantTextStarted,
    AssistantTextDelta {
        text: String,
    },
    AssistantTextCompleted,
    AssistantReasoningStarted,
    AssistantReasoningDelta {
        text: String,
    },
    AssistantReasoningCompleted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    AssistantToolCall {
        tool_call_id: ToolCallId,
        name: String,
        arguments: Value,
        #[serde(default)]
        index: u32,
    },
    AssistantMessageCompleted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_reason: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<Value>,
    },
    ToolOutcome {
        tool_call_id: ToolCallId,
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<Value>,
        #[serde(default)]
        is_error: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        terminate: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_conversation_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interrupted: Option<bool>,
    },
    ToolResultsCommitted {
        tool_call_ids: Vec<ToolCallId>,
    },
    ToolStepSettled {
        tool_call_id: ToolCallId,
        step: String,
        result: Value,
    },
    Compaction {
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kept_from_offset: Option<String>,
    },
    ChildSessionRetained {
        child_session: String,
    },
    SubmissionSettled {
        outcome: SettlementOutcome,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<SettlementError>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        answered_by_submission_id: Option<SubmissionId>,
    },
    StateWrite {
        name: String,
        value: Value,
    },
    AgentStartRun,
    AgentFinishCycle {
        cycle: u32,
    },
    MessageDataWrite {
        name: String,
        value: Value,
    },
    MessageMetadata {
        metadata: Value,
    },
    ResourceSnapshot {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        instructions: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        environment: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resources: Option<Value>,
    },
    /// Harness addition: a gate asked for approval; the batch stays open.
    ToolSuspended {
        tool_call_id: ToolCallId,
        name: String,
        effective_args: Value,
        reason: String,
        kind: String,
    },
    /// Harness addition: a suspension was answered exactly once.
    ToolApprovalAnswered {
        tool_call_id: ToolCallId,
        approved: bool,
        answered_by: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Harness addition: audit of a gate decision. Overwrites are paths only.
    ToolGateDecision {
        tool_call_id: ToolCallId,
        gate: String,
        verdict: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        overwritten_paths: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl RecordBody {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ConversationCreated { .. } => "conversation_created",
            Self::UserMessage { .. } => "user_message",
            Self::Signal { .. } => "signal",
            Self::AssistantMessageStarted { .. } => "assistant_message_started",
            Self::AssistantTextStarted => "assistant_text_started",
            Self::AssistantTextDelta { .. } => "assistant_text_delta",
            Self::AssistantTextCompleted => "assistant_text_completed",
            Self::AssistantReasoningStarted => "assistant_reasoning_started",
            Self::AssistantReasoningDelta { .. } => "assistant_reasoning_delta",
            Self::AssistantReasoningCompleted { .. } => "assistant_reasoning_completed",
            Self::AssistantToolCall { .. } => "assistant_tool_call",
            Self::AssistantMessageCompleted { .. } => "assistant_message_completed",
            Self::ToolOutcome { .. } => "tool_outcome",
            Self::ToolResultsCommitted { .. } => "tool_results_committed",
            Self::ToolStepSettled { .. } => "tool_step_settled",
            Self::Compaction { .. } => "compaction",
            Self::ChildSessionRetained { .. } => "child_session_retained",
            Self::SubmissionSettled { .. } => "submission_settled",
            Self::StateWrite { .. } => "state_write",
            Self::AgentStartRun => "agent_start_run",
            Self::AgentFinishCycle { .. } => "agent_finish_cycle",
            Self::MessageDataWrite { .. } => "message_data_write",
            Self::MessageMetadata { .. } => "message_metadata",
            Self::ResourceSnapshot { .. } => "resource_snapshot",
            Self::ToolSuspended { .. } => "tool_suspended",
            Self::ToolApprovalAnswered { .. } => "tool_approval_answered",
            Self::ToolGateDecision { .. } => "tool_gate_decision",
        }
    }

    pub fn is_assistant_stream(&self) -> bool {
        matches!(
            self,
            Self::AssistantMessageStarted { .. }
                | Self::AssistantTextStarted
                | Self::AssistantTextDelta { .. }
                | Self::AssistantTextCompleted
                | Self::AssistantReasoningStarted
                | Self::AssistantReasoningDelta { .. }
                | Self::AssistantReasoningCompleted { .. }
                | Self::AssistantToolCall { .. }
                | Self::AssistantMessageCompleted { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementOutcome {
    Completed,
    Failed,
    Aborted,
    Suspended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementError {
    SubmissionAborted,
    SubmissionInterrupted,
    SubmissionRetryExhausted,
    SubmissionTimeout,
    OperationFailed,
    #[serde(untagged)]
    Other(String),
}

impl SettlementError {
    pub fn as_type(&self) -> &str {
        match self {
            Self::SubmissionAborted => "submission_aborted",
            Self::SubmissionInterrupted => "submission_interrupted",
            Self::SubmissionRetryExhausted => "submission_retry_exhausted",
            Self::SubmissionTimeout => "submission_timeout",
            Self::OperationFailed => "operation_failed",
            Self::Other(s) => s,
        }
    }
}
