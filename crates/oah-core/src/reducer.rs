use crate::constants::FOLD_CHECKPOINT_INTERVAL;
use crate::error::{CoreError, Result};
use crate::id::{ConversationId, RecordId, ToolCallId};
use crate::offset::StreamOffset;
use crate::record::{Record, RecordBatch, RecordBody, SettlementError, SettlementOutcome};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Pure fold of the conversation log. Amortized O(1) per appended record:
/// no deep clone of the whole state on each batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FoldState {
    pub conversation_id: Option<ConversationId>,
    pub uid: Option<String>,
    pub incarnation: u64,
    pub created: bool,
    pub messages: Vec<ProjectedMessage>,
    pub settlements: Vec<ProjectedSettlement>,
    pub persistent_state: BTreeMap<String, Value>,
    /// Buffered writes that have not committed with a tool batch / hook checkpoint.
    pub state_overlay: BTreeMap<String, Value>,
    pub streaming: Option<StreamingAssistant>,
    pub open_batch: Option<OpenToolBatch>,
    pub records_applied: u64,
    pub last_record_id: Option<RecordId>,
    pub head: StreamOffset,
    pub compaction_summary: Option<String>,
}

impl Default for FoldState {
    fn default() -> Self {
        Self {
            conversation_id: None,
            uid: None,
            incarnation: 0,
            created: false,
            messages: Vec::new(),
            settlements: Vec::new(),
            persistent_state: BTreeMap::new(),
            state_overlay: BTreeMap::new(),
            streaming: None,
            open_batch: None,
            records_applied: 0,
            last_record_id: None,
            head: StreamOffset::ORIGIN,
            compaction_summary: None,
        }
    }
}

impl FoldState {
    pub fn should_checkpoint(&self) -> bool {
        self.records_applied > 0 && self.records_applied % FOLD_CHECKPOINT_INTERVAL == 0
    }

    pub fn effective_state(&self) -> BTreeMap<String, Value> {
        let mut out = self.persistent_state.clone();
        out.append(&mut self.state_overlay.clone());
        out
    }

    pub fn discard_overlay(&mut self) {
        self.state_overlay.clear();
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedMessage {
    pub id: String,
    pub role: String,
    pub parts: Vec<ProjectedPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ProjectedPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "reasoning")]
    Reasoning {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    #[serde(rename = "dynamic-tool")]
    DynamicTool {
        tool_call_id: String,
        tool_name: String,
        state: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        input: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_text: Option<String>,
    },
    #[serde(rename = "data")]
    Data { name: String, value: Value },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedSettlement {
    pub submission_id: String,
    pub outcome: SettlementOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SettlementError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamingAssistant {
    pub message_id: String,
    pub text: String,
    pub reasoning: String,
    pub reasoning_signature: Option<String>,
    pub tool_calls: Vec<ProjectedToolCall>,
    pub metadata: Option<Value>,
    pub text_open: bool,
    pub reasoning_open: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedToolCall {
    pub tool_call_id: ToolCallId,
    pub name: String,
    pub arguments: Value,
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenToolBatch {
    pub expected: Vec<ToolCallId>,
    pub outcomes: BTreeMap<String, RecordBody>,
    pub suspended: BTreeMap<String, RecordBody>,
}

/// Apply one durable batch. Two barriers:
/// 1. no non-stream append while a message is streaming;
/// 2. no append past an uncommitted tool batch except outcomes / suspend / commit.
pub fn reduce_batch(state: &mut FoldState, batch: &RecordBatch) -> Result<()> {
    for (i, record) in batch.records.iter().enumerate() {
        reduce_record(state, record)?;
        state.records_applied = state.records_applied.saturating_add(1);
        state.last_record_id = Some(record.id.clone());
        state.head = StreamOffset::new(batch.seq, i as i64);
    }
    Ok(())
}

fn reduce_record(state: &mut FoldState, record: &Record) -> Result<()> {
    if let Some(cid) = &state.conversation_id {
        if cid != &record.conversation_id {
            return Err(CoreError::invariant(
                "record conversationId does not match fold state",
            ));
        }
    } else {
        state.conversation_id = Some(record.conversation_id.clone());
    }

    enforce_barriers(state, &record.body)?;

    match &record.body {
        RecordBody::ConversationCreated { uid } => {
            if state.created {
                return Err(CoreError::invariant("conversation_created already applied"));
            }
            state.created = true;
            state.uid = Some(uid.clone());
        }
        RecordBody::UserMessage { body, .. } => {
            state.messages.push(ProjectedMessage {
                id: record.id.to_string(),
                role: "user".into(),
                parts: vec![ProjectedPart::Text { text: body.clone() }],
                metadata: None,
                submission_id: record.submission_id.as_ref().map(ToString::to_string),
            });
        }
        RecordBody::Signal {
            signal_type,
            body,
            attributes,
            tag_name,
        } => {
            let tag = tag_name.as_deref().unwrap_or("signal");
            let mut xml = format!("<{tag} type=\"{}\"", escape_attr(signal_type));
            if let Some(Value::Object(map)) = attributes {
                for (k, v) in map {
                    let val = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    xml.push_str(&format!(" {k}=\"{}\"", escape_attr(&val)));
                }
            }
            xml.push('>');
            xml.push_str(&escape_text(body));
            xml.push_str(&format!("</{tag}>"));
            state.messages.push(ProjectedMessage {
                id: record.id.to_string(),
                role: "user".into(),
                parts: vec![ProjectedPart::Text { text: xml }],
                metadata: None,
                submission_id: record.submission_id.as_ref().map(ToString::to_string),
            });
        }
        RecordBody::AssistantMessageStarted { metadata } => {
            state.streaming = Some(StreamingAssistant {
                message_id: record.id.to_string(),
                text: String::new(),
                reasoning: String::new(),
                reasoning_signature: None,
                tool_calls: Vec::new(),
                metadata: metadata.clone(),
                text_open: false,
                reasoning_open: false,
            });
        }
        RecordBody::AssistantTextStarted => {
            streaming_mut(state)?.text_open = true;
        }
        RecordBody::AssistantTextDelta { text } => {
            streaming_mut(state)?.text.push_str(text);
        }
        RecordBody::AssistantTextCompleted => {
            streaming_mut(state)?.text_open = false;
        }
        RecordBody::AssistantReasoningStarted => {
            streaming_mut(state)?.reasoning_open = true;
        }
        RecordBody::AssistantReasoningDelta { text } => {
            streaming_mut(state)?.reasoning.push_str(text);
        }
        RecordBody::AssistantReasoningCompleted { signature } => {
            let s = streaming_mut(state)?;
            s.reasoning_open = false;
            s.reasoning_signature = signature.clone();
        }
        RecordBody::AssistantToolCall {
            tool_call_id,
            name,
            arguments,
            index,
        } => {
            let s = streaming_mut(state)?;
            if s.tool_calls
                .iter()
                .any(|c| &c.tool_call_id == tool_call_id && c.arguments != *arguments)
            {
                return Err(CoreError::invariant(
                    "same tool call id with different content",
                ));
            }
            if !s.tool_calls.iter().any(|c| &c.tool_call_id == tool_call_id) {
                s.tool_calls.push(ProjectedToolCall {
                    tool_call_id: tool_call_id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                    index: *index,
                });
            }
        }
        RecordBody::AssistantMessageCompleted { .. } => {
            let streaming = state
                .streaming
                .take()
                .ok_or_else(|| CoreError::invariant("assistant_message_completed without start"))?;
            let mut parts = Vec::new();
            if !streaming.reasoning.is_empty() {
                parts.push(ProjectedPart::Reasoning {
                    text: streaming.reasoning,
                    signature: streaming.reasoning_signature,
                });
            }
            if !streaming.text.is_empty() {
                parts.push(ProjectedPart::Text {
                    text: streaming.text,
                });
            }
            let expected: Vec<ToolCallId> = streaming
                .tool_calls
                .iter()
                .map(|c| c.tool_call_id.clone())
                .collect();
            for call in &streaming.tool_calls {
                parts.push(ProjectedPart::DynamicTool {
                    tool_call_id: call.tool_call_id.to_string(),
                    tool_name: call.name.clone(),
                    state: "input-available".into(),
                    input: Some(call.arguments.clone()),
                    output: None,
                    error_text: None,
                });
            }
            state.messages.push(ProjectedMessage {
                id: streaming.message_id,
                role: "assistant".into(),
                parts,
                metadata: streaming.metadata,
                submission_id: record.submission_id.as_ref().map(ToString::to_string),
            });
            if !expected.is_empty() {
                state.open_batch = Some(OpenToolBatch {
                    expected,
                    outcomes: BTreeMap::new(),
                    suspended: BTreeMap::new(),
                });
            }
        }
        RecordBody::ToolOutcome {
            tool_call_id,
            output,
            is_error,
            ..
        } => {
            let batch = open_batch_mut(state)?;
            let key = tool_call_id.to_string();
            batch
                .outcomes
                .entry(key)
                .or_insert_with(|| record.body.clone());
            apply_tool_part(
                state,
                tool_call_id,
                if *is_error {
                    "output-error"
                } else {
                    "output-available"
                },
                output.clone(),
                *is_error,
            )?;
        }
        RecordBody::ToolSuspended { tool_call_id, .. } => {
            let batch = open_batch_mut(state)?;
            batch
                .suspended
                .insert(tool_call_id.to_string(), record.body.clone());
            apply_tool_part(
                state,
                tool_call_id,
                "input-available",
                None,
                false,
            )?;
        }
        RecordBody::ToolResultsCommitted { tool_call_ids } => {
            let batch = state
                .open_batch
                .take()
                .ok_or_else(|| CoreError::invariant("tool_results_committed without open batch"))?;
            if batch.expected.len() != tool_call_ids.len() {
                return Err(CoreError::invariant(
                    "commit does not list every call once",
                ));
            }
            for (expected, got) in batch.expected.iter().zip(tool_call_ids.iter()) {
                if expected != got {
                    return Err(CoreError::invariant(
                        "commit lists calls out of order or with a different id",
                    ));
                }
            }
            // Overlay writes that travelled with this commit become durable.
            state.persistent_state.append(&mut state.state_overlay);
            state.state_overlay.clear();
        }
        RecordBody::StateWrite { name, value } => {
            state.state_overlay.insert(name.clone(), value.clone());
        }
        RecordBody::MessageDataWrite { name, value } => {
            if let Some(last) = state.messages.last_mut() {
                last.parts.push(ProjectedPart::Data {
                    name: name.clone(),
                    value: value.clone(),
                });
            }
        }
        RecordBody::MessageMetadata { metadata } => {
            if let Some(last) = state.messages.last_mut() {
                last.metadata = Some(deep_merge(last.metadata.clone(), metadata.clone()));
            }
            state.persistent_state.append(&mut state.state_overlay);
            state.state_overlay.clear();
        }
        RecordBody::SubmissionSettled {
            outcome,
            error,
            ..
        } => {
            if state.open_batch.is_some() {
                return Err(CoreError::invariant(
                    "cannot settle while a tool batch is uncommitted",
                ));
            }
            if state.streaming.is_some() {
                return Err(CoreError::invariant("cannot settle while a message is streaming"));
            }
            // Settlement drops leftover bookkeeping.
            state.state_overlay.clear();
            if let Some(id) = &record.submission_id {
                state.settlements.push(ProjectedSettlement {
                    submission_id: id.to_string(),
                    outcome: *outcome,
                    error: error.clone(),
                });
            }
        }
        RecordBody::Compaction { summary, .. } => {
            state.compaction_summary = Some(summary.clone());
        }
        RecordBody::AgentStartRun
        | RecordBody::AgentFinishCycle { .. }
        | RecordBody::ResourceSnapshot { .. }
        | RecordBody::ChildSessionRetained { .. }
        | RecordBody::ToolStepSettled { .. }
        | RecordBody::ToolApprovalAnswered { .. }
        | RecordBody::ToolGateDecision { .. } => {}
    }
    Ok(())
}

fn enforce_barriers(state: &FoldState, body: &RecordBody) -> Result<()> {
    if state.streaming.is_some() && !body.is_assistant_stream() {
        return Err(CoreError::invariant(
            "no append while a message is streaming",
        ));
    }
    if state.open_batch.is_some() {
        let allowed = matches!(
            body,
            RecordBody::ToolOutcome { .. }
                | RecordBody::ToolSuspended { .. }
                | RecordBody::ToolResultsCommitted { .. }
                | RecordBody::ToolStepSettled { .. }
                | RecordBody::ToolGateDecision { .. }
                | RecordBody::StateWrite { .. }
                | RecordBody::ToolApprovalAnswered { .. }
                | RecordBody::ChildSessionRetained { .. }
        );
        if !allowed {
            return Err(CoreError::invariant(
                "no append past an uncommitted tool batch",
            ));
        }
    }
    Ok(())
}

fn streaming_mut(state: &mut FoldState) -> Result<&mut StreamingAssistant> {
    state
        .streaming
        .as_mut()
        .ok_or_else(|| CoreError::invariant("assistant stream record without a started message"))
}

fn open_batch_mut(state: &mut FoldState) -> Result<&mut OpenToolBatch> {
    state
        .open_batch
        .as_mut()
        .ok_or_else(|| CoreError::invariant("tool outcome without an open batch"))
}

fn apply_tool_part(
    state: &mut FoldState,
    tool_call_id: &ToolCallId,
    part_state: &str,
    output: Option<Value>,
    is_error: bool,
) -> Result<()> {
    let last = state
        .messages
        .last_mut()
        .ok_or_else(|| CoreError::invariant("tool outcome with no assistant message"))?;
    for part in &mut last.parts {
        if let ProjectedPart::DynamicTool {
            tool_call_id: id,
            state,
            output: out,
            error_text,
            ..
        } = part
        {
            if id == tool_call_id.as_str() {
                *state = part_state.to_string();
                *out = output.clone();
                *error_text = if is_error {
                    Some(output_as_text(&output))
                } else {
                    None
                };
                return Ok(());
            }
        }
    }
    Ok(())
}

fn output_as_text(output: &Option<Value>) -> String {
    match output {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => "tool error".into(),
    }
}

fn deep_merge(base: Option<Value>, overlay: Value) -> Value {
    match (base, overlay) {
        (Some(Value::Object(mut a)), Value::Object(b)) => {
            for (k, v) in b {
                let merged = deep_merge(a.remove(&k), v);
                a.insert(k, merged);
            }
            Value::Object(a)
        }
        (_, overlay) => overlay,
    }
}

fn escape_attr(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
}

fn escape_text(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::id::{ConversationId, InstanceId};
    use crate::record::Record;
    use crate::time::UnixMillis;
    use crate::AgentName;
    use serde_json::json;

    fn conv() -> ConversationId {
        ConversationId::new(
            &AgentName::parse("support-desk").unwrap(),
            &InstanceId::parse("t1").unwrap(),
        )
    }

    fn batch(records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            path: conv().to_string(),
            seq: 1,
            records,
            submission_id: None,
            attempt_id: None,
        }
    }

    fn rec(body: RecordBody) -> Record {
        Record::new(conv(), conv().to_string(), UnixMillis(1), body)
    }

    #[test]
    fn user_then_assistant_text() {
        let mut state = FoldState::default();
        let records = vec![
            rec(RecordBody::ConversationCreated {
                uid: "u1".into(),
            }),
            rec(RecordBody::UserMessage {
                body: "hello".into(),
                attachments: vec![],
                joined: None,
            }),
            rec(RecordBody::AssistantMessageStarted { metadata: None }),
            rec(RecordBody::AssistantTextStarted),
            rec(RecordBody::AssistantTextDelta {
                text: "hi".into(),
            }),
            rec(RecordBody::AssistantTextCompleted),
            rec(RecordBody::AssistantMessageCompleted {
                stop_reason: Some("stop".into()),
                usage: None,
            }),
        ];
        reduce_batch(&mut state, &batch(records)).unwrap();
        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.messages[1].role, "assistant");
        assert!(state.streaming.is_none());
    }

    #[test]
    fn barrier_rejects_user_while_streaming() {
        let mut state = FoldState::default();
        reduce_batch(
            &mut state,
            &batch(vec![
                rec(RecordBody::ConversationCreated { uid: "u".into() }),
                rec(RecordBody::AssistantMessageStarted { metadata: None }),
            ]),
        )
        .unwrap();
        let err = reduce_batch(
            &mut state,
            &batch(vec![rec(RecordBody::UserMessage {
                body: "no".into(),
                attachments: vec![],
                joined: None,
            })]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("streaming"));
    }

    #[test]
    fn child_session_retained_allowed_during_open_batch() {
        let mut state = FoldState::default();
        let call = ToolCallId::parse("call_task1").unwrap();
        reduce_batch(
            &mut state,
            &batch(vec![
                rec(RecordBody::ConversationCreated { uid: "u".into() }),
                rec(RecordBody::AssistantMessageStarted { metadata: None }),
                rec(RecordBody::AssistantToolCall {
                    tool_call_id: call.clone(),
                    name: "task".into(),
                    arguments: json!({"prompt": "x"}),
                    index: 0,
                }),
                rec(RecordBody::AssistantMessageCompleted {
                    stop_reason: Some("tool_use".into()),
                    usage: None,
                }),
                rec(RecordBody::ChildSessionRetained {
                    child_session: "task:parent:task_01".into(),
                }),
                rec(RecordBody::ToolOutcome {
                    tool_call_id: call.clone(),
                    name: "task".into(),
                    output: Some(json!({"ok": true})),
                    is_error: false,
                    terminate: None,
                    child_conversation_id: None,
                    interrupted: None,
                }),
                rec(RecordBody::ToolResultsCommitted {
                    tool_call_ids: vec![call],
                }),
            ]),
        )
        .unwrap();
        assert!(state.open_batch.is_none());
    }

    #[test]
    fn tool_batch_commit_order() {
        let mut state = FoldState::default();
        let call = ToolCallId::parse("call_01").unwrap();
        reduce_batch(
            &mut state,
            &batch(vec![
                rec(RecordBody::ConversationCreated { uid: "u".into() }),
                rec(RecordBody::AssistantMessageStarted { metadata: None }),
                rec(RecordBody::AssistantToolCall {
                    tool_call_id: call.clone(),
                    name: "lookup".into(),
                    arguments: json!({"id": "1"}),
                    index: 0,
                }),
                rec(RecordBody::AssistantMessageCompleted {
                    stop_reason: Some("tool_use".into()),
                    usage: None,
                }),
                rec(RecordBody::ToolOutcome {
                    tool_call_id: call.clone(),
                    name: "lookup".into(),
                    output: Some(json!({"ok": true})),
                    is_error: false,
                    terminate: None,
                    child_conversation_id: None,
                    interrupted: None,
                }),
                rec(RecordBody::ToolResultsCommitted {
                    tool_call_ids: vec![call],
                }),
            ]),
        )
        .unwrap();
        assert!(state.open_batch.is_none());
        match &state.messages[0].parts[0] {
            ProjectedPart::DynamicTool { state, .. } => {
                assert_eq!(state, "output-available");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn discard_overlay_clears_buffer() {
        let mut state = FoldState::default();
        state.state_overlay.insert("k".into(), json!(1));
        state.discard_overlay();
        assert!(state.state_overlay.is_empty());
    }
}
