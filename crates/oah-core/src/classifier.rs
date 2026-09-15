use crate::id::SubmissionId;
use crate::record::{Record, RecordBody, SettlementError};
use serde::{Deserialize, Serialize};

/// Recovery class applied to the log after a submission's input record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryClass {
    Absent,
    AdvancedPastInput,
    ResumeFromInput,
    ToolResultsPartial,
    Completed,
    ResumeFromResults,
    ToolUseUnresolved,
    Suspended,
    Overflow,
    TransientRetry,
    StreamContinuation,
    AbortedPartial,
    TerminalError,
}

/// What the coordinator should do after classify + budget checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoverAction {
    Requeue,
    SettleCompleted,
    SettleAborted,
    SettleFailed { error: SettlementError },
    ReplaceAttemptAndResume,
    ParkSuspended,
}

#[derive(Debug, Clone)]
pub struct ClassifierInput<'a> {
    pub submission_id: &'a SubmissionId,
    pub records: &'a [Record],
    pub abort_requested: bool,
    pub attempts_exhausted: bool,
    pub deadline_passed: bool,
    pub retry_count: u32,
}

/// Port of Flue's classifier, plus `suspended`, plus the follow-up-signal fix.
pub fn classify(input: &ClassifierInput<'_>) -> RecoveryClass {
    let sid = input.submission_id.as_str();
    let relevant: Vec<&Record> = input
        .records
        .iter()
        .filter(|r| r.submission_id.as_ref().map(ToString::to_string).as_deref() == Some(sid))
        .collect();

    let input_pos = relevant.iter().position(|r| {
        matches!(
            r.body,
            RecordBody::UserMessage { .. } | RecordBody::Signal { .. }
        )
    });
    let Some(input_idx) = input_pos else {
        return RecoveryClass::Absent;
    };

    let after = &relevant[input_idx + 1..];

    if after.iter().any(|r| {
        matches!(r.body, RecordBody::UserMessage { joined: Some(false), .. })
            || matches!(r.body, RecordBody::UserMessage { joined: None, .. })
                && r.submission_id.as_ref().map(ToString::to_string).as_deref() != Some(sid)
    }) {
        // A later non-joined user message in this session belongs to another
        // submission; if it appears after our input in the filtered set it
        // still means the log advanced. The filter above keeps only our
        // submission, so this branch is reserved for an unfiltered walk.
    }

    // Walk the unfiltered log for "advanced past input".
    if let Some(abs) = input.records.iter().position(|r| r.id == relevant[input_idx].id) {
        let later_foreign_user = input.records[abs + 1..].iter().any(|r| {
            let other = r
                .submission_id
                .as_ref()
                .map(ToString::to_string)
                .as_deref()
                != Some(sid);
            !matches!(r.body, RecordBody::UserMessage { joined: Some(true), .. })
                && matches!(r.body, RecordBody::UserMessage { .. })
                && other
        });
        if later_foreign_user {
            return RecoveryClass::AdvancedPastInput;
        }
    }

    if after.iter().any(|r| matches!(r.body, RecordBody::Compaction { .. }))
        && after.iter().any(|r| {
            matches!(&r.body, RecordBody::Signal { signal_type, .. } if signal_type == "context_overflow")
        })
    {
        return RecoveryClass::Overflow;
    }

    let assistant_msgs: Vec<&&Record> = after
        .iter()
        .filter(|r| matches!(r.body, RecordBody::AssistantMessageCompleted { .. }))
        .collect();

    if assistant_msgs.is_empty() {
        if after.iter().any(|r| {
            matches!(
                r.body,
                RecordBody::AssistantMessageStarted { .. }
                    | RecordBody::AssistantTextDelta { .. }
                    | RecordBody::AssistantReasoningDelta { .. }
            )
        }) {
            return classify_interrupted_stream(after);
        }
        return RecoveryClass::ResumeFromInput;
    }

    let last_completed = assistant_msgs[assistant_msgs.len() - 1];
    let last_idx_in_after = after
        .iter()
        .rposition(|r| r.id == last_completed.id)
        .unwrap_or(after.len().saturating_sub(1));
    let tail = &after[last_idx_in_after + 1..];

    let tool_calls = collect_tool_calls_for_message(after, last_completed);
    if !tool_calls.is_empty() {
        return classify_tool_reply(after, &tool_calls);
    }

    let stop = match &last_completed.body {
        RecordBody::AssistantMessageCompleted { stop_reason, .. } => {
            stop_reason.as_deref().unwrap_or("stop")
        }
        _ => "stop",
    };

    if matches!(stop, "stop" | "length" | "refusal") {
        let follow_up = tail.iter().any(|r| {
            matches!(
                r.body,
                RecordBody::Signal { .. }
                    | RecordBody::UserMessage { joined: Some(true), .. }
                    | RecordBody::AgentFinishCycle { .. }
            )
        });
        if follow_up {
            return RecoveryClass::ResumeFromInput;
        }
        return RecoveryClass::Completed;
    }

    if tail.iter().any(|r| matches_retryable(r)) {
        if input.retry_count < 3 {
            return RecoveryClass::TransientRetry;
        }
        return RecoveryClass::TerminalError;
    }

    RecoveryClass::TerminalError
}

fn classify_interrupted_stream(after: &[&Record]) -> RecoveryClass {
    let has_text_or_thinking = after.iter().any(|r| {
        matches!(
            r.body,
            RecordBody::AssistantTextDelta { .. } | RecordBody::AssistantReasoningDelta { .. }
        )
    });
    let has_tool = after
        .iter()
        .any(|r| matches!(r.body, RecordBody::AssistantToolCall { .. }));
    if has_text_or_thinking && !has_tool {
        RecoveryClass::StreamContinuation
    } else {
        RecoveryClass::AbortedPartial
    }
}

fn collect_tool_calls_for_message(after: &[&Record], completed: &Record) -> Vec<String> {
    let mut ids = Vec::new();
    for r in after {
        if r.id == completed.id {
            break;
        }
        if let RecordBody::AssistantToolCall { tool_call_id, .. } = &r.body {
            ids.push(tool_call_id.to_string());
        }
    }
    // Tool calls belong to the last started message. Take those after the last
    // AssistantMessageStarted that precedes `completed`.
    let start_pos = after
        .iter()
        .position(|r| r.id == completed.id)
        .unwrap_or(after.len());
    let started = after[..start_pos]
        .iter()
        .rposition(|r| matches!(r.body, RecordBody::AssistantMessageStarted { .. }));
    ids.clear();
    if let Some(s) = started {
        for r in &after[s..start_pos] {
            if let RecordBody::AssistantToolCall { tool_call_id, .. } = &r.body {
                ids.push(tool_call_id.to_string());
            }
        }
    }
    ids
}

fn classify_tool_reply(after: &[&Record], tool_calls: &[String]) -> RecoveryClass {
    let mut outcomes: Vec<(&str, &RecordBody)> = Vec::new();
    for r in after {
        match &r.body {
            RecordBody::ToolOutcome { tool_call_id, .. }
            | RecordBody::ToolSuspended { tool_call_id, .. } => {
                outcomes.push((tool_call_id.as_str(), &r.body));
            }
            _ => {}
        }
    }

    if outcomes.iter().any(|(_, b)| matches!(b, RecordBody::ToolSuspended { .. })) {
        let unanswered = outcomes.iter().any(|(id, b)| {
            matches!(b, RecordBody::ToolSuspended { tool_call_id, .. } if tool_call_id.as_str() == *id)
                && !after.iter().any(|r| {
                    matches!(
                        &r.body,
                        RecordBody::ToolApprovalAnswered { tool_call_id, .. }
                            if tool_call_id.as_str() == *id
                    )
                })
        });
        if unanswered {
            return RecoveryClass::Suspended;
        }
    }

    let present: Vec<&str> = outcomes.iter().map(|(id, _)| *id).collect();
    let missing = tool_calls.iter().any(|id| !present.iter().any(|p| p == id));
    if present.is_empty() {
        return RecoveryClass::ToolUseUnresolved;
    }
    if missing {
        return RecoveryClass::ToolResultsPartial;
    }

    let all_terminate = !outcomes.is_empty()
        && outcomes.iter().all(|(_, b)| {
            matches!(
                b,
                RecordBody::ToolOutcome {
                    terminate: Some(true),
                    ..
                }
            )
        });
    if all_terminate {
        return RecoveryClass::Completed;
    }
    RecoveryClass::ResumeFromResults
}

fn matches_retryable(record: &Record) -> bool {
    if let RecordBody::Signal { signal_type, .. } = &record.body {
        return signal_type == "submission_interrupted"
            || signal_type == "context_overflow"
            || signal_type == "transient_retry";
    }
    false
}

/// Reconcile a `running` row whose owner is dead.
pub fn recover_action(class: &RecoveryClass, input: &ClassifierInput<'_>) -> RecoverAction {
    if matches!(class, RecoveryClass::Completed) {
        return RecoverAction::SettleCompleted;
    }
    if input.abort_requested {
        return RecoverAction::SettleAborted;
    }
    if matches!(class, RecoveryClass::Suspended) {
        return RecoverAction::ParkSuspended;
    }
    if input.attempts_exhausted {
        let error = if matches!(class, RecoveryClass::Absent) {
            SettlementError::SubmissionInterrupted
        } else {
            SettlementError::SubmissionRetryExhausted
        };
        return RecoverAction::SettleFailed { error };
    }
    if input.deadline_passed {
        return RecoverAction::SettleFailed {
            error: SettlementError::SubmissionTimeout,
        };
    }
    match class {
        RecoveryClass::Absent => RecoverAction::Requeue,
        RecoveryClass::AdvancedPastInput => RecoverAction::SettleFailed {
            error: SettlementError::OperationFailed,
        },
        RecoveryClass::TerminalError => RecoverAction::SettleFailed {
            error: SettlementError::OperationFailed,
        },
        RecoveryClass::AbortedPartial if input.abort_requested => RecoverAction::SettleAborted,
        _ => RecoverAction::ReplaceAttemptAndResume,
    }
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

    fn rec(sid: &SubmissionId, body: RecordBody) -> Record {
        Record::new(conv(), conv().to_string(), UnixMillis(1), body).with_submission(sid.clone())
    }

    fn input<'a>(sid: &'a SubmissionId, records: &'a [Record]) -> ClassifierInput<'a> {
        ClassifierInput {
            submission_id: sid,
            records,
            abort_requested: false,
            attempts_exhausted: false,
            deadline_passed: false,
            retry_count: 0,
        }
    }

    #[test]
    fn absent_without_input() {
        let sid = SubmissionId::parse("sub_01").unwrap();
        let records = vec![rec(
            &sid,
            RecordBody::ConversationCreated { uid: "u".into() },
        )];
        assert_eq!(classify(&input(&sid, &records)), RecoveryClass::Absent);
    }

    #[test]
    fn resume_from_input_when_no_assistant() {
        let sid = SubmissionId::parse("sub_01").unwrap();
        let records = vec![
            rec(&sid, RecordBody::ConversationCreated { uid: "u".into() }),
            rec(
                &sid,
                RecordBody::UserMessage {
                    body: "hi".into(),
                    attachments: vec![],
                    joined: None,
                },
            ),
        ];
        assert_eq!(
            classify(&input(&sid, &records)),
            RecoveryClass::ResumeFromInput
        );
    }

    #[test]
    fn completed_stop_with_nothing_after() {
        let sid = SubmissionId::parse("sub_01").unwrap();
        let records = vec![
            rec(&sid, RecordBody::ConversationCreated { uid: "u".into() }),
            rec(
                &sid,
                RecordBody::UserMessage {
                    body: "hi".into(),
                    attachments: vec![],
                    joined: None,
                },
            ),
            rec(&sid, RecordBody::AssistantMessageStarted { metadata: None }),
            rec(
                &sid,
                RecordBody::AssistantMessageCompleted {
                    stop_reason: Some("stop".into()),
                    usage: None,
                },
            ),
        ];
        assert_eq!(classify(&input(&sid, &records)), RecoveryClass::Completed);
    }

    #[test]
    fn follow_up_signal_does_not_settle_completed() {
        let sid = SubmissionId::parse("sub_01").unwrap();
        let records = vec![
            rec(&sid, RecordBody::ConversationCreated { uid: "u".into() }),
            rec(
                &sid,
                RecordBody::UserMessage {
                    body: "hi".into(),
                    attachments: vec![],
                    joined: None,
                },
            ),
            rec(&sid, RecordBody::AssistantMessageStarted { metadata: None }),
            rec(
                &sid,
                RecordBody::AssistantMessageCompleted {
                    stop_reason: Some("stop".into()),
                    usage: None,
                },
            ),
            rec(
                &sid,
                RecordBody::Signal {
                    signal_type: "verify".into(),
                    body: "run tests".into(),
                    attributes: None,
                    tag_name: None,
                },
            ),
        ];
        assert_eq!(
            classify(&input(&sid, &records)),
            RecoveryClass::ResumeFromInput
        );
    }

    #[test]
    fn suspended_unanswered() {
        let sid = SubmissionId::parse("sub_01").unwrap();
        let call = crate::id::ToolCallId::parse("call_01").unwrap();
        let records = vec![
            rec(&sid, RecordBody::ConversationCreated { uid: "u".into() }),
            rec(
                &sid,
                RecordBody::UserMessage {
                    body: "hi".into(),
                    attachments: vec![],
                    joined: None,
                },
            ),
            rec(&sid, RecordBody::AssistantMessageStarted { metadata: None }),
            rec(
                &sid,
                RecordBody::AssistantToolCall {
                    tool_call_id: call.clone(),
                    name: "bash".into(),
                    arguments: json!({"command": "ls"}),
                    index: 0,
                },
            ),
            rec(
                &sid,
                RecordBody::AssistantMessageCompleted {
                    stop_reason: Some("tool_use".into()),
                    usage: None,
                },
            ),
            rec(
                &sid,
                RecordBody::ToolSuspended {
                    tool_call_id: call,
                    name: "bash".into(),
                    effective_args: json!({}),
                    reason: "needs approval".into(),
                    kind: "approval".into(),
                },
            ),
        ];
        assert_eq!(classify(&input(&sid, &records)), RecoveryClass::Suspended);
    }
}
