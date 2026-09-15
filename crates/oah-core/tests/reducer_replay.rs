//! Checkpoint-seeded replay equals full replay (G2 / §13).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use oah_core::id::{ConversationId, InstanceId, ToolCallId};
use oah_core::record::{Record, RecordBatch, RecordBody};
use oah_core::reducer::{reduce_batch, FoldState};
use oah_core::time::UnixMillis;
use oah_core::AgentName;
use serde_json::json;

fn conv() -> ConversationId {
    ConversationId::new(
        &AgentName::parse("support-desk").unwrap(),
        &InstanceId::parse("replay").unwrap(),
    )
}

fn rec(body: RecordBody) -> Record {
    Record::new(conv(), conv().to_string(), UnixMillis(1), body)
}

#[test]
fn checkpoint_replay_matches_full() {
    let call = ToolCallId::parse("call_replay1").unwrap();
    let records: Vec<Record> = vec![
        rec(RecordBody::ConversationCreated { uid: "uid-1".into() }),
        rec(RecordBody::UserMessage {
            body: "first".into(),
            attachments: vec![],
            joined: None,
        }),
        rec(RecordBody::AssistantMessageStarted { metadata: None }),
        rec(RecordBody::AssistantTextStarted),
        rec(RecordBody::AssistantTextDelta {
            text: "working".into(),
        }),
        rec(RecordBody::AssistantTextCompleted),
        rec(RecordBody::AssistantToolCall {
            tool_call_id: call.clone(),
            name: "lookup_ticket".into(),
            arguments: json!({"id": "42"}),
            index: 0,
        }),
        rec(RecordBody::AssistantMessageCompleted {
            stop_reason: Some("tool_use".into()),
            usage: None,
        }),
        rec(RecordBody::ToolOutcome {
            tool_call_id: call.clone(),
            name: "lookup_ticket".into(),
            output: Some(json!({"status": "open"})),
            is_error: false,
            terminate: None,
            child_conversation_id: None,
            interrupted: None,
        }),
        rec(RecordBody::ToolResultsCommitted {
            tool_call_ids: vec![call],
        }),
        rec(RecordBody::StateWrite {
            name: "attempts".into(),
            value: json!(1),
        }),
        rec(RecordBody::AssistantMessageStarted { metadata: None }),
        rec(RecordBody::AssistantTextDelta { text: "done".into() }),
        rec(RecordBody::AssistantMessageCompleted {
            stop_reason: Some("stop".into()),
            usage: None,
        }),
    ];

    let mut full = FoldState::default();
    reduce_batch(
        &mut full,
        &RecordBatch {
            path: conv().to_string(),
            seq: 1,
            records: records.clone(),
            submission_id: None,
            attempt_id: None,
        },
    )
    .unwrap();

    let split = 6;
    let mut seeded = FoldState::default();
    reduce_batch(
        &mut seeded,
        &RecordBatch {
            path: conv().to_string(),
            seq: 1,
            records: records[..split].to_vec(),
            submission_id: None,
            attempt_id: None,
        },
    )
    .unwrap();
    let checkpoint = serde_json::to_value(&seeded).unwrap();
    let mut restored: FoldState = serde_json::from_value(checkpoint).unwrap();
    reduce_batch(
        &mut restored,
        &RecordBatch {
            path: conv().to_string(),
            seq: 1,
            records: records[split..].to_vec(),
            submission_id: None,
            attempt_id: None,
        },
    )
    .unwrap();

    // Heads differ because batch seq is reused; compare the conversation fold.
    assert_eq!(full.messages, restored.messages);
    assert_eq!(full.persistent_state, restored.persistent_state);
    assert_eq!(full.uid, restored.uid);
    assert_eq!(full.open_batch, restored.open_batch);
    assert_eq!(full.streaming, restored.streaming);
}
