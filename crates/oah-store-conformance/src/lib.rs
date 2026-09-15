//! Store contract suite. Adapters must pass every case.

#![forbid(unsafe_code)]

use oah_core::{ConversationId, OwnerId, Principal, SessionKey, UnixMillis};
use oah_store::{AdmitRequest, DeliveryKind, Store, StoreError, SubmissionStatus};
use serde_json::json;

fn cid(instance: &str) -> Result<ConversationId, String> {
    ConversationId::parse(format!("agents/support-desk/{instance}")).map_err(|e| e.to_string())
}

fn admit_req(conversation: &ConversationId, body: &str, key: Option<&str>) -> AdmitRequest {
    AdmitRequest {
        conversation_id: conversation.clone(),
        session_key: SessionKey::root(conversation),
        kind: DeliveryKind::User,
        payload: json!({ "body": body }),
        principal: Principal::anonymous(),
        idempotency_key: key.map(str::to_string),
        uid: None,
        max_attempts: 10,
    }
}

pub async fn admit_idempotent_same_payload(store: &dyn Store) -> Result<(), String> {
    store.migrate().await.map_err(|e| e.to_string())?;
    let conversation = cid("conformance")?;
    let a = store
        .admit(admit_req(&conversation, "hello", Some("k1")), UnixMillis(1))
        .await
        .map_err(|e| e.to_string())?;
    let b = store
        .admit(admit_req(&conversation, "hello", Some("k1")), UnixMillis(2))
        .await
        .map_err(|e| e.to_string())?;
    if a.submission_id != b.submission_id || !b.deduplicated {
        return Err("same payload must deduplicate".into());
    }
    Ok(())
}

pub async fn admit_idempotent_conflict(store: &dyn Store) -> Result<(), String> {
    store.migrate().await.map_err(|e| e.to_string())?;
    let conversation = cid("conflict")?;
    store
        .admit(admit_req(&conversation, "a", Some("k2")), UnixMillis(1))
        .await
        .map_err(|e| e.to_string())?;
    let err = store
        .admit(admit_req(&conversation, "b", Some("k2")), UnixMillis(2))
        .await
        .err()
        .ok_or_else(|| "expected conflict".to_string())?;
    match err {
        StoreError::Conflict(msg) if msg == "submission_conflict" => Ok(()),
        other => Err(format!("wrong error: {other}")),
    }
}

pub async fn claim_fifo_and_session_fence(store: &dyn Store) -> Result<(), String> {
    store.migrate().await.map_err(|e| e.to_string())?;
    let conversation = cid("fence")?;
    let first = store
        .admit(admit_req(&conversation, "one", None), UnixMillis(1))
        .await
        .map_err(|e| e.to_string())?;
    store
        .admit(admit_req(&conversation, "two", None), UnixMillis(2))
        .await
        .map_err(|e| e.to_string())?;
    let owner = OwnerId::new();
    let claim = store
        .claim_runnable(&owner, UnixMillis(3), 30_000)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "expected first claim".to_string())?;
    if claim.row.submission_id != first.submission_id {
        return Err("must claim the earlier submission".into());
    }
    let blocked = store
        .claim_runnable(&owner, UnixMillis(4), 30_000)
        .await
        .map_err(|e| e.to_string())?;
    if blocked.is_some() {
        return Err("session fence: later queued row must wait".into());
    }
    store
        .reserve_settlement(&first.submission_id)
        .await
        .map_err(|e| e.to_string())?;
    store
        .finalize_settlement(&first.submission_id, UnixMillis(5), None)
        .await
        .map_err(|e| e.to_string())?;
    let next = store
        .claim_runnable(&owner, UnixMillis(6), 30_000)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "second claim after settle".to_string())?;
    if next.row.payload.get("body").and_then(|v| v.as_str()) != Some("two") {
        return Err("second claim should be the later body".into());
    }
    Ok(())
}

pub async fn suspend_answer_once(store: &dyn Store) -> Result<(), String> {
    store.migrate().await.map_err(|e| e.to_string())?;
    let conversation = cid("ask")?;
    let receipt = store
        .admit(admit_req(&conversation, "refund", None), UnixMillis(1))
        .await
        .map_err(|e| e.to_string())?;
    let owner = OwnerId::new();
    let claim = store
        .claim_runnable(&owner, UnixMillis(2), 30_000)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "claim".to_string())?;
    store
        .put_suspension(oah_store::Suspension {
            submission_id: claim.row.submission_id.clone(),
            tool_call_id: "call_01".into(),
            tool: "refund".into(),
            effective_args: json!({"amount": 10}),
            reason: "needs approval".into(),
            kind: "approval".into(),
            created_at: UnixMillis(2),
            answered_at: None,
            approved: None,
            answered_by: None,
        })
        .await
        .map_err(|e| e.to_string())?;
    store
        .suspend(&claim.row.submission_id, UnixMillis(3))
        .await
        .map_err(|e| e.to_string())?;
    let row = store
        .get_submission(&receipt.submission_id)
        .await
        .map_err(|e| e.to_string())?;
    if row.status != SubmissionStatus::Suspended {
        return Err(format!("expected suspended, got {:?}", row.status));
    }
    store
        .answer_suspension(
            &receipt.submission_id,
            "call_01",
            true,
            "reviewer",
            UnixMillis(4),
        )
        .await
        .map_err(|e| e.to_string())?;
    let again = store
        .answer_suspension(
            &receipt.submission_id,
            "call_01",
            false,
            "reviewer",
            UnixMillis(5),
        )
        .await
        .err()
        .ok_or_else(|| "second answer must fail".to_string())?;
    if !matches!(again, StoreError::AlreadyAnswered) {
        return Err(format!("expected AlreadyAnswered, got {again}"));
    }
    let row = store
        .get_submission(&receipt.submission_id)
        .await
        .map_err(|e| e.to_string())?;
    if row.status != SubmissionStatus::Queued {
        return Err("answered suspension must requeue".into());
    }
    Ok(())
}

pub async fn create_only_uid_conflict(store: &dyn Store) -> Result<(), String> {
    store.migrate().await.map_err(|e| e.to_string())?;
    let conversation = cid("uid-once")?;
    let mut first = admit_req(&conversation, "a", None);
    first.uid = Some("uid_fixed".into());
    store
        .admit(first, UnixMillis(1))
        .await
        .map_err(|e| e.to_string())?;
    let mut create_only = admit_req(&conversation, "b", None);
    create_only.uid = Some(String::new());
    match store.admit(create_only, UnixMillis(2)).await {
        Err(StoreError::Conflict(msg)) if msg == "agent_instance_exists" => Ok(()),
        other => Err(format!("expected agent_instance_exists, got {other:?}")),
    }
}

pub async fn append_rejects_stale_attempt(store: &dyn Store) -> Result<(), String> {
    store.migrate().await.map_err(|e| e.to_string())?;
    let conversation = cid("fence-append")?;
    let receipt = store
        .admit(admit_req(&conversation, "hello", None), UnixMillis(1))
        .await
        .map_err(|e| e.to_string())?;
    let owner = OwnerId::new();
    let claim = store
        .claim_runnable(&owner, UnixMillis(2), 30_000)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "claim".to_string())?;
    let rec = oah_core::Record::new(
        conversation.clone(),
        claim.row.session_key.to_string(),
        UnixMillis(3),
        oah_core::RecordBody::UserMessage {
            body: "hello".into(),
            attachments: vec![],
            joined: None,
        },
    )
    .with_submission(receipt.submission_id.clone())
    .with_attempt(claim.attempt_id.clone());
    store
        .append(
            conversation.as_str(),
            vec![rec.clone()],
            Some(&receipt.submission_id),
            Some(&claim.attempt_id),
        )
        .await
        .map_err(|e| e.to_string())?;
    let stale = oah_core::AttemptId::new();
    match store
        .append(
            conversation.as_str(),
            vec![rec],
            Some(&receipt.submission_id),
            Some(&stale),
        )
        .await
    {
        Err(StoreError::Conflict(msg)) if msg == "stale_append_fence" => Ok(()),
        other => Err(format!("expected stale_append_fence, got {other:?}")),
    }
}
