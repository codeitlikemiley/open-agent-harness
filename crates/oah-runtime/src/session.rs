use crate::agent::{PrepareCx, RenderCx};
use crate::gates::{GateCx, GatedCall, Verdict};
use crate::tools::RegistryExecutor;
use crate::{Runtime, RuntimeError};
use oah_core::{
    classify, recover_action, AttemptId, ClassifierInput, ConversationId, FoldState, Record,
    RecordBody, RecoverAction, RecoveryClass, SessionKey, SettlementError, SettlementOutcome,
    StreamOffset, SubmissionId, UnixMillis,
};
use oah_loop::{LoopConfig, LoopHost, PreparedCall, ToolResult};
use oah_model::{ContentBlock, Message, MessageRole};
use oah_sandbox::SandboxProvider;
use oah_store::{Claim, DeliveryKind, StoreError, Suspension};
use serde_json::Value;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    SettledCompleted,
    SettledFailed,
    SettledAborted,
    Suspended,
}

pub async fn process_claim(
    rt: &Runtime,
    claim: Claim,
    cancel: CancellationToken,
) -> Result<ProcessOutcome, RuntimeError> {
    let now = UnixMillis::now_system();
    if claim.row.abort_requested {
        return settle(
            rt,
            &claim.row.conversation_id,
            &claim.row.session_key,
            &claim.row.submission_id,
            &claim.attempt_id,
            SettlementOutcome::Aborted,
            Some(SettlementError::SubmissionAborted),
            now,
        )
        .await;
    }

    let agent_name = claim
        .row
        .conversation_id
        .agent()
        .map_err(RuntimeError::from)?;
    let instance = claim
        .row
        .conversation_id
        .instance()
        .map_err(RuntimeError::from)?;
    let agent = rt.agent(&agent_name)?;

    let records = rt
        .store
        .read_all(claim.row.conversation_id.as_str())
        .await?;
    ensure_conversation(rt, &claim, &records, now).await?;

    let records = rt
        .store
        .read_all(claim.row.conversation_id.as_str())
        .await?;
    // Recovery classifier is for a *resumed* attempt. A fresh claim has no
    // input record yet. That is not `Absent` (which means requeue).
    let already_applied = claim.row.input_applied_at.is_some()
        || records.iter().any(|r| {
            r.submission_id.as_ref() == Some(&claim.row.submission_id)
                && matches!(
                    r.body,
                    RecordBody::UserMessage { .. } | RecordBody::Signal { .. }
                )
        });
    if already_applied {
        let class = classify(&ClassifierInput {
            submission_id: &claim.row.submission_id,
            records: &records,
            abort_requested: claim.row.abort_requested,
            attempts_exhausted: claim.row.attempt_count >= claim.row.max_attempts,
            deadline_passed: false,
            retry_count: 0,
        });
        match recover_action(
            &class,
            &ClassifierInput {
                submission_id: &claim.row.submission_id,
                records: &records,
                abort_requested: claim.row.abort_requested,
                attempts_exhausted: claim.row.attempt_count >= claim.row.max_attempts
                    && matches!(class, RecoveryClass::Absent | RecoveryClass::TerminalError),
                deadline_passed: false,
                retry_count: 0,
            },
        ) {
            RecoverAction::SettleCompleted => {
                return settle(
                    rt,
                    &claim.row.conversation_id,
                    &claim.row.session_key,
                    &claim.row.submission_id,
                    &claim.attempt_id,
                    SettlementOutcome::Completed,
                    None,
                    now,
                )
                .await;
            }
            RecoverAction::SettleAborted => {
                return settle(
                    rt,
                    &claim.row.conversation_id,
                    &claim.row.session_key,
                    &claim.row.submission_id,
                    &claim.attempt_id,
                    SettlementOutcome::Aborted,
                    Some(SettlementError::SubmissionAborted),
                    now,
                )
                .await;
            }
            RecoverAction::SettleFailed { error } => {
                return settle(
                    rt,
                    &claim.row.conversation_id,
                    &claim.row.session_key,
                    &claim.row.submission_id,
                    &claim.attempt_id,
                    SettlementOutcome::Failed,
                    Some(error),
                    now,
                )
                .await;
            }
            RecoverAction::ParkSuspended => {
                let pending = rt.store.list_suspensions(&claim.row.submission_id).await?;
                if pending.iter().any(|s| s.answered_at.is_none()) {
                    return Ok(ProcessOutcome::Suspended);
                }
                // Store already answered; continue into resume (do not re-propose).
            }
            RecoverAction::Requeue => {
                rt.store.requeue(&claim.row.submission_id).await?;
                return Ok(ProcessOutcome::SettledFailed);
            }
            RecoverAction::ReplaceAttemptAndResume => {}
        }
    }

    let props = agent.prepare(PrepareCx {
        id: &instance,
        principal: &claim.row.principal,
    });
    let mut cx = RenderCx::new(&instance, &claim.row.principal, &props);
    let instructions = agent.render(&mut cx)?;
    if !cx.frame.skills.is_empty() {
        let _ = cx.use_tool(crate::tools::activate_skill_tool(cx.frame.skills.clone()));
    }
    if !cx.frame.subagents.is_empty() || cx.frame.tools.iter().any(|t| t.name == "task") {
        if !cx.frame.tools.iter().any(|t| t.name == "task") {
            let _ = cx.use_tool(crate::tools::task_tool());
        }
        if !cx.frame.tools.iter().any(|t| t.name == "step") {
            let _ = cx.use_tool(crate::tools::step_run_tool());
        }
    }
    if !cx.frame.mcp_servers.is_empty() {
        if let Some(mcp) = &rt.mcp {
            for name in mcp.tool_names() {
                if let Some((server, _)) = oah_mcp::parse_mcp_tool_name(&name) {
                    if cx.frame.mcp_servers.iter().any(|s| s == &server)
                        && !cx.frame.tools.iter().any(|t| t.name == name)
                    {
                        let _ = cx.use_tool(crate::tools::mcp_bridge_tool(mcp.clone(), name));
                    }
                }
            }
        }
    }
    if let Some(decl) = &cx.frame.sandbox {
        let key = oah_core::SandboxKey::scoped(claim.row.conversation_id.as_str(), &decl.name);
        let spec = oah_sandbox::SandboxSpec {
            cwd: decl.cwd.clone(),
        };
        let driver = rt
            .sandboxes
            .acquire(&key, &spec)
            .await
            .map_err(|e| RuntimeError::Other(e.to_string()))?;
        for tool in crate::sandbox_tools::sandbox_tool_defs(driver) {
            if !cx.frame.tools.iter().any(|t| t.name == tool.name) {
                let _ = cx.use_tool(tool);
            }
        }
    }
    let system = cx.join_instructions(instructions);
    write_render_side_effects(rt, &claim, &cx).await?;

    let model_spec = cx
        .frame
        .model
        .as_ref()
        .map(|m| m.spec.clone())
        .unwrap_or_else(|| rt.default_model.clone());

    append_input(rt, &claim, now).await?;
    rt.store
        .mark_input_applied(&claim.row.submission_id, now)
        .await?;

    let pending = rt.store.list_suspensions(&claim.row.submission_id).await?;
    if pending.iter().any(|s| s.answered_at.is_none()) {
        rt.store.suspend(&claim.row.submission_id, now).await?;
        return Ok(ProcessOutcome::Suspended);
    }

    let tools: HashMap<_, _> = cx
        .frame
        .tools
        .iter()
        .cloned()
        .map(|t| (t.name.clone(), t))
        .collect();
    let specs: Vec<_> = cx.frame.tools.iter().map(|t| t.spec()).collect();
    let gates = cx.frame.gates.clone();

    let history = rebuild_messages(
        &rt.store
            .read_all(claim.row.conversation_id.as_str())
            .await?,
        &claim.row.submission_id,
    );

    let affinity = affinity_id(&agent_name, &instance);
    let request = oah_loop::make_request(
        model_spec,
        system,
        history,
        specs,
        rt.max_output_tokens,
        Some(affinity),
    );

    let store = rt.store.clone();
    let conv = claim.row.conversation_id.clone();
    let parent_session = claim.row.session_key.clone();
    let submission = claim.row.submission_id.clone();
    let attempt = claim.attempt_id.clone();
    let spawn: crate::tools::SpawnFn = std::sync::Arc::new(move |req: oah_store::AdmitRequest| {
        let store = store.clone();
        let conv = conv.clone();
        let parent_session = parent_session.clone();
        let submission = submission.clone();
        let attempt = attempt.clone();
        Box::pin(async move {
            let child_session = req.session_key.to_string();
            let receipt = store
                .admit(req, UnixMillis::now_system())
                .await
                .map_err(|e| e.to_string())?;
            let rec = Record::new(
                conv.clone(),
                parent_session.to_string(),
                UnixMillis::now_system(),
                RecordBody::ChildSessionRetained {
                    child_session,
                },
            )
            .with_submission(submission.clone())
            .with_attempt(attempt.clone());
            store
                .append(
                    conv.as_str(),
                    vec![rec],
                    Some(&submission),
                    Some(&attempt),
                )
                .await
                .map_err(|e| e.to_string())?;
            Ok(receipt)
        })
    });
    let exec = RegistryExecutor {
        tools: tools.clone(),
        principal: claim.row.principal.clone(),
        spawn: Some(spawn),
        session: Some(claim.row.session_key.clone()),
        conversation: Some(claim.row.conversation_id.clone()),
    };
    let mut host = LoopHost {
        model: rt.model.as_ref(),
        tools: &exec,
        request,
        cancel: cancel.clone(),
        config: LoopConfig::default(),
    };

    if let Some(answered) = pending.iter().rev().find(|s| s.answered_at.is_some()) {
        resume_answered_approval(rt, &claim, &mut host, &tools, answered).await?;
    }

    let mut turns = 0u32;
    loop {
        if cancel.is_cancelled() || claim_aborted(rt, &claim.row.submission_id).await? {
            return settle(
                rt,
                &claim.row.conversation_id,
                &claim.row.session_key,
                &claim.row.submission_id,
                &claim.attempt_id,
                SettlementOutcome::Aborted,
                Some(SettlementError::SubmissionAborted),
                UnixMillis::now_system(),
            )
            .await;
        }

        let mut outcome = host
            .run_model()
            .await
            .map_err(|e| RuntimeError::Other(e.to_string()))?;
        turns = turns.saturating_add(1);

        record_assistant_turn(rt, &claim, &outcome).await?;

        if outcome.aborted {
            return settle(
                rt,
                &claim.row.conversation_id,
                &claim.row.session_key,
                &claim.row.submission_id,
                &claim.attempt_id,
                SettlementOutcome::Aborted,
                Some(SettlementError::SubmissionAborted),
                UnixMillis::now_system(),
            )
            .await;
        }

        if !outcome.tool_calls.is_empty() {
            let log = rt
                .store
                .read_all(claim.row.conversation_id.as_str())
                .await?;
            let gated = run_gates(rt, &claim, &gates, &outcome.tool_calls).await?;
            if let Some(ask) = gated.ask {
                record_tool_results(rt, &claim, &gated.denied, true).await?;
                let tool_call_id = oah_core::ToolCallId::from_model_id(ask.tool_call_id.clone());
                append_record(
                    rt,
                    &claim,
                    RecordBody::ToolSuspended {
                        tool_call_id: tool_call_id.clone(),
                        name: ask.name.clone(),
                        effective_args: ask.arguments.clone(),
                        reason: ask.reason.clone(),
                        kind: "approval".into(),
                    },
                )
                .await
                .ok();
                let sus = Suspension {
                    submission_id: claim.row.submission_id.clone(),
                    tool_call_id: ask.tool_call_id.clone(),
                    tool: ask.name.clone(),
                    effective_args: ask.arguments.clone(),
                    reason: ask.reason.clone(),
                    kind: "approval".into(),
                    created_at: UnixMillis::now_system(),
                    answered_at: None,
                    approved: None,
                    answered_by: None,
                };
                rt.store.put_suspension(sus).await?;
                rt.store
                    .suspend(&claim.row.submission_id, UnixMillis::now_system())
                    .await?;
                info!(
                    submission = %claim.row.submission_id,
                    tool = %ask.name,
                    "submission suspended for approval"
                );
                return Ok(ProcessOutcome::Suspended);
            }

            let mut to_run = Vec::new();
            let mut reused = gated.denied;
            let blocked: std::collections::HashSet<String> = reused.iter().map(|r| r.id.clone()).collect();
            for call in &outcome.tool_calls {
                if blocked.contains(&call.id) {
                    continue;
                }
                match resume_tool_decision(&tools, call, &log, &claim.attempt_id) {
                    ToolResume::Skip(result) => reused.push(result),
                    ToolResume::Run => to_run.push(call.clone()),
                }
            }
            let executed = host.execute_calls(&to_run).await;
            reused.extend(executed);
            outcome.tool_results = reused;
            record_tool_results(rt, &claim, &outcome.tool_results, false).await?;
            host.apply_results(&outcome);
            if oah_loop::should_continue(&outcome) && turns < 32 {
                continue;
            }
        } else {
            host.apply_results(&outcome);
        }

        break;
    }

    settle(
        rt,
        &claim.row.conversation_id,
        &claim.row.session_key,
        &claim.row.submission_id,
        &claim.attempt_id,
        SettlementOutcome::Completed,
        None,
        UnixMillis::now_system(),
    )
    .await
}

struct AskInfo {
    tool_call_id: String,
    name: String,
    arguments: Value,
    reason: String,
}

struct GatedBatch {
    denied: Vec<ToolResult>,
    ask: Option<AskInfo>,
}

enum ToolResume {
    Run,
    Skip(ToolResult),
}

async fn run_gates(
    rt: &Runtime,
    claim: &Claim,
    gates: &[std::sync::Arc<dyn crate::gates::ToolGate>],
    calls: &[PreparedCall],
) -> Result<GatedBatch, RuntimeError> {
    let mut denied = Vec::new();
    let mut ask = None;
    let cx = GateCx {
        principal: &claim.row.principal,
    };
    for call in calls {
        let mut gated = GatedCall {
            name: call.name.clone(),
            tool_call_id: call.id.clone(),
            arguments: call.arguments.clone(),
        };
        let mut verdict = Verdict::Allow;
        for gate in gates {
            match gate.check(&mut gated, &cx).await {
                Verdict::Allow => {}
                other => {
                    verdict = other;
                    break;
                }
            }
        }
        match verdict {
            Verdict::Allow => {}
            Verdict::Deny { reason } => {
                append_record(
                    rt,
                    claim,
                    RecordBody::ToolGateDecision {
                        tool_call_id: oah_core::ToolCallId::from_model_id(call.id.clone()),
                        gate: "chain".into(),
                        verdict: "deny".into(),
                        overwritten_paths: vec![],
                        reason: Some(reason.clone()),
                    },
                )
                .await
                .ok();
                denied.push(ToolResult {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    output: Value::String(reason),
                    is_error: true,
                    terminate: false,
                });
            }
            Verdict::Ask { reason, .. } => {
                append_record(
                    rt,
                    claim,
                    RecordBody::ToolGateDecision {
                        tool_call_id: oah_core::ToolCallId::from_model_id(call.id.clone()),
                        gate: "chain".into(),
                        verdict: "ask".into(),
                        overwritten_paths: vec![],
                        reason: Some(reason.clone()),
                    },
                )
                .await
                .ok();
                ask = Some(AskInfo {
                    tool_call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: gated.arguments,
                    reason,
                });
            }
        }
    }
    Ok(GatedBatch { denied, ask })
}

fn resume_tool_decision(
    tools: &HashMap<String, crate::tools::ToolDef>,
    call: &PreparedCall,
    log: &[Record],
    current_attempt: &AttemptId,
) -> ToolResume {
    if let Some(existing) = log.iter().find_map(|r| match &r.body {
        RecordBody::ToolOutcome {
            tool_call_id,
            name,
            output,
            is_error,
            terminate,
            ..
        } if tool_call_id.as_str() == call.id => Some(ToolResult {
            id: call.id.clone(),
            name: name.clone(),
            output: output.clone().unwrap_or(Value::Null),
            is_error: *is_error,
            terminate: terminate.unwrap_or(false),
        }),
        _ => None,
    }) {
        return ToolResume::Skip(existing);
    }
    let durable = tools.get(&call.name).map(|t| t.durable).unwrap_or(true);
    if !durable
        && log.iter().any(|r| {
            matches!(
                &r.body,
                RecordBody::AssistantToolCall { tool_call_id, .. } if tool_call_id.as_str() == call.id
            ) && r.attempt_id.as_ref() != Some(current_attempt)
        })
    {
        return ToolResume::Skip(oah_loop::interrupted_result(&call.name, &call.id));
    }
    ToolResume::Run
}

async fn resume_answered_approval(
    rt: &Runtime,
    claim: &Claim,
    host: &mut oah_loop::LoopHost<'_, dyn oah_model::ModelClient, RegistryExecutor>,
    tools: &HashMap<String, crate::tools::ToolDef>,
    answered: &Suspension,
) -> Result<(), RuntimeError> {
    let log = rt
        .store
        .read_all(claim.row.conversation_id.as_str())
        .await?;
    let already = log.iter().any(|r| matches!(
        &r.body,
        RecordBody::ToolApprovalAnswered { tool_call_id, .. }
            if tool_call_id.as_str() == answered.tool_call_id
    ));
    let has_outcome = log.iter().any(|r| matches!(
        &r.body,
        RecordBody::ToolOutcome { tool_call_id, .. }
            if tool_call_id.as_str() == answered.tool_call_id
    ));
    if has_outcome {
        return Ok(());
    }
    if !already {
        let tool_call_id = oah_core::ToolCallId::from_model_id(answered.tool_call_id.clone());
        append_record(
            rt,
            claim,
            RecordBody::ToolApprovalAnswered {
                tool_call_id: tool_call_id.clone(),
                approved: answered.approved.unwrap_or(false),
                answered_by: answered
                    .answered_by
                    .clone()
                    .unwrap_or_else(|| "unknown".into()),
                reason: None,
            },
        )
        .await?;
    }
    let result = if answered.approved == Some(true) {
        if tools.contains_key(&answered.tool) {
            let call = PreparedCall {
                index: 0,
                id: answered.tool_call_id.clone(),
                name: answered.tool.clone(),
                arguments: answered.effective_args.clone(),
            };
            host.execute_calls(std::slice::from_ref(&call))
                .await
                .into_iter()
                .next()
                .unwrap_or_else(|| ToolResult {
                    id: answered.tool_call_id.clone(),
                    name: answered.tool.clone(),
                    output: Value::String("approved tool produced no result".into()),
                    is_error: true,
                    terminate: false,
                })
        } else {
            oah_loop::tool_not_found(&answered.tool, &answered.tool_call_id)
        }
    } else {
        ToolResult {
            id: answered.tool_call_id.clone(),
            name: answered.tool.clone(),
            output: Value::String("denied by reviewer".into()),
            is_error: true,
            terminate: false,
        }
    };
    record_tool_results(rt, claim, std::slice::from_ref(&result), false).await?;
    host.apply_tool_results(std::slice::from_ref(&result));
    Ok(())
}

async fn write_render_side_effects(
    rt: &Runtime,
    claim: &Claim,
    cx: &RenderCx<'_>,
) -> Result<(), RuntimeError> {
    if cx.frame.start_run {
        append_record(rt, claim, RecordBody::AgentStartRun).await.ok();
    }
    for (name, value) in &cx.frame.pending_state {
        append_record(
            rt,
            claim,
            RecordBody::StateWrite {
                name: name.clone(),
                value: value.clone(),
            },
        )
        .await
        .ok();
    }
    for (name, value) in &cx.frame.pending_data {
        append_record(
            rt,
            claim,
            RecordBody::MessageDataWrite {
                name: name.clone(),
                value: value.clone(),
            },
        )
        .await
        .ok();
    }
    if cx.frame.finish_cycles > 0 {
        append_record(
            rt,
            claim,
            RecordBody::AgentFinishCycle {
                cycle: cx.frame.finish_cycles,
            },
        )
        .await
        .ok();
    }
    if !cx.frame.resources.is_empty() {
        let instructions = cx
            .frame
            .resources
            .iter()
            .map(|r| format!("{}: {}", r.name, r.body))
            .collect::<Vec<_>>()
            .join("\n");
        append_record(
            rt,
            claim,
            RecordBody::ResourceSnapshot {
                instructions: Some(instructions),
                environment: None,
                resources: None,
            },
        )
        .await
        .ok();
    }
    for disp in &cx.frame.pending_dispatches {
        if let (Ok(agent), Ok(instance)) = (
            oah_core::AgentName::parse(&disp.agent),
            oah_core::InstanceId::parse(&disp.instance),
        ) {
            rt.dispatch(
                &agent,
                &instance,
                disp.body.clone(),
                claim.row.principal.clone(),
                None,
                None,
            )
            .await
            .ok();
        }
    }
    Ok(())
}

async fn ensure_conversation(
    rt: &Runtime,
    claim: &Claim,
    records: &[Record],
    now: UnixMillis,
) -> Result<(), RuntimeError> {
    if records
        .iter()
        .any(|r| matches!(r.body, RecordBody::ConversationCreated { .. }))
    {
        return Ok(());
    }
    let uid = rt
        .store
        .stream_head(claim.row.conversation_id.as_str())
        .await
        .map(|(_, _, uid)| uid)
        .unwrap_or_else(|_| format!("uid_{}", claim.row.submission_id));
    let rec = Record::new(
        claim.row.conversation_id.clone(),
        claim.row.session_key.to_string(),
        now,
        RecordBody::ConversationCreated { uid },
    )
    .with_submission(claim.row.submission_id.clone())
    .with_attempt(claim.attempt_id.clone());
    rt.store
        .append(
            claim.row.conversation_id.as_str(),
            vec![rec],
            Some(&claim.row.submission_id),
            Some(&claim.attempt_id),
        )
        .await?;
    Ok(())
}

async fn append_input(rt: &Runtime, claim: &Claim, now: UnixMillis) -> Result<(), RuntimeError> {
    let already = rt
        .store
        .read_all(claim.row.conversation_id.as_str())
        .await?
        .iter()
        .any(|r| {
            r.submission_id.as_ref() == Some(&claim.row.submission_id)
                && matches!(r.body, RecordBody::UserMessage { .. } | RecordBody::Signal { .. })
        });
    if already {
        return Ok(());
    }
    let body = claim
        .row
        .payload
        .get("body")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let rec = match claim.row.kind {
        DeliveryKind::Signal => Record::new(
            claim.row.conversation_id.clone(),
            claim.row.session_key.to_string(),
            now,
            RecordBody::Signal {
                signal_type: claim
                    .row
                    .payload
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("signal")
                    .to_string(),
                body,
                attributes: None,
                tag_name: None,
            },
        ),
        DeliveryKind::User => Record::new(
            claim.row.conversation_id.clone(),
            claim.row.session_key.to_string(),
            now,
            RecordBody::UserMessage {
                body,
                attachments: vec![],
                joined: None,
            },
        ),
    }
    .with_submission(claim.row.submission_id.clone())
    .with_attempt(claim.attempt_id.clone());
    rt.store
        .append(
            claim.row.conversation_id.as_str(),
            vec![rec],
            Some(&claim.row.submission_id),
            Some(&claim.attempt_id),
        )
        .await?;
    Ok(())
}

async fn record_assistant_turn(
    rt: &Runtime,
    claim: &Claim,
    outcome: &oah_loop::TurnOutcome,
) -> Result<(), RuntimeError> {
    let now = UnixMillis::now_system();
    let mut recs = vec![Record::new(
        claim.row.conversation_id.clone(),
        claim.row.session_key.to_string(),
        now,
        RecordBody::AssistantMessageStarted { metadata: None },
    )
    .with_submission(claim.row.submission_id.clone())
    .with_attempt(claim.attempt_id.clone())];

    for block in &outcome.assistant.content {
        match block {
            ContentBlock::Thinking { text, signature } => {
                recs.push(stamp(
                    claim,
                    now,
                    RecordBody::AssistantReasoningStarted,
                ));
                recs.push(stamp(
                    claim,
                    now,
                    RecordBody::AssistantReasoningDelta { text: text.clone() },
                ));
                recs.push(stamp(
                    claim,
                    now,
                    RecordBody::AssistantReasoningCompleted {
                        signature: signature.clone(),
                    },
                ));
            }
            ContentBlock::Text { text } => {
                recs.push(stamp(claim, now, RecordBody::AssistantTextStarted));
                recs.push(stamp(
                    claim,
                    now,
                    RecordBody::AssistantTextDelta { text: text.clone() },
                ));
                recs.push(stamp(claim, now, RecordBody::AssistantTextCompleted));
            }
            ContentBlock::ToolUse { id, name, input } => {
                let tool_call_id = oah_core::ToolCallId::from_model_id(id.clone());
                recs.push(stamp(
                    claim,
                    now,
                    RecordBody::AssistantToolCall {
                        tool_call_id,
                        name: name.clone(),
                        arguments: input.clone(),
                        index: 0,
                    },
                ));
            }
            _ => {}
        }
    }
    recs.push(stamp(
        claim,
        now,
        RecordBody::AssistantMessageCompleted {
            stop_reason: Some(outcome.stop.as_str().to_string()),
            usage: None,
        },
    ));
    rt.store
        .append(
            claim.row.conversation_id.as_str(),
            recs,
            Some(&claim.row.submission_id),
            Some(&claim.attempt_id),
        )
        .await?;
    Ok(())
}

async fn record_tool_results(
    rt: &Runtime,
    claim: &Claim,
    results: &[ToolResult],
    hold_open: bool,
) -> Result<(), RuntimeError> {
    let now = UnixMillis::now_system();
    let mut recs = Vec::new();
    let mut ids = Vec::new();
    for r in results {
        let tool_call_id = oah_core::ToolCallId::from_model_id(r.id.clone());
        ids.push(tool_call_id.clone());
        let interrupted = r
            .output
            .get("type")
            .and_then(|v| v.as_str())
            == Some("interrupted");
        recs.push(stamp(
            claim,
            now,
            RecordBody::ToolOutcome {
                tool_call_id,
                name: r.name.clone(),
                output: Some(r.output.clone()),
                is_error: r.is_error,
                terminate: Some(r.terminate),
                child_conversation_id: None,
                interrupted: interrupted.then_some(true),
            },
        ));
    }
    if hold_open {
        // Leave the batch uncommitted; classifier sees tool_suspended separately.
    } else if !ids.is_empty() {
        recs.push(stamp(
            claim,
            now,
            RecordBody::ToolResultsCommitted {
                tool_call_ids: ids,
            },
        ));
    }
    if !recs.is_empty() {
        rt.store
            .append(
                claim.row.conversation_id.as_str(),
                recs,
                Some(&claim.row.submission_id),
                Some(&claim.attempt_id),
            )
            .await?;
    }
    Ok(())
}

fn stamp(claim: &Claim, now: UnixMillis, body: RecordBody) -> Record {
    Record::new(
        claim.row.conversation_id.clone(),
        claim.row.session_key.to_string(),
        now,
        body,
    )
    .with_submission(claim.row.submission_id.clone())
    .with_attempt(claim.attempt_id.clone())
}

async fn append_record(rt: &Runtime, claim: &Claim, body: RecordBody) -> Result<(), RuntimeError> {
    let rec = stamp(claim, UnixMillis::now_system(), body);
    rt.store
        .append(
            claim.row.conversation_id.as_str(),
            vec![rec],
            Some(&claim.row.submission_id),
            Some(&claim.attempt_id),
        )
        .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn settle(
    rt: &Runtime,
    conversation: &ConversationId,
    session: &SessionKey,
    submission: &SubmissionId,
    attempt: &AttemptId,
    outcome: SettlementOutcome,
    error: Option<SettlementError>,
    now: UnixMillis,
) -> Result<ProcessOutcome, RuntimeError> {
    if let Err(err) = rt.store.reserve_settlement(submission).await {
        if matches!(err, StoreError::Conflict(_)) {
            warn!(%submission, "settlement already reserved");
        } else {
            return Err(err.into());
        }
    }
    let rec = Record::new(
        conversation.clone(),
        session.to_string(),
        now,
        RecordBody::SubmissionSettled {
            outcome,
            error: error.clone(),
            answered_by_submission_id: None,
        },
    )
    .with_submission(submission.clone())
    .with_attempt(attempt.clone());
    rt.store
        .append(conversation.as_str(), vec![rec], Some(submission), Some(attempt))
        .await?;
    let err_s = error.map(|e| e.as_type().to_string());
    rt.store.finalize_settlement(submission, now, err_s).await?;
    Ok(match outcome {
        SettlementOutcome::Completed => ProcessOutcome::SettledCompleted,
        SettlementOutcome::Aborted => ProcessOutcome::SettledAborted,
        SettlementOutcome::Failed => ProcessOutcome::SettledFailed,
        SettlementOutcome::Suspended => ProcessOutcome::Suspended,
    })
}

pub async fn drain_pending_settlements(rt: &Runtime) -> Result<u32, RuntimeError> {
    let rows = rt.store.list_pending_settlements().await?;
    let now = UnixMillis::now_system();
    let mut n = 0u32;
    for row in rows {
        let Some(attempt) = row.attempt_id.clone() else {
            rt.store
                .finalize_settlement(
                    &row.submission_id,
                    now,
                    Some(SettlementError::SubmissionInterrupted.as_type().to_string()),
                )
                .await?;
            n = n.saturating_add(1);
            continue;
        };
        let records = rt.store.read_all(row.conversation_id.as_str()).await?;
        let already = records.iter().any(|r| {
            r.submission_id.as_ref() == Some(&row.submission_id)
                && matches!(r.body, RecordBody::SubmissionSettled { .. })
        });
        if already {
            rt.store
                .finalize_settlement(&row.submission_id, now, row.error.clone())
                .await?;
        } else {
            let class = classify(&ClassifierInput {
                submission_id: &row.submission_id,
                records: &records,
                abort_requested: row.abort_requested,
                attempts_exhausted: row.attempt_count >= row.max_attempts,
                deadline_passed: false,
                retry_count: 0,
            });
            let action = recover_action(
                &class,
                &ClassifierInput {
                    submission_id: &row.submission_id,
                    records: &records,
                    abort_requested: row.abort_requested,
                    attempts_exhausted: row.attempt_count >= row.max_attempts
                        && matches!(class, RecoveryClass::Absent | RecoveryClass::TerminalError),
                    deadline_passed: false,
                    retry_count: 0,
                },
            );
            let (outcome, error) = match action {
                RecoverAction::SettleCompleted => (SettlementOutcome::Completed, None),
                RecoverAction::SettleAborted => (
                    SettlementOutcome::Aborted,
                    Some(SettlementError::SubmissionAborted),
                ),
                RecoverAction::SettleFailed { error } => (SettlementOutcome::Failed, Some(error)),
                RecoverAction::ParkSuspended
                | RecoverAction::Requeue
                | RecoverAction::ReplaceAttemptAndResume => {
                    (SettlementOutcome::Completed, None)
                }
            };
            settle(
                rt,
                &row.conversation_id,
                &row.session_key,
                &row.submission_id,
                &attempt,
                outcome,
                error,
                now,
            )
            .await?;
        }
        n = n.saturating_add(1);
    }
    Ok(n)
}

async fn claim_aborted(rt: &Runtime, id: &SubmissionId) -> Result<bool, RuntimeError> {
    Ok(rt.store.get_submission(id).await?.abort_requested)
}

fn rebuild_messages(records: &[Record], current: &SubmissionId) -> Vec<Message> {
    let mut fold = FoldState::default();
    // Apply linearly; ignore invariant failures on a partial tail.
    let mut batch_recs = Vec::new();
    for rec in records {
        batch_recs.push(rec.clone());
    }
    let _ = oah_core::reduce_batch(
        &mut fold,
        &oah_core::RecordBatch {
            path: String::new(),
            seq: 1,
            records: batch_recs,
            submission_id: Some(current.clone()),
            attempt_id: None,
        },
    );
    let mut out = Vec::new();
    for msg in fold.messages {
        let role = if msg.role == "assistant" {
            MessageRole::Assistant
        } else {
            MessageRole::User
        };
        let mut content = Vec::new();
        for part in msg.parts {
            match part {
                oah_core::ProjectedPart::Text { text } => {
                    content.push(ContentBlock::Text { text });
                }
                oah_core::ProjectedPart::Reasoning { text, signature } => {
                    content.push(ContentBlock::Thinking { text, signature });
                }
                oah_core::ProjectedPart::DynamicTool {
                    tool_call_id,
                    tool_name,
                    input,
                    output,
                    state,
                    error_text,
                } => {
                    if role == MessageRole::Assistant {
                        content.push(ContentBlock::ToolUse {
                            id: tool_call_id,
                            name: tool_name,
                            input: input.unwrap_or(Value::Null),
                        });
                    } else {
                        content.push(ContentBlock::ToolResult {
                            tool_use_id: tool_call_id,
                            content: output.or_else(|| error_text.map(Value::String)),
                            is_error: state == "output-error",
                        });
                    }
                }
                oah_core::ProjectedPart::Data { .. } => {}
            }
        }
        if !content.is_empty() {
            out.push(Message { role, content });
        }
    }
    let _ = StreamOffset::ORIGIN;
    out
}

fn affinity_id(agent: &oah_core::AgentName, instance: &oah_core::InstanceId) -> String {
    format!("aff_{}_{}", agent.as_str(), instance.as_str())
}
