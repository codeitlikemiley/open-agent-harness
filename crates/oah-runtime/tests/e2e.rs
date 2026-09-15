#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use oah_core::{AgentName, InstanceId, Principal};
use oah_model::{MockModel, ScriptedTurn};
use oah_runtime::agent::{Agent, Instructions, RenderCx, RenderError};
use oah_runtime::gates::{NamedDenyGate, ToolGate};
use oah_runtime::tools::{lookup_ticket_tool, refund_tool, ToolDef, ToolOutput};
use oah_runtime::{Coordinator, Runtime};
use oah_store::{CrashInjectStore, Store};
use oah_store_memory::MemoryStore;
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct Desk {
    name: AgentName,
}

impl Agent for Desk {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_tool(lookup_ticket_tool())?;
        Ok(Instructions("desk".into()))
    }
}

struct RefundDesk {
    name: AgentName,
}

impl Agent for RefundDesk {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_tool(refund_tool())?;
        cx.use_tool_gate(Arc::new(NamedDenyGate {
            name: "pay".into(),
            deny: vec![],
            ask: vec!["refund".into()],
        }) as Arc<dyn ToolGate>);
        Ok(Instructions("refund desk".into()))
    }
}

fn counter_tool(hits: Arc<AtomicU32>) -> ToolDef {
    ToolDef {
        name: "counter".into(),
        description: "Non-durable increment.".into(),
        input_schema: json!({"type":"object","properties":{}}),
        durable: false,
        run: Arc::new(move |_cx, _input| {
            let hits = hits.clone();
            Box::pin(async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(ToolOutput::json(json!({"n": 1})))
            })
        }),
    }
}

fn pay_tool(hits: Arc<AtomicU32>) -> ToolDef {
    ToolDef {
        name: "pay".into(),
        description: "Charge a card.".into(),
        input_schema: json!({"type":"object","properties":{}}),
        durable: true,
        run: Arc::new(move |_cx, _input| {
            let hits = hits.clone();
            Box::pin(async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(ToolOutput::json(json!({"charged": true})))
            })
        }),
    }
}

struct DenyPay {
    name: AgentName,
    hits: Arc<AtomicU32>,
}

impl Agent for DenyPay {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_tool(pay_tool(self.hits.clone()))?;
        cx.use_tool_gate(Arc::new(NamedDenyGate {
            name: "policy".into(),
            deny: vec!["pay".into()],
            ask: vec![],
        }) as Arc<dyn ToolGate>);
        Ok(Instructions("deny pay".into()))
    }
}

struct CounterAgent {
    name: AgentName,
    hits: Arc<AtomicU32>,
}

impl Agent for CounterAgent {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_tool(counter_tool(self.hits.clone()))?;
        Ok(Instructions("counter".into()))
    }
}

async fn runtime_with(
    store: Arc<dyn oah_store::Store>,
    model: MockModel,
    agent: Arc<dyn Agent>,
) -> Arc<Runtime> {
    Arc::new(
        Runtime::builder()
            .store(store)
            .model(Arc::new(model))
            .agent(agent)
            .demo(true)
            .build()
            .unwrap(),
    )
}

#[tokio::test]
async fn lookup_round_trip() {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::support_desk();
    let agent = Arc::new(Desk {
        name: AgentName::parse("support-desk").unwrap(),
    });
    let rt = runtime_with(store, model, agent).await;
    let receipt = rt
        .dispatch(
            &AgentName::parse("support-desk").unwrap(),
            &InstanceId::parse("t-e2e").unwrap(),
            "ticket 42 cannot export",
            Principal::anonymous(),
            None,
            None,
        )
        .await
        .unwrap();
    let coord = Coordinator::new(rt.clone());
    coord.tick(&CancellationToken::new()).await.unwrap();
    let row = rt.store.get_submission(&receipt.submission_id).await.unwrap();
    assert_eq!(row.status, oah_store::SubmissionStatus::Settled);
}

#[tokio::test]
async fn approval_resume_executes_stored_args() {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![
        ScriptedTurn::Tool {
            name: "refund".into(),
            arguments: json!({"ticket_id": "42", "amount": 12}),
            id: "call_refund1".into(),
        },
        ScriptedTurn::Text("refund issued".into()),
    ]);
    let agent = Arc::new(RefundDesk {
        name: AgentName::parse("support-desk").unwrap(),
    });
    let rt = runtime_with(store, model, agent).await;
    let agent_name = AgentName::parse("support-desk").unwrap();
    let instance = InstanceId::parse("refund-1").unwrap();
    rt.dispatch(
        &agent_name,
        &instance,
        "please refund 12",
        Principal::anonymous(),
        None,
        None,
    )
    .await
    .unwrap();
    let coord = Coordinator::new(rt.clone());
    coord.tick(&CancellationToken::new()).await.unwrap();
    let pending = rt
        .store
        .list_pending_approvals(&oah_core::ConversationId::new(&agent_name, &instance))
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].tool, "refund");
    rt.answer(&pending[0].submission_id, &pending[0].tool_call_id, true, "tester")
        .await
        .unwrap();
    coord.tick(&CancellationToken::new()).await.unwrap();
    let records = rt
        .store
        .read_all(oah_core::ConversationId::new(&agent_name, &instance).as_str())
        .await
        .unwrap();
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::ToolApprovalAnswered { approved: true, .. }
    )));
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::ToolOutcome { name, .. } if name == "refund"
    )));
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::AssistantTextDelta { text } if text.contains("refund issued")
    )));
}

#[tokio::test]
async fn denied_tool_does_not_execute() {
    let hits = Arc::new(AtomicU32::new(0));
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![
        ScriptedTurn::Tool {
            name: "pay".into(),
            arguments: json!({}),
            id: "call_pay1".into(),
        },
        ScriptedTurn::Text("cannot pay".into()),
    ]);
    let agent = Arc::new(DenyPay {
        name: AgentName::parse("support-desk").unwrap(),
        hits: hits.clone(),
    });
    let rt = runtime_with(store, model, agent).await;
    let agent_name = AgentName::parse("support-desk").unwrap();
    let instance = InstanceId::parse("deny-1").unwrap();
    let receipt = rt
        .dispatch(
            &agent_name,
            &instance,
            "charge the card",
            Principal::anonymous(),
            None,
            None,
        )
        .await
        .unwrap();
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    let row = rt.store.get_submission(&receipt.submission_id).await.unwrap();
    assert_eq!(row.status, oah_store::SubmissionStatus::Settled);
    let records = rt
        .store
        .read_all(oah_core::ConversationId::new(&agent_name, &instance).as_str())
        .await
        .unwrap();
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::ToolGateDecision { verdict, .. } if verdict == "deny"
    )));
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::ToolOutcome { name, is_error: true, .. } if name == "pay"
    )));
}

#[tokio::test]
async fn nondurable_runs_on_first_attempt() {
    let hits = Arc::new(AtomicU32::new(0));
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![
        ScriptedTurn::Tool {
            name: "counter".into(),
            arguments: json!({}),
            id: "call_c1".into(),
        },
        ScriptedTurn::Text("done".into()),
    ]);
    let agent = Arc::new(CounterAgent {
        name: AgentName::parse("support-desk").unwrap(),
        hits: hits.clone(),
    });
    let rt = runtime_with(store, model, agent).await;
    let receipt = rt
        .dispatch(
            &AgentName::parse("support-desk").unwrap(),
            &InstanceId::parse("first-run").unwrap(),
            "count",
            Principal::anonymous(),
            None,
            None,
        )
        .await
        .unwrap();
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let row = rt.store.get_submission(&receipt.submission_id).await.unwrap();
    assert_eq!(row.status, oah_store::SubmissionStatus::Settled);
    let records = rt
        .store
        .read_all("agents/support-desk/first-run")
        .await
        .unwrap();
    assert!(records.iter().any(|r| match &r.body {
        oah_core::RecordBody::ToolOutcome {
            name,
            is_error: false,
            output: Some(out),
            ..
        } if name == "counter" => out.get("n") == Some(&json!(1)),
        _ => false,
    }));
}

#[tokio::test]
async fn crash_injection_does_not_double_run_nondurable() {
    let hits = Arc::new(AtomicU32::new(0));
    let mut last_ok = 0u32;
    for fail_at in 1..=48 {
        hits.store(0, Ordering::SeqCst);
        let mem = Arc::new(MemoryStore::new());
        mem.migrate().await.unwrap();
        let crash = Arc::new(CrashInjectStore::new(mem.clone()));
        crash.fail_at(fail_at);
        let model = MockModel::scripted(vec![
            ScriptedTurn::Tool {
                name: "counter".into(),
                arguments: json!({}),
                id: "call_c1".into(),
            },
            ScriptedTurn::Text("done".into()),
        ]);
        let agent = Arc::new(CounterAgent {
            name: AgentName::parse("support-desk").unwrap(),
            hits: hits.clone(),
        });
        let rt = runtime_with(crash.clone(), model, agent).await;
        let _ = rt
            .dispatch(
                &AgentName::parse("support-desk").unwrap(),
                &InstanceId::parse(format!("c{fail_at}")).unwrap(),
                "count",
                Principal::anonymous(),
                None,
                None,
            )
            .await;
        let coord = Coordinator::new(rt.clone());
        let _ = coord.tick(&CancellationToken::new()).await;
        crash.disable();
        let _ = coord.tick(&CancellationToken::new()).await;
        let n = hits.load(Ordering::SeqCst);
        assert!(n <= 1, "fail_at={fail_at} ran counter {n} times");
        if n == 1 {
            last_ok = n;
        }
    }
    let _ = last_ok;
}

#[tokio::test]
async fn crash_injection_thousand_passes() {
    let hits = Arc::new(AtomicU32::new(0));
    for i in 1..=1000 {
        hits.store(0, Ordering::SeqCst);
        let mem = Arc::new(MemoryStore::new());
        mem.migrate().await.unwrap();
        let crash = Arc::new(CrashInjectStore::new(mem.clone()));
        crash.fail_at(((i as u64) % 12) + 1);
        let model = MockModel::scripted(vec![
            ScriptedTurn::Tool {
                name: "counter".into(),
                arguments: json!({}),
                id: "call_c1".into(),
            },
            ScriptedTurn::Text("done".into()),
        ]);
        let agent = Arc::new(CounterAgent {
            name: AgentName::parse("support-desk").unwrap(),
            hits: hits.clone(),
        });
        let rt = runtime_with(crash.clone(), model, agent).await;
        let _ = rt
            .dispatch(
                &AgentName::parse("support-desk").unwrap(),
                &InstanceId::parse("loop").unwrap(),
                "count",
                Principal::anonymous(),
                Some(format!("k{i}")),
                None,
            )
            .await;
        let coord = Coordinator::new(rt.clone());
        let _ = coord.tick(&CancellationToken::new()).await;
        crash.disable();
        let _ = coord.tick(&CancellationToken::new()).await;
        assert!(hits.load(Ordering::SeqCst) <= 1);
    }
}

struct TaskDesk {
    name: AgentName,
}

impl Agent for TaskDesk {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_subagent("researcher");
        Ok(Instructions("delegate research".into()))
    }
}

#[tokio::test]
async fn task_admits_child_session() {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![
        ScriptedTurn::Tool {
            name: "task".into(),
            arguments: json!({"prompt": "summarize ticket 42"}),
            id: "call_task1".into(),
        },
        ScriptedTurn::Text("parent queued the researcher".into()),
        ScriptedTurn::Text("child finished".into()),
    ]);
    let agent = Arc::new(TaskDesk {
        name: AgentName::parse("support-desk").unwrap(),
    });
    let rt = runtime_with(store, model, agent).await;
    let agent_name = AgentName::parse("support-desk").unwrap();
    let instance = InstanceId::parse("task-1").unwrap();
    rt.dispatch(
        &agent_name,
        &instance,
        "research this",
        Principal::anonymous(),
        None,
        None,
    )
    .await
    .unwrap();
    let coord = Coordinator::new(rt.clone());
    coord.tick(&CancellationToken::new()).await.unwrap();
    let cid = oah_core::ConversationId::new(&agent_name, &instance);
    let records = rt.store.read_all(cid.as_str()).await.unwrap();
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::ChildSessionRetained { .. }
    )));
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::ToolOutcome { name, .. } if name == "task"
    )));
    let children: Vec<_> = records
        .iter()
        .filter_map(|r| match &r.body {
            oah_core::RecordBody::ChildSessionRetained { child_session } => {
                Some(child_session.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(children.len(), 1);
    assert!(children[0].starts_with("task:"));
    let rows_have_child = records.iter().any(|r| r.session.starts_with("task:"));
    assert!(rows_have_child);
    let mut fold = oah_core::FoldState::default();
    oah_core::reduce_batch(
        &mut fold,
        &oah_core::RecordBatch {
            path: cid.to_string(),
            seq: 1,
            records: records.clone(),
            submission_id: None,
            attempt_id: None,
        },
    )
    .unwrap();
}

#[tokio::test]
async fn terminalizing_head_recovers_on_tick() {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![ScriptedTurn::Text("ok".into())]);
    let agent = Arc::new(Desk {
        name: AgentName::parse("support-desk").unwrap(),
    });
    let rt = runtime_with(store, model, agent).await;
    let agent_name = AgentName::parse("support-desk").unwrap();
    let instance = InstanceId::parse("term-1").unwrap();
    let first = rt
        .dispatch(
            &agent_name,
            &instance,
            "hello",
            Principal::anonymous(),
            None,
            None,
        )
        .await
        .unwrap();
    let owner = rt.owner.clone();
    let claim = rt
        .store
        .claim_runnable(&owner, oah_core::UnixMillis::now_system(), 30_000)
        .await
        .unwrap()
        .unwrap();
    rt.store
        .reserve_settlement(&claim.row.submission_id)
        .await
        .unwrap();
    let stuck = rt.store.get_submission(&first.submission_id).await.unwrap();
    assert_eq!(stuck.status, oah_store::SubmissionStatus::Terminalizing);
    let second = rt
        .dispatch(
            &agent_name,
            &instance,
            "follow up",
            Principal::anonymous(),
            None,
            None,
        )
        .await
        .unwrap();
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    let first_row = rt.store.get_submission(&first.submission_id).await.unwrap();
    assert_eq!(first_row.status, oah_store::SubmissionStatus::Settled);
    assert!(first_row.error.is_none());
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    let second_row = rt.store.get_submission(&second.submission_id).await.unwrap();
    assert_eq!(second_row.status, oah_store::SubmissionStatus::Settled);
}

#[tokio::test]
async fn terminalizing_abort_drains_as_aborted() {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![ScriptedTurn::Text("ok".into())]);
    let agent = Arc::new(Desk {
        name: AgentName::parse("support-desk").unwrap(),
    });
    let rt = runtime_with(store, model, agent).await;
    let agent_name = AgentName::parse("support-desk").unwrap();
    let instance = InstanceId::parse("term-abort").unwrap();
    let receipt = rt
        .dispatch(
            &agent_name,
            &instance,
            "hello",
            Principal::anonymous(),
            None,
            None,
        )
        .await
        .unwrap();
    rt.abort(&agent_name, &instance).await.unwrap();
    let owner = rt.owner.clone();
    let claim = rt
        .store
        .claim_runnable(&owner, oah_core::UnixMillis::now_system(), 30_000)
        .await
        .unwrap()
        .unwrap();
    rt.store
        .reserve_settlement(&claim.row.submission_id)
        .await
        .unwrap();
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    let row = rt.store.get_submission(&receipt.submission_id).await.unwrap();
    assert_eq!(row.status, oah_store::SubmissionStatus::Settled);
    assert_eq!(row.error.as_deref(), Some("submission_aborted"));
    let records = rt
        .store
        .read_all(oah_core::ConversationId::new(&agent_name, &instance).as_str())
        .await
        .unwrap();
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::SubmissionSettled {
            outcome: oah_core::SettlementOutcome::Aborted,
            ..
        }
    )));
}

#[tokio::test]
async fn vendor_tool_call_id_is_preserved() {
    let hits = Arc::new(AtomicU32::new(0));
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![
        ScriptedTurn::Tool {
            name: "counter".into(),
            arguments: json!({}),
            id: "toolu_abc123".into(),
        },
        ScriptedTurn::Text("done".into()),
    ]);
    let agent = Arc::new(CounterAgent {
        name: AgentName::parse("support-desk").unwrap(),
        hits: hits.clone(),
    });
    let rt = runtime_with(store, model, agent).await;
    rt.dispatch(
        &AgentName::parse("support-desk").unwrap(),
        &InstanceId::parse("vendor-id").unwrap(),
        "count",
        Principal::anonymous(),
        None,
        None,
    )
    .await
    .unwrap();
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let records = rt
        .store
        .read_all("agents/support-desk/vendor-id")
        .await
        .unwrap();
    let proposed = records.iter().find_map(|r| match &r.body {
        oah_core::RecordBody::AssistantToolCall { tool_call_id, name, .. }
            if name == "counter" =>
        {
            Some(tool_call_id.as_str().to_string())
        }
        _ => None,
    });
    let outcome = records.iter().find_map(|r| match &r.body {
        oah_core::RecordBody::ToolOutcome { tool_call_id, name, .. } if name == "counter" => {
            Some(tool_call_id.as_str().to_string())
        }
        _ => None,
    });
    assert_eq!(proposed.as_deref(), Some("toolu_abc123"));
    assert_eq!(outcome.as_deref(), Some("toolu_abc123"));
}

struct McpDesk {
    name: AgentName,
}

impl Agent for McpDesk {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_mcp("echo");
        Ok(Instructions("mcp desk".into()))
    }
}

#[tokio::test]
async fn mcp_use_exposes_bridged_tool() {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let mut mcp = oah_mcp::InProcessMcp::new(oah_mcp::McpAllowlist::new([
        oah_mcp::mcp_tool_name("echo", "ping"),
    ]));
    mcp.register("echo", "ping", |args| {
        Ok(json!({"content":[{"type":"text","text": args.to_string()}]}))
    });
    let model = MockModel::scripted(vec![
        ScriptedTurn::Tool {
            name: "mcp__echo__ping".into(),
            arguments: json!({"hello": "oah"}),
            id: "call_mcp1".into(),
        },
        ScriptedTurn::Text("pong".into()),
    ]);
    let rt = Arc::new(
        Runtime::builder()
            .store(store)
            .model(Arc::new(model))
            .mcp(Arc::new(mcp))
            .agent(Arc::new(McpDesk {
                name: AgentName::parse("support-desk").unwrap(),
            }))
            .demo(true)
            .build()
            .unwrap(),
    );
    let agent_name = AgentName::parse("support-desk").unwrap();
    let instance = InstanceId::parse("mcp-1").unwrap();
    rt.dispatch(
        &agent_name,
        &instance,
        "ping",
        Principal::anonymous(),
        None,
        None,
    )
    .await
    .unwrap();
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    let records = rt
        .store
        .read_all(oah_core::ConversationId::new(&agent_name, &instance).as_str())
        .await
        .unwrap();
    assert!(records.iter().any(|r| match &r.body {
        oah_core::RecordBody::ToolOutcome {
            name,
            is_error: false,
            output: Some(out),
            ..
        } if name == "mcp__echo__ping" => out.to_string().contains("hello"),
        _ => false,
    }));
}

struct SandboxDesk {
    name: AgentName,
}

impl Agent for SandboxDesk {
    fn name(&self) -> &AgentName {
        &self.name
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_sandbox("virtual")?;
        Ok(Instructions("file tools".into()))
    }
}

#[tokio::test]
async fn sandbox_write_then_read() {
    let store = Arc::new(MemoryStore::new());
    store.migrate().await.unwrap();
    let model = MockModel::scripted(vec![
        ScriptedTurn::Tool {
            name: "write".into(),
            arguments: json!({"path": "/workspace/note.txt", "contents": "hello-oah"}),
            id: "call_write1".into(),
        },
        ScriptedTurn::Tool {
            name: "read".into(),
            arguments: json!({"path": "/workspace/note.txt"}),
            id: "call_read1".into(),
        },
        ScriptedTurn::Text("read back hello-oah".into()),
    ]);
    let agent = Arc::new(SandboxDesk {
        name: AgentName::parse("support-desk").unwrap(),
    });
    let rt = runtime_with(store, model, agent).await;
    let agent_name = AgentName::parse("support-desk").unwrap();
    let instance = InstanceId::parse("sbx-1").unwrap();
    rt.dispatch(
        &agent_name,
        &instance,
        "write a note",
        Principal::anonymous(),
        None,
        None,
    )
    .await
    .unwrap();
    Coordinator::new(rt.clone())
        .tick(&CancellationToken::new())
        .await
        .unwrap();
    let records = rt
        .store
        .read_all(oah_core::ConversationId::new(&agent_name, &instance).as_str())
        .await
        .unwrap();
    assert!(records.iter().any(|r| matches!(
        &r.body,
        oah_core::RecordBody::ToolOutcome { name, is_error: false, .. } if name == "write"
    )));
    assert!(records.iter().any(|r| match &r.body {
        oah_core::RecordBody::ToolOutcome {
            name,
            output: Some(out),
            is_error: false,
            ..
        } if name == "read" => out.as_str() == Some("hello-oah"),
        _ => false,
    }));
}
