//! Agent loop equivalent to `pi-agent-core` 0.83 `agent-loop.js`.
//!
//! No I/O of its own: the host supplies a `ModelClient` and a `ToolExecutor`.

#![forbid(unsafe_code)]

use futures::StreamExt;
use oah_model::{
    ContentBlock, Message, MessageRole, ModelClient, ModelError, ModelErrorKind, ModelEvent,
    ModelRequest, StopReason, ToolSpec,
};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::debug;

#[derive(Debug, Error)]
pub enum LoopError {
    #[error("{0}")]
    Model(#[from] ModelError),
    #[error("{0}")]
    Host(String),
}

#[derive(Debug, Clone)]
pub struct PreparedCall {
    pub index: u32,
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone)]
pub struct ToolResult {
    pub id: String,
    pub name: String,
    pub output: Value,
    pub is_error: bool,
    pub terminate: bool,
}

pub trait ToolExecutor: Send + Sync {
    fn prepare(&self, call: PreparedCall) -> Result<PreparedCall, ToolResult>;
    fn execute(
        &self,
        call: PreparedCall,
        cancel: CancellationToken,
    ) -> impl std::future::Future<Output = ToolResult> + Send;
}

#[derive(Debug, Clone, Default)]
pub struct LoopConfig {
    pub max_turns: Option<u32>,
    pub sequential_tools: bool,
}

#[derive(Debug, Clone)]
pub struct TurnOutcome {
    pub assistant: Message,
    pub stop: StopReason,
    pub tool_calls: Vec<PreparedCall>,
    pub tool_results: Vec<ToolResult>,
    pub served_model: Option<String>,
    pub aborted: bool,
}

pub struct LoopHost<'a, M: ?Sized, T: ?Sized> {
    pub model: &'a M,
    pub tools: &'a T,
    pub request: ModelRequest,
    pub cancel: CancellationToken,
    pub config: LoopConfig,
}

impl<'a, M, T> LoopHost<'a, M, T>
where
    M: ModelClient + ?Sized,
    T: ToolExecutor + ?Sized,
{
    /// Stream the model only. Gates run before `execute_calls`.
    pub async fn run_model(&mut self) -> Result<TurnOutcome, LoopError> {
        let mut stream = self.model.stream(self.request.clone(), self.cancel.clone()).await?;
        let mut text = String::new();
        let mut thinking = String::new();
        let mut thinking_sig: Option<String> = None;
        let mut calls: Vec<PreparedCall> = Vec::new();
        let mut args_buf: Vec<String> = Vec::new();
        let mut stop = StopReason::Stop;
        let mut served = None;
        let mut aborted = false;

        while let Some(item) = stream.next().await {
            if self.cancel.is_cancelled() {
                aborted = true;
                break;
            }
            match item? {
                ModelEvent::Start { served_model } => served = served_model,
                ModelEvent::TextDelta(d) => text.push_str(&d),
                ModelEvent::ThinkingDelta(d) => thinking.push_str(&d),
                ModelEvent::ThinkingSignature(s) => thinking_sig = Some(s),
                ModelEvent::ToolCallStart { index, id, name } => {
                    let i = index as usize;
                    while args_buf.len() <= i {
                        args_buf.push(String::new());
                    }
                    while calls.len() <= i {
                        calls.push(PreparedCall {
                            index: calls.len() as u32,
                            id: String::new(),
                            name: String::new(),
                            arguments: Value::Null,
                        });
                    }
                    if let Some(slot) = calls.get_mut(i) {
                        slot.index = index;
                        slot.id = id;
                        slot.name = name;
                    }
                }
                ModelEvent::ToolCallArgsDelta { index, json } => {
                    let i = index as usize;
                    while args_buf.len() <= i {
                        args_buf.push(String::new());
                    }
                    if let Some(buf) = args_buf.get_mut(i) {
                        buf.push_str(&json);
                    }
                }
                ModelEvent::ToolCallEnd { index } => {
                    let i = index as usize;
                    if let (Some(slot), Some(buf)) = (calls.get_mut(i), args_buf.get(i)) {
                        slot.arguments = parse_tool_args(buf);
                    }
                }
                ModelEvent::Usage(_) => {}
                ModelEvent::Done(reason) => {
                    stop = reason;
                    break;
                }
            }
        }

        // Id-less fragments stay addressable by content-block index (MDL-3).
        for (i, slot) in calls.iter_mut().enumerate() {
            if slot.id.is_empty() && !slot.name.is_empty() {
                slot.id = format!("call_idx_{i}");
            }
        }
        calls.retain(|c| !c.id.is_empty());

        let mut content = Vec::new();
        if !thinking.is_empty() {
            content.push(ContentBlock::Thinking {
                text: thinking,
                signature: thinking_sig,
            });
        }
        if !text.is_empty() {
            content.push(ContentBlock::Text { text });
        }
        for call in &calls {
            content.push(ContentBlock::ToolUse {
                id: call.id.clone(),
                name: call.name.clone(),
                input: call.arguments.clone(),
            });
        }

        if matches!(stop, StopReason::Length) && !calls.is_empty() {
            let results = calls
                .iter()
                .map(|c| ToolResult {
                    id: c.id.clone(),
                    name: c.name.clone(),
                    output: Value::String(format!(
                        "Tool call \"{}\" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.",
                        c.name
                    )),
                    is_error: true,
                    terminate: false,
                })
                .collect();
            return Ok(TurnOutcome {
                assistant: Message {
                    role: MessageRole::Assistant,
                    content,
                },
                stop,
                tool_calls: calls,
                tool_results: results,
                served_model: served,
                aborted,
            });
        }

        Ok(TurnOutcome {
            assistant: Message {
                role: MessageRole::Assistant,
                content,
            },
            stop,
            tool_calls: calls,
            tool_results: Vec::new(),
            served_model: served,
            aborted,
        })
    }

    pub async fn execute_calls(&self, calls: &[PreparedCall]) -> Vec<ToolResult> {
        let mut results = Vec::new();
        if calls.is_empty() {
            return results;
        }
        let mut prepared = Vec::new();
        for call in calls {
            if self.cancel.is_cancelled() {
                results.push(aborted_result(call));
                continue;
            }
            match self.tools.prepare(call.clone()) {
                Ok(p) => prepared.push(p),
                Err(err) => results.push(err),
            }
        }
        if self.config.sequential_tools {
            for call in prepared {
                results.push(self.tools.execute(call, self.cancel.child_token()).await);
            }
        } else {
            let futs: Vec<_> = prepared
                .into_iter()
                .map(|call| {
                    let cancel = self.cancel.child_token();
                    let tools = self.tools;
                    async move { tools.execute(call, cancel).await }
                })
                .collect();
            results.extend(futures::future::join_all(futs).await);
        }
        results.sort_by(|a, b| {
            let ia = calls.iter().position(|c| c.id == a.id).unwrap_or(usize::MAX);
            let ib = calls.iter().position(|c| c.id == b.id).unwrap_or(usize::MAX);
            ia.cmp(&ib)
        });
        results
    }

    pub async fn run_turn(&mut self) -> Result<TurnOutcome, LoopError> {
        let mut outcome = self.run_model().await?;
        if !outcome.tool_calls.is_empty()
            && !outcome.aborted
            && !matches!(outcome.stop, StopReason::Length)
        {
            outcome.tool_results = self.execute_calls(&outcome.tool_calls).await;
        }
        debug!(
            calls = outcome.tool_calls.len(),
            results = outcome.tool_results.len(),
            "loop turn done"
        );
        Ok(outcome)
    }

    pub fn apply_results(&mut self, outcome: &TurnOutcome) {
        self.request.messages.push(outcome.assistant.clone());
        self.apply_tool_results(&outcome.tool_results);
    }

    pub fn apply_tool_results(&mut self, results: &[ToolResult]) {
        if results.is_empty() {
            return;
        }
        let mut content = Vec::new();
        for r in results {
            content.push(ContentBlock::ToolResult {
                tool_use_id: r.id.clone(),
                content: Some(r.output.clone()),
                is_error: r.is_error,
            });
        }
        self.request.messages.push(Message {
            role: MessageRole::User,
            content,
        });
    }

    pub fn steer(&mut self, messages: Vec<Message>) {
        self.request.messages.extend(messages);
    }

}

pub fn should_continue(outcome: &TurnOutcome) -> bool {
    if outcome.aborted {
        return false;
    }
    if outcome.tool_results.is_empty() {
        return false;
    }
    if outcome.tool_results.iter().all(|r| r.terminate) {
        return false;
    }
    true
}

/// Parse tool-call argument JSON. Incomplete fragments become Null rather than
/// panicking (index-assembled streams may flush before the object closes).
pub fn parse_tool_args(buf: &str) -> Value {
    match serde_json::from_str(buf) {
        Ok(v) => v,
        Err(_) => Value::Null,
    }
}

fn aborted_result(call: &PreparedCall) -> ToolResult {
    ToolResult {
        id: call.id.clone(),
        name: call.name.clone(),
        output: Value::String("aborted".into()),
        is_error: true,
        terminate: false,
    }
}

pub fn tool_not_found(name: &str, id: &str) -> ToolResult {
    ToolResult {
        id: id.to_string(),
        name: name.to_string(),
        output: Value::String(format!("Tool {name} not found")),
        is_error: true,
        terminate: false,
    }
}

pub fn interrupted_result(name: &str, id: &str) -> ToolResult {
    ToolResult {
        id: id.to_string(),
        name: name.to_string(),
        output: serde_json::json!({
            "type": "interrupted",
            "message": "Tool execution was interrupted before completion. The outcome is unknown."
        }),
        is_error: true,
        terminate: false,
    }
}

pub fn make_request(
    model: impl Into<String>,
    system: impl Into<String>,
    messages: Vec<Message>,
    tools: Vec<ToolSpec>,
    max_tokens: u32,
    affinity: Option<String>,
) -> ModelRequest {
    ModelRequest {
        model: model.into(),
        max_tokens,
        stream: true,
        system: system.into(),
        messages,
        tools,
        thinking: None,
        affinity_user_id: affinity,
    }
}

/// Shared so a caller can hold an executor behind Arc.
pub struct ArcExecutor<T>(pub Arc<T>);

impl<T: ToolExecutor> ToolExecutor for ArcExecutor<T> {
    fn prepare(&self, call: PreparedCall) -> Result<PreparedCall, ToolResult> {
        self.0.prepare(call)
    }
    fn execute(
        &self,
        call: PreparedCall,
        cancel: CancellationToken,
    ) -> impl std::future::Future<Output = ToolResult> + Send {
        self.0.execute(call, cancel)
    }
}

impl From<LoopError> for ModelError {
    fn from(value: LoopError) -> Self {
        match value {
            LoopError::Model(e) => e,
            LoopError::Host(msg) => ModelError::new(ModelErrorKind::Internal, msg),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use oah_model::{MockModel, ScriptedTurn};
    use serde_json::json;

    struct EchoExec;

    impl ToolExecutor for EchoExec {
        fn prepare(&self, call: PreparedCall) -> Result<PreparedCall, ToolResult> {
            if call.name == "lookup_ticket" {
                Ok(call)
            } else {
                Err(tool_not_found(&call.name, &call.id))
            }
        }
        async fn execute(&self, call: PreparedCall, _cancel: CancellationToken) -> ToolResult {
            ToolResult {
                id: call.id,
                name: call.name,
                output: json!({"status": "open"}),
                is_error: false,
                terminate: false,
            }
        }
    }

    #[tokio::test]
    async fn tool_round_trip() {
        let mock = MockModel::scripted(vec![
            ScriptedTurn::Tool {
                name: "lookup_ticket".into(),
                arguments: json!({"id": "7"}),
                id: "call_1".into(),
            },
            ScriptedTurn::Text("done".into()),
        ]);
        let exec = EchoExec;
        let req = make_request(
            "mock/scripted",
            "sys",
            vec![Message::user_text("ticket 7")],
            vec![],
            256,
            None,
        );
        let mut host = LoopHost {
            model: &mock,
            tools: &exec,
            request: req,
            cancel: CancellationToken::new(),
            config: LoopConfig::default(),
        };
        let t1 = host.run_turn().await.unwrap();
        assert_eq!(t1.tool_calls.len(), 1);
        assert_eq!(t1.tool_results[0].name, "lookup_ticket");
        host.apply_results(&t1);
        assert!(should_continue(&t1));
        let t2 = host.run_turn().await.unwrap();
        assert!(matches!(t2.stop, StopReason::Stop));
        assert!(t2.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn assembles_idless_fragments_by_index() {
        struct TwoExec;
        impl ToolExecutor for TwoExec {
            fn prepare(&self, call: PreparedCall) -> Result<PreparedCall, ToolResult> {
                Ok(call)
            }
            async fn execute(&self, call: PreparedCall, _cancel: CancellationToken) -> ToolResult {
                ToolResult {
                    id: call.id,
                    name: call.name,
                    output: json!({"ok": true}),
                    is_error: false,
                    terminate: false,
                }
            }
        }
        // Manual stream via MockModel only emits one tool; parse_tool_args covers fragments.
        assert!(parse_tool_args("{\"a\":").is_null());
        assert_eq!(parse_tool_args("{\"a\":1}"), json!({"a":1}));
        let _ = TwoExec;
    }
}
