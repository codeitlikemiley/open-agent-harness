use crate::gates::ToolGate;
use crate::tools::ToolDef;
use crate::Props;
use oah_core::{AgentName, InstanceId, Principal};
use oah_model::ThinkingLevel;
use oah_skills::Skill;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{hook}#{index}: {message}")]
pub struct RenderError {
    pub hook: String,
    pub index: usize,
    pub message: String,
}

impl RenderError {
    pub fn new(hook: impl Into<String>, index: usize, message: impl Into<String>) -> Self {
        Self {
            hook: hook.into(),
            index,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Durability {
    pub max_attempts: u32,
    pub timeout_ms: i64,
}

impl Default for Durability {
    fn default() -> Self {
        Self {
            max_attempts: oah_core::DEFAULT_MAX_ATTEMPTS,
            timeout_ms: oah_core::DEFAULT_TIMEOUT_MS as i64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Instructions(pub String);

impl From<String> for Instructions {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Instructions {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

pub struct PrepareCx<'a> {
    pub id: &'a InstanceId,
    pub principal: &'a Principal,
}

pub trait Agent: Send + Sync + 'static {
    fn name(&self) -> &AgentName;
    fn description(&self) -> &str {
        ""
    }
    fn durability(&self) -> Durability {
        Durability::default()
    }
    fn prepare(&self, _cx: PrepareCx<'_>) -> Props {
        Props::default()
    }
    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError>;
}

pub type AgentBox = Arc<dyn Agent>;

pub struct RenderCx<'a> {
    id: &'a InstanceId,
    principal: &'a Principal,
    props: &'a Props,
    pub frame: Frame,
}

#[derive(Default)]
pub struct Frame {
    pub model: Option<ModelDecl>,
    pub sandbox: Option<SandboxDecl>,
    pub tools: Vec<ToolDef>,
    pub gates: Vec<Arc<dyn ToolGate>>,
    pub instructions: Vec<String>,
    pub state: BTreeMap<String, Value>,
    pub skills: Vec<Skill>,
    pub mcp_servers: Vec<String>,
    pub resources: Vec<ResourceDecl>,
    pub subagents: Vec<String>,
    pub pending_state: BTreeMap<String, Value>,
    pub pending_data: BTreeMap<String, Value>,
    pub pending_dispatches: Vec<DispatchDecl>,
    pub start_run: bool,
    pub finish_cycles: u32,
    pub schedules: Vec<String>,
    hook_index: usize,
}

#[derive(Debug, Clone)]
pub struct ResourceDecl {
    pub name: String,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct DispatchDecl {
    pub agent: String,
    pub instance: String,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct ModelDecl {
    pub spec: String,
    pub thinking: ThinkingLevel,
}

#[derive(Debug, Clone)]
pub struct SandboxDecl {
    pub name: String,
    pub cwd: String,
}

impl<'a> RenderCx<'a> {
    pub fn new(id: &'a InstanceId, principal: &'a Principal, props: &'a Props) -> Self {
        Self {
            id,
            principal,
            props,
            frame: Frame::default(),
        }
    }

    pub fn id(&self) -> &InstanceId {
        self.id
    }

    pub fn principal(&self) -> &Principal {
        self.principal
    }

    pub fn prop(&self, key: &str) -> Option<&Value> {
        self.props.values.get(key)
    }

    fn next_index(&mut self) -> usize {
        let i = self.frame.hook_index;
        self.frame.hook_index = self.frame.hook_index.saturating_add(1);
        i
    }

    pub fn use_model(&mut self, spec: impl Into<String>) -> Result<ModelDeclBuilder<'_>, RenderError> {
        let index = self.next_index();
        if self.frame.model.is_some() {
            return Err(RenderError::new(
                "use_model",
                index,
                "use_model may be called exactly once per render",
            ));
        }
        self.frame.model = Some(ModelDecl {
            spec: spec.into(),
            thinking: ThinkingLevel::Medium,
        });
        Ok(ModelDeclBuilder {
            frame: &mut self.frame,
        })
    }

    pub fn use_sandbox(&mut self, name: impl Into<String>) -> Result<SandboxDeclBuilder<'_>, RenderError> {
        let index = self.next_index();
        if self.frame.sandbox.is_some() {
            return Err(RenderError::new(
                "use_sandbox",
                index,
                "use_sandbox may be called at most once per render",
            ));
        }
        self.frame.sandbox = Some(SandboxDecl {
            name: name.into(),
            cwd: "/workspace".into(),
        });
        Ok(SandboxDeclBuilder {
            frame: &mut self.frame,
        })
    }

    pub fn use_tool(&mut self, tool: ToolDef) -> Result<(), RenderError> {
        let index = self.next_index();
        if self.frame.tools.iter().any(|t| t.name == tool.name) {
            return Err(RenderError::new(
                "use_tool",
                index,
                format!("duplicate tool name {}", tool.name),
            ));
        }
        self.frame.tools.push(tool);
        Ok(())
    }

    pub fn use_instruction(&mut self, text: impl Into<String>) {
        let _ = self.next_index();
        self.frame.instructions.push(text.into());
    }

    pub fn use_tool_gate(&mut self, gate: Arc<dyn ToolGate>) {
        let _ = self.next_index();
        self.frame.gates.push(gate);
    }

    pub fn use_persistent_state(&mut self, name: &str, default: Value) -> Value {
        let _ = self.next_index();
        self.frame
            .state
            .entry(name.to_string())
            .or_insert(default)
            .clone()
    }

    pub fn use_skill(&mut self, skill: Skill) -> Result<(), RenderError> {
        let index = self.next_index();
        if self.frame.skills.iter().any(|s| s.name == skill.name) {
            return Err(RenderError::new(
                "use_skill",
                index,
                format!("duplicate skill {}", skill.name),
            ));
        }
        self.frame.instructions.push(format!(
            "Skill {} ({}). Call activate_skill with name \"{}\" to load the full body.",
            skill.name, skill.description, skill.name
        ));
        self.frame.skills.push(skill);
        Ok(())
    }

    pub fn use_mcp(&mut self, server: impl Into<String>) {
        let _ = self.next_index();
        self.frame.mcp_servers.push(server.into());
    }

    pub fn use_subagent(&mut self, name: impl Into<String>) {
        let _ = self.next_index();
        self.frame.subagents.push(name.into());
    }

    pub fn use_resource(&mut self, name: impl Into<String>, body: impl Into<String>) {
        let _ = self.next_index();
        self.frame.resources.push(ResourceDecl {
            name: name.into(),
            body: body.into(),
        });
    }

    pub fn start_run(&mut self) {
        let _ = self.next_index();
        self.frame.start_run = true;
    }

    pub fn finish_cycle(&mut self) -> Result<u32, RenderError> {
        let index = self.next_index();
        if self.frame.finish_cycles >= oah_core::MAX_AGENT_FINISH_CYCLES {
            return Err(RenderError::new(
                "finish_cycle",
                index,
                "MAX_AGENT_FINISH_CYCLES exceeded",
            ));
        }
        self.frame.finish_cycles = self.frame.finish_cycles.saturating_add(1);
        Ok(self.frame.finish_cycles)
    }

    pub fn write_state(&mut self, name: impl Into<String>, value: Value) {
        let _ = self.next_index();
        self.frame.pending_state.insert(name.into(), value);
    }

    pub fn write_message_data(&mut self, name: impl Into<String>, value: Value) {
        let _ = self.next_index();
        self.frame.pending_data.insert(name.into(), value);
    }

    pub fn dispatch(
        &mut self,
        agent: impl Into<String>,
        instance: impl Into<String>,
        body: impl Into<String>,
    ) {
        let _ = self.next_index();
        self.frame.pending_dispatches.push(DispatchDecl {
            agent: agent.into(),
            instance: instance.into(),
            body: body.into(),
        });
    }

    pub fn use_schedule(&mut self, cron: impl Into<String>) {
        let _ = self.next_index();
        self.frame.schedules.push(cron.into());
    }

    pub fn join_instructions(&self, returned: Instructions) -> String {
        let mut parts = vec![returned.0];
        parts.extend(self.frame.instructions.iter().cloned());
        for resource in &self.frame.resources {
            parts.push(format!("# {}\n{}", resource.name, resource.body));
        }
        parts
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n")
    }
}

pub struct ModelDeclBuilder<'a> {
    frame: &'a mut Frame,
}

impl ModelDeclBuilder<'_> {
    pub fn thinking(self, level: ThinkingLevel) -> Self {
        if let Some(m) = &mut self.frame.model {
            m.thinking = level;
        }
        self
    }
}

pub struct SandboxDeclBuilder<'a> {
    frame: &'a mut Frame,
}

impl SandboxDeclBuilder<'_> {
    pub fn cwd(self, cwd: impl Into<String>) -> Self {
        if let Some(s) = &mut self.frame.sandbox {
            s.cwd = cwd.into();
        }
        self
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use oah_core::Principal;
    use serde_json::json;

    #[test]
    fn sixteen_hooks_compile_and_index() {
        let id = InstanceId::parse("t").unwrap();
        let principal = Principal::anonymous();
        let props = Props::default();
        let mut cx = RenderCx::new(&id, &principal, &props);
        cx.use_model("mock/scripted").unwrap();
        cx.use_sandbox("virtual").unwrap().cwd("/workspace");
        cx.use_tool(crate::tools::lookup_ticket_tool()).unwrap();
        cx.use_instruction("be brief");
        cx.use_tool_gate(std::sync::Arc::new(crate::gates::NamedDenyGate {
            name: "n".into(),
            deny: vec![],
            ask: vec![],
        }));
        cx.use_persistent_state("n", json!(0));
        cx.use_mcp("echo");
        cx.use_subagent("researcher");
        cx.use_resource("env", "linux");
        cx.start_run();
        cx.finish_cycle().unwrap();
        cx.write_state("n", json!(1));
        cx.write_message_data("note", json!("x"));
        cx.dispatch("support-desk", "child", "go");
        cx.use_schedule("0 0 * * *");
        let skill = oah_skills::parse_skill_md(
            "---\nname: demo\ndescription: d\n---\nbody {{id}}\n",
        )
        .unwrap();
        cx.use_skill(skill).unwrap();
        assert_eq!(cx.frame.hook_index, 16);
        assert!(cx.frame.start_run);
        assert_eq!(cx.frame.finish_cycles, 1);
        assert_eq!(cx.frame.mcp_servers.len(), 1);
        assert_eq!(cx.frame.pending_dispatches.len(), 1);
        assert_eq!(cx.frame.tools.len(), 1);
    }
}
