use crate::agent::{Agent, Instructions, RenderCx, RenderError};
use crate::tools::{lookup_ticket_tool, refund_tool, step_run_tool, task_tool};
use oah_core::AgentName;
use oah_model::ThinkingLevel;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct AgentFrontmatter {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub thinking: Option<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub skills: Vec<String>,
}

pub struct MarkdownAgent {
    name: AgentName,
    description: String,
    model: String,
    thinking: ThinkingLevel,
    tools: Vec<String>,
    skills: Vec<String>,
    body: String,
}

impl MarkdownAgent {
    pub fn parse(source: &str) -> Result<Self, String> {
        let (fm, body) = split_frontmatter(source)?;
        let name = AgentName::parse(&fm.name).map_err(|e| e.to_string())?;
        let thinking = fm
            .thinking
            .as_deref()
            .and_then(ThinkingLevel::parse)
            .unwrap_or(ThinkingLevel::Off);
        Ok(Self {
            name,
            description: fm.description,
            model: fm.model.unwrap_or_else(|| "mock/scripted".into()),
            thinking,
            tools: fm.tools,
            skills: fm.skills,
            body: body.trim().to_string(),
        })
    }

    pub fn from_file(path: &Path) -> Result<Self, String> {
        let src = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        Self::parse(&src)
    }

    pub fn load_dir(dir: &Path) -> Result<Vec<Self>, String> {
        let mut out = Vec::new();
        if !dir.exists() {
            return Ok(out);
        }
        let entries = std::fs::read_dir(dir).map_err(|e| e.to_string())?;
        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("md") {
                out.push(Self::from_file(&path)?);
            }
        }
        Ok(out)
    }
}

impl Agent for MarkdownAgent {
    fn name(&self) -> &AgentName {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model(&self.model)?.thinking(self.thinking);
        for tool in &self.tools {
            match tool.as_str() {
                "lookup_ticket" => cx.use_tool(lookup_ticket_tool())?,
                "refund" => cx.use_tool(refund_tool())?,
                "task" => cx.use_tool(task_tool())?,
                "step" => cx.use_tool(step_run_tool())?,
                _ => {}
            }
        }
        if !self.skills.is_empty() {
            if let Ok(all) = oah_skills::load_dir(std::path::Path::new("skills")) {
                for skill in all {
                    if self.skills.iter().any(|n| n == &skill.name) {
                        cx.use_skill(skill)?;
                    }
                }
            }
        }
        let body = self.body.replace("{{id}}", cx.id().as_str());
        Ok(Instructions(body))
    }
}

fn split_frontmatter(source: &str) -> Result<(AgentFrontmatter, String), String> {
    let trimmed = source.trim_start();
    if !trimmed.starts_with("---") {
        return Err("AGENT.md must start with YAML frontmatter".into());
    }
    let rest = trimmed.strip_prefix("---").unwrap_or(trimmed);
    let rest = rest.strip_prefix('\n').unwrap_or(rest);
    let end = rest
        .find("\n---")
        .ok_or_else(|| "AGENT.md frontmatter is not closed".to_string())?;
    let yaml = &rest[..end];
    let body = rest[end + 4..].trim_start_matches('\n').to_string();
    let fm: AgentFrontmatter = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
    Ok((fm, body))
}
