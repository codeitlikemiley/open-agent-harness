//! SKILL.md progressive disclosure. Tool name: `skill:<name>:<sha16>`.

#![forbid(unsafe_code)]

use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SkillError {
    #[error("{0}")]
    Parse(String),
    #[error("{0}")]
    Io(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct SkillFrontmatter {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub body: String,
    pub sha16: String,
}

impl Skill {
    /// `skill:<name>:<sha16>`. 16 hex chars of SHA-256 over canonical body.
    pub fn tool_name(&self) -> String {
        format!("skill:{}:{}", self.name, self.sha16)
    }

    pub fn substitute_id(&self, id: &str) -> String {
        self.body.replace("{{id}}", id)
    }

    /// Full body after activation (progressive disclosure).
    pub fn activate(&self, id: &str) -> String {
        format!(
            "# {}\n\n{}\n\n{}",
            self.name,
            self.description,
            self.substitute_id(id)
        )
    }
}

pub fn parse_skill_md(source: &str) -> Result<Skill, SkillError> {
    let (fm, body) = split_frontmatter(source)?;
    if fm.name.is_empty() || fm.name.len() > 40 {
        return Err(SkillError::Parse(
            "skill name must be 1..=40 characters".into(),
        ));
    }
    if !fm
        .name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(SkillError::Parse(
            "skill name must be alphanumeric / '-' / '_'".into(),
        ));
    }
    let body = body.trim().to_string();
    let sha16 = sha16_of(&body);
    Ok(Skill {
        name: fm.name,
        description: fm.description,
        body,
        sha16,
    })
}

pub fn sha16_of(body: &str) -> String {
    let digest = Sha256::digest(body.as_bytes());
    hex::encode(&digest[..8])
}

pub fn parse_skill_tool_name(raw: &str) -> Option<(String, String)> {
    let rest = raw.strip_prefix("skill:")?;
    let (name, sha) = rest.rsplit_once(':')?;
    if name.is_empty() || sha.len() != 16 {
        return None;
    }
    Some((name.to_string(), sha.to_string()))
}

pub fn load_dir(dir: &Path) -> Result<Vec<Skill>, SkillError> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    let entries = std::fs::read_dir(dir).map_err(|e| SkillError::Io(e.to_string()))?;
    for entry in entries {
        let entry = entry.map_err(|e| SkillError::Io(e.to_string()))?;
        let path = entry.path();
        if path.is_dir() {
            let skill_md = path.join("SKILL.md");
            if skill_md.exists() {
                out.push(from_file(&skill_md)?);
            }
            continue;
        }
        if path.file_name().and_then(|s| s.to_str()) == Some("SKILL.md")
            || path.extension().and_then(|s| s.to_str()) == Some("md")
        {
            out.push(from_file(&path)?);
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

pub fn from_file(path: &Path) -> Result<Skill, SkillError> {
    let src = std::fs::read_to_string(path).map_err(|e| SkillError::Io(e.to_string()))?;
    parse_skill_md(&src)
}

fn split_frontmatter(source: &str) -> Result<(SkillFrontmatter, String), SkillError> {
    let trimmed = source.trim_start();
    if !trimmed.starts_with("---") {
        return Err(SkillError::Parse("SKILL.md must start with YAML frontmatter".into()));
    }
    let rest = trimmed.strip_prefix("---").unwrap_or(trimmed);
    let rest = rest.strip_prefix('\n').unwrap_or(rest);
    let end = rest
        .find("\n---")
        .ok_or_else(|| SkillError::Parse("SKILL.md frontmatter is not closed".into()))?;
    let yaml = &rest[..end];
    let body = rest[end + 4..].trim_start_matches('\n').to_string();
    let fm: SkillFrontmatter =
        serde_yaml::from_str(yaml).map_err(|e| SkillError::Parse(e.to_string()))?;
    Ok((fm, body))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    const SAMPLE: &str = "---\nname: invoice-export\ndescription: Export failures\n---\nTicket {{id}} cannot export CSV.\n";

    #[test]
    fn parse_and_name() {
        let skill = parse_skill_md(SAMPLE).unwrap();
        assert_eq!(skill.name, "invoice-export");
        assert_eq!(skill.sha16.len(), 16);
        assert_eq!(
            skill.tool_name(),
            format!("skill:invoice-export:{}", skill.sha16)
        );
        assert!(parse_skill_tool_name(&skill.tool_name()).is_some());
        assert!(skill.substitute_id("42").contains("Ticket 42"));
        assert!(skill.activate("42").contains("# invoice-export"));
    }

    #[test]
    fn sha_is_stable() {
        let a = parse_skill_md(SAMPLE).unwrap();
        let b = parse_skill_md(SAMPLE).unwrap();
        assert_eq!(a.sha16, b.sha16);
    }
}
