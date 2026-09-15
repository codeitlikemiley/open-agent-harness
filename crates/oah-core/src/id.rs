use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use ulid::Ulid;

macro_rules! prefixed_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new() -> Self {
                Self(format!(concat!($prefix, "{}"), Ulid::new()))
            }

            pub fn from_ulid(ulid: Ulid) -> Self {
                Self(format!(concat!($prefix, "{}"), ulid))
            }

            pub fn parse(raw: impl Into<String>) -> Result<Self> {
                let raw = raw.into();
                if raw.starts_with($prefix) && raw.len() > $prefix.len() {
                    Ok(Self(raw))
                } else {
                    Err(CoreError::InvalidId(format!(
                        "expected {}\u2026, got {raw}",
                        $prefix
                    )))
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

prefixed_id!(SubmissionId, "sub_");
prefixed_id!(AttemptId, "att_");
prefixed_id!(RecordId, "rec_");
prefixed_id!(TurnId, "trn_");
prefixed_id!(OperationId, "op_");
prefixed_id!(OwnerId, "own_");
prefixed_id!(AttachmentId, "attch_");
prefixed_id!(ToolCallId, "call_");
prefixed_id!(ScheduleId, "sch_");

impl ToolCallId {
    pub fn from_model_id(raw: impl Into<String>) -> Self {
        let raw = raw.into();
        if raw.is_empty() {
            return Self::new();
        }
        match Self::parse(raw.clone()) {
            Ok(id) => id,
            Err(_) => Self(raw),
        }
    }
}

impl SubmissionId {
    /// Idempotency id: `sub_ik_` + first 32 hex chars of SHA-256 over a
    /// domain-separated preimage (agent, instance, key).
    pub fn from_idempotency_key(agent: &AgentName, instance: &InstanceId, key: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"oah:idempotency:v1:");
        hasher.update(agent.as_str().as_bytes());
        hasher.update(0u8.to_be_bytes());
        hasher.update(instance.as_str().as_bytes());
        hasher.update(0u8.to_be_bytes());
        hasher.update(key.as_bytes());
        let digest = hasher.finalize();
        let hex = hex::encode(&digest[..16]);
        Self(format!("sub_ik_{hex}"))
    }
}

/// Durable agent identity. Must match `^[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*$`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentName(String);

fn is_valid_agent_name(raw: &str) -> bool {
    let mut chars = raw.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    let bytes = chars.as_str().as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_alphanumeric() {
        i += 1;
    }
    while i < bytes.len() {
        if bytes[i] != b'-' {
            return false;
        }
        i += 1;
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphanumeric() {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    true
}

impl AgentName {
    pub fn parse(raw: impl AsRef<str>) -> Result<Self> {
        let raw = raw.as_ref();
        if is_valid_agent_name(raw) {
            Ok(Self(raw.to_string()))
        } else {
            Err(CoreError::InvalidAgentName(raw.to_string()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AgentName {
    type Error = CoreError;
    fn try_from(value: String) -> Result<Self> {
        Self::parse(value)
    }
}

impl From<AgentName> for String {
    fn from(value: AgentName) -> Self {
        value.0
    }
}

impl fmt::Display for AgentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for AgentName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Conversation instance id (the `<id>` in `agents/<name>/<id>`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstanceId(String);

impl InstanceId {
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        if raw.is_empty() || raw.contains('/') || raw.contains(':') {
            return Err(CoreError::InvalidId(format!(
                "instance id must be a non-empty path segment, got {raw}"
            )));
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for InstanceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Storage path `agents/<identity>/<id>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConversationId(String);

impl ConversationId {
    pub fn new(agent: &AgentName, instance: &InstanceId) -> Self {
        Self(format!("agents/{agent}/{instance}"))
    }

    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        let rest = raw
            .strip_prefix("agents/")
            .ok_or_else(|| CoreError::InvalidId(format!("conversation id must start with agents/, got {raw}")))?;
        let (agent, instance) = rest.split_once('/').ok_or_else(|| {
            CoreError::InvalidId(format!("conversation id must be agents/<name>/<id>, got {raw}"))
        })?;
        let agent = AgentName::parse(agent)?;
        let instance = InstanceId::parse(instance)?;
        Ok(Self::new(&agent, &instance))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn agent(&self) -> Result<AgentName> {
        let rest = self.0.strip_prefix("agents/").unwrap_or(&self.0);
        let agent = rest.split('/').next().unwrap_or("");
        AgentName::parse(agent)
    }

    pub fn instance(&self) -> Result<InstanceId> {
        let rest = self.0.strip_prefix("agents/").unwrap_or(&self.0);
        let instance = rest.split_once('/').map(|(_, i)| i).unwrap_or("");
        InstanceId::parse(instance)
    }
}

impl fmt::Display for ConversationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ConversationId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Session key. Root sessions equal the conversation path; children use
/// `task:<parentSession>:task_<ulid>` or `action:<id>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionKey(String);

impl SessionKey {
    pub fn root(conversation: &ConversationId) -> Self {
        Self(conversation.as_str().to_string())
    }

    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(CoreError::InvalidId("session key must not be empty".into()));
        }
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Child session: `task:<parentSession>:task_<ulid>`.
    pub fn task_child(parent: &SessionKey) -> Self {
        Self(format!("task:{}:task_{}", parent.as_str(), Ulid::new()))
    }

    pub fn action(id: impl AsRef<str>) -> Self {
        Self(format!("action:{}", id.as_ref()))
    }

    pub fn is_root(&self) -> bool {
        !self.0.starts_with("task:") && !self.0.starts_with("action:")
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for SessionKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Framework-owned sandbox identity. The model never names a sandbox.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SandboxKey(String);

impl SandboxKey {
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(CoreError::InvalidId("sandbox key must not be empty".into()));
        }
        Ok(Self(raw))
    }

    pub fn scoped(scope: &str, name: &str) -> Self {
        Self(format!("{scope}:{name}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn agent_name_accepts_kebab() {
        assert!(AgentName::parse("support-desk").is_ok());
        assert!(AgentName::parse("Triage").is_ok());
        assert!(AgentName::parse("a").is_ok());
    }

    #[test]
    fn agent_name_rejects_bad() {
        assert!(AgentName::parse("1bad").is_err());
        assert!(AgentName::parse("-bad").is_err());
        assert!(AgentName::parse("bad_name").is_err());
        assert!(AgentName::parse("bad--name").is_err());
        assert!(AgentName::parse("bad-").is_err());
    }

    #[test]
    fn conversation_roundtrip() {
        let agent = AgentName::parse("support-desk").unwrap();
        let id = InstanceId::parse("ticket-42").unwrap();
        let cid = ConversationId::new(&agent, &id);
        assert_eq!(cid.as_str(), "agents/support-desk/ticket-42");
        let parsed = ConversationId::parse(cid.as_str()).unwrap();
        assert_eq!(parsed, cid);
    }

    #[test]
    fn idempotency_is_stable() {
        let agent = AgentName::parse("support-desk").unwrap();
        let id = InstanceId::parse("ticket-42").unwrap();
        let a = SubmissionId::from_idempotency_key(&agent, &id, "k1");
        let b = SubmissionId::from_idempotency_key(&agent, &id, "k1");
        let c = SubmissionId::from_idempotency_key(&agent, &id, "k2");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.as_str().starts_with("sub_ik_"));
        assert_eq!(a.as_str().len(), "sub_ik_".len() + 32);
    }

    #[test]
    fn task_child_session() {
        let cid = ConversationId::parse("agents/support-desk/t1").unwrap();
        let root = SessionKey::root(&cid);
        let child = SessionKey::task_child(&root);
        assert!(child.as_str().starts_with("task:"));
        assert!(!child.is_root());
        assert!(root.is_root());
    }

    #[test]
    fn tool_call_id_from_model_keeps_vendor_prefix() {
        assert_eq!(ToolCallId::from_model_id("call_c1").as_str(), "call_c1");
        assert_eq!(
            ToolCallId::from_model_id("toolu_abc123").as_str(),
            "toolu_abc123"
        );
        assert!(ToolCallId::parse("toolu_abc123").is_err());
        assert!(ToolCallId::from_model_id("").as_str().starts_with("call_"));
    }
}
