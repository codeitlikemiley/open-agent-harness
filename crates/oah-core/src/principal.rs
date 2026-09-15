use serde::{Deserialize, Serialize};

/// Who admitted a submission. Persisted on the ledger; durable processing
/// never sees the original HTTP request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Principal {
    pub kind: PrincipalKind,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Anonymous,
    User,
    Key,
    Schedule,
    Channel,
    System,
}

impl Principal {
    pub fn anonymous() -> Self {
        Self {
            kind: PrincipalKind::Anonymous,
            id: "anonymous".into(),
            display: None,
        }
    }

    pub fn system() -> Self {
        Self {
            kind: PrincipalKind::System,
            id: "system".into(),
            display: None,
        }
    }
}

impl Default for Principal {
    fn default() -> Self {
        Self::anonymous()
    }
}
