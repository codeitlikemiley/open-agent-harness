use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{kind}: {message}")]
pub struct ModelError {
    pub kind: ModelErrorKind,
    pub message: String,
    pub retryable: bool,
}

impl ModelError {
    pub fn new(kind: ModelErrorKind, message: impl Into<String>) -> Self {
        let retryable = kind.is_retryable();
        Self {
            kind,
            message: message.into(),
            retryable,
        }
    }

    pub fn from_gateway_type(error_type: &str, message: impl Into<String>) -> Self {
        let kind = ModelErrorKind::from_gateway_type(error_type);
        Self::new(kind, message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelErrorKind {
    Authentication,
    BudgetExhausted,
    InvalidRequest,
    NoViableModel,
    InvalidModelQualifier,
    UnsupportedField,
    RateLimit,
    Upstream,
    Overloaded,
    AtCapacity,
    QuotaReserveHeld,
    NoCredential,
    UpstreamTimeout,
    Internal,
    Interrupted,
    ContextOverflow,
    Cancelled,
    SpendRefused,
}

impl ModelErrorKind {
    pub fn from_gateway_type(t: &str) -> Self {
        match t {
            "authentication_error" => Self::Authentication,
            "budget_exhausted" => Self::BudgetExhausted,
            "invalid_request" => Self::InvalidRequest,
            "no_viable_model" => Self::NoViableModel,
            "invalid_model_qualifier" => Self::InvalidModelQualifier,
            "unsupported_field" => Self::UnsupportedField,
            "rate_limit_error" => Self::RateLimit,
            "upstream_error" => Self::Upstream,
            "overloaded" => Self::Overloaded,
            "at_capacity" => Self::AtCapacity,
            "quota_reserve_held" => Self::QuotaReserveHeld,
            "no_credential" | "no_credential_of_kind" => Self::NoCredential,
            "upstream_timeout" | "stream_idle" => Self::UpstreamTimeout,
            "context_overflow" => Self::ContextOverflow,
            "internal_error" => Self::Internal,
            _ => Self::Upstream,
        }
    }

    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::RateLimit
                | Self::Upstream
                | Self::Overloaded
                | Self::AtCapacity
                | Self::QuotaReserveHeld
                | Self::UpstreamTimeout
                | Self::Internal
                | Self::Interrupted
        )
    }

    pub fn never_retry(self) -> bool {
        matches!(
            self,
            Self::Authentication
                | Self::BudgetExhausted
                | Self::InvalidRequest
                | Self::NoViableModel
                | Self::InvalidModelQualifier
                | Self::UnsupportedField
                | Self::NoCredential
                | Self::SpendRefused
        )
    }
}

impl std::fmt::Display for ModelErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Authentication => "authentication_error",
            Self::BudgetExhausted => "budget_exhausted",
            Self::InvalidRequest => "invalid_request",
            Self::NoViableModel => "no_viable_model",
            Self::InvalidModelQualifier => "invalid_model_qualifier",
            Self::UnsupportedField => "unsupported_field",
            Self::RateLimit => "rate_limit_error",
            Self::Upstream => "upstream_error",
            Self::Overloaded => "overloaded",
            Self::AtCapacity => "at_capacity",
            Self::QuotaReserveHeld => "quota_reserve_held",
            Self::NoCredential => "no_credential",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::Internal => "internal_error",
            Self::Interrupted => "interrupted",
            Self::ContextOverflow => "context_overflow",
            Self::Cancelled => "cancelled",
            Self::SpendRefused => "spend_refused",
        };
        f.write_str(s)
    }
}
