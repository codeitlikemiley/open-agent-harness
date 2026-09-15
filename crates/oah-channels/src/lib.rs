//! Inbound webhooks → harness signals. Slack v0 HMAC, GitHub sha256, bearer.

#![forbid(unsafe_code)]

use hmac::{Hmac, Mac};
use oah_core::{ConversationId, Principal};
use oah_store::{AdmitRequest, DeliveryKind};
use serde_json::{json, Value};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

#[derive(Debug, Clone, Default)]
pub struct ChannelSecrets {
    pub slack: Option<String>,
    pub github: Option<String>,
    pub bearer: Option<String>,
}

pub const MAX_BODY_BYTES: usize = 64 * 1024;
const SLACK_MAX_SKEW_SECS: i64 = 300;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ChannelError {
    #[error("payload larger than 64KiB")]
    TooLarge,
    #[error("missing or invalid signature")]
    Unauthorized,
    #[error("timestamp skew")]
    Skew,
    #[error("{0}")]
    Other(String),
}

pub fn reject_oversize(body: &[u8]) -> Result<(), ChannelError> {
    if body.len() > MAX_BODY_BYTES {
        Err(ChannelError::TooLarge)
    } else {
        Ok(())
    }
}

fn ct_eq(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// Slack Signing Secret v0: `v0={hex(hmac_sha256(secret, "v0:{ts}:{body}"))}`.
pub fn verify_slack_v0(
    secret: &str,
    timestamp: &str,
    body: &[u8],
    signature: &str,
    now_unix_secs: i64,
) -> Result<(), ChannelError> {
    reject_oversize(body)?;
    let ts: i64 = timestamp.parse().map_err(|_| ChannelError::Unauthorized)?;
    if (now_unix_secs - ts).abs() > SLACK_MAX_SKEW_SECS {
        return Err(ChannelError::Skew);
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|e| ChannelError::Other(e.to_string()))?;
    mac.update(b"v0:");
    mac.update(timestamp.as_bytes());
    mac.update(b":");
    mac.update(body);
    let hex = hex::encode(mac.finalize().into_bytes());
    let expected = format!("v0={hex}");
    if ct_eq(&expected, signature) {
        Ok(())
    } else {
        Err(ChannelError::Unauthorized)
    }
}

/// GitHub `X-Hub-Signature-256: sha256={hex}`.
pub fn verify_github(secret: &str, body: &[u8], signature: &str) -> Result<(), ChannelError> {
    reject_oversize(body)?;
    let got = signature
        .strip_prefix("sha256=")
        .ok_or(ChannelError::Unauthorized)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|e| ChannelError::Other(e.to_string()))?;
    mac.update(body);
    let hex = hex::encode(mac.finalize().into_bytes());
    if ct_eq(&hex, got) {
        Ok(())
    } else {
        Err(ChannelError::Unauthorized)
    }
}

/// `Authorization: Bearer <token>` (constant-time).
pub fn verify_bearer(expected: &str, authorization: &str) -> Result<(), ChannelError> {
    let got = authorization
        .strip_prefix("Bearer ")
        .or_else(|| authorization.strip_prefix("bearer "))
        .ok_or(ChannelError::Unauthorized)?;
    if expected.is_empty() || !ct_eq(expected, got) {
        Err(ChannelError::Unauthorized)
    } else {
        Ok(())
    }
}

pub fn webhook_signal(
    conversation: ConversationId,
    channel: &str,
    body: Value,
    principal: Principal,
) -> AdmitRequest {
    AdmitRequest {
        conversation_id: conversation.clone(),
        session_key: oah_core::SessionKey::root(&conversation),
        kind: DeliveryKind::Signal,
        payload: json!({
            "type": format!("channel:{channel}"),
            "body": body,
        }),
        principal,
        idempotency_key: None,
        uid: None,
        max_attempts: 10,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn slack_roundtrip() {
        let secret = "topsecret";
        let ts = "1000";
        let body = b"{\"ok\":true}";
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(b"v0:1000:");
        mac.update(body);
        let sig = format!("v0={}", hex::encode(mac.finalize().into_bytes()));
        verify_slack_v0(secret, ts, body, &sig, 1000).unwrap();
        assert!(verify_slack_v0(secret, ts, body, "v0=dead", 1000).is_err());
        assert!(verify_slack_v0(secret, ts, body, &sig, 1000 + 400).is_err());
    }

    #[test]
    fn github_and_bearer() {
        let secret = "gh";
        let body = b"push";
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        verify_github(secret, body, &sig).unwrap();
        verify_bearer("tok", "Bearer tok").unwrap();
        assert!(verify_bearer("tok", "Bearer no").is_err());
    }

    #[test]
    fn oversize() {
        let big = vec![0u8; MAX_BODY_BYTES + 1];
        assert_eq!(reject_oversize(&big), Err(ChannelError::TooLarge));
    }
}
