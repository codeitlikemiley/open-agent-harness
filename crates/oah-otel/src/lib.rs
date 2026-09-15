//! Observability helpers. Content capture is **off** by default.

#![forbid(unsafe_code)]

use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;

pub const CONTENT_CAPTURE_DEFAULT: bool = false;

#[derive(Debug, Clone, Copy)]
pub struct CapturePolicy {
    pub content: bool,
}

impl Default for CapturePolicy {
    fn default() -> Self {
        Self {
            content: CONTENT_CAPTURE_DEFAULT,
        }
    }
}

/// Redact emails, bearer tokens, and `sk-` / `oag_` secrets from text.
pub fn redact_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while !rest.is_empty() {
        if let Some(idx) = rest.find('@') {
            let before = &rest[..idx];
            let start = before
                .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '_' && c != '-')
                .map(|i| i + 1)
                .unwrap_or(0);
            let after = &rest[idx + 1..];
            let end_rel = after
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-')
                .unwrap_or(after.len());
            if start < idx && end_rel > 0 && after[..end_rel].contains('.') {
                out.push_str(&rest[..start]);
                out.push_str("[redacted-email]");
                rest = &after[end_rel..];
                continue;
            }
        }
        if let Some(idx) = find_secret(rest) {
            out.push_str(&rest[..idx.0]);
            out.push_str("[redacted-secret]");
            rest = &rest[idx.1..];
            continue;
        }
        out.push_str(rest);
        break;
    }
    out
}

fn find_secret(s: &str) -> Option<(usize, usize)> {
    for prefix in ["sk-", "oag_", "Bearer ", "bearer "] {
        if let Some(i) = s.find(prefix) {
            let start = i;
            let rest = &s[i + prefix.len()..];
            let n = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                .count();
            if n >= 8 {
                let end = i + prefix.len() + n;
                return Some((start, end));
            }
        }
    }
    None
}

pub fn redact_value(value: &Value, policy: CapturePolicy) -> Value {
    if !policy.content {
        return json!({"captured": false});
    }
    match value {
        Value::String(s) => Value::String(redact_text(s)),
        Value::Array(arr) => Value::Array(arr.iter().map(|v| redact_value(v, policy)).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k.clone(), redact_value(v, policy));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

#[derive(Default)]
pub struct Histograms {
    samples: Mutex<HashMap<String, Vec<f64>>>,
}

impl Histograms {
    pub fn record(&self, name: &str, value: f64) {
        if let Ok(mut g) = self.samples.lock() {
            g.entry(name.to_string()).or_default().push(value);
        }
    }

    pub fn snapshot(&self) -> HashMap<String, Vec<f64>> {
        self.samples
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn redacts_email_and_secret() {
        let s = redact_text("write amina@example.com with oag_live_abcdefghij");
        assert!(!s.contains("amina@example.com"));
        assert!(!s.contains("oag_live_abcdefghij"));
        assert!(s.contains("[redacted-email]"));
        assert!(s.contains("[redacted-secret]"));
    }

    #[test]
    fn capture_off_drops_content() {
        let v = redact_value(&json!({"text":"secret"}), CapturePolicy { content: false });
        assert_eq!(v, json!({"captured": false}));
    }

    #[test]
    fn histogram_records() {
        let h = Histograms::default();
        h.record("oah.loop.ms", 12.0);
        assert_eq!(h.snapshot().get("oah.loop.ms").unwrap().len(), 1);
    }
}
