use crate::error::{CoreError, Result};
use serde_json::{Map, Value};

/// Recursively sort object keys so two equal values serialize identically.
/// Used wherever Flue compares JSON by string (tool schemas, settlement records).
pub fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = Map::new();
            for key in keys {
                if let Some(val) = map.get(key) {
                    out.insert(key.clone(), canonical_value(val));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        other => other.clone(),
    }
}

pub fn canonical_string(value: &Value) -> Result<String> {
    serde_json::to_string(&canonical_value(value)).map_err(|err| CoreError::Json(err.to_string()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn object_keys_are_sorted() {
        let a = json!({"b": 1, "a": 2});
        let b = json!({"a": 2, "b": 1});
        assert_eq!(canonical_string(&a).unwrap(), canonical_string(&b).unwrap());
        assert_eq!(canonical_string(&a).unwrap(), r#"{"a":2,"b":1}"#);
    }
}
