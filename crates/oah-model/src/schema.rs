use oah_core::canonical_value;
use serde_json::{json, Value};

pub fn empty_object_schema() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

pub fn strip_schema_meta(mut schema: Value) -> Value {
    if let Value::Object(map) = &mut schema {
        map.remove("$schema");
        map.remove("$id");
        map.remove("title");
    }
    canonical_value(&schema)
}

pub fn tool_schema_bytes(schema: &Value) -> Result<Vec<u8>, String> {
    let canon = strip_schema_meta(schema.clone());
    serde_json::to_vec(&canon).map_err(|e| e.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn schema_bytes_are_stable() {
        let a = json!({"type":"object","properties":{"b":{"type":"string"},"a":{"type":"number"}},"$schema":"https://json-schema.org/draft-07/schema#"});
        let b = json!({"properties":{"a":{"type":"number"},"b":{"type":"string"}},"type":"object"});
        assert_eq!(tool_schema_bytes(&a).unwrap(), tool_schema_bytes(&b).unwrap());
    }
}
