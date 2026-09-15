//! Six built-in sandbox tools (byte caps from BTOOL-5).

use crate::{clip_bytes, clip_lines, ExecOptions, SandboxDriver, GREP_MAX_LINE, GREP_MAX_MATCHES, GLOB_MAX, READ_MAX_BYTES, READ_MAX_LINES};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn read_tool(sb: &dyn SandboxDriver, path: &str) -> Result<String, String> {
    let raw = sb.read_file(path).await.map_err(|e| e.to_string())?;
    Ok(clip_lines(&raw, READ_MAX_LINES, READ_MAX_BYTES))
}

pub async fn write_tool(sb: &dyn SandboxDriver, path: &str, contents: &str) -> Result<Value, String> {
    sb.write_file(path, contents.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({"ok": true, "path": path, "bytes": contents.len()}))
}

pub async fn edit_tool(
    sb: &dyn SandboxDriver,
    path: &str,
    old: &str,
    new: &str,
) -> Result<Value, String> {
    let raw = sb.read_file(path).await.map_err(|e| e.to_string())?;
    let count = raw.matches(old).count();
    if count != 1 {
        return Err(format!(
            "edit requires exactly one match of old_string (found {count})"
        ));
    }
    let updated = raw.replacen(old, new, 1);
    sb.write_file(path, updated.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({"ok": true, "path": path}))
}

pub async fn bash_tool(sb: &dyn SandboxDriver, command: &str) -> Result<Value, String> {
    let out = sb
        .exec(
            command,
            ExecOptions {
                cwd: None,
                timeout_ms: Some(30_000),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({
        "stdout": clip_bytes(&out.stdout, READ_MAX_BYTES),
        "exit_code": out.exit_code,
    }))
}

pub async fn grep_tool(sb: &dyn SandboxDriver, path: &str, pattern: &str) -> Result<Value, String> {
    let raw = sb.read_file(path).await.map_err(|e| e.to_string())?;
    let mut matches = Vec::new();
    for (i, line) in raw.lines().enumerate() {
        if line.contains(pattern) {
            let clipped = clip_bytes(line, GREP_MAX_LINE);
            matches.push(json!({"line": i + 1, "text": clipped}));
            if matches.len() >= GREP_MAX_MATCHES {
                break;
            }
        }
    }
    Ok(json!({"matches": matches}))
}

pub async fn glob_tool(sb: &dyn SandboxDriver, dir: &str, suffix: &str) -> Result<Value, String> {
    let names = sb.read_dir(dir).await.map_err(|e| e.to_string())?;
    let mut hits: Vec<String> = names
        .into_iter()
        .filter(|n| suffix.is_empty() || n.ends_with(suffix) || n.contains(suffix.trim_start_matches('*')))
        .take(GLOB_MAX)
        .collect();
    hits.sort();
    Ok(json!({"paths": hits}))
}

pub fn driver_arc(sb: Arc<dyn SandboxDriver>) -> Arc<dyn SandboxDriver> {
    sb
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::VirtualSandbox;

    #[tokio::test]
    async fn file_tools_roundtrip() {
        let sb = VirtualSandbox::new();
        write_tool(&sb, "/workspace/a.txt", "alpha\nbeta\n").await.unwrap();
        let read = read_tool(&sb, "/workspace/a.txt").await.unwrap();
        assert!(read.contains("alpha"));
        edit_tool(&sb, "/workspace/a.txt", "beta", "gamma").await.unwrap();
        let grepped = grep_tool(&sb, "/workspace/a.txt", "gamma").await.unwrap();
        assert_eq!(grepped["matches"].as_array().unwrap().len(), 1);
        let listing = glob_tool(&sb, "/workspace", ".txt").await.unwrap();
        assert_eq!(listing["paths"][0], "a.txt");
    }
}
