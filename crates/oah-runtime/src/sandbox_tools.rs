//! Built-in sandbox tools registered when `use_sandbox` is called.

use crate::tools::{ToolDef, ToolError, ToolOutput};
use oah_sandbox::{tools as sb, SandboxDriver};
use serde_json::json;
use std::sync::Arc;

pub fn sandbox_tool_defs(driver: Arc<dyn SandboxDriver>) -> Vec<ToolDef> {
    vec![
        file_tool(
            "read",
            "Read a file from the sandbox.",
            json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"]
            }),
            driver.clone(),
            |sb, input| {
                Box::pin(async move {
                    let path = input.get("path").and_then(|v| v.as_str()).unwrap_or("");
                    let text = sb::read_tool(sb.as_ref(), path)
                        .await
                        .map_err(ToolError::Message)?;
                    Ok(ToolOutput::text(text))
                })
            },
        ),
        file_tool(
            "write",
            "Write a file in the sandbox.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "contents": { "type": "string" }
                },
                "required": ["path", "contents"]
            }),
            driver.clone(),
            |sb, input| {
                Box::pin(async move {
                    let path = input.get("path").and_then(|v| v.as_str()).unwrap_or("");
                    let contents = input
                        .get("contents")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let v = sb::write_tool(sb.as_ref(), path, contents)
                        .await
                        .map_err(ToolError::Message)?;
                    Ok(ToolOutput::json(v))
                })
            },
        ),
        file_tool(
            "edit",
            "Replace exactly one occurrence of old_string in a sandbox file.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "old_string": { "type": "string" },
                    "new_string": { "type": "string" }
                },
                "required": ["path", "old_string", "new_string"]
            }),
            driver.clone(),
            |sb, input| {
                Box::pin(async move {
                    let path = input.get("path").and_then(|v| v.as_str()).unwrap_or("");
                    let old = input
                        .get("old_string")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let new = input
                        .get("new_string")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let v = sb::edit_tool(sb.as_ref(), path, old, new)
                        .await
                        .map_err(ToolError::Message)?;
                    Ok(ToolOutput::json(v))
                })
            },
        ),
        file_tool(
            "bash",
            "Run a command in the sandbox (unsupported on virtual).",
            json!({
                "type": "object",
                "properties": { "command": { "type": "string" } },
                "required": ["command"]
            }),
            driver.clone(),
            |sb, input| {
                Box::pin(async move {
                    let cmd = input
                        .get("command")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let v = sb::bash_tool(sb.as_ref(), cmd)
                        .await
                        .map_err(ToolError::Message)?;
                    Ok(ToolOutput::json(v))
                })
            },
        ),
        file_tool(
            "grep",
            "Search a sandbox file for a literal pattern.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "pattern": { "type": "string" }
                },
                "required": ["path", "pattern"]
            }),
            driver.clone(),
            |sb, input| {
                Box::pin(async move {
                    let path = input.get("path").and_then(|v| v.as_str()).unwrap_or("");
                    let pattern = input
                        .get("pattern")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let v = sb::grep_tool(sb.as_ref(), path, pattern)
                        .await
                        .map_err(ToolError::Message)?;
                    Ok(ToolOutput::json(v))
                })
            },
        ),
        file_tool(
            "glob",
            "List sandbox directory entries matching a suffix.",
            json!({
                "type": "object",
                "properties": {
                    "dir": { "type": "string" },
                    "suffix": { "type": "string" }
                },
                "required": ["dir"]
            }),
            driver,
            |sb, input| {
                Box::pin(async move {
                    let dir = input.get("dir").and_then(|v| v.as_str()).unwrap_or("/");
                    let suffix = input.get("suffix").and_then(|v| v.as_str()).unwrap_or("");
                    let v = sb::glob_tool(sb.as_ref(), dir, suffix)
                        .await
                        .map_err(ToolError::Message)?;
                    Ok(ToolOutput::json(v))
                })
            },
        ),
    ]
}

fn file_tool(
    name: &str,
    description: &str,
    schema: serde_json::Value,
    driver: Arc<dyn SandboxDriver>,
    run: impl Fn(
            Arc<dyn SandboxDriver>,
            serde_json::Value,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ToolOutput, ToolError>> + Send>,
        > + Send
        + Sync
        + 'static,
) -> ToolDef {
    let run = Arc::new(run);
    ToolDef {
        name: name.into(),
        description: description.into(),
        input_schema: schema,
        durable: true,
        run: Arc::new(move |_cx, input| {
            let driver = driver.clone();
            let run = run.clone();
            Box::pin(async move { run(driver, input).await })
        }),
    }
}
