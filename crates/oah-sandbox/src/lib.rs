//! Sandbox traits, an in-memory `virtual` driver, and the six built-in tools.

#![forbid(unsafe_code)]

pub mod boxctl;
pub mod local;
pub mod tools;

use async_trait::async_trait;
use oah_core::SandboxKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("{0}")]
    Died(String),
    #[error("aborted (orphaned={orphaned})")]
    Aborted { orphaned: bool },
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Io(String),
}

#[derive(Debug, Clone, Default)]
pub struct ExecOptions {
    pub cwd: Option<String>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ExecOutput {
    pub stdout: String,
    pub exit_code: i32,
}

#[derive(Debug, Clone)]
pub struct FileStat {
    pub is_dir: bool,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Capabilities {
    pub exec_stream: bool,
    pub computer_use: bool,
}

#[async_trait]
pub trait SandboxDriver: Send + Sync {
    async fn exec(&self, cmd: &str, o: ExecOptions) -> Result<ExecOutput, SandboxError>;
    async fn read_file(&self, path: &str) -> Result<String, SandboxError>;
    async fn write_file(&self, path: &str, data: &[u8]) -> Result<(), SandboxError>;
    async fn stat(&self, path: &str) -> Result<FileStat, SandboxError>;
    async fn read_dir(&self, path: &str) -> Result<Vec<String>, SandboxError>;
    async fn exists(&self, path: &str) -> bool;
    async fn mkdir(&self, path: &str, recursive: bool) -> Result<(), SandboxError>;
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
}

#[derive(Debug, Clone)]
pub struct SandboxStatus {
    pub ready: bool,
}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn acquire(
        &self,
        key: &SandboxKey,
        spec: &SandboxSpec,
    ) -> Result<Arc<dyn SandboxDriver>, SandboxError>;
    async fn status(&self, _key: &SandboxKey) -> Result<SandboxStatus, SandboxError> {
        Ok(SandboxStatus { ready: true })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SandboxSpec {
    pub cwd: String,
}

/// In-memory filesystem. File tools only; `exec` is rejected.
pub struct VirtualSandbox {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl VirtualSandbox {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Default for VirtualSandbox {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize(path: &str) -> String {
    let mut out = String::new();
    if !path.starts_with('/') {
        out.push('/');
    }
    out.push_str(path);
    while out.contains("//") {
        out = out.replace("//", "/");
    }
    out
}

#[async_trait]
impl SandboxDriver for VirtualSandbox {
    async fn exec(&self, _cmd: &str, _o: ExecOptions) -> Result<ExecOutput, SandboxError> {
        Err(SandboxError::Unsupported(
            "virtual sandbox has no exec; use the local or box provider".into(),
        ))
    }

    async fn read_file(&self, path: &str) -> Result<String, SandboxError> {
        let files = self.files.lock().await;
        let bytes = files
            .get(&normalize(path))
            .ok_or_else(|| SandboxError::Io(format!("not found: {path}")))?;
        String::from_utf8(bytes.clone()).map_err(|e| SandboxError::Io(e.to_string()))
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<(), SandboxError> {
        let path = normalize(path);
        let mut files = self.files.lock().await;
        if let Some(parent) = parent_of(&path) {
            files.entry(parent).or_insert_with(Vec::new);
        }
        files.insert(path, data.to_vec());
        Ok(())
    }

    async fn stat(&self, path: &str) -> Result<FileStat, SandboxError> {
        let files = self.files.lock().await;
        let p = normalize(path);
        if let Some(b) = files.get(&p) {
            return Ok(FileStat {
                is_dir: false,
                size: b.len() as u64,
            });
        }
        let prefix = format!("{p}/");
        if files.keys().any(|k| k.starts_with(&prefix)) {
            return Ok(FileStat {
                is_dir: true,
                size: 0,
            });
        }
        Err(SandboxError::Io(format!("not found: {path}")))
    }

    async fn read_dir(&self, path: &str) -> Result<Vec<String>, SandboxError> {
        let files = self.files.lock().await;
        let prefix = {
            let n = normalize(path);
            if n.ends_with('/') {
                n
            } else {
                format!("{n}/")
            }
        };
        let mut names = Vec::new();
        for key in files.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                let name = rest.split('/').next().unwrap_or(rest);
                if !name.is_empty() && !names.iter().any(|n| n == name) {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        Ok(names)
    }

    async fn exists(&self, path: &str) -> bool {
        self.stat(path).await.is_ok()
    }

    async fn mkdir(&self, path: &str, _recursive: bool) -> Result<(), SandboxError> {
        let mut files = self.files.lock().await;
        files.entry(normalize(path)).or_insert_with(Vec::new);
        Ok(())
    }
}

fn parent_of(path: &str) -> Option<String> {
    let trimmed = path.trim_end_matches('/');
    trimmed.rfind('/').map(|i| trimmed[..i].to_string())
}

pub struct VirtualProvider {
    boxes: Mutex<BTreeMap<String, Arc<VirtualSandbox>>>,
}

impl VirtualProvider {
    pub fn new() -> Self {
        Self {
            boxes: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Default for VirtualProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SandboxProvider for VirtualProvider {
    async fn acquire(
        &self,
        key: &SandboxKey,
        _spec: &SandboxSpec,
    ) -> Result<Arc<dyn SandboxDriver>, SandboxError> {
        let mut boxes = self.boxes.lock().await;
        let entry = boxes
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(VirtualSandbox::new()));
        Ok(entry.clone())
    }
}

/// Built-in tool helpers (byte-based limits, BTOOL-5).
pub const READ_MAX_LINES: usize = 2000;
pub const READ_MAX_BYTES: usize = 50 * 1024;
pub const GREP_MAX_MATCHES: usize = 100;
pub const GREP_MAX_LINE: usize = 500;
pub const GLOB_MAX: usize = 1000;

pub fn clip_bytes(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n…(truncated)", &s[..end])
}

pub fn clip_lines(s: &str, max_lines: usize, max_bytes: usize) -> String {
    let mut out = String::new();
    for (i, line) in s.lines().enumerate() {
        if i >= max_lines {
            out.push_str("\n…(truncated, more lines)");
            break;
        }
        if out.len().saturating_add(line.len()) > max_bytes {
            out.push_str("\n…(truncated)");
            break;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(line);
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn virtual_roundtrip() {
        let sb = VirtualSandbox::new();
        sb.write_file("/workspace/a.txt", b"hello").await.unwrap();
        assert_eq!(sb.read_file("/workspace/a.txt").await.unwrap(), "hello");
        assert!(sb.exists("/workspace/a.txt").await);
        let listing = sb.read_dir("/workspace").await.unwrap();
        assert_eq!(listing, vec!["a.txt".to_string()]);
    }
}
