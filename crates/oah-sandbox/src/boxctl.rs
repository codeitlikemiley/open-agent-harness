//! `hexuria/box` / `box-control` driver methods.
//!
//! Production talks HTTP to box-control. Tests use `MockBoxControl`.

use crate::{Capabilities, ExecOptions, ExecOutput, FileStat, SandboxDriver, SandboxError};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait]
pub trait BoxControl: Send + Sync {
    async fn exec(&self, sandbox_id: &str, cmd: &str) -> Result<ExecOutput, SandboxError>;
    async fn read_file(&self, sandbox_id: &str, path: &str) -> Result<String, SandboxError>;
    async fn write_file(
        &self,
        sandbox_id: &str,
        path: &str,
        data: &[u8],
    ) -> Result<(), SandboxError>;
}

pub struct BoxSandbox {
    id: String,
    control: Arc<dyn BoxControl>,
}

impl BoxSandbox {
    pub fn new(id: impl Into<String>, control: Arc<dyn BoxControl>) -> Self {
        Self {
            id: id.into(),
            control,
        }
    }
}

#[async_trait]
impl SandboxDriver for BoxSandbox {
    async fn exec(&self, cmd: &str, _o: ExecOptions) -> Result<ExecOutput, SandboxError> {
        self.control.exec(&self.id, cmd).await
    }
    async fn read_file(&self, path: &str) -> Result<String, SandboxError> {
        self.control.read_file(&self.id, path).await
    }
    async fn write_file(&self, path: &str, data: &[u8]) -> Result<(), SandboxError> {
        self.control.write_file(&self.id, path, data).await
    }
    async fn stat(&self, path: &str) -> Result<FileStat, SandboxError> {
        let data = self.read_file(path).await?;
        Ok(FileStat {
            is_dir: false,
            size: data.len() as u64,
        })
    }
    async fn read_dir(&self, _path: &str) -> Result<Vec<String>, SandboxError> {
        Err(SandboxError::Unsupported(
            "box read_dir is not in the v0 driver".into(),
        ))
    }
    async fn exists(&self, path: &str) -> bool {
        self.read_file(path).await.is_ok()
    }
    async fn mkdir(&self, _path: &str, _recursive: bool) -> Result<(), SandboxError> {
        Err(SandboxError::Unsupported(
            "box mkdir is not in the v0 driver".into(),
        ))
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            exec_stream: true,
            computer_use: true,
        }
    }
}

/// In-process box-control stand-in.
pub struct MockBoxControl {
    files: Mutex<BTreeMap<(String, String), Vec<u8>>>,
    execs: Mutex<u32>,
}

impl MockBoxControl {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(BTreeMap::new()),
            execs: Mutex::new(0),
        }
    }

    pub async fn exec_count(&self) -> u32 {
        *self.execs.lock().await
    }
}

impl Default for MockBoxControl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BoxControl for MockBoxControl {
    async fn exec(&self, _sandbox_id: &str, cmd: &str) -> Result<ExecOutput, SandboxError> {
        let mut n = self.execs.lock().await;
        *n = n.saturating_add(1);
        Ok(ExecOutput {
            stdout: format!("box-exec:{cmd}"),
            exit_code: 0,
        })
    }

    async fn read_file(&self, sandbox_id: &str, path: &str) -> Result<String, SandboxError> {
        let files = self.files.lock().await;
        let bytes = files
            .get(&(sandbox_id.to_string(), path.to_string()))
            .ok_or_else(|| SandboxError::Io(format!("not found: {path}")))?;
        String::from_utf8(bytes.clone()).map_err(|e| SandboxError::Io(e.to_string()))
    }

    async fn write_file(
        &self,
        sandbox_id: &str,
        path: &str,
        data: &[u8],
    ) -> Result<(), SandboxError> {
        let mut files = self.files.lock().await;
        files.insert((sandbox_id.to_string(), path.to_string()), data.to_vec());
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_box_roundtrip() {
        let ctl = Arc::new(MockBoxControl::new());
        let sb = BoxSandbox::new("sb_1", ctl.clone());
        sb.write_file("/n.txt", b"n").await.unwrap();
        assert_eq!(sb.read_file("/n.txt").await.unwrap(), "n");
        let out = sb.exec("uname", ExecOptions::default()).await.unwrap();
        assert!(out.stdout.contains("box-exec"));
        assert!(sb.capabilities().computer_use);
        assert_eq!(ctl.exec_count().await, 1);
    }
}
