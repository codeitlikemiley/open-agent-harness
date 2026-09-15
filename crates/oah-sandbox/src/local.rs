//! Local filesystem + bash driver. Refused when `OAH_HOSTED=1`.

use crate::{Capabilities, ExecOptions, ExecOutput, FileStat, SandboxDriver, SandboxError};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tokio::time::{timeout, Duration};

pub struct LocalSandbox {
    root: PathBuf,
}

impl LocalSandbox {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, SandboxError> {
        if std::env::var("OAH_HOSTED").ok().as_deref() == Some("1") {
            return Err(SandboxError::Unsupported(
                "local sandbox refused when OAH_HOSTED=1".into(),
            ));
        }
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|e| SandboxError::Io(e.to_string()))?;
        Ok(Self { root })
    }

    fn resolve(&self, path: &str) -> Result<PathBuf, SandboxError> {
        let stripped = path.trim_start_matches('/');
        let joined = self.root.join(stripped);
        let canon_root = self
            .root
            .canonicalize()
            .unwrap_or_else(|_| self.root.clone());
        if let Ok(canon) = joined.canonicalize() {
            if !canon.starts_with(&canon_root) {
                return Err(SandboxError::Io("path escapes sandbox root".into()));
            }
            return Ok(canon);
        }
        if let Some(parent) = joined.parent() {
            if parent.exists() {
                let parent = parent
                    .canonicalize()
                    .map_err(|e| SandboxError::Io(e.to_string()))?;
                if !parent.starts_with(&canon_root) {
                    return Err(SandboxError::Io("path escapes sandbox root".into()));
                }
            }
        }
        Ok(joined)
    }
}

#[async_trait]
impl SandboxDriver for LocalSandbox {
    async fn exec(&self, cmd: &str, o: ExecOptions) -> Result<ExecOutput, SandboxError> {
        if std::env::var("OAH_HOSTED").ok().as_deref() == Some("1") {
            return Err(SandboxError::Unsupported(
                "local sandbox refused when OAH_HOSTED=1".into(),
            ));
        }
        let cwd = match &o.cwd {
            Some(p) => self.resolve(p)?,
            None => self.root.clone(),
        };
        let limit = Duration::from_millis(o.timeout_ms.unwrap_or(30_000));
        let child = Command::new("bash")
            .arg("-lc")
            .arg(cmd)
            .current_dir(&cwd)
            .output();
        let output = timeout(limit, child)
            .await
            .map_err(|_| SandboxError::Io("exec timed out".into()))?
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let combined = if stderr.is_empty() {
            stdout
        } else {
            format!("{stdout}{stderr}")
        };
        Ok(ExecOutput {
            stdout: combined,
            exit_code: output.status.code().unwrap_or(1),
        })
    }

    async fn read_file(&self, path: &str) -> Result<String, SandboxError> {
        let p = self.resolve(path)?;
        tokio::fs::read_to_string(p)
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<(), SandboxError> {
        let p = self.resolve(path)?;
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SandboxError::Io(e.to_string()))?;
        }
        tokio::fs::write(p, data)
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))
    }

    async fn stat(&self, path: &str) -> Result<FileStat, SandboxError> {
        let p = self.resolve(path)?;
        let meta = tokio::fs::metadata(p)
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        Ok(FileStat {
            is_dir: meta.is_dir(),
            size: meta.len(),
        })
    }

    async fn read_dir(&self, path: &str) -> Result<Vec<String>, SandboxError> {
        let p = self.resolve(path)?;
        let mut rd = tokio::fs::read_dir(p)
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))?;
        let mut names = Vec::new();
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))?
        {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
        names.sort();
        Ok(names)
    }

    async fn exists(&self, path: &str) -> bool {
        self.stat(path).await.is_ok()
    }

    async fn mkdir(&self, path: &str, _recursive: bool) -> Result<(), SandboxError> {
        let p = self.resolve(path)?;
        tokio::fs::create_dir_all(p)
            .await
            .map_err(|e| SandboxError::Io(e.to_string()))
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            exec_stream: false,
            computer_use: false,
        }
    }
}

pub fn hosted_refused() -> bool {
    std::env::var("OAH_HOSTED").ok().as_deref() == Some("1")
}

pub fn root_or(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_path_buf()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_read_exec() {
        if hosted_refused() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("oah-local-{}", std::process::id()));
        let sb = LocalSandbox::open(&dir).unwrap();
        sb.write_file("/hello.txt", b"hi").await.unwrap();
        assert_eq!(sb.read_file("/hello.txt").await.unwrap(), "hi");
        let out = sb
            .exec("printf ok", ExecOptions::default())
            .await
            .unwrap();
        assert_eq!(out.stdout, "ok");
        assert_eq!(out.exit_code, 0);
        let _ = std::fs::remove_dir_all(dir);
    }
}
