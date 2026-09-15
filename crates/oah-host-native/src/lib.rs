//! Tokio + axum host: config, sample agents, coordinator, `serve`.

#![forbid(unsafe_code)]

use oah_channels::ChannelSecrets;
use oah_core::AgentName;
use oah_http::{agent_router, AppState};
use oah_model::{MockModel, ModelClient};
use oah_oag::{OagClient, OagConfig};
use oah_runtime::agent::{Agent, Instructions, RenderCx, RenderError};
use oah_runtime::gates::{NamedDenyGate, ToolGate};
use oah_runtime::markdown::MarkdownAgent;
use oah_runtime::tools::{lookup_ticket_tool, refund_tool};
use oah_runtime::{Coordinator, Runtime};
use oah_schedules::ScheduleBook;
use oah_store::Store;
use oah_store_sqlite::SqliteStore;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Debug, Error)]
pub enum HostError {
    #[error("{0}")]
    Config(String),
    #[error("{0}")]
    Io(String),
    #[error("{0}")]
    Runtime(String),
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ConfigFile {
    #[serde(default)]
    pub server: ServerCfg,
    #[serde(default)]
    pub store: StoreCfg,
    #[serde(default)]
    pub gateway: GatewayCfg,
    #[serde(default)]
    pub model: ModelCfg,
    #[serde(default)]
    pub channels: ChannelsCfg,
    #[serde(default)]
    pub postgres: PostgresCfg,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerCfg {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_public")]
    pub public_base: String,
}

fn default_bind() -> String {
    "0.0.0.0:43147".into()
}
fn default_public() -> String {
    "http://127.0.0.1:43147".into()
}

impl Default for ServerCfg {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            public_base: default_public(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreCfg {
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default = "default_db")]
    pub path: String,
}

fn default_backend() -> String {
    "sqlite".into()
}

fn default_db() -> String {
    ".oah/dev.db".into()
}

impl Default for StoreCfg {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            path: default_db(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ChannelsCfg {
    #[serde(default)]
    pub slack: String,
    #[serde(default)]
    pub github: String,
    #[serde(default)]
    pub bearer: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PostgresCfg {
    #[serde(default)]
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GatewayCfg {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelCfg {
    #[serde(default = "default_model")]
    pub default: String,
    #[serde(default = "default_max")]
    pub max_output_tokens: u32,
}

fn default_model() -> String {
    "mock/scripted".into()
}
fn default_max() -> u32 {
    4096
}

impl Default for ModelCfg {
    fn default() -> Self {
        Self {
            default: default_model(),
            max_output_tokens: default_max(),
        }
    }
}

impl ConfigFile {
    pub fn load(path: &Path) -> Result<Self, HostError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path).map_err(|e| HostError::Io(e.to_string()))?;
        let mut cfg: Self = toml::from_str(&raw).map_err(|e| HostError::Config(e.to_string()))?;
        if let Ok(bind) = std::env::var("OAH_SERVER__BIND") {
            cfg.server.bind = bind;
        }
        if let Ok(url) = std::env::var("OAH_GATEWAY__URL") {
            cfg.gateway.url = url;
        }
        if let Ok(key) = std::env::var("OAH_GATEWAY__KEY") {
            cfg.gateway.key = key;
        }
        if let Ok(p) = std::env::var("OAH_STORE__PATH") {
            cfg.store.path = p;
        }
        if let Ok(b) = std::env::var("OAH_STORE__BACKEND") {
            cfg.store.backend = b;
        }
        if let Ok(u) = std::env::var("OAH_POSTGRES__URL") {
            cfg.postgres.url = u;
        }
        if let Ok(s) = std::env::var("OAH_CHANNELS__SLACK") {
            cfg.channels.slack = s;
        }
        if let Ok(s) = std::env::var("OAH_CHANNELS__GITHUB") {
            cfg.channels.github = s;
        }
        if let Ok(s) = std::env::var("OAH_CHANNELS__BEARER") {
            cfg.channels.bearer = s;
        }
        Ok(cfg)
    }

    pub fn channel_secrets(&self) -> ChannelSecrets {
        ChannelSecrets {
            slack: nonempty(&self.channels.slack),
            github: nonempty(&self.channels.github),
            bearer: nonempty(&self.channels.bearer),
        }
    }
}

fn nonempty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

pub struct SupportDesk {
    name: AgentName,
}

impl SupportDesk {
    pub fn new() -> Result<Self, HostError> {
        Ok(Self {
            name: AgentName::parse("support-desk").map_err(|e| HostError::Config(e.to_string()))?,
        })
    }
}

impl Agent for SupportDesk {
    fn name(&self) -> &AgentName {
        &self.name
    }

    fn description(&self) -> &str {
        "Triage a customer ticket, look it up, and draft a reply."
    }

    fn render(&self, cx: &mut RenderCx<'_>) -> Result<Instructions, RenderError> {
        cx.use_model("mock/scripted")?;
        cx.use_sandbox("virtual")?;
        cx.use_subagent("researcher");
        cx.use_mcp("echo");
        cx.use_tool(lookup_ticket_tool())?;
        cx.use_tool(refund_tool())?;
        if let Ok(skills) = oah_skills::load_dir(std::path::Path::new("skills")) {
            for skill in skills {
                cx.use_skill(skill)?;
            }
        }
        cx.use_tool_gate(Arc::new(NamedDenyGate {
            name: "sensitive".into(),
            deny: vec![],
            ask: vec!["refund".into()],
        }) as Arc<dyn ToolGate>);
        Ok(Instructions(format!(
            "You are the support-desk agent for conversation {}.\n\
             Use lookup_ticket when the user mentions a ticket id.\n\
             Reply with a short diagnosis and a suggested customer reply.",
            cx.id()
        )))
    }
}

fn demo_mcp() -> oah_mcp::InProcessMcp {
    let mut mcp = oah_mcp::InProcessMcp::new(oah_mcp::McpAllowlist::new([
        oah_mcp::mcp_tool_name("echo", "ping"),
    ]));
    mcp.register("echo", "ping", |args| {
        Ok(serde_json::json!({"content":[{"type":"text","text": args.to_string()}]}))
    });
    mcp
}

pub async fn build_runtime(cfg: &ConfigFile, demo: bool, fresh: bool) -> Result<Arc<Runtime>, HostError> {
    if cfg!(not(debug_assertions)) && demo {
        return Err(HostError::Config(
            "a release binary refuses to start in mock mode (MDL-12)".into(),
        ));
    }
    if fresh {
        let _ = std::fs::remove_file(&cfg.store.path);
    }
    let store: Arc<dyn Store> = if cfg.store.backend == "postgres" {
        if cfg.postgres.url.is_empty() {
            return Err(HostError::Config(
                "store.backend=postgres requires postgres.url / OAH_POSTGRES__URL".into(),
            ));
        }
        let pg = oah_store_postgres::PostgresStore::connect(&cfg.postgres.url)
            .await
            .map_err(|e| HostError::Io(e.to_string()))?;
        pg.migrate()
            .await
            .map_err(|e| HostError::Io(e.to_string()))?;
        Arc::new(pg)
    } else {
        let sqlite = SqliteStore::open(&cfg.store.path).map_err(|e| HostError::Io(e.to_string()))?;
        sqlite
            .migrate()
            .await
            .map_err(|e| HostError::Io(e.to_string()))?;
        Arc::new(sqlite)
    };

    let model: Arc<dyn ModelClient> = if !cfg.gateway.url.is_empty() && !cfg.gateway.key.is_empty() {
        let client = OagClient::new(OagConfig::new(&cfg.gateway.url, &cfg.gateway.key))
            .map_err(|e| HostError::Config(e.to_string()))?;
        Arc::new(client)
    } else {
        if !demo && !cfg!(debug_assertions) {
            return Err(HostError::Config(
                "gateway URL/key missing; pass --demo only in debug builds".into(),
            ));
        }
        Arc::new(MockModel::support_desk())
    };

    let mut builder = Runtime::builder()
        .store(store)
        .model(model)
        .default_model(cfg.model.default.clone())
        .max_output_tokens(cfg.model.max_output_tokens)
        .demo(demo)
        .mcp(Arc::new(demo_mcp()))
        .agent(Arc::new(
            SupportDesk::new().map_err(|e| HostError::Runtime(e.to_string()))?,
        ));

    let agents_dir = PathBuf::from("agents");
    if let Ok(mds) = MarkdownAgent::load_dir(&agents_dir) {
        for agent in mds {
            if agent.name().as_str() == "support-desk" {
                continue;
            }
            builder = builder.agent(Arc::new(agent));
        }
    }

    let rt = builder
        .build()
        .map_err(|e| HostError::Runtime(e.to_string()))?;
    Ok(Arc::new(rt))
}

pub async fn serve(cfg: ConfigFile, demo: bool) -> Result<(), HostError> {
    serve_inner(cfg, demo, false).await
}

/// Local development host: always demo/mock-capable, optional `--fresh` wipe.
pub async fn serve_dev(cfg: ConfigFile, fresh: bool) -> Result<(), HostError> {
    serve_inner(cfg, true, fresh).await
}

async fn serve_inner(cfg: ConfigFile, demo: bool, fresh: bool) -> Result<(), HostError> {
    let runtime = build_runtime(&cfg, demo, fresh).await?;
    let cancel = CancellationToken::new();
    let coord = Arc::new(Coordinator::new(runtime.clone()));
    let coord_run = coord.clone();
    let cancel_c = cancel.clone();
    tokio::spawn(async move {
        coord_run.run(cancel_c).await;
    });

    let schedules = Arc::new(tokio::sync::Mutex::new(ScheduleBook::default()));
    let sched_rt = runtime.clone();
    let sched_book = schedules.clone();
    let cancel_s = cancel.clone();
    tokio::spawn(async move {
        loop {
            if cancel_s.is_cancelled() {
                break;
            }
            let due = {
                let mut book = sched_book.lock().await;
                book.claim_and_advance(oah_core::UnixMillis::now_system())
            };
            if let Ok(due) = due {
                for (_id, _claimed, req) in due {
                    let _ = sched_rt.admit(req).await;
                }
            }
            tokio::select! {
                _ = cancel_s.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        }
    });

    let mcp = runtime
        .mcp
        .clone()
        .unwrap_or_else(|| Arc::new(demo_mcp()));
    let state = AppState {
        runtime,
        public_base: cfg.server.public_base.clone(),
        channels: cfg.channel_secrets(),
        schedules,
        mcp,
    };
    let app = agent_router(state);
    let addr: SocketAddr = cfg
        .server
        .bind
        .parse()
        .map_err(|e| HostError::Config(format!("bind: {e}")))?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| HostError::Io(e.to_string()))?;
    info!(%addr, "oah serve");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancel.cancel();
        })
        .await
        .map_err(|e| HostError::Io(e.to_string()))?;
    Ok(())
}

pub async fn run_once(
    cfg: &ConfigFile,
    agent: &str,
    instance: &str,
    message: &str,
    demo: bool,
    json: bool,
) -> Result<i32, HostError> {
    let runtime = build_runtime(cfg, demo, false).await?;
    let name = AgentName::parse(agent).map_err(|e| HostError::Config(e.to_string()))?;
    let inst = oah_core::InstanceId::parse(instance).map_err(|e| HostError::Config(e.to_string()))?;
    let receipt = runtime
        .dispatch(
            &name,
            &inst,
            message,
            oah_core::Principal::anonymous(),
            None,
            None,
        )
        .await
        .map_err(|e| HostError::Runtime(e.to_string()))?;
    let coord = Coordinator::new(runtime.clone());
    let cancel = CancellationToken::new();
    let _ = coord.tick(&cancel).await;
    let row = runtime
        .store
        .get_submission(&receipt.submission_id)
        .await
        .map_err(|e| HostError::Runtime(e.to_string()))?;
    let records = runtime
        .store
        .read_all(oah_core::ConversationId::new(&name, &inst).as_str())
        .await
        .unwrap_or_default();
    let text = records
        .iter()
        .rev()
        .find_map(|r| match &r.body {
            oah_core::RecordBody::AssistantTextDelta { text } => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default();
    if json {
        println!(
            "{}",
            serde_json::json!({
                "submissionId": receipt.submission_id.to_string(),
                "status": format!("{:?}", row.status),
                "text": text,
            })
        );
    } else {
        println!("{text}");
    }
    Ok(match row.status {
        oah_store::SubmissionStatus::Settled if row.error.is_none() => 0,
        _ => 1,
    })
}

pub fn config_path() -> PathBuf {
    PathBuf::from("oah.toml")
}
