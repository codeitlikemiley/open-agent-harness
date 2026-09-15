use clap::{Parser, Subcommand};
use oah_host_native::{build_runtime, config_path, run_once, serve, serve_dev, ConfigFile};
use oah_runtime::Agent;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "oah", about = "open-agent-harness. Run and serve Rust agents.")]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run one message against an agent and print the reply.
    Run {
        /// AGENT.md path or registered agent name.
        agent: String,
        #[arg(short = 'm', long)]
        message: String,
        #[arg(long, default_value = "cli")]
        id: String,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        demo: bool,
    },
    /// Serve the HTTP API and demo console.
    Serve {
        #[arg(long)]
        demo: bool,
        #[arg(long)]
        bind: Option<String>,
    },
    /// Local development server (demo model, optional fresh store).
    Dev {
        #[arg(long)]
        bind: Option<String>,
        #[arg(long)]
        fresh: bool,
    },
    /// Apply store migrations / format-version check.
    Migrate,
    /// Inspect a conversation ledger.
    Inspect { target: String },
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg_path = cli.config.unwrap_or_else(config_path);
    let mut cfg = match ConfigFile::load(&cfg_path) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(1);
        }
    };

    let result = match cli.cmd {
        Command::Serve { demo, bind } => {
            if let Some(b) = bind {
                cfg.server.bind = b;
            }
            serve(cfg, demo || cfg!(debug_assertions)).await.map(|()| 0)
        }
        Command::Dev { bind, fresh } => {
            if let Some(b) = bind {
                cfg.server.bind = b;
            }
            serve_dev(cfg, fresh).await.map(|()| 0)
        }
        Command::Run {
            agent,
            message,
            id,
            json,
            demo,
        } => {
            let name = if agent.ends_with(".md") {
                match oah_runtime::MarkdownAgent::from_file(std::path::Path::new(&agent)) {
                    Ok(a) => a.name().to_string(),
                    Err(err) => {
                        eprintln!("{err}");
                        return ExitCode::from(1);
                    }
                }
            } else {
                agent
            };
            run_once(&cfg, &name, &id, &message, demo || cfg!(debug_assertions), json).await
        }
        Command::Migrate => {
            match build_runtime(&cfg, true, false).await {
                Ok(_) => {
                    println!("store ready at {}", cfg.store.path);
                    Ok(0)
                }
                Err(err) => Err(err),
            }
        }
        Command::Inspect { target } => inspect(&cfg, &target).await,
    };

    match result {
        Ok(0) => ExitCode::SUCCESS,
        Ok(code) => ExitCode::from(code as u8),
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}

async fn inspect(cfg: &ConfigFile, target: &str) -> Result<i32, oah_host_native::HostError> {
    let rt = build_runtime(cfg, true, false).await?;
    let path = if target.starts_with("agents/") {
        target.to_string()
    } else {
        format!("agents/{target}")
    };
    match rt.store.read_all(&path).await {
        Ok(records) => {
            println!("{} records", records.len());
            for rec in records {
                println!(
                    "{}  {}",
                    rec.kind(),
                    serde_json::to_string(&rec.body).unwrap_or_default()
                );
            }
            Ok(0)
        }
        Err(err) => {
            eprintln!("{err}");
            Ok(1)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_dev_fresh() {
        let cli = Cli::try_parse_from(["oah", "dev", "--fresh", "--bind", "127.0.0.1:43147"]).unwrap();
        match cli.cmd {
            Command::Dev { fresh, bind } => {
                assert!(fresh);
                assert_eq!(bind.as_deref(), Some("127.0.0.1:43147"));
            }
            other => panic!("expected Dev, got {other:?}"),
        }
    }

    #[test]
    fn parses_run_serve_migrate_inspect() {
        assert!(matches!(
            Cli::try_parse_from(["oah", "migrate"]).unwrap().cmd,
            Command::Migrate
        ));
        assert!(matches!(
            Cli::try_parse_from(["oah", "inspect", "support-desk/t"]).unwrap().cmd,
            Command::Inspect { .. }
        ));
        assert!(matches!(
            Cli::try_parse_from(["oah", "serve", "--demo"]).unwrap().cmd,
            Command::Serve { demo: true, .. }
        ));
        assert!(matches!(
            Cli::try_parse_from(["oah", "run", "support-desk", "-m", "hi"]).unwrap().cmd,
            Command::Run { .. }
        ));
    }
}
