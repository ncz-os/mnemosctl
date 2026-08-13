use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mnemosctl::{
    Config, ImportOptions, SyncOptions, build_http_client, create_memory, format_peers, get_memory,
    health, import_jsonl_with_options, list_peers, pretty_json, search_memories,
    sync_from_host_with_page_size,
};
use std::io::{self, Read};
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "mnemosctl")]
#[command(about = "Desktop CLI for the MNEMOS memory system")]
struct Cli {
    /// TCP connection deadline in seconds.
    #[arg(long, global = true, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    connect_timeout_secs: u64,
    /// Maximum idle time while reading a response in seconds.
    #[arg(long, global = true, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    read_timeout_secs: u64,
    /// Total deadline for each HTTP request in seconds.
    #[arg(long, global = true, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..))]
    request_timeout_secs: u64,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// GET /health and pretty-print the JSON response.
    Health,
    /// Search MNEMOS memories.
    Search {
        query: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, default_value_t = false)]
        semantic: bool,
    },
    /// Create a memory from --content or stdin.
    Create {
        #[arg(long)]
        content: Option<String>,
        #[arg(long, default_value = "facts")]
        category: String,
    },
    /// Fetch one memory by ID.
    Get { id: String },
    /// Pull memories from a remote MNEMOS host into local sqlite.
    SyncFrom {
        host: String,
        /// Number of memories fetched and committed per page.
        // .range() lives on RangedU64ValueParser, not on the generic
        // value_parser!(usize), so the bound is expressed on u64 and narrowed
        // at the use site. usize here would not compile.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..=500))]
        page_size: u64,
        #[arg(long, value_name = "N", num_args = 0..=1, default_missing_value = "100")]
        progress: Option<usize>,
    },
    /// List federation peers.
    Peers,
    /// Import newline-delimited JSON memories.
    Import {
        file: PathBuf,
        #[arg(long, value_name = "N", num_args = 0..=1, default_missing_value = "100")]
        progress: Option<usize>,
        #[arg(long, default_value_t = false)]
        skip_bad: bool,
    },
    /// Print the resolved configuration.
    Config,
}

#[tokio::main]
async fn main() {
    init_tracing();

    if let Err(error) = run().await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load().context("load MNEMOS configuration")?;
    let client = build_http_client(
        Duration::from_secs(cli.connect_timeout_secs),
        Duration::from_secs(cli.read_timeout_secs),
        Duration::from_secs(cli.request_timeout_secs),
    )?;

    match cli.command {
        Commands::Health => {
            let response = health(&client, &config).await?;
            println!("{}", pretty_json(&response)?);
        }
        Commands::Search {
            query,
            limit,
            namespace,
            semantic,
        } => {
            let response = search_memories(
                &client,
                &config,
                &query,
                limit,
                namespace.as_deref(),
                semantic,
            )
            .await?;
            println!("{}", pretty_json(&response)?);
        }
        Commands::Create { content, category } => {
            let content = match content {
                Some(content) => content,
                None => read_stdin().context("read memory content from stdin")?,
            };
            let response = create_memory(&client, &config, &content, &category).await?;
            println!("{}", pretty_json(&response)?);
        }
        Commands::Get { id } => {
            let response = get_memory(&client, &config, &id).await?;
            println!("{}", pretty_json(&response)?);
        }
        Commands::SyncFrom {
            host,
            page_size,
            progress,
        } => {
            sync_from_host_with_page_size(
                &client,
                &config.api_key,
                &host,
                SyncOptions {
                    progress_every: progress,
                },
                page_size as usize,
            )
            .await?;
        }
        Commands::Peers => {
            let response = list_peers(&client, &config).await?;
            for line in format_peers(&response) {
                println!("{line}");
            }
        }
        Commands::Import {
            file,
            progress,
            skip_bad,
        } => {
            let result = import_jsonl_with_options(
                &client,
                &config,
                &file,
                ImportOptions {
                    skip_bad,
                    progress_every: progress,
                },
            )
            .await?;
            println!(
                "success={} skipped_existing={} fail={}",
                result.imported, result.skipped_existing, result.failed
            );
        }
        Commands::Config => {
            println!("base_url={}", config.base_url);
            println!("api_key={}", config.masked_api_key());
        }
    }

    Ok(())
}

fn read_stdin() -> Result<String> {
    let mut content = String::new();
    io::stdin()
        .read_to_string(&mut content)
        .context("read stdin")?;
    Ok(content)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
