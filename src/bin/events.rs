use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use gethostname::gethostname;
use reqwest_sse::EventSource;
use tokio_stream::StreamExt;

use event_server::{EmitEventPayload, Event, EventLevel};

/// Dave's Event Server CLI Client
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Event daemon url
    #[arg(short, long, default_value = "http://localhost:3000")]
    url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Emit a new event
    Emit(EmitCommand),

    /// View or stream recent events
    Tail(TailCommand),
}

#[derive(Debug, Parser)]
struct EmitCommand {
    /// Component name
    #[arg(short, long, default_value = "CLI")]
    source: String,

    /// Hostname
    #[arg(short = 'H', long)]
    hostname: Option<String>,

    /// Severity level
    #[arg(short, long, default_value_t = EventLevel::Info)]
    level: EventLevel,

    /// Message data
    message: String,
}

#[derive(Debug, Parser)]
struct TailCommand {}

/*
 * Todo List
 *
 * 1. Don't hardcode URL
 *  - should this be a config file? env var? CLI arg? combo of all?
 */

async fn emit_subcommand(url: &str, cmd: EmitCommand) -> Result<()> {
    // get hostname of system if not provided by the user
    let hostname = match cmd.hostname {
        Some(n) => n,
        None => gethostname().into_string().expect("failed to gethostname"),
    };

    let body = EmitEventPayload {
        hostname,
        source: cmd.source,
        level: cmd.level,
        message: cmd.message,
        data: None,
    };

    // TODO: clean this up
    let url = format!("{}/events", url);
    let client = reqwest::Client::new();
    let event: Event =
        client.post(&url).json(&body).send().await?.json().await?;

    println!("emited event!");
    println!("{:#?}", event);

    Ok(())
}

async fn tail_subcommand(url: &str, _cmd: TailCommand) -> Result<()> {
    let url = format!("{}/event-stream", url);

    let mut events = reqwest::get(&url)
        .await
        .context("failed to request URL")?
        .events()
        .await
        .context("failed to parse response as SSE")?;

    while let Some(Ok(event)) = events.next().await {
        let event: Event = serde_json::from_str(&event.data)
            .context("failed to parse JSON data")?;
        println!("{} [{}]: {}", event.source, event.level, event.message);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let url = &args.url;

    match args.command {
        Commands::Emit(cmd) => emit_subcommand(url, cmd).await,
        Commands::Tail(cmd) => tail_subcommand(url, cmd).await,
    }
}
