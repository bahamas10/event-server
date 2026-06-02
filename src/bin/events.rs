use anyhow::Result;
use clap::{Parser, Subcommand};
use gethostname::gethostname;

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

fn emit_subcommand(url: &str, cmd: EmitCommand) -> Result<()> {
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
    let client = reqwest::blocking::Client::new();
    let event: Event = client.post(&url).json(&body).send()?.json()?;

    println!("emited event!");
    println!("{:#?}", event);

    Ok(())
}

fn tail_subcommand(url: &str, _cmd: TailCommand) -> Result<()> {
    let url = format!("{}/events", url);
    let events: Vec<Event> = reqwest::blocking::get(&url)?.json()?;

    for event in events {
        println!("{} [{}]: {}", event.source, event.level, event.message);
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let url = &args.url;

    match args.command {
        Commands::Emit(cmd) => emit_subcommand(url, cmd),
        Commands::Tail(cmd) => tail_subcommand(url, cmd),
    }
}
