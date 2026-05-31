use anyhow::Result;
use clap::{Parser, Subcommand};

use event_server::Event;

/// Dave's Event Server CLI Client
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Create a new event
    Create(CreateCommand),

    /// View or stream recent events
    Tail(TailCommand),
}

#[derive(Debug, Parser)]
struct CreateCommand {
    /// Component name
    #[arg(short, long, default_value = "CLI")]
    component: String,

    /// Severity level
    #[arg(short, long, default_value = "info")]
    level: String, // TODO this should use the libraries enum

    /// Message data
    message: String,
}

#[derive(Debug, Parser)]
struct TailCommand {}

// TODO: use the struct that is defined in the server code
#[derive(serde::Serialize, Debug)]
struct CreateEventPayload {
    pub component: String,
    pub level: String, // TODO: lmao
    pub message: String,
}

/*
 * Todo List
 *
 * 1. Don't hardcode URL
 *  - should this be a config file? env var? CLI arg? combo of all?
 */

fn create_subcommand(cmd: CreateCommand) -> Result<()> {
    let body = CreateEventPayload {
        component: cmd.component,
        level: cmd.level,
        message: cmd.message,
    };

    let client = reqwest::blocking::Client::new();
    let event: Event = client
        .post("http://localhost:3000/events")
        .json(&body)
        .send()?
        .json()?;

    println!("created event!");
    println!("{:#?}", event);

    Ok(())
}

fn tail_subcommand(_cmd: TailCommand) -> Result<()> {
    let events: Vec<Event> =
        reqwest::blocking::get("http://localhost:3000/events")?.json()?;

    for event in events {
        // TODO: event should implement Display
        println!("{} [{:?}]: {}", event.component, event.level, event.message);
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Create(cmd) => create_subcommand(cmd),
        Commands::Tail(cmd) => tail_subcommand(cmd),
    }
}
