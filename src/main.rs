/*!
 * Basic syslog-inspired event server service
 *
 * Author: Dave Eddy <ysap@daveeddy.com>
 * Date: May 27, 2026
 * License: MIT
 */

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::env;
use std::fs;
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::State,
    response::Json,
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    routing::{get, post},
};
use futures_util::stream::Stream;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use time::UtcDateTime;
use tokio::sync::{RwLock, broadcast};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use event_server::{Event, EventLevel};

mod config;
use config::Config;

/// Shared state for every incoming request
#[derive(Clone)]
struct AppState {
    config: Config,
    events: Arc<RwLock<VecDeque<Event>>>,
    tx: broadcast::Sender<Event>,
}

#[derive(Deserialize, Debug)]
struct CreateEventPayload {
    pub component: String,
    pub level: EventLevel,
    pub message: String,
    pub data: Option<JsonValue>,
}

/// POST /events
async fn post_events(
    State(state): State<AppState>,
    Json(payload): Json<CreateEventPayload>,
) -> Json<Event> {
    let ts = UtcDateTime::now();
    let id = Uuid::new_v7(uuid::Timestamp::from_unix(
        uuid::timestamp::context::NoContext,
        ts.unix_timestamp() as u64,
        ts.nanosecond(),
    ));

    // create a new event
    let event = Event {
        id,
        created: ts,
        component: payload.component,
        message: payload.message,
        level: payload.level,
        data: payload.data.unwrap_or_default(),
    };

    // write the event to our internal ring buffer
    {
        let mut events = state.events.write().await;
        events.push_back(event.clone());

        // shrink the ring buffer here if it is too big
        while events.len() > state.config.max_events {
            let _ = events.pop_front();
        }
    }

    // broadcast the new event exists
    let n = state.tx.send(event.clone()).expect("failed to broadcast event");
    println!("broadcasted new event to {} subscribers", n);

    // return the event to the client
    Json(event)
}

/// GET /events
async fn get_events(State(state): State<AppState>) -> Json<Vec<Event>> {
    let events = {
        let events = state.events.read().await;
        events.clone()
    };

    Json(events.into())
}

/// GET /ping
async fn get_ping(State(_state): State<AppState>) -> Json<JsonValue> {
    println!("index handler hit");

    Json(serde_json::json!("pong"))
}

/// GET /event-stream
async fn get_event_stream(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    // create a new event receiver
    let rx = state.tx.subscribe();
    let stream = BroadcastStream::new(rx).map(|e| {
        let e = e.unwrap();
        let event =
            SseEvent::default().id(e.id.to_string()).json_data(e).unwrap();
        Ok(event)
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn persist_file_task(
    mut rx: broadcast::Receiver<Event>,
    file: String,
    events: Arc<RwLock<VecDeque<Event>>>,
) -> ! {
    println!("[persist-task] started");
    loop {
        let _event =
            rx.recv().await.expect("[persist-task] failed to receive event");

        // serialize the events to disk if set
        let s = {
            let items = events.read().await;
            serde_json::to_string(&*items)
                .expect("failed to JSON stringify events")
        };
        fs::write(&file, s).expect("failed to serialize data to disk");
        println!("[persist-task] serialized events to {}", file);
    }
}

async fn execute_program_task(
    mut rx: broadcast::Receiver<Event>,
    prog: String,
) -> ! {
    println!("[exec-task] started");

    let env: HashMap<String, String> = env::vars()
        .filter(|(k, _)| k == "TERM" || k == "TZ" || k == "LANG" || k == "PATH")
        .collect();

    loop {
        let event =
            rx.recv().await.expect("[exec-task] failed to receive event");

        println!("[exec-task] {}", prog);

        let mut env = env.clone();
        env.insert("EVENT_ID".into(), event.id.to_string());
        env.insert("EVENT_COMPONENT".into(), event.component);
        env.insert("EVENT_LEVEL".into(), format!("{:?}", event.level)); // TODO not this
        // lol
        env.insert("EVENT_MESSAGE".into(), event.message);
        env.insert("EVENT_DATA".into(), event.data.to_string());

        let output = match Command::new(&prog).env_clear().envs(&env).output() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to run: {}", prog);
                eprintln!("{:#?}", e);
                continue;
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("[exec-task] (finish): {}\n{}", prog, stdout);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // read config
    let config: Config = {
        let s = fs::read_to_string("config.toml").context("failed to read config")?;
        toml::from_str(&s).context("failed to parse config toml")?
    };
    println!("read config: {:#?}", config);

    // initalize events
    let events: VecDeque<Event> = {
        if let Some(file) = &config.persist.file {
            // JSON file specified in the config - read it
            println!("reading cached events in {}", file);
            let s = fs::read_to_string(file).context("failed to read cached events")?;
            serde_json::from_str(&s).context("failed to parse cached events as JSON")?
        } else {
            // start with an empty cache
            println!("persist file not set - not reading cached data");
            VecDeque::new()
        }
    };
    println!("have {} cached events", events.len());

    // create the broadcast channel to keep track of events internally
    let (tx, _rx) =
        broadcast::channel(config.internal.tokio_broadcast_channel_size);
    let events = Arc::new(RwLock::new(events));

    // create task to serialize persistent file on new event
    if let Some(file) = &config.persist.file {
        let rx = tx.subscribe();
        tokio::spawn(persist_file_task(rx, file.to_string(), events.clone()));
    }

    // create a task to fork and exec when new event is seen
    if let Some(prog) = &config.exec_program {
        let rx = tx.subscribe();
        tokio::spawn(execute_program_task(rx, prog.to_string()));
    }

    // create the webserver
    let shared_state =
        AppState { events: events.clone(), config: config.clone(), tx };
    let app = Router::new()
        .route("/ping", get(get_ping))
        .route("/events", get(get_events))
        .route("/events", post(post_events))
        .route("/event-stream", get(get_event_stream))
        .with_state(shared_state);

    // start the webserver and block forever
    let listener = tokio::net::TcpListener::bind(&config.http_server.listen)
        .await?;
    println!("listening: http://{}", config.http_server.listen);
    axum::serve(listener, app).await?;

    unreachable!("HTTP server died?!")
}
