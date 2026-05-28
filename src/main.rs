/*!
 * Basic syslog-inspired event server service
 *
 * Author: Dave Eddy <ysap@daveeddy.com>
 * Date: May 27, 2026
 * License: MIT
 */

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, RwLock};

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
use tokio::sync::broadcast;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use event_server::{Event, EventLevel};

/// Shared state for every incoming request
#[derive(Clone)]
struct AppState {
    config: Config,
    events: Arc<RwLock<VecDeque<Event>>>,
    tx: broadcast::Sender<Event>,
}

#[derive(Clone)]
struct Config {
    max_events: usize,
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
        let mut events = state.events.write().expect("failed to acquire lock");
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
        let events = state.events.read().expect("failed to acquire lock");
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
        let foo =
            SseEvent::default().id(e.id.to_string()).json_data(e).unwrap();
        Ok(foo)
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[tokio::main]
async fn main() -> ! {
    // TODO: make this config / CLI args?
    let listen = "127.0.0.1:3000";
    let config = Config { max_events: 5 };

    // TODO: wrap this data type and have it encapsulate the max size
    let events = VecDeque::new();
    let (tx, mut rx) = broadcast::channel(16); // TODO: 16? lol sure
    let shared_state =
        AppState { events: Arc::new(RwLock::new(events)), config, tx };

    tokio::spawn(async move {
        loop {
            let event = rx.recv().await.unwrap();
            println!("got event: {:?}", event);

            // TODO: fork and exec when new event is seen (optionally)
        }
    });

    let app = Router::new()
        .route("/ping", get(get_ping))
        .route("/events", get(get_events))
        .route("/events", post(post_events))
        .route("/event-stream", get(get_event_stream))
        .with_state(shared_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    println!("Listening: http://{}", listen);
    axum::serve(listener, app).await.unwrap();

    unreachable!("HTTP server died?!")
}
