/*!
 * Basic syslog-inspired event server service
 *
 * Author: Dave Eddy <ysap@daveeddy.com>
 * Date: May 27, 2026
 * License: MIT
 */

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use axum::{
    Router,
    extract::State,
    response::Json,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use time::UtcDateTime;
use uuid::Uuid;

use event_server::{Event, EventLevel};

/// Shared state for every incoming request
#[derive(Clone)]
struct AppState {
    config: Config,
    events: Arc<RwLock<VecDeque<Event>>>,
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

    // TODO: dispatch this new event to exec queue (need a broadcast stream
    // probably)

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
#[allow(unused)]
async fn get_event_stream(State(_state): State<AppState>) {
    // TODO: return a stream of SSE somehow, connect a live stream
}

#[tokio::main]
async fn main() {
    // TODO: make this config / CLI args?
    let listen = "127.0.0.1:3000";
    let config = Config { max_events: 5 };

    // TODO: wrap this data type and have it encapsulate the max size
    let events = VecDeque::new();
    let shared_state =
        AppState { events: Arc::new(RwLock::new(events)), config };

    let app = Router::new()
        .route("/ping", get(get_ping))
        .route("/events", get(get_events))
        .route("/events", post(post_events))
        .with_state(shared_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    println!("Listening: http://{}", listen);
    axum::serve(listener, app).await.unwrap();
}
