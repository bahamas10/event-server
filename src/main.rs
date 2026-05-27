use std::sync::{Arc, RwLock};

use axum::{
    Router,
    extract::State,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as SerdeValue;
use time::UtcDateTime;
use uuid::Uuid;

/// Shared state for every incoming request
#[derive(Clone)]
struct AppState {
    events: Arc<RwLock<Vec<Event>>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
enum EventLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Critical,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Event {
    pub id: Uuid,
    pub created: UtcDateTime,
    pub message: String,
    pub level: EventLevel,
    /*
     * TODO
    pub component: String, "nginx"
    pub data: JsonValue, "arbitrary data"
    */
}

#[derive(Deserialize, Debug)]
struct CreateEventPayload {
    pub message: String,
    pub level: EventLevel,
}

/// POST /events
async fn post_events(
    State(state): State<AppState>,
    Json(payload): Json<CreateEventPayload>,
) -> Json<Event> {
    let ts = UtcDateTime::now();
    let event = Event {
        //id: Uuid::new_v7(ts), TODO: re-use the same timestamp
        id: Uuid::now_v7(),
        created: ts,
        message: payload.message,
        level: payload.level,
    };

    // write the event to our internal ring buffer ... TODO make this a ring
    // buffer
    {
        let mut events = state.events.write().expect("failed to acquire lock");
        events.push(event.clone());
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

    Json(events)
}

/// GET /ping
async fn get_ping(State(_state): State<AppState>) -> Json<SerdeValue> {
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
    let listen = "127.0.0.1:3000";

    let shared_state = AppState { events: Arc::new(RwLock::new(vec![])) };

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
