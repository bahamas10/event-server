use axum::{
    Router,
    extract::State,
    response::Json,
    routing::{get, post},
};
use std::sync::{Arc, RwLock};
use serde_json::Value as SerdeValue;

#[derive(Clone)]
struct AppState {
    events: Arc<RwLock<Vec<Event>>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Event {
    pub message: String,
    pub level: String,
}

#[derive(serde::Deserialize, Debug)]
struct CreateEventPayload {
    pub message: String,
    pub level: String,
}

/// POST /events
async fn post_events(
    State(state): State<AppState>,
    Json(payload): Json<CreateEventPayload>
) -> String {
    let event = Event {
        message: payload.message,
        level: payload.level,
        // TODO: add a UUID field here and return it to the user
    };

    // write the event to our internal ring buffer ... TODO make this a ring
    // buffer
    {
        let mut events = state.events.write().expect("failed to acquire lock");
        events.push(event);
    }


    // TODO: dispatch this new event to exec queue

    // TODO: Return the UUID
    "UUID here lol".into()
}

/// GET /events
async fn get_events(
    State(state): State<AppState>,
) -> String {
    // TODO: this should probably not do the serialization here and return a
    // string? or should it? idk lol.  maybe just set the content-type somehow.

    let events = state.events.read().expect("failed to acquire lock");
    serde_json::to_string_pretty(&*events).expect("failed to serialize")
}

/// GET /
async fn get_index(
    State(_state): State<AppState>,
) -> Json<SerdeValue> {
    println!("index handler hit");

    Json(serde_json::json!({"name":"dave"}))
}

#[tokio::main]
async fn main() {
    let listen = "127.0.0.1:3000";

    let shared_state = AppState {
        events: Arc::new(RwLock::new(vec![])),
    };

    let app = Router::new()
        .route("/", get(get_index))
        .route("/events", get(get_events))
        .route("/events", post(post_events))
        .with_state(shared_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    println!("Listening: http://{}", listen);
    axum::serve(listener, app).await.unwrap();
}
