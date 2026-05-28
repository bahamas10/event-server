# ⚡️ event-server

A fast, lightweight, syslog-inspired event server service built in Rust.

`event-server` acts as a centralized hub for tracking system events, application logs, and notifications. It provides a RESTful API for submitting and retrieving events, alongside a real-time Server-Sent Events (SSE) stream for immediate broadcasting to subscribers.

## Features

* **Real-time Broadcasting:** Native support for Server-Sent Events (SSE) to stream events to subscribers the moment they occur.
* **In-Memory Ring Buffer:** Maintains a configurable maximum number of recent events in memory (default: 5) to prevent unbounded memory growth.
* **Time-Sortable IDs:** Utilizes UUIDv7 for native, time-sortable event identifiers.
* **Structured Logging:** Events support arbitrary JSON data payloads for highly flexible telemetry.
* **High Performance:** Built on `axum` and `tokio` for robust, asynchronous I/O.

---

## Getting Started

### Prerequisites

* Rust (Edition 2024)
* Cargo

### Running the Server

To start the server, clone the repository and run:

bash
cargo run

By default, the server will listen on `http://127.0.0.1:3000`.

### Running the CLI Client

A basic CLI client is included to fetch and print the current event buffer:

bash
cargo run --bin events

---

## API Reference

### `POST /events`

Creates a new event and broadcasts it to all SSE subscribers.

**Request Body (JSON):**
json
{
  "component": "nginx",
  "level": "info",
  "message": "Worker process started",
  "data": {
    "pid": 1234,
    "user": "www-data"
  }
}

*Note: `data` is an optional field and defaults to an empty object if omitted.*

### `GET /events`

Returns an array of the most recent events stored in the in-memory ring buffer.

### `GET /event-stream`

Opens a Server-Sent Events (SSE) connection. Clients connecting to this endpoint will receive real-time JSON payloads of events the exact moment they are `POST`ed to the server.

### `GET /ping`

Standard health-check endpoint. Returns `"pong"`.

---

## Data Models

### Event Levels

Severity levels dictate the urgency of the event. Valid levels (case-insensitive in JSON) are:

* `trace`
* `debug`
* `info`
* `warn`
* `critical`

### Event Object

When retrieving an event (or listening via SSE), the server generates the `id` and `created` timestamp automatically:

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | UUIDv7 | Auto-generated, time-sortable unique identifier. |
| `created` | UtcDateTime | Auto-generated timestamp of when the server received the event. |
| `component` | String | The originating system or service (e.g., "db", "nagios"). |
| `level` | String | The severity of the event. |
| `message` | String | A human-readable description. |
| `data` | JSON | Any arbitrary JSON data attached by the client. |

---

## Roadmap & TODOs

### More Advanced Features

* CLI support for `POST`ing (creating) events directly.
* Support for running/executing an arbitrary program when a new event is fired.
* Configurable CLI arguments for listen address (`127.0.0.1:3000`) and max event limits.

### Data Persistence

* Cache events to disk.

* Read events from disk on application startup.

### Code Health & Security

* Limit `POST` payload size to prevent abuse.

* Remove all `unwrap()` and `expect()` calls and replace with proper error handling.

---

## License

MIT
