use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use time::UtcDateTime;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum EventLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Critical,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    // Fields generated automatically by the server
    /// UUID (v7) for the event
    pub id: Uuid,

    /// Timestamp for the event
    pub created: UtcDateTime,

    // Fields taken from the client
    /// Name of component (like "nginx" or "nagios")
    pub component: String,

    /// Severity level
    pub level: EventLevel,

    /// Any string message
    pub message: String,

    /// Any arbitrary JSON data
    pub data: JsonValue,
}
