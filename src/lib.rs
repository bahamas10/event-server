use std::fmt;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use time::UtcDateTime;
use uuid::Uuid;

#[derive(Serialize, Deserialize, ValueEnum, Debug, Clone)]
#[serde(rename_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum EventLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Critical,
}

impl fmt::Display for EventLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    // Fields generated automatically by the server
    /// UUID (v7) for the event
    pub id: Uuid,

    /// Timestamp for the event
    pub created: UtcDateTime,

    // Fields taken from the client
    /// Hostname of event origination
    pub hostname: String,

    /// Name of source (like "nginx" or "nagios")
    pub source: String,

    /// Severity level
    pub level: EventLevel,

    /// Any string message
    pub message: String,

    /// Any arbitrary JSON data
    pub data: JsonValue,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EmitEventPayload {
    pub source: String,
    pub hostname: String,
    pub level: EventLevel,
    pub message: String,
    pub data: Option<JsonValue>,
}
