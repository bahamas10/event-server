use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub max_events: usize,
    pub exec_program: Option<String>,
    pub log_level: String,
    pub persist: PersistConfig,
    pub http_server: HttpServerConfig,
    pub internal: InternalConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersistConfig {
    pub file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpServerConfig {
    pub listen: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InternalConfig {
    pub tokio_broadcast_channel_size: usize,
}
