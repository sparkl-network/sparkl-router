use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub chain: ChainConfig,
    pub node_auth: NodeAuthConfig,
    pub metrics: MetricsConfig,
    pub portal: PortalConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    #[serde(default = "default_router_url")]
    pub router_url: String,
}

fn default_router_url() -> String {
    "http://127.0.0.1:3001".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    pub rpc_url: String,
    pub registry_contract: String,
    pub escrow_contract: String,
    #[serde(default = "default_session_cache_ttl")]
    pub session_cache_ttl_secs: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_session_cache_ttl() -> u64 {
    12
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeAuthConfig {
    #[serde(default = "default_challenge_window")]
    pub challenge_window_blocks: u64,
    #[serde(default = "default_ping_interval")]
    pub ping_interval_secs: u64,
    #[serde(default = "default_pong_timeout")]
    pub pong_timeout_secs: u64,
}

fn default_challenge_window() -> u64 {
    10
}

fn default_ping_interval() -> u64 {
    30
}

fn default_pong_timeout() -> u64 {
    10
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    pub bind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PortalConfig {
    pub admin_token: String,
    #[serde(default = "default_stale_threshold")]
    pub stale_threshold_secs: u64,
    /// Minimum seconds between per-node `/v1/models` re-fetches on WSS pong (heartbeat).
    #[serde(default = "default_models_refresh_on_pong")]
    pub models_refresh_on_pong_secs: u64,
}

fn default_stale_threshold() -> u64 {
    40
}

fn default_models_refresh_on_pong() -> u64 {
    60
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path))
            .add_source(config::Environment::with_prefix("SPARKL_ROUTER").separator("__"))
            .build()
            .context("failed to load config")?;
        settings
            .try_deserialize()
            .context("failed to deserialize config")
    }
}
