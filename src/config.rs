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
    #[serde(default)]
    pub capacity: CapacityConfig,
    #[serde(default)]
    pub settlement: SettlementConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    #[serde(default = "default_router_url")]
    pub router_url: String,
    /// Max wait for a provider node to complete a forwarded `/v1/chat/completions` request.
    #[serde(default = "default_upstream_timeout_secs")]
    pub upstream_timeout_secs: u64,
}

fn default_upstream_timeout_secs() -> u64 {
    120
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
    #[serde(default = "default_ping_interval")]
    pub ping_interval_secs: u64,
    #[serde(default = "default_pong_timeout")]
    pub pong_timeout_secs: u64,
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

#[derive(Debug, Clone, Deserialize)]
pub struct CapacityConfig {
    /// Max queued waiters as a fraction of declared per-model concurrency (1.0 = 100%).
    #[serde(default = "default_queue_depth_ratio")]
    pub queue_depth_ratio: f64,
    /// Max time a request waits in the capacity queue before HTTP 429.
    #[serde(default = "default_queue_wait_timeout_secs")]
    pub queue_wait_timeout_secs: u64,
}

fn default_queue_depth_ratio() -> f64 {
    1.0
}

fn default_queue_wait_timeout_secs() -> u64 {
    60
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            queue_depth_ratio: default_queue_depth_ratio(),
            queue_wait_timeout_secs: default_queue_wait_timeout_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SettlementConfig {
    /// Directory for persisted router keys (`record-usage-key.json`). Default: `<config-dir>/data`.
    #[serde(default)]
    pub data_dir: String,
    #[serde(default)]
    pub enabled: bool,
    /// Optional override for the `recordUsageRole` key. When empty, load or generate `record-usage-key.json` under `data_dir`.
    #[serde(default)]
    pub record_usage_private_key: String,
    /// Registry owner key used once to call `setRecordUsage` when the on-chain role differs from this router's key.
    #[serde(default)]
    pub registry_owner_private_key: String,
    #[serde(default = "default_true")]
    pub record_usage_enabled: bool,
    #[serde(default = "default_record_usage_token_chunk")]
    pub record_usage_token_chunk: u64,
    #[serde(default = "default_record_usage_flush_interval_secs")]
    pub record_usage_flush_interval_secs: u64,
    #[serde(default = "default_true")]
    pub enforce_session_budget: bool,
}

fn default_record_usage_token_chunk() -> u64 {
    10_000
}

fn default_record_usage_flush_interval_secs() -> u64 {
    60
}

impl Default for SettlementConfig {
    fn default() -> Self {
        Self {
            data_dir: String::new(),
            enabled: false,
            record_usage_private_key: String::new(),
            registry_owner_private_key: String::new(),
            record_usage_enabled: true,
            record_usage_token_chunk: default_record_usage_token_chunk(),
            record_usage_flush_interval_secs: default_record_usage_flush_interval_secs(),
            enforce_session_budget: true,
        }
    }
}

impl SettlementConfig {
    pub fn is_active(&self, chain_enabled: bool) -> bool {
        chain_enabled
            && self.enabled
            && self.record_usage_enabled
            && !self.record_usage_private_key.trim().is_empty()
    }
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
