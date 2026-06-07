pub mod record_usage_key;
pub mod registry;
pub mod session;
pub mod usage;
pub mod watcher;

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::Result;
use moka::future::Cache;
use tracing::{debug, warn};

use crate::config::ChainConfig;
use session::{fetch_session, ChainSession};

#[derive(Clone)]
pub struct ChainVerifier {
    chain: ChainConfig,
    cache: Cache<u64, ChainSession>,
}

impl ChainVerifier {
    pub fn new(chain: ChainConfig) -> Self {
        let ttl = Duration::from_secs(chain.session_cache_ttl_secs.max(1));
        let cache = Cache::builder().time_to_live(ttl).build();
        Self { chain, cache }
    }

    pub fn chain_config(&self) -> &ChainConfig {
        &self.chain
    }

    pub async fn get_session(&self, session_id: u64) -> Result<ChainSession> {
        if let Some(cached) = self.cache.get(&session_id).await {
            return Ok(cached);
        }
        let session = fetch_session(&self.chain, session_id).await?;
        if session.is_open() {
            self.cache.insert(session_id, session.clone()).await;
        }
        Ok(session)
    }

    pub async fn warm_session(&self, session_id: u64) {
        match fetch_session(&self.chain, session_id).await {
            Ok(s) if s.is_open() => {
                self.cache.insert(session_id, s).await;
                debug!(session_id, "warmed session cache");
            }
            Ok(_) => {}
            Err(e) => warn!(%e, session_id, "failed to warm session"),
        }
    }

    pub async fn evict_session(&self, session_id: u64) {
        self.cache.invalidate(&session_id).await;
    }

    pub async fn is_node_registered(&self, node_id: &[u8; 32]) -> Result<bool> {
        if !self.chain.enabled {
            return Ok(true);
        }
        registry::is_node_registered(&self.chain, node_id).await
    }

    pub async fn latest_block(&self) -> Result<u64> {
        registry::fetch_latest_block(&self.chain).await
    }
}

pub fn session_is_open(s: &ChainSession) -> bool {
    s.is_open()
}

pub fn zero_address() -> Address {
    Address::ZERO
}
