use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use alloy_primitives::{Address, FixedBytes};
use dashmap::DashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::chain::ChainVerifier;
use crate::config::Config;
use crate::consumer::models::ModelsCatalog;
use crate::protocol::{InboundFrame, RouterToNodeFrame};
use crate::tunnel::registry::TunnelRegistry;

pub type NodeId = [u8; 32];

/// Per-provider WSS tunnel (no WebSocket stored here — only channels).
pub struct NodeTunnel {
    pub node_id: NodeId,
    pub sender: mpsc::Sender<RouterToNodeFrame>,
    pub pending: Arc<DashMap<Uuid, mpsc::Sender<InboundFrame>>>,
    pub connected_at: Instant,
    pub last_pong_at: Arc<AtomicI64>,
    /// Last time models were re-fetched from this tunnel (pong-driven refresh).
    pub last_models_refresh_at: Arc<AtomicI64>,
    pub model_count: Arc<AtomicI64>,
    shutdown: mpsc::Sender<()>,
}

impl NodeTunnel {
    pub fn new(
        node_id: NodeId,
        sender: mpsc::Sender<RouterToNodeFrame>,
        shutdown: mpsc::Sender<()>,
    ) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            node_id,
            sender,
            pending: Arc::new(DashMap::new()),
            connected_at: Instant::now(),
            last_pong_at: Arc::new(AtomicI64::new(now)),
            last_models_refresh_at: Arc::new(AtomicI64::new(0)),
            model_count: Arc::new(AtomicI64::new(0)),
            shutdown,
        }
    }

    pub fn touch_pong(&self) {
        self.last_pong_at
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
    }

    pub fn last_pong_timestamp(&self) -> i64 {
        self.last_pong_at.load(Ordering::Relaxed)
    }

    pub fn in_flight_count(&self) -> usize {
        self.pending.len()
    }

    pub fn signal_shutdown(&self) {
        let _ = self.shutdown.try_send(());
    }

    pub async fn send_frame(&self, frame: RouterToNodeFrame) -> anyhow::Result<()> {
        self.sender
            .send(frame)
            .await
            .map_err(|_| anyhow::anyhow!("tunnel send channel closed"))
    }
}

#[derive(Clone)]
pub struct RouterState {
    pub config: Arc<Config>,
    pub started_at: Instant,
    pub tunnels: TunnelRegistry,
    pub chain: Arc<ChainVerifier>,
    pub models: Arc<ModelsCatalog>,
    pub metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
}

/// Authenticated escrow session attached to consumer requests.
#[derive(Clone, Debug)]
pub struct AuthenticatedSession {
    pub session_id: u64,
    pub user: Address,
    pub node_id: FixedBytes<32>,
}

impl RouterState {
    pub fn new(
        config: Config,
        chain: ChainVerifier,
        models: ModelsCatalog,
        metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    ) -> Self {
        Self {
            config: Arc::new(config),
            started_at: Instant::now(),
            tunnels: TunnelRegistry::new(),
            chain: Arc::new(chain),
            models: Arc::new(models),
            metrics_handle,
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn tunnel_count(&self) -> usize {
        self.tunnels.len()
    }
}
