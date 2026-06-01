use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::protocol::InboundFrame;
use crate::state::{NodeId, NodeTunnel, RouterState};

const MODELS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
pub struct ModelsCatalog {
    /// Per-node model ids (for eviction on disconnect).
    by_node: Arc<RwLock<HashMap<NodeId, HashSet<String>>>>,
    /// Union of all models across connected nodes (served to clients).
    union: Arc<RwLock<HashSet<String>>>,
    last_refresh: Arc<RwLock<Option<Instant>>>,
}

impl ModelsCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn contains(&self, model: &str) -> bool {
        self.union.read().await.contains(model)
    }

    pub async fn list(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.union.read().await.iter().cloned().collect();
        ids.sort();
        ids
    }

    /// OpenAI-style list from the in-memory cache only (no tunnel fan-out).
    pub async fn list_cached_json(&self) -> Value {
        let ids = self.list().await;
        let data: Vec<Value> = ids
            .into_iter()
            .map(|id| {
                json!({
                    "id": id,
                    "object": "model",
                    "created": 0,
                    "owned_by": "sparkl"
                })
            })
            .collect();
        json!({
            "object": "list",
            "data": data
        })
    }

    /// Register or replace models advertised by a connected node.
    pub async fn upsert_node(&self, node_id: NodeId, model_ids: HashSet<String>) {
        let count = model_ids.len();
        self.by_node.write().await.insert(node_id, model_ids);
        self.rebuild_union().await;
        info!(?node_id, model_count = count, "models catalog updated for node");
    }

    /// Remove a node's models when its tunnel disconnects.
    pub async fn remove_node(&self, node_id: &NodeId) {
        if self.by_node.write().await.remove(node_id).is_some() {
            self.rebuild_union().await;
            info!(?node_id, "removed node from models catalog");
        }
    }

    async fn rebuild_union(&self) {
        let by_node = self.by_node.read().await;
        let mut merged = HashSet::new();
        for ids in by_node.values() {
            merged.extend(ids.iter().cloned());
        }
        *self.union.write().await = merged;
        *self.last_refresh.write().await = Some(Instant::now());
    }

    /// Fetch `/v1/models` from one tunnel and update the cache (connect + heartbeat).
    pub async fn refresh_tunnel(&self, node_id: NodeId, tunnel: &NodeTunnel) {
        match fetch_models_from_tunnel(tunnel, MODELS_FETCH_TIMEOUT).await {
            Ok(ids) => {
                tunnel
                    .model_count
                    .store(ids.len() as i64, Ordering::Relaxed);
                let set: HashSet<String> = ids.into_iter().collect();
                self.upsert_node(node_id, set).await;
            }
            Err(e) => warn!(?node_id, %e, "failed to refresh models from tunnel"),
        }
    }

    /// Background / admin: refresh every connected tunnel (optional full rebuild).
    pub async fn refresh_all_tunnels(&self, state: &RouterState) {
        for (node_id, tunnel) in state.tunnels.iter() {
            self.refresh_tunnel(node_id, &tunnel).await;
        }
    }
}

/// After WSS `ready`, populate catalog for this node.
pub fn spawn_tunnel_models_refresh(state: RouterState, node_id: NodeId, tunnel: Arc<NodeTunnel>) {
    tokio::spawn(async move {
        state.models.refresh_tunnel(node_id, &tunnel).await;
    });
}

/// On `pong` heartbeat, throttle model re-fetch per node.
pub fn maybe_refresh_on_pong(
    state: RouterState,
    node_id: NodeId,
    tunnel: Arc<NodeTunnel>,
    min_interval_secs: u64,
) {
    let now = chrono::Utc::now().timestamp();
    let last = tunnel.last_models_refresh_at.load(Ordering::Relaxed);
    if last > 0 && (now - last) < min_interval_secs as i64 {
        return;
    }
    tunnel
        .last_models_refresh_at
        .store(now, Ordering::Relaxed);
    debug!(?node_id, "refreshing models catalog on pong");
    tokio::spawn(async move {
        state.models.refresh_tunnel(node_id, &tunnel).await;
    });
}

async fn fetch_models_from_tunnel(
    tunnel: &NodeTunnel,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
    use crate::tunnel::dispatch::forward_http_request;

    let pending = forward_http_request(
        tunnel,
        "GET",
        "/v1/models",
        json!({}),
        None,
        timeout,
    )
    .await?;

    let mut rx = pending.rx;
    let result = tokio::time::timeout(timeout, async {
        let mut body = String::new();
        while let Some(frame) = rx.recv().await {
            match frame {
                InboundFrame::Response { status, headers: _ } if status != 200 => {
                    anyhow::bail!("models returned status {status}");
                }
                InboundFrame::Chunk(data) => body.push_str(&data),
                InboundFrame::End { status: 200 } => break,
                InboundFrame::End { status } => anyhow::bail!("models end status {status}"),
                InboundFrame::Error { message, .. } => anyhow::bail!("models error: {message}"),
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(body)
    })
    .await;

    let body = match result {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => return Err(e),
        Err(_) => anyhow::bail!("models request timed out"),
    };

    parse_model_ids(&body)
}

fn parse_model_ids(body: &str) -> anyhow::Result<Vec<String>> {
    let v: Value = serde_json::from_str(body)?;
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow::anyhow!("invalid models response"))?;
    let mut ids = Vec::new();
    for item in data {
        if let Some(id) = item.get("id").and_then(|x| x.as_str()) {
            ids.push(id.to_string());
        }
    }
    Ok(ids)
}

pub async fn list_models_handler(state: RouterState) -> Value {
    state.models.list_cached_json().await
}

pub fn node_id_key(id: &NodeId) -> String {
    format!("0x{}", hex::encode(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn catalog_upsert_remove_rebuilds_union() {
        let cat = ModelsCatalog::new();
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];
        let mut a = HashSet::new();
        a.insert("gpt-4o".into());
        cat.upsert_node(n1, a).await;
        assert!(cat.contains("gpt-4o").await);

        let mut b = HashSet::new();
        b.insert("llama3:8b".into());
        cat.upsert_node(n2, b).await;
        assert!(cat.contains("llama3:8b").await);

        cat.remove_node(&n1).await;
        assert!(!cat.contains("gpt-4o").await);
        assert!(cat.contains("llama3:8b").await);
    }
}
