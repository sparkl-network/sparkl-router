use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::warn;

use crate::protocol::InboundFrame;
use crate::state::{NodeId, RouterState};
use crate::tunnel::dispatch::forward_http_request;

#[derive(Clone, Default)]
pub struct ModelsCatalog {
    model_ids: Arc<RwLock<HashSet<String>>>,
    last_refresh: Arc<RwLock<Option<Instant>>>,
}

impl ModelsCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn contains(&self, model: &str) -> bool {
        self.model_ids.read().await.contains(model)
    }

    pub async fn list(&self) -> Vec<String> {
        self.model_ids.read().await.iter().cloned().collect()
    }

    pub async fn refresh_from_tunnels(&self, state: &RouterState) {
        let mut merged = HashSet::new();
        for (node_id, tunnel) in state.tunnels.iter() {
            match fetch_models_from_tunnel(&tunnel, Duration::from_secs(10)).await {
                Ok(ids) => {
                    tunnel
                        .model_count
                        .store(ids.len() as i64, std::sync::atomic::Ordering::Relaxed);
                    merged.extend(ids);
                }
                Err(e) => warn!(?node_id, %e, "failed to fetch models from tunnel"),
            }
        }
        *self.model_ids.write().await = merged;
        *self.last_refresh.write().await = Some(Instant::now());
    }
}

async fn fetch_models_from_tunnel(
    tunnel: &crate::state::NodeTunnel,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
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
    state.models.refresh_from_tunnels(&state).await;
    let ids = state.models.list().await;
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

pub fn node_id_key(id: &NodeId) -> String {
    format!("0x{}", hex::encode(id))
}
