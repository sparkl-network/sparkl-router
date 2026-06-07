use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::node_auth::parse_node_id_hex;
use crate::protocol::InboundFrame;
use crate::state::{NodeId, NodeTunnel, RouterState};
use crate::tunnel::registry::TunnelRegistry;

use crate::capacity::{CapacityTracker, ModelCapacityKey};

use super::offerings::{build_offering_with_load, tunnel_status_for_pong, ProviderOffering};

const MODELS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
pub struct ModelsCatalog {
    /// Per-node full model objects keyed by model id.
    by_node: Arc<RwLock<HashMap<NodeId, HashMap<String, Value>>>>,
    /// Merged union across nodes (served to clients).
    union: Arc<RwLock<HashMap<String, Value>>>,
    /// Flat per-node model offerings for catalog queries.
    offerings: Arc<RwLock<Vec<ProviderOffering>>>,
    last_refresh: Arc<RwLock<Option<Instant>>>,
}

impl ModelsCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn contains(&self, model: &str) -> bool {
        self.union.read().await.contains_key(model)
    }

    pub async fn list(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.union.read().await.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// OpenAI-style list from the in-memory cache only (no tunnel fan-out).
    pub async fn list_cached_json(&self) -> Value {
        let union = self.union.read().await;
        let mut ids: Vec<String> = union.keys().cloned().collect();
        ids.sort();
        let data: Vec<Value> = ids
            .into_iter()
            .filter_map(|id| union.get(&id).cloned())
            .collect();
        json!({
            "object": "list",
            "data": data
        })
    }

    /// Register or replace models advertised by a connected node.
    pub async fn upsert_node(&self, node_id: NodeId, models: HashMap<String, Value>) {
        let count = models.len();
        self.by_node.write().await.insert(node_id, models);
        self.rebuild_union().await;
        info!(
            node_id = %crate::node_auth::node_id_hex(&node_id),
            model_count = count,
            "models catalog updated for node"
        );
    }

    /// Remove a node's models when its tunnel disconnects.
    pub async fn remove_node(&self, node_id: &NodeId) {
        if self.by_node.write().await.remove(node_id).is_some() {
            self.rebuild_union().await;
            info!(
                node_id = %crate::node_auth::node_id_hex(node_id),
                "removed node from models catalog"
            );
        }
    }

    async fn rebuild_union(&self) {
        let by_node = self.by_node.read().await;
        let mut merged: HashMap<String, Value> = HashMap::new();
        for (node_id, models) in by_node.iter() {
            for (id, entry) in models {
                merge_model_entry(&mut merged, id, *node_id, entry);
            }
        }
        *self.union.write().await = merged;

        let mut flat = Vec::new();
        for (node_id, models) in by_node.iter() {
            for (model_id, entry) in models {
                flat.push(build_offering_with_load(*node_id, model_id, entry, "unknown", None, None));
            }
        }
        flat.sort_by(|a, b| a.model_id.cmp(&b.model_id).then(a.node_id.cmp(&b.node_id)));
        *self.offerings.write().await = flat;

        *self.last_refresh.write().await = Some(Instant::now());
    }

    pub async fn model_capacity_events(
        &self,
        capacity: &CapacityTracker,
        tracker_snap: &std::collections::HashMap<ModelCapacityKey, (u32, u32)>,
    ) -> Vec<crate::telemetry::ModelCapacityEvent> {
        let by_node = self.by_node.read().await;
        let mut events = Vec::new();
        for (node_id, models) in by_node.iter() {
            for model_id in models.keys() {
                let key = ModelCapacityKey {
                    node_id: *node_id,
                    model_id: model_id.clone(),
                };
                let (active, queued) = tracker_snap
                    .get(&key)
                    .copied()
                    .unwrap_or_else(|| {
                        let snap = capacity.snapshot(&key);
                        (snap.active_requests, snap.queued_requests)
                    });
                let concurrency = models
                    .get(model_id)
                    .and_then(|e| e.pointer("/sparkl/concurrency").and_then(Value::as_u64))
                    .unwrap_or(0) as u32;
                events.push(crate::telemetry::ModelCapacityEvent {
                    node_id: node_id_key(node_id),
                    model_id: model_id.clone(),
                    active_requests: active,
                    queued_requests: queued,
                    concurrency,
                });
            }
        }
        events.sort_by(|a, b| a.model_id.cmp(&b.model_id).then(a.node_id.cmp(&b.node_id)));
        events
    }

    pub async fn concurrency_for(&self, node_id: NodeId, model_id: &str) -> u32 {
        let by_node = self.by_node.read().await;
        by_node
            .get(&node_id)
            .and_then(|models| models.get(model_id))
            .and_then(|entry| {
                entry
                    .pointer("/sparkl/concurrency")
                    .and_then(Value::as_u64)
            })
            .unwrap_or(0) as u32
    }

    /// Filtered provider offerings with live tunnel status and router capacity overlay.
    pub async fn query_providers(
        &self,
        filters: &super::catalog::ProviderQueryFilters,
        stale_threshold_secs: u64,
        tunnels: &TunnelRegistry,
        capacity: &CapacityTracker,
    ) -> Vec<ProviderOffering> {
        let by_node = self.by_node.read().await;
        let mut base = Vec::new();
        for (node_id, models) in by_node.iter() {
            for (model_id, entry) in models {
                let key = ModelCapacityKey {
                    node_id: *node_id,
                    model_id: model_id.clone(),
                };
                let snap = capacity.snapshot(&key);
                base.push(build_offering_with_load(
                    *node_id,
                    model_id,
                    entry,
                    "unknown",
                    Some(snap.active_requests),
                    Some(snap.queued_requests),
                ));
            }
        }
        base.sort_by(|a, b| a.model_id.cmp(&b.model_id).then(a.node_id.cmp(&b.node_id)));

        base.into_iter()
            .filter_map(|mut o| {
                let node_bytes = parse_node_id_hex(&o.node_id).ok()?;
                let status = if let Some(t) = tunnels.get(&node_bytes) {
                    tunnel_status_for_pong(t.last_pong_timestamp(), stale_threshold_secs)
                } else {
                    "offline"
                };
                o.tunnel_status = status.to_string();

                if filters.online_only && status == "offline" {
                    return None;
                }
                if let Some(ref model) = filters.model {
                    if &o.model_id != model {
                        return None;
                    }
                }
                if let Some(ref q) = filters.quantization {
                    if &o.quantization != q {
                        return None;
                    }
                }
                if let Some(ref p) = filters.parameter_count {
                    if &o.parameter_count != p {
                        return None;
                    }
                }
                if let Some(min) = filters.min_context_length {
                    if o.context_length < min {
                        return None;
                    }
                }
                if let Some(min) = filters.min_available_slots {
                    if o.available_slots < min {
                        return None;
                    }
                }
                if !filters.features_any.is_empty()
                    && !filters
                        .features_any
                        .iter()
                        .any(|k| o.features.contains_key(k))
                {
                    return None;
                }
                for key in &filters.features_all {
                    if !o.features.contains_key(key) {
                        return None;
                    }
                }
                for (key, needle) in &filters.feature_value_contains {
                    match o.features.get(key) {
                        Some(v) if v.contains(needle) => {}
                        _ => return None,
                    }
                }
                Some(o)
            })
            .collect()
    }

    /// Fetch `/v1/models` from one tunnel and update the cache (connect + heartbeat).
    pub async fn refresh_tunnel(&self, node_id: NodeId, tunnel: &Arc<NodeTunnel>) {
        match fetch_models_from_tunnel(tunnel, MODELS_FETCH_TIMEOUT).await {
            Ok(models) => {
                tunnel
                    .model_count
                    .store(models.len() as i64, Ordering::Relaxed);
                self.upsert_node(node_id, models).await;
            }
            Err(e) => warn!(
                node_id = %crate::node_auth::node_id_hex(&node_id),
                %e,
                "failed to refresh models from tunnel"
            ),
        }
    }

    /// Background / admin: refresh every connected tunnel (optional full rebuild).
    pub async fn refresh_all_tunnels(&self, state: &RouterState) {
        for (node_id, tunnel) in state.tunnels.iter() {
            self.refresh_tunnel(node_id, &tunnel).await;
        }
    }
}

fn merge_model_entry(merged: &mut HashMap<String, Value>, id: &str, node_id: NodeId, entry: &Value) {
    let provider = provider_snapshot(node_id, id, entry);
    match merged.get_mut(id) {
        None => {
            let mut out = entry.clone();
            if let Some(obj) = out.as_object_mut() {
                let sparkl = obj
                    .entry("sparkl")
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .expect("sparkl object");
                sparkl.insert("providers".to_string(), json!([provider.clone()]));
                apply_aggregated_load(sparkl, std::slice::from_ref(&provider));
            }
            merged.insert(id.to_string(), out);
        }
        Some(existing) => {
            let Some(obj) = existing.as_object_mut() else {
                return;
            };
            let sparkl = obj
                .entry("sparkl")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .expect("sparkl object");
            let providers = sparkl
                .entry("providers")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("providers array");
            providers.push(provider.clone());
            let provider_refs: Vec<Value> = providers.clone();
            apply_aggregated_load(sparkl, &provider_refs);
        }
    }
}

fn provider_snapshot(node_id: NodeId, model_id: &str, entry: &Value) -> Value {
    let o = build_offering_with_load(node_id, model_id, entry, "unknown", None, None);
    json!({
        "node_id": o.node_id,
        "model_id": o.model_id,
        "context_length": o.context_length,
        "quantization": o.quantization,
        "parameter_count": o.parameter_count,
        "source_url": o.source_url,
        "features": o.features,
        "active_requests": o.active_requests,
        "active_sessions": o.active_sessions,
        "queued_requests": o.queued_requests,
        "concurrency": o.concurrency,
        "available_slots": o.available_slots,
    })
}

fn apply_aggregated_load(sparkl: &mut serde_json::Map<String, Value>, providers: &[Value]) {
    let mut total_active = 0u64;
    let mut total_available = 0u64;
    for p in providers {
        let active = p
            .get("active_requests")
            .or_else(|| p.get("active_sessions"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let concurrency = p.get("concurrency").and_then(Value::as_u64).unwrap_or(0);
        total_active = total_active.saturating_add(active);
        if concurrency > 0 {
            total_available = total_available.saturating_add(concurrency.saturating_sub(active));
        }
    }
    sparkl.insert("active_requests".to_string(), json!(total_active));
    sparkl.insert("active_sessions".to_string(), json!(total_active));
    if total_available > 0 || providers.iter().any(|p| {
        p.get("concurrency").and_then(Value::as_u64).unwrap_or(0) > 0
    }) {
        sparkl.insert("available_slots".to_string(), json!(total_available));
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
    debug!(
        node_id = %crate::node_auth::node_id_hex(&node_id),
        "refreshing models catalog on pong"
    );
    tokio::spawn(async move {
        state.models.refresh_tunnel(node_id, &tunnel).await;
    });
}

async fn fetch_models_from_tunnel(
    tunnel: &Arc<NodeTunnel>,
    timeout: Duration,
) -> anyhow::Result<HashMap<String, Value>> {
    use crate::tunnel::dispatch::forward_http_request;

    let mut pending = forward_http_request(
        tunnel,
        "GET",
        "/v1/models",
        json!({}),
        None,
        timeout,
    )
    .await?;

    let result = tokio::time::timeout(timeout, async {
        let mut body = String::new();
        while let Some(frame) = pending.rx.recv().await {
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

    parse_model_entries(&body)
}

pub fn parse_model_entries(body: &str) -> anyhow::Result<HashMap<String, Value>> {
    let v: Value = serde_json::from_str(body)?;
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow::anyhow!("invalid models response"))?;
    let mut out = HashMap::new();
    for item in data {
        if let Some(id) = item.get("id").and_then(|x| x.as_str()) {
            out.insert(id.to_string(), item.clone());
        }
    }
    Ok(out)
}

pub async fn list_models_handler(state: RouterState) -> Value {
    state.models.list_cached_json().await
}

pub fn node_id_key(id: &NodeId) -> String {
    crate::node_auth::node_id_hex(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_model(id: &str, active: u64, concurrency: u64) -> Value {
        sample_model_with_features(id, active, concurrency, json!({ "mtp": "8-token" }))
    }

    fn sample_model_with_features(
        id: &str,
        active: u64,
        concurrency: u64,
        features: Value,
    ) -> Value {
        json!({
            "id": id,
            "object": "model",
            "context_length": 128000,
            "sparkl": {
                "quantization": "Q4_K_M",
                "parameter_count": "27B",
                "concurrency": concurrency,
                "active_sessions": active,
                "features": features,
            }
        })
    }

    #[tokio::test]
    async fn catalog_upsert_remove_rebuilds_union() {
        let cat = ModelsCatalog::new();
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];
        let mut a = HashMap::new();
        a.insert("gpt-4o".into(), sample_model("gpt-4o", 1, 8));
        cat.upsert_node(n1, a).await;
        assert!(cat.contains("gpt-4o").await);

        let mut b = HashMap::new();
        b.insert("llama3:8b".into(), sample_model("llama3:8b", 0, 4));
        cat.upsert_node(n2, b).await;
        assert!(cat.contains("llama3:8b").await);

        cat.remove_node(&n1).await;
        assert!(!cat.contains("gpt-4o").await);
        assert!(cat.contains("llama3:8b").await);
    }

    #[tokio::test]
    async fn catalog_merges_same_id_across_nodes() {
        let cat = ModelsCatalog::new();
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];
        let mut a = HashMap::new();
        a.insert("shared".into(), sample_model("shared", 2, 8));
        cat.upsert_node(n1, a).await;

        let mut b = HashMap::new();
        b.insert("shared".into(), sample_model("shared", 1, 8));
        cat.upsert_node(n2, b).await;

        let list = cat.list_cached_json().await;
        let data = list.get("data").and_then(Value::as_array).expect("data");
        assert_eq!(data.len(), 1);
        let entry = &data[0];
        assert_eq!(
            entry.pointer("/sparkl/active_sessions").and_then(Value::as_u64),
            Some(3)
        );
        let providers = entry
            .pointer("/sparkl/providers")
            .and_then(Value::as_array)
            .expect("providers");
        assert_eq!(providers.len(), 2);
    }

    #[test]
    fn parse_model_entries_keeps_metadata() {
        let body = r#"{"data":[{"id":"m1","context_length":100,"sparkl":{"concurrency":4}}]}"#;
        let map = parse_model_entries(body).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["m1"]["context_length"], 100);
    }

    #[tokio::test]
    async fn query_providers_filters_by_model_and_features() {
        use crate::tunnel::registry::TunnelRegistry;

        let cat = ModelsCatalog::new();
        let n1 = [1u8; 32];
        let mut a = HashMap::new();
        a.insert(
            "shared".into(),
            sample_model_with_features("shared", 2, 8, json!({ "mtp": "a", "speculative": "dflash" })),
        );
        cat.upsert_node(n1, a).await;

        let tunnels = TunnelRegistry::new();
        let filters = crate::consumer::catalog::ProviderQueryFilters {
            model: Some("shared".into()),
            features_all: vec!["mtp".into()],
            min_available_slots: Some(1),
            online_only: false,
            ..Default::default()
        };
        let capacity = CapacityTracker::new();
        let hits = cat
            .query_providers(&filters, 40, &tunnels, &capacity)
            .await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].available_slots, 8);
        assert_eq!(hits[0].active_requests, 0);
    }

    #[tokio::test]
    async fn provider_snapshot_includes_features() {
        let entry = sample_model_with_features("m", 1, 4, json!({ "speculative": "dflash" }));
        let p = provider_snapshot([9u8; 32], "m", &entry);
        assert_eq!(p["features"]["speculative"], "dflash");
        assert_eq!(p["available_slots"], 3);
    }
}
