use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use crate::state::NodeId;

use super::models::node_id_key;

#[derive(Debug, Clone, Serialize)]
pub struct ProviderOffering {
    pub node_id: String,
    pub model_id: String,
    pub tunnel_status: String,
    pub context_length: u32,
    pub quantization: String,
    pub parameter_count: String,
    pub source_url: String,
    pub features: HashMap<String, String>,
    pub concurrency: u32,
    pub active_requests: u32,
    pub queued_requests: u32,
    /// Deprecated alias of `active_requests` for backward compatibility.
    pub active_sessions: u32,
    pub available_slots: u32,
}

pub fn build_offering(
    node_id: NodeId,
    model_id: &str,
    entry: &Value,
    tunnel_status: &str,
) -> ProviderOffering {
    build_offering_with_load(node_id, model_id, entry, tunnel_status, None, None)
}

pub fn build_offering_with_load(
    node_id: NodeId,
    model_id: &str,
    entry: &Value,
    tunnel_status: &str,
    router_active: Option<u32>,
    router_queued: Option<u32>,
) -> ProviderOffering {
    let catalog_active = entry
        .pointer("/sparkl/active_requests")
        .or_else(|| entry.pointer("/sparkl/active_sessions"))
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;
    let active_requests = router_active.unwrap_or(catalog_active);
    let queued_requests = router_queued.unwrap_or(0);
    let concurrency = entry
        .pointer("/sparkl/concurrency")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;
    let available_slots = if concurrency > 0 {
        concurrency.saturating_sub(active_requests)
    } else {
        u32::MAX
    };

    let context_length = entry
        .get("context_length")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;

    let sparkl = entry.get("sparkl");
    let quantization = sparkl
        .and_then(|s| s.get("quantization"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let parameter_count = sparkl
        .and_then(|s| s.get("parameter_count"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let source_url = sparkl
        .and_then(|s| s.get("source_url"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let features = parse_features_map(sparkl.and_then(|s| s.get("features")));

    ProviderOffering {
        node_id: node_id_key(&node_id),
        model_id: model_id.to_string(),
        tunnel_status: tunnel_status.to_string(),
        context_length,
        quantization,
        parameter_count,
        source_url,
        features,
        concurrency,
        active_requests,
        queued_requests,
        active_sessions: active_requests,
        available_slots,
    }
}

pub fn parse_features_map(v: Option<&Value>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(Value::Object(map)) = v else {
        return out;
    };
    for (k, val) in map {
        if let Some(s) = val.as_str() {
            out.insert(k.clone(), s.to_string());
        }
    }
    out
}

pub fn tunnel_status_for_pong(last_pong: i64, stale_secs: u64) -> &'static str {
    let now = chrono::Utc::now().timestamp();
    let age = now.saturating_sub(last_pong);
    if age <= stale_secs as i64 {
        "online"
    } else if age <= (stale_secs * 2) as i64 {
        "degraded"
    } else {
        "offline"
    }
}
