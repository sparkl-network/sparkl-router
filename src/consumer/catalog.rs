use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::Json;
use serde_json::{json, Value};

use crate::state::RouterState;

/// Allowed feature keys for solo `[[models]].features` (mirrors sparkl-solo).
pub const FEATURE_KEY_DOCS: &[(&str, &str)] = &[
    ("mtp", "MTP / multi-token prediction setup (e.g. draft depth, backend note)"),
    (
        "speculative",
        "Speculative decoding path (e.g. dflash, eagle, draft model name)",
    ),
    (
        "multimodal",
        "Vision / image input support (e.g. resolution limit, template)",
    ),
    (
        "long_context",
        "Long-context claim (e.g. effective window, yarn note)",
    ),
];

#[derive(Debug, Default)]
pub struct ProviderQueryFilters {
    pub model: Option<String>,
    pub features_any: Vec<String>,
    pub features_all: Vec<String>,
    pub feature_value_contains: HashMap<String, String>,
    pub quantization: Option<String>,
    pub parameter_count: Option<String>,
    pub min_context_length: Option<u32>,
    pub min_available_slots: Option<u32>,
    pub online_only: bool,
}

impl ProviderQueryFilters {
    pub fn from_query_map(query: &HashMap<String, String>) -> Self {
        let model = query.get("model").cloned();
        let features_any = split_csv(query.get("features_any").map(|s| s.as_str()));
        let features_all = split_csv(query.get("features_all").map(|s| s.as_str()));
        let quantization = query.get("quantization").cloned();
        let parameter_count = query.get("parameter_count").cloned();
        let min_context_length = query
            .get("min_context_length")
            .and_then(|s| s.parse().ok());
        let min_available_slots = query
            .get("min_available_slots")
            .and_then(|s| s.parse().ok());
        let online_only = query
            .get("online_only")
            .map(|s| s != "false" && s != "0")
            .unwrap_or(true);

        let mut feature_value_contains = HashMap::new();
        for (k, v) in query {
            if let Some(key) = k.strip_prefix("feature_") {
                if !key.is_empty() {
                    feature_value_contains.insert(key.to_string(), v.clone());
                }
            }
        }

        Self {
            model,
            features_any,
            features_all,
            feature_value_contains,
            quantization,
            parameter_count,
            min_context_length,
            min_available_slots,
            online_only,
        }
    }
}

fn split_csv(s: Option<&str>) -> Vec<String> {
    s.map(|t| {
        t.split(',')
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect()
    })
    .unwrap_or_default()
}

pub async fn list_feature_catalog() -> Json<Value> {
    let data: Vec<Value> = FEATURE_KEY_DOCS
        .iter()
        .map(|(key, description)| {
            json!({
                "key": key,
                "description": description,
            })
        })
        .collect();
    Json(json!({
        "object": "feature_catalog",
        "data": data
    }))
}

pub async fn list_providers(
    State(state): State<RouterState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let filters = ProviderQueryFilters::from_query_map(&query);
    let stale = state.config.portal.stale_threshold_secs;
    let providers = state
        .models
        .query_providers(&filters, stale, &state.tunnels, &state.capacity)
        .await;

    Json(json!({
        "object": "provider_list",
        "data": providers
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_feature_value_query_params() {
        let mut q = HashMap::new();
        q.insert("model".into(), "m".into());
        q.insert("features_all".into(), "mtp".into());
        q.insert("feature_speculative".into(), "dflash".into());
        q.insert("min_available_slots".into(), "1".into());
        let f = ProviderQueryFilters::from_query_map(&q);
        assert_eq!(f.model.as_deref(), Some("m"));
        assert_eq!(f.features_all, vec!["mtp"]);
        assert_eq!(
            f.feature_value_contains.get("speculative"),
            Some(&"dflash".to_string())
        );
    }
}
