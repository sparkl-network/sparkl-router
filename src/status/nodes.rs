use std::sync::atomic::Ordering;

use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::node_auth::parse_node_id_hex;
use crate::state::RouterState;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct NodeStatus {
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moniker: Option<String>,
    pub status: &'static str,
    pub connected_at: Option<DateTime<Utc>>,
    pub last_pong_at: Option<DateTime<Utc>>,
    pub uptime_secs: Option<u64>,
    pub in_flight_requests: usize,
    pub model_count: i64,
}

#[derive(Debug, Serialize)]
pub struct NodesListResponse {
    pub router_uptime_secs: u64,
    pub tunnel_count: usize,
    pub nodes: Vec<NodeStatus>,
}

fn derive_status(last_pong: i64, stale: u64) -> &'static str {
    let now = Utc::now().timestamp();
    let age = now.saturating_sub(last_pong);
    if age <= stale as i64 {
        "online"
    } else if age <= (stale * 2) as i64 {
        "degraded"
    } else {
        "offline"
    }
}

fn tunnel_status(tunnel: &crate::state::NodeTunnel, stale: u64) -> NodeStatus {
    let last_pong = tunnel.last_pong_timestamp();
    let status = derive_status(last_pong, stale);
    NodeStatus {
        node_id: format!("0x{}", hex::encode(tunnel.node_id)),
        moniker: tunnel.moniker.clone(),
        status,
        connected_at: Some(
            Utc::now() - chrono::Duration::seconds(tunnel.connected_at.elapsed().as_secs() as i64),
        ),
        last_pong_at: DateTime::from_timestamp(last_pong, 0),
        uptime_secs: Some(tunnel.connected_at.elapsed().as_secs()),
        in_flight_requests: tunnel.in_flight_count(),
        model_count: tunnel.model_count.load(Ordering::Relaxed),
    }
}

pub async fn list_nodes(State(state): State<RouterState>) -> Json<NodesListResponse> {
    let stale = state.config.portal.stale_threshold_secs;
    let nodes: Vec<NodeStatus> = state
        .tunnels
        .iter()
        .map(|(_, t)| tunnel_status(&t, stale))
        .collect();

    Json(NodesListResponse {
        router_uptime_secs: state.uptime_secs(),
        tunnel_count: nodes.len(),
        nodes,
    })
}

pub async fn get_node(
    State(state): State<RouterState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeStatus>, (axum::http::StatusCode, String)> {
    let id = parse_node_id_hex(&node_id)
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

    let stale = state.config.portal.stale_threshold_secs;

    if let Some(tunnel) = state.tunnels.get(&id) {
        return Ok(Json(tunnel_status(&tunnel, stale)));
    }

    Ok(Json(NodeStatus {
        node_id: format!("0x{}", hex::encode(id)),
        moniker: None,
        status: "offline",
        connected_at: None,
        last_pong_at: None,
        uptime_secs: None,
        in_flight_requests: 0,
        model_count: 0,
    }))
}
