use std::collections::HashMap;
use std::sync::atomic::Ordering;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tracing::warn;

use crate::consumer::offerings::tunnel_status_for_pong;
use crate::node_auth::node_id_hex;
use crate::state::RouterState;
use crate::telemetry::{NodeStatusEvent, TelemetryBus, TelemetryEvent};

type HmacSha256 = Hmac<Sha256>;

pub async fn status_subscribe(
    ws: WebSocketUpgrade,
    State(state): State<RouterState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !authorize_subscribe(&state, &headers, &query) {
        return Err((StatusCode::UNAUTHORIZED, "invalid subscribe credentials".into()));
    }
    Ok(ws.on_upgrade(move |socket| handle_subscribe(socket, state)))
}

fn authorize_subscribe(
    state: &RouterState,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
) -> bool {
    let expected = state.config.portal.admin_token.trim();
    if expected.is_empty() {
        return false;
    }

    if let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = auth.strip_prefix("Bearer ").or_else(|| auth.strip_prefix("bearer "))
        {
            if token.trim() == expected {
                return true;
            }
        }
    }

    if let (Some(token), Some(exp)) = (query.get("token"), query.get("exp")) {
        return verify_subscribe_token(expected, token, exp);
    }

    false
}

fn verify_subscribe_token(admin_token: &str, token: &str, exp: &str) -> bool {
    let Ok(expiry) = exp.parse::<i64>() else {
        return false;
    };
    let now = chrono::Utc::now().timestamp();
    if expiry < now {
        return false;
    }

    let Ok(mut mac) = HmacSha256::new_from_slice(admin_token.as_bytes()) else {
        return false;
    };
    mac.update(exp.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    expected == token.trim().to_lowercase()
}

async fn handle_subscribe(socket: WebSocket, state: RouterState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.telemetry.subscribe();

    if send_snapshot(&state, &mut sender).await.is_err() {
        warn!("failed to send telemetry snapshot");
        return;
    }

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(ev) => {
                        if matches!(ev, TelemetryEvent::Snapshot { .. }) {
                            continue;
                        }
                        let text = match serde_json::to_string(&ev) {
                            Ok(t) => t,
                            Err(e) => {
                                warn!(%e, "failed to serialize telemetry event");
                                continue;
                            }
                        };
                        if sender.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        warn!(%e, "telemetry websocket read error");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn send_snapshot(
    state: &RouterState,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> Result<(), ()> {
    let stale = state.config.portal.stale_threshold_secs;
    let mut nodes = Vec::new();
    for (_, tunnel) in state.tunnels.iter() {
        let status = tunnel_status_for_pong(tunnel.last_pong_timestamp(), stale);
        nodes.push(NodeStatusEvent {
            node_id: node_id_hex(&tunnel.node_id),
            moniker: tunnel.moniker.clone(),
            status: status.to_string(),
            in_flight_requests: tunnel.in_flight_count(),
            model_count: tunnel.model_count.load(Ordering::Relaxed),
        });
    }
    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    let tracker_snap = state.capacity.all_snapshots();
    let models = state
        .models
        .model_capacity_events(&state.capacity, &tracker_snap)
        .await;

    let snapshot = TelemetryBus::build_snapshot(nodes, models);
    let text = serde_json::to_string(&snapshot).map_err(|_| ())?;
    sender.send(Message::Text(text.into())).await.map_err(|_| ())
}
