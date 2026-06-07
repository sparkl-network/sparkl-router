use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::consumer::offerings::tunnel_status_for_pong;
use crate::node_auth::node_id_hex;
use crate::protocol::{InboundFrame, NodeToRouterFrame, RouterToNodeFrame};
use crate::state::{NodeId, NodeTunnel, RouterState};

pub async fn run_tunnel_lifecycle(
    node_id: NodeId,
    tunnel: Arc<NodeTunnel>,
    socket: WebSocket,
    mut frame_rx: tokio::sync::mpsc::Receiver<RouterToNodeFrame>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    state: RouterState,
    config: Arc<Config>,
) {
    let tunnels = state.tunnels.clone();
    let models_refresh_secs = config.portal.models_refresh_on_pong_secs;
    let (ws_tx, mut ws_rx) = socket.split();
    let ws_tx = Arc::new(Mutex::new(ws_tx));

    let ws_tx_writer = Arc::clone(&ws_tx);
    let writer = tokio::spawn(async move {
        while let Some(frame) = frame_rx.recv().await {
            match frame.to_json() {
                Ok(json) => {
                    let mut guard = ws_tx_writer.lock().await;
                    if guard.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                Err(e) => warn!(%e, "failed to serialize outbound frame"),
            }
        }
    });

    let ping_interval = Duration::from_secs(config.node_auth.ping_interval_secs.max(1));
    let pong_timeout = Duration::from_secs(config.node_auth.pong_timeout_secs.max(1));
    let tunnel_ping = Arc::clone(&tunnel);
    let ping_sender = tunnel.clone();
    let pinger = tokio::spawn(async move {
        let mut interval = tokio::time::interval(ping_interval);
        loop {
            interval.tick().await;
            let last = tunnel_ping.last_pong_timestamp();
            let now = chrono::Utc::now().timestamp();
            if now - last > (ping_interval + pong_timeout).as_secs() as i64 {
                warn!(node_id = %node_id_hex(&node_id), "pong timeout, closing tunnel");
                tunnel_ping.signal_shutdown();
                break;
            }
            if ping_sender
                .send_frame(RouterToNodeFrame::Ping)
                .await
                .is_err()
            {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_inbound_text(&state, Arc::clone(&tunnel), &text, models_refresh_secs);
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let mut guard = ws_tx.lock().await;
                        let _ = guard.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        warn!(%e, "websocket read error");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    writer.abort();
    pinger.abort();
    tunnel.fail_all_pending();
    state.capacity.clear_node(&node_id);
    tunnels.remove(&node_id);
    state.models.remove_node(&node_id).await;
    state.telemetry.emit_node_status(
        node_id,
        tunnel.moniker.clone(),
        "offline",
        0,
        0,
    );
    info!(node_id = %node_id_hex(&node_id), "tunnel disconnected");
}

fn handle_inbound_text(
    state: &RouterState,
    tunnel: Arc<NodeTunnel>,
    text: &str,
    models_refresh_secs: u64,
) {
    let frame = match NodeToRouterFrame::parse(text) {
        Ok(f) => f,
        Err(e) => {
            warn!(%e, "invalid frame from node");
            return;
        }
    };

    match frame {
        NodeToRouterFrame::Pong => {
            tunnel.touch_pong();
            debug!(node_id = %node_id_hex(&tunnel.node_id), "pong received");
            let stale = state.config.portal.stale_threshold_secs;
            let status = tunnel_status_for_pong(tunnel.last_pong_timestamp(), stale);
            state.telemetry.emit_node_status(
                tunnel.node_id,
                tunnel.moniker.clone(),
                status,
                tunnel.in_flight_count(),
                tunnel.model_count.load(Ordering::Relaxed),
            );
            crate::consumer::models::maybe_refresh_on_pong(
                state.clone(),
                tunnel.node_id,
                Arc::clone(&tunnel),
                models_refresh_secs,
            );
        }
        NodeToRouterFrame::Response {
            rid,
            status,
            headers,
        } => {
            tunnel.route_inbound(rid, InboundFrame::Response { status, headers });
        }
        NodeToRouterFrame::Chunk { rid, data } => {
            tunnel.route_inbound(rid, InboundFrame::Chunk(data));
        }
        NodeToRouterFrame::End { rid, status } => {
            tunnel.route_inbound(rid, InboundFrame::End { status });
        }
        NodeToRouterFrame::Error { rid, code, message } => {
            tunnel.route_inbound(rid, InboundFrame::Error { code, message });
        }
        NodeToRouterFrame::ActivateResponse { rid, api_key } => {
            tunnel.route_inbound(rid, InboundFrame::ActivateResponse { api_key });
        }
        NodeToRouterFrame::Auth { .. } => {
            warn!("unexpected auth frame after handshake");
        }
    }
}
