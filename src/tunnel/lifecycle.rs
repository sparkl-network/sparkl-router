use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::protocol::{InboundFrame, NodeToRouterFrame, RouterToNodeFrame};
use crate::state::{NodeId, NodeTunnel};
use crate::tunnel::registry::TunnelRegistry;

pub async fn run_tunnel_lifecycle(
    node_id: NodeId,
    tunnel: Arc<NodeTunnel>,
    socket: WebSocket,
    mut frame_rx: tokio::sync::mpsc::Receiver<RouterToNodeFrame>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
    tunnels: TunnelRegistry,
    config: Arc<Config>,
) {
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
                warn!(?node_id, "pong timeout, closing tunnel");
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
                        handle_inbound_text(&tunnel, &text);
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
    tunnels.remove(&node_id);
    info!(?node_id, "tunnel disconnected");
}

fn handle_inbound_text(tunnel: &NodeTunnel, text: &str) {
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
            debug!(?tunnel.node_id, "pong received");
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
