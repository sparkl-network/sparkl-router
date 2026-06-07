use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tracing::{info, warn};

use crate::node_auth::{
    connect_challenge_payload, parse_node_id_hex, random_nonce, verify_connect_signature,
};
use crate::protocol::{NodeToRouterFrame, RouterToNodeFrame};
use crate::state::{NodeTunnel, RouterState};
use crate::tunnel::lifecycle::run_tunnel_lifecycle;

pub async fn node_connect(
    ws: WebSocketUpgrade,
    State(state): State<RouterState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_node_connect(socket, state))
}

async fn handle_node_connect(mut socket: WebSocket, state: RouterState) {
    let config = state.config.clone();

    let block = match state.chain.latest_block().await {
        Ok(b) => b,
        Err(e) => {
            warn!(%e, "failed to fetch block for challenge");
            let _ = socket.close().await;
            return;
        }
    };

    let nonce = random_nonce();
    let challenge = RouterToNodeFrame::Challenge {
        nonce: hex::encode(nonce),
        block,
    };

    if send_frame(&mut socket, &challenge).await.is_err() {
        return;
    }

    let auth_text = match recv_text(&mut socket).await {
        Some(t) => t,
        None => return,
    };

    let auth = match NodeToRouterFrame::parse(&auth_text) {
        Ok(NodeToRouterFrame::Auth {
            node_id,
            signature,
            ed25519_pubkey,
            moniker,
        }) => (node_id, signature, ed25519_pubkey, moniker),
        _ => {
            warn!("expected auth frame");
            let _ = socket.close().await;
            return;
        }
    };

    let (node_id_str, signature, ed25519_pubkey, moniker) = auth;
    let node_id = match parse_node_id_hex(&node_id_str) {
        Ok(id) => id,
        Err(e) => {
            warn!(%e, "invalid node_id in auth");
            let _ = socket.close().await;
            return;
        }
    };

    let pubkey_bytes = match ed25519_pubkey {
        Some(ref hex_pk) => match decode_pubkey(hex_pk) {
            Ok(pk) => pk,
            Err(e) => {
                warn!(%e, "invalid ed25519_pubkey");
                let _ = socket.close().await;
                return;
            }
        },
        None => {
            warn!("auth frame missing ed25519_pubkey");
            let _ = socket.close().await;
            return;
        }
    };

    let payload = connect_challenge_payload(&nonce, block);
    if let Err(e) = verify_connect_signature(&payload, &signature, &pubkey_bytes) {
        warn!(%e, "connect signature verification failed");
        let _ = socket.close().await;
        return;
    }

    let moniker = match normalize_tunnel_moniker(moniker) {
        Ok(m) => m,
        Err(e) => {
            warn!(%e, "invalid moniker in auth");
            let _ = socket.close().await;
            return;
        }
    };

    if config.chain.enabled {
        match state.chain.is_node_registered(&node_id).await {
            Ok(true) => {}
            Ok(false) => {
                warn!(node_id = %node_id_str, "node not commercially registered on-chain");
                let _ = socket.close().await;
                return;
            }
            Err(e) => {
                warn!(%e, "registry check failed");
                let _ = socket.close().await;
                return;
            }
        }
    }

    if let Some(old) = state.tunnels.remove(&node_id) {
        old.signal_shutdown();
        old.fail_all_pending();
    }

    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel::<RouterToNodeFrame>(256);
    let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    let tunnel = Arc::new(NodeTunnel::new(
        node_id,
        moniker,
        frame_tx,
        shutdown_tx,
    ));

    state.tunnels.insert(node_id, Arc::clone(&tunnel));

    let ready = RouterToNodeFrame::Ready {
        router_url: config.server.router_url.clone(),
    };
    if send_frame(&mut socket, &ready).await.is_err() {
        state.tunnels.remove(&node_id);
        return;
    }

    info!(
        node_id = %node_id_str,
        moniker = tunnel.moniker.as_deref().unwrap_or(""),
        "node tunnel ready"
    );
    crate::metrics::inc_tunnel_connected();

    state.telemetry.emit_node_status(
        node_id,
        tunnel.moniker.clone(),
        "online",
        tunnel.in_flight_count(),
        tunnel.model_count.load(std::sync::atomic::Ordering::Relaxed),
    );

    crate::consumer::models::spawn_tunnel_models_refresh(
        state.clone(),
        node_id,
        Arc::clone(&tunnel),
    );

    run_tunnel_lifecycle(
        node_id,
        tunnel,
        socket,
        frame_rx,
        shutdown_rx,
        state,
        config,
    )
    .await;
}

const MAX_MONIKER_LEN: usize = 128;

fn normalize_tunnel_moniker(raw: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(s) = raw else {
        return Ok(None);
    };
    let m = s.trim().to_string();
    if m.is_empty() {
        return Ok(None);
    }
    if m.len() > MAX_MONIKER_LEN {
        anyhow::bail!("moniker exceeds {MAX_MONIKER_LEN} characters");
    }
    Ok(Some(m))
}

fn decode_pubkey(hex_pk: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = hex::decode(hex_pk.trim().strip_prefix("0x").unwrap_or(hex_pk.trim()))?;
    if bytes.len() != 32 {
        anyhow::bail!("ed25519 pubkey must be 32 bytes");
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

async fn send_frame(socket: &mut WebSocket, frame: &RouterToNodeFrame) -> anyhow::Result<()> {
    let json = frame.to_json()?;
    socket
        .send(Message::Text(json.into()))
        .await
        .map_err(|e| anyhow::anyhow!("ws send failed: {e}"))
}

async fn recv_text(socket: &mut WebSocket) -> Option<String> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(t))) => return Some(t.to_string()),
            Some(Ok(Message::Ping(p))) => {
                let _ = socket.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Close(_))) | None => return None,
            Some(Err(_)) => return None,
            _ => {}
        }
    }
}
