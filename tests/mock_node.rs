//! Helpers for integration tests: mock provider node WSS client.

use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use serde_json::json;
use sparkl_router::node_auth::{connect_challenge_payload, node_id_from_ed25519_pubkey};
use sparkl_router::protocol::RouterToNodeFrame;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub struct MockNodeKeys {
    pub signing_key: SigningKey,
    pub node_id: [u8; 32],
    pub pubkey_hex: String,
}

impl MockNodeKeys {
    pub fn generate() -> Self {
        let mut secret = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut secret);
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let node_id = node_id_from_ed25519_pubkey(&pubkey);
        Self {
            signing_key,
            node_id,
            pubkey_hex: hex::encode(pubkey),
        }
    }
}

pub async fn connect_mock_node_with_moniker(
    router_ws_url: &str,
    keys: &MockNodeKeys,
    moniker: Option<&str>,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let (mut ws, _) = connect_async(router_ws_url).await?;

    let challenge_text = recv_json(&mut ws).await?;
    let challenge: RouterToNodeFrame = serde_json::from_value(challenge_text)?;
    let (nonce_hex, block) = match challenge {
        RouterToNodeFrame::Challenge { nonce, block } => (nonce, block),
        _ => anyhow::bail!("expected challenge"),
    };

    let mut nonce = [0u8; 32];
    let decoded = hex::decode(nonce_hex)?;
    nonce[..decoded.len()].copy_from_slice(&decoded);

    let payload = connect_challenge_payload(&nonce, block);
    let sig = keys.signing_key.sign(&payload);

    let mut auth = json!({
        "type": "auth",
        "node_id": format!("0x{}", hex::encode(keys.node_id)),
        "signature": hex::encode(sig.to_bytes()),
        "ed25519_pubkey": keys.pubkey_hex,
    });
    if let Some(m) = moniker.map(str::trim).filter(|s| !s.is_empty()) {
        auth["moniker"] = json!(m);
    }
    ws.send(Message::Text(auth.to_string().into())).await?;

    let ready_text = recv_json(&mut ws).await?;
    let ready: RouterToNodeFrame = serde_json::from_value(ready_text)?;
    match ready {
        RouterToNodeFrame::Ready { .. } => Ok(ws),
        _ => anyhow::bail!("expected ready"),
    }
}

pub async fn respond_pong(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> anyhow::Result<()> {
    loop {
        let msg = ws.next().await;
        match msg {
            Some(Ok(Message::Text(t))) => {
                let v: serde_json::Value = serde_json::from_str(&t)?;
                if v.get("type").and_then(|x| x.as_str()) == Some("ping") {
                    ws.send(Message::Text(
                        serde_json::json!({"type":"pong"}).to_string().into(),
                    ))
                    .await?;
                    return Ok(());
                }
            }
            _ => anyhow::bail!("no ping received"),
        }
    }
}

pub async fn connect_mock_node(
    router_ws_url: &str,
    keys: &MockNodeKeys,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    connect_mock_node_with_moniker(router_ws_url, keys, None).await
}

async fn recv_json(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> anyhow::Result<serde_json::Value> {
    loop {
        if let Some(Ok(Message::Text(t))) = ws.next().await {
            return Ok(serde_json::from_str(&t)?);
        }
    }
}
