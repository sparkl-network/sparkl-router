use std::time::Duration;

use alloy_primitives::Address;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use secp256k1::ecdsa::{RecoverableSignature, RecoveryId};
use secp256k1::{Message, Secp256k1};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::chain::session_is_open;
use crate::protocol::InboundFrame;
use crate::state::RouterState;
use crate::tunnel::dispatch::forward_activate;

const ACTIVATE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
pub struct ActivateBody {
    pub signature: String,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(default)]
    pub message: Option<String>,
}

pub async fn activate_session(
    State(state): State<RouterState>,
    Path(session_id_str): Path<String>,
    Json(body): Json<ActivateBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session_id = parse_session_id(&session_id_str)
        .map_err(|e| activate_error(StatusCode::BAD_REQUEST, &e.to_string()))?;

    let chain_sess = state
        .chain
        .get_session(session_id)
        .await
        .map_err(|e| activate_error(StatusCode::UNAUTHORIZED, &e.to_string()))?;

    if !session_is_open(&chain_sess) {
        return Err(activate_error(StatusCode::UNAUTHORIZED, "session not open"));
    }

    let message = body
        .message
        .clone()
        .unwrap_or_else(|| format!("sparkl-activate:{}:{}", session_id, body.block_number));

    let recovered = recover_eip191_signer(message.as_bytes(), &body.signature)
        .map_err(|_| activate_error(StatusCode::UNAUTHORIZED, "invalid signature"))?;

    if recovered != chain_sess.user {
        return Err(activate_error(
            StatusCode::UNAUTHORIZED,
            "signature must recover to session user",
        ));
    }

    let node_id: [u8; 32] = chain_sess.node_id.into();
    let tunnel = state
        .tunnels
        .get(&node_id)
        .ok_or_else(|| activate_error(StatusCode::SERVICE_UNAVAILABLE, "provider offline"))?;

    let pending = forward_activate(
        &tunnel,
        &format!("0x{:064x}", session_id),
        &body.signature,
        body.block_number,
        Some(message),
        ACTIVATE_TIMEOUT,
    )
    .await
    .map_err(|e| activate_error(StatusCode::BAD_GATEWAY, &e.to_string()))?;

    let api_key = tokio::time::timeout(ACTIVATE_TIMEOUT, async {
        let mut rx = pending.rx;
        while let Some(frame) = rx.recv().await {
            if let InboundFrame::ActivateResponse { api_key } = frame {
                return Ok(api_key);
            }
            if let InboundFrame::Error { message, .. } = frame {
                anyhow::bail!("activate error: {message}");
            }
        }
        anyhow::bail!("activate closed without response")
    })
    .await
    .map_err(|_| activate_error(StatusCode::GATEWAY_TIMEOUT, "activate timeout"))?
    .map_err(|e| activate_error(StatusCode::BAD_GATEWAY, &e.to_string()))?;

    Ok(Json(json!({
        "apiKey": api_key,
        "sessionId": format!("0x{:064x}", session_id)
    })))
}

fn parse_session_id(s: &str) -> anyhow::Result<u64> {
    let t = s.trim();
    if let Some(hex) = t.strip_prefix("0x") {
        let hex = hex.trim_start_matches('0');
        if hex.is_empty() {
            return Ok(0);
        }
        if hex.len() <= 16 {
            return u64::from_str_radix(hex, 16)
                .map_err(|e| anyhow::anyhow!("invalid session id hex: {e}"));
        }
        let padded = format!("{:0>64}", hex);
        let bytes = hex::decode(&padded[..64.min(padded.len())])
            .map_err(|e| anyhow::anyhow!("invalid session id hex: {e}"))?;
        if bytes.len() != 32 {
            anyhow::bail!("session id must be 32 bytes");
        }
        if bytes[..24] != [0u8; 24] {
            anyhow::bail!("session id exceeds u64 range");
        }
        return Ok(u64::from_be_bytes(bytes[24..32].try_into().unwrap()));
    }
    t.parse::<u64>()
        .map_err(|e| anyhow::anyhow!("invalid session id: {e}"))
}

fn recover_eip191_signer(message: &[u8], sig_hex: &str) -> Result<Address, ()> {
    let sig_bytes = hex::decode(sig_hex.strip_prefix("0x").unwrap_or(sig_hex)).map_err(|_| ())?;
    if sig_bytes.len() != 65 {
        return Err(());
    }
    let prefixed = eip191_hash(message);
    let msg = Message::from_digest(prefixed);
    let recid = RecoveryId::try_from(i32::from(sig_bytes[64])).map_err(|_| ())?;
    let sig = RecoverableSignature::from_compact(&sig_bytes[..64], recid).map_err(|_| ())?;
    let secp = Secp256k1::verification_only();
    let pubkey = secp.recover_ecdsa(msg, &sig).map_err(|_| ())?;
    let hash = alloy_primitives::keccak256(&pubkey.serialize_uncompressed()[1..]);
    Ok(Address::from_slice(&hash[12..]))
}

fn eip191_hash(message: &[u8]) -> [u8; 32] {
    use alloy_primitives::keccak256;
    let len = message.len();
    let mut prefixed = format!("\x19Ethereum Signed Message:\n{len}");
    prefixed.push_str(&String::from_utf8_lossy(message));
    keccak256(prefixed.as_bytes()).0
}

fn activate_error(status: StatusCode, msg: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": { "message": msg, "type": "activate_error" } })),
    )
}
