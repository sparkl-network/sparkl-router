//! Provider WSS connect challenge and Ed25519 verification.

use alloy_primitives::keccak256;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rand::RngCore;

pub const CONNECT_DOMAIN: &[u8] = b"sparkl-router-connect:";

/// Build the 32-byte challenge payload nodes must sign.
pub fn connect_challenge_payload(nonce: &[u8; 32], block_number: u64) -> [u8; 32] {
    let mut buf = Vec::with_capacity(CONNECT_DOMAIN.len() + 32 + 8);
    buf.extend_from_slice(CONNECT_DOMAIN);
    buf.extend_from_slice(nonce);
    buf.extend_from_slice(&block_number.to_be_bytes());
    keccak256(&buf).0
}

pub fn random_nonce() -> [u8; 32] {
    let mut n = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut n);
    n
}

pub fn verify_connect_signature(
    payload: &[u8; 32],
    signature_hex: &str,
    pubkey_bytes: &[u8; 32],
) -> anyhow::Result<()> {
    let sig_bytes = hex::decode(signature_hex.trim_start_matches("0x"))
        .map_err(|e| anyhow::anyhow!("invalid signature hex: {e}"))?;
    if sig_bytes.len() != 64 {
        anyhow::bail!("signature must be 64 bytes");
    }
    let mut arr = [0u8; 64];
    arr.copy_from_slice(&sig_bytes);
    let signature = Signature::from_bytes(&arr);
    let verifying_key = VerifyingKey::from_bytes(pubkey_bytes)
        .map_err(|e| anyhow::anyhow!("invalid ed25519 pubkey: {e}"))?;
    verifying_key
        .verify(payload, &signature)
        .map_err(|e| anyhow::anyhow!("signature verification failed: {e}"))?;
    Ok(())
}

pub fn parse_node_id_hex(node_id: &str) -> anyhow::Result<[u8; 32]> {
    let hex_str = node_id.trim().strip_prefix("0x").unwrap_or(node_id.trim());
    let bytes = hex::decode(hex_str).map_err(|e| anyhow::anyhow!("invalid node_id hex: {e}"))?;
    if bytes.len() != 32 {
        anyhow::bail!("node_id must be 32 bytes");
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Software/mock nodes: `node_id = keccak256(ed25519_pubkey)` when no libp2p peer id is sent.
pub fn node_id_from_ed25519_pubkey(pubkey: &[u8; 32]) -> [u8; 32] {
    keccak256(pubkey).0
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::{Signer, SigningKey};

    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let signing_key = SigningKey::from_bytes(&rand::random());
        let verifying_key = signing_key.verifying_key();
        let nonce = random_nonce();
        let payload = connect_challenge_payload(&nonce, 42);
        let sig = signing_key.sign(&payload);
        verify_connect_signature(&payload, &hex::encode(sig.to_bytes()), verifying_key.as_bytes())
            .unwrap();
    }
}
