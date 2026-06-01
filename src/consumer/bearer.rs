use anyhow::{Context, Result};

/// Parsed `sk_<base58(32-byte sessionId || 32-byte secret)>`.
#[derive(Debug, Clone)]
pub struct ParsedBearer {
    pub session_id: u64,
}

pub fn parse_sk_bearer(token: &str) -> Result<ParsedBearer> {
    let raw = token
        .strip_prefix("sk_")
        .context("bearer token must start with sk_")?;
    let bytes = bs58::decode(raw)
        .into_vec()
        .context("invalid base58 in bearer token")?;
    if bytes.len() != 64 {
        anyhow::bail!("sk_ token must decode to 64 bytes");
    }
    let mut session_bytes = [0u8; 32];
    session_bytes.copy_from_slice(&bytes[..32]);
    if session_bytes[..24] != [0u8; 24] {
        anyhow::bail!("session id exceeds u64 range in bearer token");
    }
    let session_id = u64::from_be_bytes(session_bytes[24..32].try_into().unwrap());
    Ok(ParsedBearer { session_id })
}

pub fn parse_authorization_header(authz: &str) -> Result<ParsedBearer> {
    let token = authz
        .strip_prefix("Bearer ")
        .or_else(|| authz.strip_prefix("bearer "))
        .context("Authorization must be Bearer sk_...")?
        .trim();
    parse_sk_bearer(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_session_id_zero_padded() {
        let mut payload = [0u8; 64];
        payload[31] = 7; // session id 7 in last byte of first 32
        let token = format!("sk_{}", bs58::encode(payload).into_string());
        let parsed = parse_sk_bearer(&token).unwrap();
        assert_eq!(parsed.session_id, 7);
    }
}
