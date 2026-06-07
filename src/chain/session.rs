use alloy::providers::ProviderBuilder;
use alloy_primitives::{Address, FixedBytes, U256};
use anyhow::{Context, Result, ensure};

use crate::config::ChainConfig;

alloy::sol! {
    #[sol(rpc)]
    interface ISettlementEscrowSessions {
        function sessions(uint256 sessionId) external view;
    }
}

/// Static head of `SettlementEscrow.Session` through `outputTokensRecorded`
/// (excludes pricing snapshot fields and dynamic `string name`).
const SESSION_HEAD_WORDS: usize = 13;
/// Words in the static session head when ABI returns a tuple with trailing `string`.
const SESSION_STRUCT_OFFSET: usize = 0x20;

#[derive(Debug, Clone)]
pub struct ChainSession {
    pub user: Address,
    pub node_id: FixedBytes<32>,
    pub model_id: FixedBytes<32>,
    pub tier: u8,
    pub locked_internal: u128,
    pub usage_recorded: u128,
    pub input_tokens_recorded: u64,
    pub output_tokens_recorded: u64,
    pub settled: bool,
}

impl ChainSession {
    pub fn is_open(&self) -> bool {
        self.user != Address::ZERO && !self.settled
    }

    pub fn budget_exhausted(&self) -> bool {
        self.usage_recorded >= self.locked_internal && self.locked_internal > 0
    }
}

fn word(data: &[u8], index: usize) -> Result<&[u8]> {
    let start = index * 32;
    let end = start + 32;
    ensure!(
        data.len() >= end,
        "sessions() returndata too short (need {end} bytes, got {})",
        data.len()
    );
    Ok(&data[start..end])
}

fn address_at(data: &[u8], index: usize) -> Result<Address> {
    let w = word(data, index)?;
    Ok(Address::from_slice(&w[12..32]))
}

fn bytes32_at(data: &[u8], index: usize) -> Result<FixedBytes<32>> {
    Ok(FixedBytes::from_slice(word(data, index)?))
}

fn u256_at(data: &[u8], index: usize) -> Result<U256> {
    Ok(U256::from_be_slice(word(data, index)?))
}

fn u64_at(data: &[u8], index: usize) -> Result<u64> {
    let w = word(data, index)?;
    Ok(u64::from_be_bytes(w[24..32].try_into().unwrap()))
}

fn bool_at(data: &[u8], index: usize) -> Result<bool> {
    Ok(word(data, index)?[31] != 0)
}

/// `sessions()` returns a struct with dynamic `string name`, so returndata is prefixed
/// with a word offset (typically `0x20`) before the static field head.
fn session_head_bytes(data: &[u8]) -> Result<&[u8]> {
    ensure!(
        data.len() >= 32,
        "sessions() returndata too short (need at least 32 bytes, got {})",
        data.len()
    );
    let offset = u256_at(data, 0)?;
    let base = if offset == U256::from(SESSION_STRUCT_OFFSET) {
        let start = SESSION_STRUCT_OFFSET;
        ensure!(
            data.len() >= start + SESSION_HEAD_WORDS * 32,
            "sessions() returndata too short for offset head (need {}, got {})",
            start + SESSION_HEAD_WORDS * 32,
            data.len()
        );
        &data[start..]
    } else {
        ensure!(
            data.len() >= SESSION_HEAD_WORDS * 32,
            "sessions() returndata too short for session head"
        );
        data
    };
    Ok(base)
}

/// Decode session getter output without the trailing `string name` (alloy cannot decode it).
fn decode_session_head(data: &[u8]) -> Result<ChainSession> {
    let head = session_head_bytes(data)?;

    // Layout matches SettlementEscrow.Session through outputTokensRecorded.
    Ok(ChainSession {
        user: address_at(head, 0)?,
        node_id: bytes32_at(head, 1)?,
        model_id: bytes32_at(head, 2)?,
        tier: word(head, 3)?[31],
        locked_internal: u128::try_from(u256_at(head, 4)?).unwrap_or(u128::MAX),
        usage_recorded: u128::try_from(u256_at(head, 5)?).unwrap_or(u128::MAX),
        input_tokens_recorded: u64_at(head, 11)?,
        output_tokens_recorded: u64_at(head, 12)?,
        settled: bool_at(head, 10)?,
    })
}

pub async fn fetch_session(chain: &ChainConfig, session_id: u64) -> Result<ChainSession> {
    let escrow_addr: Address = chain
        .escrow_contract
        .trim()
        .parse()
        .context("invalid escrow_contract")?;
    if escrow_addr == Address::ZERO {
        anyhow::bail!("escrow_contract is zero address");
    }

    let rpc_url = chain
        .rpc_url
        .trim()
        .parse::<reqwest::Url>()
        .context("invalid rpc_url")?;

    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let escrow = ISettlementEscrowSessions::new(escrow_addr, &provider);

    let raw = escrow
        .sessions(U256::from(session_id))
        .call_raw()
        .await
        .map_err(|e| anyhow::anyhow!("sessions() eth_call failed: {e}"))?;

    decode_session_head(raw.as_ref()).context("sessions() returndata decode failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256};

    #[test]
    fn decode_session_head_skips_abi_struct_offset() {
        let raw = hex::decode(
            "0000000000000000000000000000000000000000000000000000000000000020\
             000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266\
             877f5a2d994b90eaaf84250d4898ab049539460eeecb2614216526d4cefd2ed8\
             19901b5106ee310093b779e131891ec328afec79c93bff6c3872832f6156ba31\
             0000000000000000000000000000000000000000000000000000000000000000\
             0000000000000000000000000000000000000000000000000000de0b6b3a7640000\
             0000000000000000000000000000000000000000000000000000000000000000\
             0000000000000000000000000000000000000000000000000000000000000000\
             0000000000000000000000000000000000000000000000000000000000000000\
             0000000000000000000000000000000000000000000000000000de0b6b3a7640000\
             000000000000000000000000000000000000000000000000000000006a22d31c\
             0000000000000000000000000000000000000000000000000000000000000000\
             0000000000000000000000000000000000000000000000000000000000000000\
             0000000000000000000000000000000000000000000000000000000000000000",
        )
        .expect("hex");

        let session = decode_session_head(&raw).expect("decode");
        assert_eq!(
            session.user,
            address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
        );
        assert_eq!(
            session.node_id,
            b256!("0x877f5a2d994b90eaaf84250d4898ab049539460eeecb2614216526d4cefd2ed8")
        );
        assert!(!session.settled);
        assert!(session.is_open());
    }
}
