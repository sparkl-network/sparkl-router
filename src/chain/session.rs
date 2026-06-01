use alloy::providers::ProviderBuilder;
use alloy_primitives::{Address, FixedBytes, U256};
use anyhow::{Context, Result};

use crate::config::ChainConfig;

alloy::sol!(
    #[sol(rpc)]
    SettlementEscrow,
    concat!(env!("CARGO_MANIFEST_DIR"), "/abi/SettlementEscrow.json")
);

#[derive(Debug, Clone)]
pub struct ChainSession {
    pub user: Address,
    pub node_id: FixedBytes<32>,
    pub model_id: FixedBytes<32>,
    pub settled: bool,
}

impl ChainSession {
    pub fn is_open(&self) -> bool {
        self.user != Address::ZERO && !self.settled
    }
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
    let escrow = SettlementEscrow::new(escrow_addr, &provider);

    let s = escrow
        .sessions(U256::from(session_id))
        .call()
        .await
        .context("sessions() eth_call failed")?;

    Ok(ChainSession {
        user: s.user,
        node_id: s.nodeId,
        model_id: s.modelId,
        settled: s.settled,
    })
}
