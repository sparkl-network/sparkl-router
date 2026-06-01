use alloy::providers::{Provider, ProviderBuilder};
use alloy_primitives::{Address, FixedBytes};
use anyhow::{Context, Result};

use crate::config::ChainConfig;

alloy::sol!(
    #[sol(rpc)]
    ProviderRegistry,
    concat!(env!("CARGO_MANIFEST_DIR"), "/abi/ProviderRegistry.json")
);

pub async fn fetch_node_operator(chain: &ChainConfig, node_id: &[u8; 32]) -> Result<Address> {
    let registry_addr: Address = chain
        .registry_contract
        .trim()
        .parse()
        .context("invalid registry_contract")?;
    if registry_addr == Address::ZERO {
        anyhow::bail!("registry_contract is zero address");
    }

    let rpc_url = chain
        .rpc_url
        .trim()
        .parse::<reqwest::Url>()
        .context("invalid rpc_url")?;

    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let registry = ProviderRegistry::new(registry_addr, &provider);
    let id = FixedBytes::<32>::from(*node_id);

    registry
        .nodeOperator(id)
        .call()
        .await
        .context("nodeOperator() eth_call failed")
}

pub async fn is_node_registered(chain: &ChainConfig, node_id: &[u8; 32]) -> Result<bool> {
    let op = fetch_node_operator(chain, node_id).await?;
    Ok(op != Address::ZERO)
}

pub async fn fetch_latest_block(chain: &ChainConfig) -> Result<u64> {
    let rpc_url = chain
        .rpc_url
        .trim()
        .parse::<reqwest::Url>()
        .context("invalid rpc_url")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let block = provider
        .get_block_number()
        .await
        .context("get_block_number failed")?;
    Ok(block)
}
