//! Poll `SessionOpened` / `SessionFundsReleased` logs to warm/evict session cache.

use std::sync::Arc;
use std::time::Duration;

use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use alloy_primitives::Address;
use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::chain::ChainVerifier;
use crate::config::ChainConfig;

alloy::sol!(
    #[sol(rpc)]
    SettlementEscrow,
    concat!(env!("CARGO_MANIFEST_DIR"), "/abi/SettlementEscrow.json")
);

pub fn spawn_chain_watcher(chain: ChainConfig, verifier: Arc<ChainVerifier>) {
    if !chain.enabled {
        info!("chain watcher disabled (chain.enabled = false)");
        return;
    }

    let escrow = chain.escrow_contract.clone();
    if escrow.trim().parse::<Address>().ok() == Some(Address::ZERO) {
        info!("chain watcher skipped (escrow_contract not configured)");
        return;
    }

    tokio::spawn(async move {
        let mut last_block: u64 = 0;
        loop {
            if let Err(e) = poll_once(&chain, &escrow, verifier.clone(), &mut last_block).await {
                warn!(%e, "chain watcher poll failed");
            }
            tokio::time::sleep(Duration::from_secs(6)).await;
        }
    });
}

async fn poll_once(
    chain: &ChainConfig,
    escrow_addr: &str,
    verifier: Arc<ChainVerifier>,
    last_block: &mut u64,
) -> Result<()> {
    let rpc_url = chain.rpc_url.trim().parse().context("invalid rpc_url")?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let head = provider.get_block_number().await?;
    if *last_block == 0 {
        *last_block = head.saturating_sub(1);
    }
    let from = *last_block + 1;
    if from > head {
        return Ok(());
    }

    let escrow: Address = escrow_addr.parse().context("invalid escrow")?;

    let opened_filter = Filter::new()
        .address(escrow)
        .event_signature(SettlementEscrow::SessionOpened::SIGNATURE_HASH)
        .from_block(from)
        .to_block(head);
    let released_filter = Filter::new()
        .address(escrow)
        .event_signature(SettlementEscrow::SessionFundsReleased::SIGNATURE_HASH)
        .from_block(from)
        .to_block(head);

    let opened = provider.get_logs(&opened_filter).await?;
    for log in opened {
        if let Ok(ev) = SettlementEscrow::SessionOpened::decode_log(&log.inner) {
            let session_id: u64 = ev.sessionId.to::<u64>();
            verifier.warm_session(session_id).await;
        }
    }

    let released = provider.get_logs(&released_filter).await?;
    for log in released {
        if let Ok(ev) = SettlementEscrow::SessionFundsReleased::decode_log(&log.inner) {
            let session_id: u64 = ev.sessionId.to::<u64>();
            verifier.evict_session(session_id).await;
        }
    }

    *last_block = head;
    Ok(())
}
