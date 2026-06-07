//! Submit batched `recordUsage` transactions as the on-chain metering role.

use std::sync::Arc;
use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy_primitives::{Address, U256};
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use crate::config::{ChainConfig, SettlementConfig};

alloy::sol!(
    #[sol(rpc)]
    SettlementEscrow,
    concat!(env!("CARGO_MANIFEST_DIR"), "/abi/SettlementEscrow.json")
);

#[derive(Clone)]
pub struct RecordUsageClient {
    escrow: Address,
    rpc_url: reqwest::Url,
    wallet: EthereumWallet,
    _signer_address: Address,
}

impl RecordUsageClient {
    pub async fn new(chain: &ChainConfig, settlement: &SettlementConfig) -> Result<Arc<Self>> {
        let escrow: Address = chain
            .escrow_contract
            .trim()
            .parse()
            .context("invalid escrow_contract")?;
        if escrow == Address::ZERO {
            anyhow::bail!("escrow_contract is zero address");
        }

        let rpc_url = chain
            .rpc_url
            .trim()
            .parse::<reqwest::Url>()
            .context("invalid rpc_url")?;

        let pk = settlement.record_usage_private_key.trim();
        let signer: PrivateKeySigner = pk.parse().context("invalid record_usage_private_key")?;
        let signer_address = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet.clone())
            .connect_http(rpc_url.clone());
        let escrow_contract = SettlementEscrow::new(escrow, provider);
        let on_chain_role = escrow_contract
            .recordUsageRole()
            .call()
            .await
            .context("recordUsageRole eth_call failed")?;
        if on_chain_role != signer_address {
            anyhow::bail!(
                "record_usage signer {signer_address} does not match on-chain recordUsageRole {on_chain_role}"
            );
        }

        info!(%signer_address, %escrow, "recordUsage client ready");
        Ok(Arc::new(Self {
            escrow,
            rpc_url,
            wallet,
            _signer_address: signer_address,
        }))
    }

    pub async fn record_usage(
        &self,
        session_id: u64,
        input_delta: u64,
        output_delta: u64,
    ) -> Result<()> {
        if input_delta == 0 && output_delta == 0 {
            debug!(session_id, "recordUsage: no-op (zero deltas)");
            return Ok(());
        }

        info!(
            session_id,
            input_delta,
            output_delta,
            %self.escrow,
            "recordUsage: sending transaction"
        );

        let provider = ProviderBuilder::new()
            .wallet(self.wallet.clone())
            .connect_http(self.rpc_url.clone());
        let escrow = SettlementEscrow::new(self.escrow, provider);

        let pending = escrow
            .recordUsage(
                U256::from(session_id),
                U256::from(input_delta),
                U256::from(output_delta),
            )
            .send()
            .await
            .with_context(|| {
                format!(
                    "recordUsage send failed (signer {} needs ETH on {} for gas)",
                    self._signer_address, self.rpc_url
                )
            })?;

        let tx_hash = pending
            .with_required_confirmations(1)
            .watch()
            .await
            .context("recordUsage confirmation failed")?;

        info!(
            session_id,
            input_delta,
            output_delta,
            ?tx_hash,
            "recordUsage confirmed"
        );
        Ok(())
    }

    pub async fn record_usage_with_retry(
        &self,
        session_id: u64,
        input_delta: u64,
        output_delta: u64,
    ) {
        const MAX_ATTEMPTS: u32 = 3;
        info!(
            session_id,
            input_delta,
            output_delta,
            max_attempts = MAX_ATTEMPTS,
            "recordUsage: begin with retry"
        );
        for attempt in 1..=MAX_ATTEMPTS {
            match self.record_usage(session_id, input_delta, output_delta).await {
                Ok(()) => {
                    crate::metrics::inc_record_usage_success();
                    return;
                }
                Err(e) => {
                    warn!(
                        %e,
                        session_id,
                        input_delta,
                        output_delta,
                        attempt,
                        "recordUsage failed"
                    );
                    if attempt < MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(2u64.pow(attempt - 1))).await;
                    }
                }
            }
        }
        crate::metrics::inc_record_usage_failures();
    }
}
