//! Persisted secp256k1 key for `recordUsageRole` (load or generate at startup).

use std::fs;
use std::path::{Path, PathBuf};

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy_primitives::{Address, U256};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::ChainConfig;

alloy::sol!(
    #[sol(rpc)]
    SettlementEscrow,
    concat!(env!("CARGO_MANIFEST_DIR"), "/abi/SettlementEscrow.json")
);

const KEY_FILENAME: &str = "record-usage-key.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRecordUsageKey {
    private_key: String,
    address: Address,
}

#[derive(Debug, Clone)]
pub struct RecordUsageKeyMaterial {
    pub private_key_hex: String,
    pub address: Address,
    pub generated: bool,
}

/// Resolve router data directory (persisted keys, etc.).
pub fn resolve_data_dir(config_path: &Path, configured: &str) -> PathBuf {
    let trimmed = configured.trim();
    if !trimmed.is_empty() {
        return PathBuf::from(trimmed);
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("data")
}

fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEY_FILENAME)
}

fn parse_signer(hex_key: &str) -> Result<PrivateKeySigner> {
    hex_key
        .trim()
        .parse::<PrivateKeySigner>()
        .context("invalid secp256k1 private key hex")
}

fn signer_to_hex(signer: &PrivateKeySigner) -> String {
    format!("0x{}", hex::encode(signer.to_bytes()))
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) {}

/// Load `recordUsage` signing key from config, disk, or generate a new one (solo-style).
pub fn load_or_generate(data_dir: &Path, config_inline_key: &str) -> Result<RecordUsageKeyMaterial> {
    fs::create_dir_all(data_dir).context("failed to create router data dir")?;
    let path = key_path(data_dir);

    let inline = config_inline_key.trim();
    if !inline.is_empty() {
        let signer = parse_signer(inline)?;
        let material = RecordUsageKeyMaterial {
            private_key_hex: signer_to_hex(&signer),
            address: signer.address(),
            generated: false,
        };
        persist_key_file(&path, &material)?;
        return Ok(material);
    }

    if path.exists() {
        let stored: StoredRecordUsageKey = serde_json::from_slice(
            &fs::read(&path).context("failed to read record-usage-key.json")?,
        )
        .context("invalid record-usage-key.json")?;
        let signer = parse_signer(&stored.private_key)?;
        let address = signer.address();
        if stored.address != address {
            warn!(
                file = %path.display(),
                file_address = %stored.address,
                derived_address = %address,
                "record-usage-key.json address mismatch; using derived address"
            );
        }
        return Ok(RecordUsageKeyMaterial {
            private_key_hex: signer_to_hex(&signer),
            address,
            generated: false,
        });
    }

    let signer = PrivateKeySigner::random();
    let material = RecordUsageKeyMaterial {
        private_key_hex: signer_to_hex(&signer),
        address: signer.address(),
        generated: true,
    };
    persist_key_file(&path, &material)?;
    info!(
        path = %path.display(),
        address = %material.address,
        "generated new recordUsage signing key"
    );
    Ok(material)
}

fn persist_key_file(path: &Path, material: &RecordUsageKeyMaterial) -> Result<()> {
    let stored = StoredRecordUsageKey {
        private_key: material.private_key_hex.clone(),
        address: material.address,
    };
    fs::write(path, serde_json::to_vec_pretty(&stored)?).context("failed to write record-usage-key.json")?;
    set_private_permissions(path);
    Ok(())
}

/// Register this router's EOA as `SettlementEscrow.recordUsageRole` when it differs on-chain.
pub async fn ensure_record_usage_role(
    chain: &ChainConfig,
    role_address: Address,
    registry_owner_private_key: &str,
) -> Result<()> {
    let escrow_addr: Address = chain
        .escrow_contract
        .trim()
        .parse()
        .context("invalid escrow_contract")?;
    if escrow_addr == Address::ZERO {
        return Ok(());
    }

    let rpc_url = chain
        .rpc_url
        .trim()
        .parse::<reqwest::Url>()
        .context("invalid rpc_url")?;

    let provider = ProviderBuilder::new().connect_http(rpc_url.clone());
    let escrow_read = SettlementEscrow::new(escrow_addr, &provider);

    let on_chain = match escrow_read.recordUsageRole().call().await {
        Ok(role) => role,
        Err(e) => {
            warn!(
                %e,
                %escrow_addr,
                "recordUsageRole eth_call failed; redeploy SettlementEscrow with recordUsageRole support"
            );
            return Ok(());
        }
    };

    if on_chain == role_address {
        info!(%role_address, %escrow_addr, "recordUsageRole already set");
        return Ok(());
    }

    let owner_pk = registry_owner_private_key.trim();
    if owner_pk.is_empty() {
        if on_chain == Address::ZERO {
            warn!(
                %role_address,
                "recordUsageRole is unset on-chain; set settlement.registry_owner_private_key (registry owner) to register this router key"
            );
        } else {
            warn!(
                on_chain = %on_chain,
                router = %role_address,
                "recordUsageRole does not match router key; configure settlement.registry_owner_private_key to update on-chain role"
            );
        }
        return Ok(());
    }

    let owner_signer = parse_signer(owner_pk)?;
    let owner_wallet = EthereumWallet::from(owner_signer);
    let provider = ProviderBuilder::new()
        .wallet(owner_wallet)
        .connect_http(rpc_url);
    let escrow = SettlementEscrow::new(escrow_addr, provider);

    info!(
        previous = %on_chain,
        next = %role_address,
        %escrow_addr,
        "submitting setRecordUsage"
    );

    let pending = escrow
        .setRecordUsage(role_address)
        .send()
        .await
        .context("setRecordUsage send failed")?;

    let tx_hash = pending
        .with_required_confirmations(1)
        .watch()
        .await
        .context("setRecordUsage confirmation failed")?;

    info!(?tx_hash, %role_address, "recordUsageRole updated on-chain");
    Ok(())
}

/// Ensure the metering EOA can pay gas for `recordUsage` txs (common local-dev oversight).
pub async fn ensure_record_usage_gas_funded(
    chain: &ChainConfig,
    role_address: Address,
    registry_owner_private_key: &str,
) -> Result<()> {
    const MIN_BALANCE_WEI: u128 = 10_000_000_000_000_000; // 0.01 ETH
    const TOP_UP_WEI: u128 = 100_000_000_000_000_000; // 0.1 ETH

    let rpc_url = chain
        .rpc_url
        .trim()
        .parse::<reqwest::Url>()
        .context("invalid rpc_url")?;

    let provider = ProviderBuilder::new().connect_http(rpc_url.clone());
    let balance = provider
        .get_balance(role_address)
        .await
        .context("recordUsageRole balance eth_call failed")?;
    if balance >= U256::from(MIN_BALANCE_WEI) {
        info!(%role_address, %balance, "recordUsageRole gas balance ok");
        return Ok(());
    }

    let owner_pk = registry_owner_private_key.trim();
    if owner_pk.is_empty() {
        warn!(
            %role_address,
            %balance,
            "recordUsageRole has no ETH for gas; set settlement.registry_owner_private_key and restart, \
             or fund this address manually (./scripts/set-record-usage-role.sh)"
        );
        return Ok(());
    }

    let owner_signer = parse_signer(owner_pk)?;
    let owner_wallet = EthereumWallet::from(owner_signer);
    let provider = ProviderBuilder::new()
        .wallet(owner_wallet)
        .connect_http(rpc_url);

    info!(
        %role_address,
        %balance,
        top_up_wei = TOP_UP_WEI,
        "funding recordUsageRole EOA for gas"
    );

    let tx = TransactionRequest::default()
        .with_to(role_address)
        .with_value(U256::from(TOP_UP_WEI));
    let pending = provider
        .send_transaction(tx)
        .await
        .context("recordUsageRole gas top-up send failed")?;

    let tx_hash = pending
        .with_required_confirmations(1)
        .watch()
        .await
        .context("recordUsageRole gas top-up confirmation failed")?;

    info!(?tx_hash, %role_address, "recordUsageRole funded for gas");
    Ok(())
}

/// Bootstrap signing key and optional on-chain role registration.
pub async fn bootstrap(
    config_path: &Path,
    chain: &ChainConfig,
    data_dir: &str,
    inline_key: &str,
    registry_owner_private_key: &str,
    register_on_chain: bool,
) -> Result<RecordUsageKeyMaterial> {
    if !chain.enabled {
        bail!("chain bootstrap requires chain.enabled = true");
    }

    let dir = resolve_data_dir(config_path, data_dir);
    let material = load_or_generate(&dir, inline_key)?;

    if register_on_chain {
        ensure_record_usage_role(chain, material.address, registry_owner_private_key).await?;
        ensure_record_usage_gas_funded(chain, material.address, registry_owner_private_key).await?;
    }

    Ok(material)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn generates_and_reload() {
        let dir = tempdir().unwrap();
        let m1 = load_or_generate(dir.path(), "").unwrap();
        assert!(m1.generated);
        let m2 = load_or_generate(dir.path(), "").unwrap();
        assert!(!m2.generated);
        assert_eq!(m1.address, m2.address);
        assert_eq!(m1.private_key_hex, m2.private_key_hex);
    }

    #[test]
    fn config_inline_overrides_file() {
        let dir = tempdir().unwrap();
        let _ = load_or_generate(dir.path(), "").unwrap();
        let signer = PrivateKeySigner::random();
        let hex = signer_to_hex(&signer);
        let m = load_or_generate(dir.path(), &hex).unwrap();
        assert_eq!(m.address, signer.address());
    }
}
