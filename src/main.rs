use sparkl_router::*;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Notify;

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::admin::require_admin_token;
use crate::chain::record_usage_key;
use crate::chain::usage::RecordUsageClient;
use crate::chain::watcher::spawn_chain_watcher;
use crate::chain::ChainVerifier;
use crate::config::Config;
use crate::consumer::models::ModelsCatalog;
use crate::consumer::usage_batch::UsageBatcher;
use crate::routes::{build_router, metrics_handler};
use crate::state::RouterState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("sparkl_router=info".parse()?);
    tracing_subscriber::fmt().with_env_filter(log_filter).init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let mut config = Config::load(&config_path).context("load config")?;
    info!(config = %config_path.display(), "loaded config");

    if config.chain.enabled {
        let key = record_usage_key::bootstrap(
            &config_path,
            &config.chain,
            &config.settlement.data_dir,
            &config.settlement.record_usage_private_key,
            &config.settlement.registry_owner_private_key,
            true,
        )
        .await
        .context("recordUsage key bootstrap")?;
        config.settlement.record_usage_private_key = key.private_key_hex;
        if key.generated {
            info!(address = %key.address, "recordUsage signing key ready (newly generated)");
        } else {
            info!(address = %key.address, "recordUsage signing key ready");
        }
    }
    info!(
        rpc_url = %config.chain.rpc_url,
        registry_contract = %config.chain.registry_contract,
        escrow_contract = %config.chain.escrow_contract,
        chain_enabled = config.chain.enabled,
        settlement_enabled = config.settlement.enabled,
        "on-chain contracts"
    );
    metrics::init_metrics();
    let metrics_handle =
        metrics::install_prometheus_recorder().context("install prometheus recorder")?;

    let chain_verifier = ChainVerifier::new(config.chain.clone());
    let chain_arc = Arc::new(chain_verifier.clone());

    let usage_batcher = if config.settlement.is_active(config.chain.enabled) {
        match RecordUsageClient::new(&config.chain, &config.settlement).await {
            Ok(client) => {
                let batcher = UsageBatcher::new(
                    config.settlement.clone(),
                    client,
                    Arc::clone(&chain_arc),
                );
                batcher.spawn_sweeper();
                info!(
                    token_chunk = config.settlement.record_usage_token_chunk,
                    flush_secs = config.settlement.record_usage_flush_interval_secs,
                    "usage batcher enabled"
                );
                Some(batcher)
            }
            Err(e) => {
                tracing::warn!(
                    %e,
                    "usage batcher disabled (recordUsageRole missing or signer mismatch; redeploy escrow and restart)"
                );
                info!(
                    "recordUsage metering inactive — run ./scripts/set-record-usage-role.sh after redeploy; \
                     per-request usage logs will show 'usage batcher disabled' until fixed"
                );
                None
            }
        }
    } else {
        info!("usage batcher disabled (settlement inactive in config)");
        None
    };

    let models = ModelsCatalog::new();
    let state = RouterState::new(
        config.clone(),
        chain_verifier,
        models,
        usage_batcher.clone(),
        metrics_handle,
    );

    spawn_chain_watcher(config.chain.clone(), chain_arc, usage_batcher);

    let app = build_router(state.clone());

    let main_addr: SocketAddr = config.server.bind.parse().context("invalid server.bind")?;
    let metrics_addr: SocketAddr = config
        .metrics
        .bind
        .parse()
        .context("invalid metrics.bind")?;

    let metrics_app = Router::new()
        .route("/metrics", get(metrics_handler))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_admin_token,
        ))
        .with_state(state.clone());

    info!(%main_addr, "sparkl-router listening");
    info!(%metrics_addr, "metrics listening");

    let main_listener = tokio::net::TcpListener::bind(main_addr)
        .await
        .context("bind main server")?;
    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr)
        .await
        .context("bind metrics server")?;

    let shutdown = Arc::new(Notify::new());
    let shutdown_trigger = Arc::clone(&shutdown);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("shutdown signal received (ctrl-c); stopping servers");
            shutdown_trigger.notify_waiters();
        }
    });

    let wait_shutdown = |notify: Arc<Notify>| async move {
        notify.notified().await;
    };

    tokio::try_join!(
        axum::serve(main_listener, app).with_graceful_shutdown(wait_shutdown(Arc::clone(
            &shutdown
        ))),
        axum::serve(metrics_listener, metrics_app)
            .with_graceful_shutdown(wait_shutdown(shutdown)),
    )?;

    if let Some(batcher) = state.usage_batcher.clone() {
        info!("draining WIP token usage to chain before exit");
        batcher.flush_all_on_shutdown().await;
    }

    info!("sparkl-router stopped");
    Ok(())
}
