use sparkl_router::*;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::admin::require_admin_token;
use crate::chain::watcher::spawn_chain_watcher;
use crate::chain::ChainVerifier;
use crate::config::Config;
use crate::consumer::models::ModelsCatalog;
use crate::routes::{build_router, metrics_handler};
use crate::state::RouterState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("sparkl_router=info".parse()?))
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));

    let config = Config::load(&config_path).context("load config")?;
    metrics::init_metrics();
    let metrics_handle =
        metrics::install_prometheus_recorder().context("install prometheus recorder")?;

    let chain_verifier = ChainVerifier::new(config.chain.clone());
    let models = ModelsCatalog::new();
    let state = RouterState::new(
        config.clone(),
        chain_verifier.clone(),
        models,
        metrics_handle,
    );

    spawn_chain_watcher(config.chain.clone(), Arc::new(chain_verifier));

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

    tokio::try_join!(
        axum::serve(main_listener, app),
        axum::serve(metrics_listener, metrics_app)
    )?;

    Ok(())
}
