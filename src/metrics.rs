use metrics::{counter, describe_counter, describe_histogram, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub fn init_metrics() {
    describe_counter!(
        "sparkl_router_requests_forwarded_total",
        "Total HTTP requests forwarded to provider tunnels"
    );
    describe_counter!(
        "sparkl_router_tunnel_connections_total",
        "Total provider tunnel connections established"
    );
    describe_histogram!(
        "sparkl_router_chunk_latency_seconds",
        "Latency from chunk received to forwarded to consumer"
    );
}

pub fn install_prometheus_recorder() -> anyhow::Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("prometheus install: {e}"))
}

pub fn inc_requests_forwarded() {
    counter!("sparkl_router_requests_forwarded_total").increment(1);
}

pub fn inc_tunnel_connected() {
    counter!("sparkl_router_tunnel_connections_total").increment(1);
}

pub fn observe_chunk_latency(seconds: f64) {
    histogram!("sparkl_router_chunk_latency_seconds").record(seconds);
}
