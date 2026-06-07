//! Per-session accumulation and batched flush to `recordUsage` on-chain.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::chain::usage::RecordUsageClient;
use crate::chain::ChainVerifier;
use crate::config::SettlementConfig;

#[derive(Debug)]
struct SessionBatch {
    watermark_input: u64,
    watermark_output: u64,
    pending_input: u64,
    pending_output: u64,
    last_flush_at: Instant,
    last_activity: Instant,
}

impl SessionBatch {
    fn new(watermark_input: u64, watermark_output: u64) -> Self {
        let now = Instant::now();
        Self {
            watermark_input,
            watermark_output,
            pending_input: 0,
            pending_output: 0,
            last_flush_at: now,
            last_activity: now,
        }
    }

    fn pending_total(&self) -> u64 {
        self.pending_input.saturating_add(self.pending_output)
    }
}

#[derive(Clone)]
pub struct UsageBatcher {
    inner: Arc<UsageBatcherInner>,
}

struct UsageBatcherInner {
    config: SettlementConfig,
    client: Arc<RecordUsageClient>,
    chain: Arc<ChainVerifier>,
    sessions: DashMap<u64, Mutex<SessionBatch>>,
}

impl UsageBatcher {
    pub fn new(
        config: SettlementConfig,
        client: Arc<RecordUsageClient>,
        chain: Arc<ChainVerifier>,
    ) -> Self {
        Self {
            inner: Arc::new(UsageBatcherInner {
                config,
                client,
                chain,
                sessions: DashMap::new(),
            }),
        }
    }

    pub fn spawn_sweeper(&self) {
        let flush_interval =
            Duration::from_secs(self.inner.config.record_usage_flush_interval_secs.max(1));
        let sweep_every = Duration::from_secs(flush_interval.as_secs().min(10).max(5));
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sweep_every).await;
                this.sweep_timeouts().await;
            }
        });
    }

    async fn sweep_timeouts(&self) {
        let flush_interval =
            Duration::from_secs(self.inner.config.record_usage_flush_interval_secs.max(1));
        let ids: Vec<u64> = self
            .inner
            .sessions
            .iter()
            .map(|e| *e.key())
            .collect();
        for session_id in ids {
            let should_flush = {
                let entry = match self.inner.sessions.get(&session_id) {
                    Some(e) => e,
                    None => continue,
                };
                let guard = entry.lock().await;
                guard.pending_total() > 0
                    && guard.last_flush_at.elapsed() >= flush_interval
            };
            if should_flush {
                let _ = self.flush_session(session_id, true, false).await;
            }
        }
    }

    pub async fn ensure_session(&self, session_id: u64) {
        if self.inner.sessions.contains_key(&session_id) {
            return;
        }
        match self.inner.chain.get_session(session_id).await {
            Ok(chain) => {
                self.inner.sessions.entry(session_id).or_insert_with(|| {
                    Mutex::new(SessionBatch::new(
                        chain.input_tokens_recorded,
                        chain.output_tokens_recorded,
                    ))
                });
            }
            Err(e) => warn!(%e, session_id, "failed to init usage batch watermarks"),
        }
    }

    pub async fn add_usage(&self, session_id: u64, input: u64, output: u64) {
        if input == 0 && output == 0 {
            return;
        }
        self.ensure_session(session_id).await;
        let chunk = self.inner.config.record_usage_token_chunk.max(1);
        let (pending_input, pending_output, should_flush) = {
            let entry = match self.inner.sessions.get(&session_id) {
                Some(e) => e,
                None => return,
            };
            let mut guard = entry.lock().await;
            guard.pending_input = guard.pending_input.saturating_add(input);
            guard.pending_output = guard.pending_output.saturating_add(output);
            guard.last_activity = Instant::now();
            let should_flush = guard.pending_total() >= chunk;
            (
                guard.pending_input,
                guard.pending_output,
                should_flush,
            )
        };
        info!(
            session_id,
            input,
            output,
            pending_input,
            pending_output,
            token_chunk = chunk,
            should_flush,
            "recordUsage: accumulated token usage"
        );
        if should_flush {
            let _ = self.flush_session(session_id, false, false).await;
        }
    }

    pub async fn force_flush(&self, session_id: u64) {
        let _ = self.flush_session(session_id, true, false).await;
    }

    pub async fn remove_session(&self, session_id: u64) {
        let _ = self.flush_session(session_id, true, false).await;
        self.inner.sessions.remove(&session_id);
    }

    /// Flush every session with pending usage; awaits on-chain submission (shutdown path).
    pub async fn flush_all_on_shutdown(&self) {
        let ids: Vec<u64> = self
            .inner
            .sessions
            .iter()
            .map(|e| *e.key())
            .collect();
        info!(
            session_count = ids.len(),
            "recordUsage: shutdown flush of all WIP batches"
        );
        for session_id in ids {
            if let Err(e) = self.flush_session(session_id, true, true).await {
                warn!(%e, session_id, "recordUsage: shutdown flush failed for session");
            }
        }
    }

    async fn flush_session(
        &self,
        session_id: u64,
        force: bool,
        await_chain: bool,
    ) -> anyhow::Result<()> {
        self.ensure_session(session_id).await;
        let (input_delta, output_delta) = {
            let entry = match self.inner.sessions.get(&session_id) {
                Some(e) => e,
                None => return Ok(()),
            };
            let mut guard = entry.lock().await;
            if guard.pending_total() == 0 {
                return Ok(());
            }
            if !force {
                let flush_interval = Duration::from_secs(
                    self.inner.config.record_usage_flush_interval_secs.max(1),
                );
                let chunk = self.inner.config.record_usage_token_chunk.max(1);
                if guard.pending_total() < chunk && guard.last_flush_at.elapsed() < flush_interval {
                    debug!(
                        session_id,
                        pending_input = guard.pending_input,
                        pending_output = guard.pending_output,
                        ?flush_interval,
                        "recordUsage: flush skipped (below chunk and interval)"
                    );
                    return Ok(());
                }
            }
            let input_delta = guard.pending_input;
            let output_delta = guard.pending_output;
            guard.pending_input = 0;
            guard.pending_output = 0;
            guard.last_flush_at = Instant::now();
            (input_delta, output_delta)
        };

        info!(
            session_id,
            input_delta,
            output_delta,
            force,
            await_chain,
            "recordUsage: submitting batch to chain"
        );

        let client = Arc::clone(&self.inner.client);
        if await_chain {
            client
                .record_usage_with_retry(session_id, input_delta, output_delta)
                .await;
        } else {
            tokio::spawn(async move {
                client
                    .record_usage_with_retry(session_id, input_delta, output_delta)
                    .await;
            });
        }

        if let Ok(chain) = self.inner.chain.get_session(session_id).await {
            if let Some(entry) = self.inner.sessions.get(&session_id) {
                let mut guard = entry.lock().await;
                guard.watermark_input = chain.input_tokens_recorded;
                guard.watermark_output = chain.output_tokens_recorded;
            }
            self.inner.chain.evict_session(session_id).await;
            self.inner.chain.warm_session(session_id).await;
        }

        Ok(())
    }
}
