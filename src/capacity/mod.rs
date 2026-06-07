use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Notify;

use crate::state::NodeId;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ModelCapacityKey {
    pub node_id: NodeId,
    pub model_id: String,
}

#[derive(Debug, Clone, Copy)]
pub struct CapacitySnapshot {
    pub active_requests: u32,
    pub queued_requests: u32,
    pub concurrency: u32,
}

#[derive(Debug)]
pub enum AcquireError {
    QueueFull {
        active: u32,
        concurrency: u32,
        queued: u32,
    },
    WaitTimeout {
        active: u32,
        concurrency: u32,
        queued: u32,
    },
}

struct Slot {
    active: AtomicU32,
    queued: AtomicU32,
    notify: Notify,
}

#[derive(Clone)]
pub struct CapacityTracker {
    slots: Arc<DashMap<ModelCapacityKey, Arc<Slot>>>,
}

impl Default for CapacityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CapacityTracker {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(DashMap::new()),
        }
    }

    pub fn snapshot(&self, key: &ModelCapacityKey) -> CapacitySnapshot {
        let slot = self.slot_for(key);
        CapacitySnapshot {
            active_requests: slot.active.load(Ordering::Relaxed),
            queued_requests: slot.queued.load(Ordering::Relaxed),
            concurrency: 0,
        }
    }

    pub fn snapshot_with_concurrency(
        &self,
        key: &ModelCapacityKey,
        concurrency: u32,
    ) -> CapacitySnapshot {
        let mut snap = self.snapshot(key);
        snap.concurrency = concurrency;
        snap
    }

    pub fn all_snapshots(&self) -> HashMap<ModelCapacityKey, (u32, u32)> {
        self.slots
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let active = entry.value().active.load(Ordering::Relaxed);
                let queued = entry.value().queued.load(Ordering::Relaxed);
                (key, (active, queued))
            })
            .collect()
    }

    pub fn clear_node(&self, node_id: &NodeId) {
        self.slots.retain(|key, _| &key.node_id != node_id);
    }

    /// Acquire a capacity slot. `concurrency == 0` means unlimited (no tracking).
    pub async fn acquire(
        &self,
        key: ModelCapacityKey,
        concurrency: u32,
        max_queue: u32,
        wait_timeout: Duration,
    ) -> Result<CapacityGuard, AcquireError> {
        if concurrency == 0 {
            return Ok(CapacityGuard {
                tracker: self.clone(),
                key,
                unlimited: true,
            });
        }

        let slot = self.slot_for(&key);
        let deadline = tokio::time::Instant::now() + wait_timeout;

        loop {
            let active = slot.active.load(Ordering::Acquire);
            if active < concurrency {
                match slot.active.compare_exchange_weak(
                    active,
                    active + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        return Ok(CapacityGuard {
                            tracker: self.clone(),
                            key,
                            unlimited: false,
                        });
                    }
                    Err(_) => continue,
                }
            }

            let queued = slot.queued.load(Ordering::Acquire);
            if max_queue == 0 || queued >= max_queue {
                return Err(AcquireError::QueueFull {
                    active,
                    concurrency,
                    queued,
                });
            }

            match slot.queued.compare_exchange_weak(
                queued,
                queued + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let notified = tokio::time::timeout(remaining, slot.notify.notified()).await;
                    slot.queued.fetch_sub(1, Ordering::AcqRel);
                    if notified.is_err() {
                        let queued_now = slot.queued.load(Ordering::Relaxed);
                        let active_now = slot.active.load(Ordering::Relaxed);
                        return Err(AcquireError::WaitTimeout {
                            active: active_now,
                            concurrency,
                            queued: queued_now,
                        });
                    }
                    continue;
                }
                Err(_) => continue,
            }
        }
    }

    fn release_inner(&self, key: &ModelCapacityKey) {
        if let Some(slot) = self.slots.get(key) {
            slot.active.fetch_sub(1, Ordering::AcqRel);
            slot.notify.notify_one();
        }
    }

    fn slot_for(&self, key: &ModelCapacityKey) -> Arc<Slot> {
        if let Some(slot) = self.slots.get(key) {
            return Arc::clone(slot.value());
        }
        let slot = Arc::new(Slot {
            active: AtomicU32::new(0),
            queued: AtomicU32::new(0),
            notify: Notify::new(),
        });
        self.slots
            .entry(key.clone())
            .or_insert_with(|| Arc::clone(&slot))
            .clone()
    }

}

pub struct CapacityGuard {
    tracker: CapacityTracker,
    key: ModelCapacityKey,
    unlimited: bool,
}

impl Drop for CapacityGuard {
    fn drop(&mut self) {
        if !self.unlimited {
            self.tracker.release_inner(&self.key);
        }
    }
}

pub fn max_queue_depth(concurrency: u32, queue_depth_ratio: f64) -> u32 {
    if concurrency == 0 {
        return 0;
    }
    let depth = (concurrency as f64 * queue_depth_ratio).floor() as u32;
    depth.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ModelCapacityKey {
        ModelCapacityKey {
            node_id: [1u8; 32],
            model_id: "test-model".into(),
        }
    }

    #[tokio::test]
    async fn admit_up_to_concurrency() {
        let tracker = CapacityTracker::new();
        let k = key();
        let g1 = tracker
            .acquire(k.clone(), 2, 2, Duration::from_secs(1))
            .await
            .unwrap();
        let _g2 = tracker
            .acquire(k.clone(), 2, 2, Duration::from_millis(50))
            .await
            .unwrap();
        drop(g1);
        let g3 = tracker
            .acquire(k, 2, 2, Duration::from_secs(1))
            .await
            .unwrap();
        drop(g3);
    }

    #[tokio::test]
    async fn queue_full_returns_error() {
        let tracker = CapacityTracker::new();
        let k = key();
        let _g1 = tracker
            .acquire(k.clone(), 1, 0, Duration::from_secs(1))
            .await
            .unwrap();
        let result = tracker
            .acquire(k, 1, 0, Duration::from_millis(10))
            .await;
        assert!(matches!(result, Err(AcquireError::QueueFull { .. })));
    }

    #[test]
    fn max_queue_depth_scales_with_concurrency() {
        assert_eq!(max_queue_depth(4, 1.0), 4);
        assert_eq!(max_queue_depth(0, 1.0), 0);
    }
}
