use crate::data::orderbook::OrderBookRegistry;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn clamp(value: f64, min: f64, max: f64) -> f64 {
    value.max(min).min(max)
}

/// Aggregated order-flow metrics for a single time window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowSnapshot {
    pub timestamp: f64,
    pub window_seconds: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub total_volume: f64,
    pub large_buy_volume: f64,
    pub large_sell_volume: f64,
    pub trade_count: usize,
    pub buy_count: usize,
    pub sell_count: usize,
    /// Delta from the previous window (for momentum calculation).
    pub prev_delta: Option<f64>,
}

impl FlowSnapshot {
    /// Net order-flow imbalance in [-1, 1].
    /// Positive values mean more buying; negative values more selling.
    pub fn delta(&self) -> f64 {
        if self.total_volume <= 0.0 {
            return 0.0;
        }
        (self.buy_volume - self.sell_volume) / self.total_volume
    }

    /// Fraction of total volume that was buy-initiated, in [0, 1].
    /// Returns 0.5 when total volume is zero (neutral).
    pub fn buy_fraction(&self) -> f64 {
        if self.total_volume <= 0.0 {
            return 0.5;
        }
        self.buy_volume / self.total_volume
    }

    /// Net imbalance among large trades, in [-1, 1].
    pub fn large_delta(&self) -> f64 {
        let total = self.large_buy_volume + self.large_sell_volume;
        if total <= 0.0 {
            return 0.0;
        }
        (self.large_buy_volume - self.large_sell_volume) / total
    }

    /// Change in delta compared to the previous window (momentum).
    pub fn momentum(&self) -> f64 {
        match self.prev_delta {
            Some(prev) => self.delta() - prev,
            None => 0.0,
        }
    }

    /// `true` when total volume is below the "low volume" threshold of 50 USDC.
    pub fn is_low_volume(&self) -> bool {
        self.total_volume < 50.0
    }
}

/// Computes order-flow metrics from the shared `OrderBookRegistry`.
pub struct OrderFlowCalculator {
    registry: Arc<RwLock<OrderBookRegistry>>,
    /// Minimum USDC size to count as a "large" trade.
    large_trade_threshold: f64,
    /// Multiplier: a trade is large when its size exceeds
    /// `large_trade_threshold * large_trade_multiplier`.
    #[allow(dead_code)]
    large_trade_multiplier: f64,
    /// Per-window snapshots from the most recent `compute` call, keyed by
    /// `window_seconds.to_bits()`.  Using a per-window map prevents the
    /// confirmation-window call from overwriting the primary-window snapshot,
    /// which would corrupt the next cycle's momentum calculation.
    prev_snapshots: parking_lot::Mutex<HashMap<u64, FlowSnapshot>>,
}

impl OrderFlowCalculator {
    pub fn new(
        registry: Arc<RwLock<OrderBookRegistry>>,
        large_trade_threshold: f64,
        large_trade_multiplier: f64,
    ) -> Self {
        Self {
            registry,
            large_trade_threshold,
            large_trade_multiplier,
            prev_snapshots: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Compute order-flow metrics over a rolling `window_seconds` window.
    pub fn compute(&self, window_seconds: f64) -> FlowSnapshot {
        let registry = self.registry.read();
        let trades = registry.all_recent_trades(window_seconds);

        let now = now_secs();
        let mut buy_vol = 0.0_f64;
        let mut sell_vol = 0.0_f64;
        let mut large_buy = 0.0_f64;
        let mut large_sell = 0.0_f64;
        let mut buy_count = 0_usize;
        let mut sell_count = 0_usize;

        for trade in &trades {
            if trade.is_buy_initiated {
                buy_vol += trade.size_usdc;
                buy_count += 1;
                if trade.size_usdc >= self.large_trade_threshold {
                    large_buy += trade.size_usdc;
                }
            } else {
                sell_vol += trade.size_usdc;
                sell_count += 1;
                if trade.size_usdc >= self.large_trade_threshold {
                    large_sell += trade.size_usdc;
                }
            }
        }

        let total_volume = buy_vol + sell_vol;

        // Retrieve previous delta for this specific window.  Each window
        // duration has its own slot in the map so that a call for the 30s
        // confirmation window cannot clobber the 10s primary-window snapshot.
        let window_key = window_seconds.to_bits();
        let prev_delta = self
            .prev_snapshots
            .lock()
            .get(&window_key)
            .map(|s| s.delta());

        let snap = FlowSnapshot {
            timestamp: now,
            window_seconds,
            buy_volume: buy_vol,
            sell_volume: sell_vol,
            total_volume,
            large_buy_volume: large_buy,
            large_sell_volume: large_sell,
            trade_count: trades.len(),
            buy_count,
            sell_count,
            prev_delta,
        };

        // Store this window's snapshot independently of all other windows.
        self.prev_snapshots
            .lock()
            .insert(window_key, snap.clone());

        snap
    }

    /// Compute metrics for each window in `windows` simultaneously.
    ///
    /// The map key is the window duration encoded as `OrderedFloat` (the raw
    /// `f64` bits, giving a total ordering without a wrapper type).
    pub fn compute_all_windows(
        &self,
        windows: &[f64],
    ) -> HashMap<u64, FlowSnapshot> {
        let registry = self.registry.read();
        let max_window = windows.iter().cloned().fold(0.0_f64, f64::max);
        let trades = registry.all_recent_trades(max_window);
        let now = now_secs();
        drop(registry);

        windows
            .iter()
            .map(|&w| {
                let cutoff = now - w;
                let mut buy_vol = 0.0_f64;
                let mut sell_vol = 0.0_f64;
                let mut large_buy = 0.0_f64;
                let mut large_sell = 0.0_f64;
                let mut buy_count = 0_usize;
                let mut sell_count = 0_usize;

                for trade in &trades {
                    if trade.timestamp < cutoff {
                        continue;
                    }
                    if trade.is_buy_initiated {
                        buy_vol += trade.size_usdc;
                        buy_count += 1;
                        if trade.size_usdc >= self.large_trade_threshold {
                            large_buy += trade.size_usdc;
                        }
                    } else {
                        sell_vol += trade.size_usdc;
                        sell_count += 1;
                        if trade.size_usdc >= self.large_trade_threshold {
                            large_sell += trade.size_usdc;
                        }
                    }
                }

                let total_volume = buy_vol + sell_vol;
                let snap = FlowSnapshot {
                    timestamp: now,
                    window_seconds: w,
                    buy_volume: buy_vol,
                    sell_volume: sell_vol,
                    total_volume,
                    large_buy_volume: large_buy,
                    large_sell_volume: large_sell,
                    trade_count: buy_count + sell_count,
                    buy_count,
                    sell_count,
                    prev_delta: None,
                };
                (w.to_bits(), snap)
            })
            .collect()
    }

    /// Return per-second (buy_volume, sell_volume) buckets for the last minute.
    pub fn volume_profile_last_minute(&self) -> Vec<(f64, f64)> {
        let registry = self.registry.read();
        let trades = registry.all_recent_trades(60.0);
        let now = now_secs();
        drop(registry);

        let mut buckets = vec![(0.0_f64, 0.0_f64); 60];
        for trade in &trades {
            let age = now - trade.timestamp;
            if age < 0.0 || age > 60.0 {
                continue;
            }
            let bucket = clamp(age.floor(), 0.0, 59.0) as usize;
            let idx = 59 - bucket; // bucket 0 = most recent
            if trade.is_buy_initiated {
                buckets[idx].0 += trade.size_usdc;
            } else {
                buckets[idx].1 += trade.size_usdc;
            }
        }
        buckets
    }
}
