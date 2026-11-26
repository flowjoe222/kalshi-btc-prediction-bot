//! Bitcoin technical indicators computed from the live Coinbase price feed.
//!
//! Maintains a rolling window of BTC prices (2-second intervals from Coinbase)
//! and computes: EMA, SMA, RSI, VWAP proxy, momentum, and a composite signal
//! score that the direct trading loop uses to filter entries.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A single price tick with timestamp.
#[derive(Debug, Clone)]
struct PriceTick {
    price: f64,
    timestamp: f64,
}

/// Composite signal from all indicators — used by the trading loop.
#[derive(Debug, Clone)]
pub struct IndicatorSignal {
    /// Overall direction: 1.0 = strongly bullish, -1.0 = strongly bearish, 0.0 = neutral.
    pub direction_score: f64,
    /// Confidence in the signal (0.0 to 1.0).
    pub confidence: f64,
    /// RSI value (0–100). >70 = overbought, <30 = oversold.
    pub rsi: f64,
    /// Short EMA (1-min) relative to long EMA (5-min). Positive = bullish crossover.
    pub ema_crossover: f64,
    /// Price momentum over last 60 seconds (% change).
    pub momentum_1m: f64,
    /// Price momentum over last 5 minutes (% change).
    pub momentum_5m: f64,
    /// Volatility (standard deviation of returns over last 2 minutes).
    pub volatility: f64,
    /// Whether a sweep/flash crash is detected (sudden >0.5% move in <30s).
    pub sweep_detected: bool,
    /// Trend strength (0–1): how consistently price has been moving in one direction.
    pub trend_strength: f64,
}

impl IndicatorSignal {
    /// Whether this signal suggests it's safe to trade (no sweep, reasonable RSI).
    pub fn is_safe_to_trade(&self) -> bool {
        !self.sweep_detected
            && self.rsi > 20.0
            && self.rsi < 80.0
            && self.volatility < 0.005 // <0.5% 2-min vol = calm market
    }

    /// Whether indicators agree with a YES (bullish) trade.
    /// We want: bullish direction, positive EMA crossover, RSI not overbought,
    /// and momentum not strongly negative.
    pub fn supports_bullish(&self) -> bool {
        self.direction_score > 0.05
            && self.ema_crossover >= 0.0
            && self.rsi < 75.0 // Not extremely overbought
            && self.rsi > 25.0 // Not oversold (contradicts bullish)
            && self.momentum_1m > -0.002
    }

    /// Whether indicators agree with a NO (bearish) trade.
    /// We want: bearish direction, negative EMA crossover, RSI not oversold
    /// (if RSI < 25, market may bounce), and momentum not strongly positive.
    pub fn supports_bearish(&self) -> bool {
        self.direction_score < -0.05
            && self.ema_crossover <= 0.0
            && self.rsi > 25.0 // Not extremely oversold (bounce risk)
            && self.rsi < 75.0 // Not overbought (contradicts bearish)
            && self.momentum_1m < 0.002
    }
}

/// Maintains a rolling price window and computes technical indicators.
pub struct BtcIndicators {
    /// Rolling price ticks (newest at back). We keep ~10 minutes of data at 2s intervals.
    ticks: VecDeque<PriceTick>,
    /// Maximum ticks to retain (10 min / 2s = 300 ticks).
    max_ticks: usize,
    /// EMA state: short (30 ticks ≈ 1 min).
    ema_short: Option<f64>,
    /// EMA state: long (150 ticks ≈ 5 min).
    ema_long: Option<f64>,
}

impl BtcIndicators {
    pub fn new() -> Self {
        Self {
            ticks: VecDeque::new(),
            max_ticks: 600, // 20 minutes at 2s intervals
            ema_short: None,
            ema_long: None,
        }
    }

    /// Record a new BTC price from the Coinbase feed.
    /// Skips duplicate prices to avoid inflating stale data when Coinbase
    /// doesn't update (e.g., during backoff).
    pub fn record_price(&mut self, price: f64) {
        // Only skip truly identical prices (within 1 cent). Small changes are real.
        if let Some(last) = self.ticks.back() {
            let age = now_secs() - last.timestamp;
            // Always record if >5s since last tick (even if price unchanged).
            if (last.price - price).abs() < 0.01 && age < 5.0 {
                return; // Same price within 5 seconds — skip.
            }
        }

        let tick = PriceTick {
            price,
            timestamp: now_secs(),
        };
        self.ticks.push_back(tick);
        if self.ticks.len() > self.max_ticks {
            self.ticks.pop_front();
        }

        // Update EMAs — faster for 15-min binary options.
        let alpha_short = 2.0 / 21.0; // ~40s EMA
        let alpha_long = 2.0 / 91.0;  // ~3min EMA

        self.ema_short = Some(match self.ema_short {
            Some(prev) => prev + alpha_short * (price - prev),
            None => price,
        });
        self.ema_long = Some(match self.ema_long {
            Some(prev) => prev + alpha_long * (price - prev),
            None => price,
        });
    }

    /// Number of price ticks recorded so far.
    pub fn tick_count(&self) -> usize {
        self.ticks.len()
    }

    /// Compute the full indicator signal. Returns None if not enough data.
    pub fn compute(&self) -> Option<IndicatorSignal> {
        // Need at least 10 ticks to start computing.
        if self.ticks.len() < 10 {
            return None;
        }

        let prices: Vec<f64> = self.ticks.iter().map(|t| t.price).collect();
        let n = prices.len();
        let current_price = prices[n - 1];

        // ---- RSI (21-period — more stable for short-duration contracts) ----
        let rsi = self.compute_rsi(&prices, 21);

        // ---- EMA crossover ----
        let ema_crossover = match (self.ema_short, self.ema_long) {
            (Some(short), Some(long)) => (short - long) / long,
            _ => 0.0,
        };

        // ---- Momentum (timestamp-based, not tick-count-based) ----
        // Find the price closest to 60s ago and 300s ago using actual timestamps.
        let now = now_secs();
        let momentum_1m = self.price_at_seconds_ago(60.0)
            .map(|old| (current_price - old) / old)
            .unwrap_or(0.0);
        let momentum_5m = self.price_at_seconds_ago(300.0)
            .map(|old| (current_price - old) / old)
            .unwrap_or(0.0);

        // ---- Volatility (2-min rolling std dev of returns) ----
        let vol_window = n.min(60);
        let returns: Vec<f64> = prices[n - vol_window..]
            .windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();
        let volatility = if returns.len() > 1 {
            let mean = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>()
                / (returns.len() - 1) as f64;
            variance.sqrt()
        } else {
            0.0
        };

        // ---- 15-second burst detection (captures quick momentum) ----
        let momentum_15s = self.price_at_seconds_ago(15.0)
            .map(|old| (current_price - old) / old)
            .unwrap_or(0.0);
        let burst_score = (momentum_15s / 0.0012).clamp(-1.0, 1.0);

        // ---- Mean reversion signal (fade exhaustion) ----
        let reversion_score = if rsi > 72.0 && momentum_15s < -0.0005 {
            -0.25 // Overbought + pulling back → bearish reversion
        } else if rsi < 28.0 && momentum_15s > 0.0005 {
            0.25 // Oversold + bouncing → bullish reversion
        } else {
            0.0
        };

        // ---- Sweep detection (only on EXTREME moves with high vol) ----
        let sweep_detected = self.price_at_seconds_ago(30.0)
            .map(|old| (current_price - old).abs() / old > 0.0045 && volatility > 0.0025)
            .unwrap_or(false);

        // ---- Trend strength ----
        let trend_window = n.min(30);
        let ups = prices[n - trend_window..]
            .windows(2)
            .filter(|w| w[1] > w[0])
            .count();
        let trend_strength = if trend_window > 1 {
            let frac = ups as f64 / (trend_window - 1) as f64;
            (frac - 0.5).abs() * 2.0
        } else {
            0.0
        };

        // ---- Composite direction score (optimized for 15-min binaries) ----
        let rsi_bias = (rsi - 50.0) / 50.0;
        let direction_score = (
            0.30 * (ema_crossover * 1600.0).clamp(-1.0, 1.0)
            + 0.25 * (momentum_1m * 900.0).clamp(-1.0, 1.0)
            + 0.10 * (momentum_5m * 450.0).clamp(-1.0, 1.0)
            + 0.15 * rsi_bias
            + 0.10 * burst_score
            + 0.10 * reversion_score
        ).clamp(-1.0, 1.0);

        // ---- Confidence (continuous, not binary) ----
        let same_sign = if ema_crossover.signum() == momentum_1m.signum() { 1.0 } else { 0.0 };
        let confidence = (
            0.30
            + 0.25 * trend_strength
            + 0.20 * direction_score.abs()
            + 0.15 * same_sign
            + 0.10 * (1.0 - (volatility / 0.005).clamp(0.0, 1.0))
        ).clamp(0.0, 1.0);

        Some(IndicatorSignal {
            direction_score,
            confidence,
            rsi,
            ema_crossover,
            momentum_1m,
            momentum_5m,
            volatility,
            sweep_detected,
            trend_strength,
        })
    }

    /// Find the price closest to `seconds` ago using actual timestamps.
    fn price_at_seconds_ago(&self, seconds: f64) -> Option<f64> {
        let target_ts = now_secs() - seconds;
        // Binary-search-ish: find the tick closest to target_ts.
        self.ticks
            .iter()
            .rev()
            .find(|t| t.timestamp <= target_ts)
            .map(|t| t.price)
    }

    /// Standard RSI calculation over `period` data points.
    fn compute_rsi(&self, prices: &[f64], period: usize) -> f64 {
        if prices.len() < period + 1 {
            return 50.0; // neutral default
        }

        let changes: Vec<f64> = prices.windows(2).map(|w| w[1] - w[0]).collect();
        let recent = &changes[changes.len().saturating_sub(period * 2)..];

        if recent.is_empty() {
            return 50.0;
        }

        // Use exponential moving average for gains/losses (Wilder's smoothing).
        let mut avg_gain = 0.0;
        let mut avg_loss = 0.0;

        // Initialize with simple average of first `period` changes.
        let init_slice = &recent[..recent.len().min(period)];
        for &c in init_slice {
            if c > 0.0 {
                avg_gain += c;
            } else {
                avg_loss += c.abs();
            }
        }
        avg_gain /= period as f64;
        avg_loss /= period as f64;

        // Then smooth with remaining changes.
        for &c in &recent[recent.len().min(period)..] {
            let gain = if c > 0.0 { c } else { 0.0 };
            let loss = if c < 0.0 { c.abs() } else { 0.0 };
            avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
            avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;
        }

        // Handle edge cases: flat market = neutral RSI, not overbought.
        if avg_gain < 1e-10 && avg_loss < 1e-10 {
            return 50.0; // No movement = neutral
        }
        if avg_loss < 1e-10 {
            return 100.0; // All gains, no losses
        }
        if avg_gain < 1e-10 {
            return 0.0; // All losses, no gains
        }

        let rs = avg_gain / avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }
}
