//! Short-term BTC price direction predictor for 15-minute binary contracts.
//!
//! Maintains multi-timeframe candles (1m, 5m) and computes a directional
//! probability score by combining 8 pattern detectors:
//!
//! 1. Momentum continuation (5-min trend tends to persist)
//! 2. Mean reversion (extreme RSI → likely pullback)
//! 3. Volume-weighted direction (where is the money flowing?)
//! 4. Tick direction bias (consecutive up/down ticks)
//! 5. Volatility regime (high vol = bigger moves = more edge)
//! 6. Price vs VWAP (above = bullish pressure, below = bearish)
//! 7. Candle pattern (engulfing, doji, hammer detection)
//! 8. Acceleration (momentum speeding up or slowing down)
//!
//! The output is a probability (0.0 to 1.0) that BTC will be ABOVE a given
//! strike price at the end of the prediction window.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A price candle (OHLCV) for a fixed time window.
#[derive(Debug, Clone)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_ticks: u32, // Number of price ticks in this candle
    start_ts: f64,
    closed: bool,
}

impl Candle {
    fn new(price: f64, ts: f64) -> Self {
        Self {
            open: price,
            high: price,
            low: price,
            close: price,
            volume_ticks: 1,
            start_ts: ts,
            closed: false,
        }
    }

    fn update(&mut self, price: f64) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume_ticks += 1;
    }

    /// Body size as fraction of the full range. 1.0 = full body, 0.0 = doji.
    fn body_ratio(&self) -> f64 {
        let range = self.high - self.low;
        if range < 0.01 { return 0.0; }
        (self.close - self.open).abs() / range
    }

    /// True if close > open (bullish candle).
    fn is_bullish(&self) -> bool {
        self.close > self.open
    }

    /// Percentage change from open to close.
    fn pct_change(&self) -> f64 {
        if self.open <= 0.0 { return 0.0; }
        (self.close - self.open) / self.open
    }
}

/// Prediction result from the BTC predictor.
#[derive(Debug, Clone)]
pub struct DirectionPrediction {
    /// Probability that BTC will be ABOVE the strike at settlement (0.0 to 1.0).
    pub prob_above: f64,
    /// Overall confidence in this prediction (0.0 to 1.0).
    pub confidence: f64,
    /// Individual signal scores for debugging.
    pub momentum_score: f64,
    pub mean_reversion_score: f64,
    pub tick_bias_score: f64,
    pub vwap_score: f64,
    pub volatility_score: f64,
    pub candle_score: f64,
    pub acceleration_score: f64,
    /// Whether this is a high-quality setup worth trading.
    pub is_tradeable: bool,
}

/// Multi-timeframe BTC price predictor.
pub struct BtcPredictor {
    /// Raw price ticks with timestamps.
    ticks: VecDeque<(f64, f64)>, // (price, timestamp)
    /// 1-minute candles (last 30).
    candles_1m: VecDeque<Candle>,
    /// 5-minute candles (last 12).
    candles_5m: VecDeque<Candle>,
    /// Current in-progress candles.
    current_1m: Option<Candle>,
    current_5m: Option<Candle>,
    /// Volume-weighted average price (rolling).
    vwap_sum: f64,
    vwap_count: u64,
    /// Maximum ticks to retain.
    max_ticks: usize,
}

impl BtcPredictor {
    pub fn new() -> Self {
        Self {
            ticks: VecDeque::new(),
            candles_1m: VecDeque::new(),
            candles_5m: VecDeque::new(),
            current_1m: None,
            current_5m: None,
            vwap_sum: 0.0,
            vwap_count: 0,
            max_ticks: 1800, // 30 minutes at ~1 tick/sec
        }
    }

    /// Feed a new price tick.
    pub fn record_price(&mut self, price: f64) {
        let ts = now_secs();

        // Store raw tick.
        self.ticks.push_back((price, ts));
        if self.ticks.len() > self.max_ticks {
            self.ticks.pop_front();
        }

        // Update VWAP.
        self.vwap_sum += price;
        self.vwap_count += 1;
        // Reset VWAP every 15 minutes to keep it relevant.
        if self.vwap_count > 900 {
            self.vwap_sum = price * 450.0; // Seed with recent price
            self.vwap_count = 450;
        }

        // Update 1-minute candles.
        match &mut self.current_1m {
            Some(candle) if ts - candle.start_ts < 60.0 => {
                candle.update(price);
            }
            _ => {
                // Close current candle and start new one.
                if let Some(mut candle) = self.current_1m.take() {
                    candle.closed = true;
                    self.candles_1m.push_back(candle);
                    if self.candles_1m.len() > 30 {
                        self.candles_1m.pop_front();
                    }
                }
                self.current_1m = Some(Candle::new(price, ts));
            }
        }

        // Update 5-minute candles.
        match &mut self.current_5m {
            Some(candle) if ts - candle.start_ts < 300.0 => {
                candle.update(price);
            }
            _ => {
                if let Some(mut candle) = self.current_5m.take() {
                    candle.closed = true;
                    self.candles_5m.push_back(candle);
                    if self.candles_5m.len() > 12 {
                        self.candles_5m.pop_front();
                    }
                }
                self.current_5m = Some(Candle::new(price, ts));
            }
        }
    }

    /// How many ticks we have.
    pub fn tick_count(&self) -> usize {
        self.ticks.len()
    }

    /// Predict direction: probability that price will be ABOVE `strike` in
    /// `seconds_remaining` seconds. Returns None if not enough data.
    pub fn predict(&self, strike: f64, seconds_remaining: f64) -> Option<DirectionPrediction> {
        if self.ticks.len() < 60 || self.candles_1m.len() < 3 {
            return None;
        }

        let current_price = self.ticks.back()?.0;

        // ---- Signal 1: Momentum continuation ----
        // If BTC has been trending in the last 3-5 minutes, it tends to continue.
        let momentum_score = self.compute_momentum();

        // ---- Signal 2: Mean reversion ----
        // After extreme moves, price tends to pull back.
        let mean_reversion_score = self.compute_mean_reversion();

        // ---- Signal 3: Tick direction bias ----
        // Count up vs down ticks in last 30 seconds.
        let tick_bias_score = self.compute_tick_bias();

        // ---- Signal 4: Price vs VWAP ----
        // Above VWAP = bullish pressure, below = bearish.
        let vwap = if self.vwap_count > 0 {
            self.vwap_sum / self.vwap_count as f64
        } else {
            current_price
        };
        let vwap_score = ((current_price - vwap) / vwap * 500.0).clamp(-1.0, 1.0);

        // ---- Signal 5: Volatility regime ----
        // Higher vol = bigger moves = more confident predictions when we have edge.
        let volatility_score = self.compute_volatility_signal();

        // ---- Signal 6: Candle patterns ----
        let candle_score = self.compute_candle_patterns();

        // ---- Signal 7: Acceleration ----
        // Is momentum speeding up or slowing down?
        let acceleration_score = self.compute_acceleration();

        // ---- Combine signals into directional probability ----
        // Weights reduced to avoid overcounting correlated trend signals.
        // Volatility is used as a gate, not directional weight.
        let raw_score =
            0.25 * momentum_score       // Primary trend signal
            + 0.15 * mean_reversion_score // Counter-trend fade
            + 0.10 * tick_bias_score     // Immediate pressure
            + 0.15 * vwap_score          // Flow direction
            + 0.08 * candle_score        // Pattern confirmation
            + 0.12 * acceleration_score  // Trend acceleration
            + 0.00 * volatility_score;   // Gate only — not directional

        // Convert raw score (-1 to +1) to probability (0 to 1).
        // Use a sigmoid to get smooth probability estimates.
        let base_prob = 1.0 / (1.0 + (-raw_score * 2.5).exp());

        // Adjust for distance from strike.
        // If price is far above strike, probability of "above" is higher.
        let distance_factor = if strike > 0.0 {
            let dist = (current_price - strike) / strike;
            // Scale distance by time remaining — less time = distance matters more.
            let time_weight = (900.0 / seconds_remaining.max(60.0)).sqrt().min(3.0);
            (dist * time_weight * 100.0).clamp(-0.12, 0.12) // Capped: don't inflate probability from distance alone
        } else {
            0.0
        };

        let prob_above = (base_prob + distance_factor).clamp(0.05, 0.95);

        // ---- Confidence: how much do we trust this prediction? ----
        // Higher when signals agree in direction.
        // Signal agreement: how many signals point the same direction as the overall score.
        let all_signals = [
            momentum_score, mean_reversion_score, tick_bias_score,
            vwap_score, candle_score, acceleration_score,
        ];
        let signal_agreement = all_signals.iter()
            .filter(|&&s| s.signum() == raw_score.signum() || s.abs() < 0.05)
            .count() as f64 / all_signals.len() as f64;

        // Confidence rewards decisiveness: more confident when prediction is further from 50/50.
        let decisiveness = (prob_above - 0.5).abs() * 2.0; // 0 at 50%, 1 at 0% or 100%.
        let confidence = (
            0.35 * signal_agreement
            + 0.30 * decisiveness
            + 0.20 * raw_score.abs()
            + 0.15 * (1.0 - (volatility_score + 0.2).abs().min(1.0)) // Moderate vol = confident
        ).clamp(0.0, 1.0);

        // Volatility gate: only trade when vol is in the sweet spot (not dead, not chaotic).
        let vol_ok = volatility_score > -0.15; // Reject "too quiet" and "too volatile"

        // Tradeable: signal strength + agreement + enough data + good vol regime.
        let is_tradeable = raw_score.abs() > 0.20
            && signal_agreement >= 0.60
            && self.candles_1m.len() >= 8
            && vol_ok;

        Some(DirectionPrediction {
            prob_above,
            confidence,
            momentum_score,
            mean_reversion_score,
            tick_bias_score,
            vwap_score,
            volatility_score,
            candle_score,
            acceleration_score,
            is_tradeable,
        })
    }

    // ---- Individual signal computations ----

    fn compute_momentum(&self) -> f64 {
        if self.candles_1m.len() < 3 {
            return 0.0;
        }

        // Last 3 one-minute candles: are they trending?
        let recent: Vec<&Candle> = self.candles_1m.iter().rev().take(3).collect();
        let bullish_count = recent.iter().filter(|c| c.is_bullish()).count();

        // Weight more recent candles higher.
        let weighted_trend: f64 = recent.iter().enumerate().map(|(i, c)| {
            let weight = (3 - i) as f64; // 3, 2, 1
            c.pct_change() * weight
        }).sum::<f64>() / 6.0; // Normalize by sum of weights

        // Scale to -1 to 1.
        let direction = if bullish_count >= 3 {
            0.5 + (weighted_trend * 500.0).clamp(0.0, 0.5)
        } else if bullish_count == 0 {
            -0.5 + (weighted_trend * 500.0).clamp(-0.5, 0.0)
        } else {
            (weighted_trend * 800.0).clamp(-0.5, 0.5)
        };

        direction
    }

    fn compute_mean_reversion(&self) -> f64 {
        if self.ticks.len() < 60 {
            return 0.0;
        }

        // Compute a simple RSI-like metric from recent ticks.
        let recent: Vec<f64> = self.ticks.iter().rev().take(60).map(|(p, _)| *p).collect();
        let mut gains = 0.0_f64;
        let mut losses = 0.0_f64;
        let mut count = 0u32;

        for window in recent.windows(2) {
            let change = window[0] - window[1]; // Note: reversed order (newest first)
            if change > 0.0 { gains += change; }
            else { losses += change.abs(); }
            count += 1;
        }

        if count == 0 { return 0.0; }

        let avg_gain = gains / count as f64;
        let avg_loss = losses / count as f64;

        if avg_gain < 1e-10 && avg_loss < 1e-10 { return 0.0; }
        if avg_loss < 1e-10 { return -0.5; } // All gains → overbought → expect reversal DOWN

        let rs = avg_gain / avg_loss;
        let rsi = 100.0 - (100.0 / (1.0 + rs));

        // Mean reversion signal: extreme RSI suggests reversal.
        if rsi > 75.0 {
            -((rsi - 75.0) / 25.0).min(1.0) * 0.6 // Overbought → bearish reversion
        } else if rsi < 25.0 {
            ((25.0 - rsi) / 25.0).min(1.0) * 0.6 // Oversold → bullish reversion
        } else {
            0.0 // No extreme — no reversion signal
        }
    }

    fn compute_tick_bias(&self) -> f64 {
        // Count up vs down ticks in last 30 seconds.
        let now = now_secs();
        let cutoff = now - 30.0;
        let recent: Vec<&(f64, f64)> = self.ticks.iter().filter(|(_, ts)| *ts >= cutoff).collect();

        if recent.len() < 5 {
            return 0.0;
        }

        let mut ups = 0u32;
        let mut downs = 0u32;
        for window in recent.windows(2) {
            if window[1].0 > window[0].0 { ups += 1; }
            else if window[1].0 < window[0].0 { downs += 1; }
        }

        let total = (ups + downs) as f64;
        if total < 3.0 { return 0.0; }

        let bias = (ups as f64 - downs as f64) / total;
        bias.clamp(-1.0, 1.0)
    }

    fn compute_volatility_signal(&self) -> f64 {
        if self.candles_1m.len() < 5 {
            return 0.0;
        }

        // Average true range of last 5 one-minute candles.
        let ranges: Vec<f64> = self.candles_1m.iter().rev().take(5)
            .map(|c| (c.high - c.low) / c.open)
            .collect();

        let avg_range = ranges.iter().sum::<f64>() / ranges.len() as f64;

        // Classify volatility regime.
        // Higher vol = more opportunity but also more risk.
        // Return positive when vol is elevated (good for trading),
        // negative when vol is too low (no movement) or too high (chaos).
        if avg_range > 0.003 {
            -0.3 // Too volatile — chaotic
        } else if avg_range > 0.001 {
            0.5 // Good volatility — tradeable moves
        } else if avg_range > 0.0005 {
            0.2 // Moderate — some opportunity
        } else {
            -0.2 // Too quiet — no edge
        }
    }

    fn compute_candle_patterns(&self) -> f64 {
        if self.candles_1m.len() < 3 {
            return 0.0;
        }

        let candles: Vec<&Candle> = self.candles_1m.iter().rev().take(3).collect();
        let last = candles[0];
        let prev = candles[1];

        let mut score = 0.0_f64;

        // Strict bullish/bearish classification: require meaningful body.
        let last_bullish = last.close > last.open && last.body_ratio() > 0.3;
        let last_bearish = last.close < last.open && last.body_ratio() > 0.3;
        let prev_bullish = prev.close > prev.open && prev.body_ratio() > 0.3;
        let prev_bearish = prev.close < prev.open && prev.body_ratio() > 0.3;

        // Bullish engulfing: previous bearish, current bullish with bigger body.
        if prev_bearish && last_bullish
            && last.close > prev.open && last.open < prev.close
            && last.body_ratio() > 0.6
        {
            score += 0.5;
        }

        // Bearish engulfing: previous bullish, current bearish with bigger body.
        if prev_bullish && last_bearish
            && last.close < prev.open && last.open > prev.close
            && last.body_ratio() > 0.6
        {
            score -= 0.5;
        }

        // Three soldiers: 3 bullish candles with meaningful bodies.
        if candles.iter().all(|c| c.close > c.open && c.body_ratio() > 0.4) {
            score += 0.4;
        }

        // Three crows: 3 bearish candles with meaningful bodies.
        if candles.iter().all(|c| c.close < c.open && c.body_ratio() > 0.4) {
            score -= 0.4;
        }

        // Doji (body_ratio < 0.15) after a strong directional candle = reversal hint.
        if last.body_ratio() < 0.15 && prev.body_ratio() > 0.5 {
            if prev_bullish { score -= 0.15; }
            else if prev_bearish { score += 0.15; }
        }

        score.clamp(-1.0, 1.0)
    }

    fn compute_acceleration(&self) -> f64 {
        if self.candles_1m.len() < 4 {
            return 0.0;
        }

        // Compare momentum in the last 2 candles vs the 2 before that.
        let candles: Vec<&Candle> = self.candles_1m.iter().rev().take(4).collect();

        let recent_momentum = candles[0].pct_change() + candles[1].pct_change();
        let older_momentum = candles[2].pct_change() + candles[3].pct_change();

        let accel = recent_momentum - older_momentum;

        // Accelerating = momentum is increasing. Decelerating = might reverse.
        (accel * 5000.0).clamp(-1.0, 1.0)
    }
}
