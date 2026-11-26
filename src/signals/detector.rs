use crate::signals::orderflow::OrderFlowCalculator;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// The predicted direction of BTC price movement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Long,
    Short,
    Neutral,
}

/// A fully scored order-flow imbalance signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImbalanceSignal {
    pub signal_id: String,
    pub timestamp: f64,
    pub direction: Direction,
    pub primary_delta: f64,
    pub confirmation_delta: f64,
    pub total_volume_usdc: f64,
    pub large_trade_delta: f64,
    pub momentum: f64,
    /// Fraction of total volume that was buy-side (in primary window).
    pub volume_score: f64,
    pub delta_score: f64,
    pub large_trade_score: f64,
    pub momentum_score: f64,
    pub confidence: f64,
    pub threshold_used: f64,
    pub window_used: f64,
}

/// Tunable hyper-parameters for the imbalance detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectorParams {
    /// Minimum buy-fraction (or 1 - buy-fraction for Short) to fire a signal.
    pub imbalance_threshold: f64,
    /// Primary order-flow window in seconds.
    pub window_seconds: f64,
    /// Confirmation window (must not contradict).
    pub confirmation_window_seconds: f64,
    /// Minimum total trade volume required to emit a signal.
    pub min_volume_usdc: f64,
    /// Minimum composite confidence score required.
    pub confidence_min: f64,
    /// Minimum seconds between consecutive signals.
    pub cooldown_seconds: f64,
    /// Size multiplier above which a trade is considered "large".
    pub large_trade_multiplier: f64,
    /// Absolute USDC threshold above which a trade is "large".
    pub large_trade_threshold_usdc: f64,
}

impl Default for DetectorParams {
    fn default() -> Self {
        Self {
            imbalance_threshold: 0.65,
            window_seconds: 10.0,
            confirmation_window_seconds: 30.0,
            min_volume_usdc: 500.0,
            confidence_min: 0.60,
            cooldown_seconds: 45.0,
            large_trade_multiplier: 1.5,
            large_trade_threshold_usdc: 200.0,
        }
    }
}

/// Detects order-flow imbalance signals by applying a series of gates to
/// the computed flow metrics.
///
/// All gates must pass for a signal to be emitted:
/// 1. Cooldown since last signal.
/// 2. Minimum volume.
/// 3. Clear directional imbalance.
/// 4. Confirmation window does not contradict.
/// 5. Composite confidence meets minimum.
pub struct ImbalanceDetector {
    params: DetectorParams,
    calculator: Arc<OrderFlowCalculator>,
    last_signal_ts: f64,
}

impl ImbalanceDetector {
    pub fn new(params: DetectorParams, calculator: Arc<OrderFlowCalculator>) -> Self {
        Self {
            params,
            calculator,
            last_signal_ts: 0.0,
        }
    }

    pub fn update_params(&mut self, params: DetectorParams) {
        self.params = params;
    }

    pub fn get_params(&self) -> &DetectorParams {
        &self.params
    }

    /// Try to detect an imbalance signal at the current moment.
    ///
    /// Returns `None` when any gate fails.
    pub fn detect(&mut self) -> Option<ImbalanceSignal> {
        let now = now_secs();
        let p = &self.params;

        // Gate 1: Cooldown.
        if now - self.last_signal_ts < p.cooldown_seconds {
            return None;
        }

        let primary = self
            .calculator
            .compute(p.window_seconds);

        // Gate 2: Volume.
        if primary.total_volume < p.min_volume_usdc {
            return None;
        }

        let buy_frac = primary.buy_fraction();
        let thresh = p.imbalance_threshold;

        // Gate 3: Direction.
        let direction = if buy_frac >= thresh {
            Direction::Long
        } else if buy_frac <= 1.0 - thresh {
            Direction::Short
        } else {
            return None;
        };

        // Gate 4: Confirmation window must not contradict.
        let confirmation = self
            .calculator
            .compute(p.confirmation_window_seconds);
        let conf_delta = confirmation.delta();
        match direction {
            Direction::Long if conf_delta < -0.05 => return None,
            Direction::Short if conf_delta > 0.05 => return None,
            _ => {}
        }

        // Gate 5: Composite confidence.
        let delta_score = clamp(
            (((buy_frac - 0.5).abs()) - (thresh - 0.5)) / 0.20,
            0.0,
            1.0,
        );

        let volume_score = clamp(primary.total_volume / (p.min_volume_usdc * 3.0), 0.0, 1.0);

        let large_delta = primary.large_delta();
        let large_trade_score = if large_delta.abs() < 0.01 {
            0.5
        } else {
            match direction {
                Direction::Long => (large_delta + 1.0) / 2.0,
                Direction::Short => (-large_delta + 1.0) / 2.0,
                Direction::Neutral => 0.5,
            }
        };

        let momentum = primary.momentum();
        let momentum_score = match direction {
            Direction::Long => clamp((momentum + 0.2) / 0.4, 0.0, 1.0),
            Direction::Short => clamp((-momentum + 0.2) / 0.4, 0.0, 1.0),
            Direction::Neutral => 0.5,
        };

        let confidence =
            0.40 * delta_score + 0.25 * volume_score + 0.20 * large_trade_score + 0.15 * momentum_score;

        if confidence < p.confidence_min {
            return None;
        }

        self.last_signal_ts = now;

        Some(ImbalanceSignal {
            signal_id: Uuid::new_v4().to_string(),
            timestamp: now,
            direction,
            primary_delta: primary.delta(),
            confirmation_delta: conf_delta,
            total_volume_usdc: primary.total_volume,
            large_trade_delta: large_delta,
            momentum,
            volume_score,
            delta_score,
            large_trade_score,
            momentum_score,
            confidence,
            threshold_used: thresh,
            window_used: p.window_seconds,
        })
    }
}

fn clamp(value: f64, min: f64, max: f64) -> f64 {
    value.max(min).min(max)
}
