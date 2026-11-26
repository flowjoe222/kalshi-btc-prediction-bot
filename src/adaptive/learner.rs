use crate::adaptive::AdaptiveConfig;
use crate::signals::detector::DetectorParams;
use crate::signals::predictor::TradeRecommendation;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

/// A single resolved trade record stored for learning purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub signal_id: String,
    pub timestamp: f64,
    pub direction: String,
    /// `"win"` | `"loss"` | `"breakeven"` | `"expired"`
    pub outcome: String,
    pub confidence: f64,
    pub imbalance_delta: f64,
    pub volume_usdc: f64,
    pub large_trade_delta: f64,
    pub momentum: f64,
    pub threshold_used: f64,
    pub window_used: f64,
    pub pnl_usdc: f64,
    pub btc_move_bps: f64,
}

/// A single parameter adjustment event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterAdjustment {
    pub adjustment_id: String,
    pub timestamp: f64,
    pub parameter: String,
    pub old_value: f64,
    pub new_value: f64,
    pub reason: String,
    pub evidence: String,
    pub win_rate_at_time: f64,
    pub n_samples_used: usize,
}

/// A high-level insight derived from pattern analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lesson {
    pub lesson_id: String,
    pub timestamp: f64,
    pub pattern_type: String,
    pub description: String,
    pub frequency: f64,
    pub adjustment_made: String,
    pub confidence: f64,
}

/// A summary of learner state for display purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearnerStatus {
    pub total_records: usize,
    pub win_rate: f64,
    pub last_optimized_at: Option<f64>,
    pub adjustments_made: usize,
    pub lessons_learned: usize,
    pub next_optimization_in: usize,
}

/// Observes resolved trade outcomes and adaptively tunes `DetectorParams`.
pub struct AdaptiveLearner {
    config: AdaptiveConfig,
    params: Arc<Mutex<DetectorParams>>,
    records: Vec<TradeRecord>,
    adjustments: Vec<ParameterAdjustment>,
    pub lessons: Vec<Lesson>,
    last_optimized_at: Option<f64>,
    trades_since_optimize: usize,
}

impl AdaptiveLearner {
    pub fn new(config: AdaptiveConfig, params: Arc<Mutex<DetectorParams>>) -> Self {
        Self {
            config,
            params,
            records: Vec::new(),
            adjustments: Vec::new(),
            lessons: Vec::new(),
            last_optimized_at: None,
            trades_since_optimize: 0,
        }
    }

    /// Record a resolved trade outcome and potentially adjust parameters.
    ///
    /// Returns any `ParameterAdjustment`s that were applied.
    pub async fn record_outcome(
        &mut self,
        rec: &TradeRecommendation,
    ) -> Vec<ParameterAdjustment> {
        let outcome = match &rec.outcome {
            Some(o) => o.clone(),
            None => return vec![],
        };

        // Compute BTC move in bps.
        let btc_move_bps = match (rec.btc_price_at_signal, rec.btc_price_at_expiry) {
            (Some(entry), Some(exit)) => (exit - entry) / entry * 10_000.0,
            _ => 0.0,
        };

        let record = TradeRecord {
            signal_id: rec.signal_id.clone(),
            timestamp: rec.timestamp,
            direction: format!("{:?}", rec.direction),
            outcome: outcome.clone(),
            confidence: rec.confidence,
            imbalance_delta: rec.imbalance_delta,
            volume_usdc: rec.volume_usdc,
            large_trade_delta: 0.0, // not stored on recommendation
            momentum: 0.0,
            threshold_used: rec.confidence, // approximate
            window_used: 10.0,
            pnl_usdc: rec.pnl_usdc.unwrap_or(0.0),
            btc_move_bps,
        };

        self.records.push(record);
        self.trades_since_optimize += 1;

        // Trim rolling window.
        if self.records.len() > self.config.rolling_window_size {
            let excess = self.records.len() - self.config.rolling_window_size;
            self.records.drain(..excess);
        }

        // Only optimize every N trades.
        if self.trades_since_optimize < self.config.optimization_interval {
            return vec![];
        }
        if self.records.len() < self.config.min_samples_to_learn {
            return vec![];
        }

        self.trades_since_optimize = 0;
        self.last_optimized_at = Some(now_secs());

        self.optimize_params().await
    }

    async fn optimize_params(&mut self) -> Vec<ParameterAdjustment> {
        let mut new_adjustments = Vec::new();
        // Clone the Arc so the borrow of self.params ends before we call self.adjust().
        let params_arc = Arc::clone(&self.params);
        let mut params = params_arc.lock().await;

        let recent = &self.records;
        let wins: Vec<&TradeRecord> = recent.iter().filter(|r| r.outcome == "win").collect();
        let losses: Vec<&TradeRecord> = recent.iter().filter(|r| r.outcome == "loss").collect();

        let n = recent.len();
        let win_rate = if n > 0 {
            wins.len() as f64 / n as f64
        } else {
            0.0
        };
        let lr = self.config.learning_rate;
        let target = self.config.target_win_rate;

        info!(
            "AdaptiveLearner: n={n} win_rate={win_rate:.2} losses={}",
            losses.len()
        );

        // Pattern 4: Emergency tighten.
        // Call the associated function with explicit field refs so that the
        // borrow on `self.records` (through `wins`/`losses`) doesn't conflict.
        if win_rate < self.config.emergency_tighten_threshold && n >= self.config.min_samples_to_learn {
            let adj = Self::do_adjust(
                &self.config,
                &mut self.adjustments,
                &mut params,
                "emergency_tighten",
                win_rate,
                n,
            );
            if !adj.is_empty() {
                new_adjustments.extend(adj);
                self.lessons.push(Lesson {
                    lesson_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    pattern_type: "emergency_tighten".to_owned(),
                    description: format!(
                        "Emergency tighten triggered: win_rate={win_rate:.2}"
                    ),
                    frequency: win_rate,
                    adjustment_made: "tightened threshold, min_volume, confidence_min".to_owned(),
                    confidence: 0.9,
                });
                return new_adjustments;
            }
        }

        // Pattern 5: Loosen when performing well.
        if win_rate > target + 0.10 && n >= 30 {
            let old = params.imbalance_threshold;
            let new_val = clamp(
                old - lr * 0.03,
                self.config.bounds.imbalance_threshold[0],
                self.config.bounds.imbalance_threshold[1],
            );
            if (new_val - old).abs() > 1e-6 {
                params.imbalance_threshold = new_val;
                let adj = ParameterAdjustment {
                    adjustment_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    parameter: "imbalance_threshold".to_owned(),
                    old_value: old,
                    new_value: new_val,
                    reason: "Performing well — loosening threshold slightly".to_owned(),
                    evidence: format!("win_rate={win_rate:.2} n={n}"),
                    win_rate_at_time: win_rate,
                    n_samples_used: n,
                };
                self.adjustments.push(adj.clone());
                new_adjustments.push(adj);
            }
        }

        if losses.is_empty() {
            return new_adjustments;
        }

        // Pattern 1: Low-volume false positives.
        let low_vol_thresh = params.min_volume_usdc * 1.25;
        let low_vol_losses = losses
            .iter()
            .filter(|r| r.volume_usdc < low_vol_thresh)
            .count();
        let frac1 = low_vol_losses as f64 / losses.len() as f64;
        if frac1 > 0.55 {
            let old = params.min_volume_usdc;
            let new_val = clamp(
                old * (1.0 + lr * 0.15),
                self.config.bounds.min_volume_usdc[0],
                self.config.bounds.min_volume_usdc[1],
            );
            if (new_val - old).abs() > 1e-3 {
                params.min_volume_usdc = new_val;
                let adj = ParameterAdjustment {
                    adjustment_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    parameter: "min_volume_usdc".to_owned(),
                    old_value: old,
                    new_value: new_val,
                    reason: "Low-volume false positives detected".to_owned(),
                    evidence: format!("{:.0}% of losses had volume < {low_vol_thresh:.0}", frac1 * 100.0),
                    win_rate_at_time: win_rate,
                    n_samples_used: n,
                };
                self.adjustments.push(adj.clone());
                new_adjustments.push(adj);
                self.lessons.push(Lesson {
                    lesson_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    pattern_type: "low_volume_fp".to_owned(),
                    description: format!(
                        "{:.0}% of losses occurred on low-volume trades",
                        frac1 * 100.0
                    ),
                    frequency: frac1,
                    adjustment_made: format!("min_volume_usdc: {old:.0} → {new_val:.0}"),
                    confidence: frac1,
                });
            }
        }

        // Pattern 2: Borderline threshold signals.
        let thresh_half = params.imbalance_threshold - 0.5;
        let borderline_losses = losses
            .iter()
            .filter(|r| (r.imbalance_delta.abs() - thresh_half).abs() < 0.03)
            .count();
        let frac2 = borderline_losses as f64 / losses.len() as f64;
        if frac2 > 0.45 && win_rate < target {
            let old = params.imbalance_threshold;
            let new_val = clamp(
                old + lr * 0.05,
                self.config.bounds.imbalance_threshold[0],
                self.config.bounds.imbalance_threshold[1],
            );
            if (new_val - old).abs() > 1e-6 {
                params.imbalance_threshold = new_val;
                let adj = ParameterAdjustment {
                    adjustment_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    parameter: "imbalance_threshold".to_owned(),
                    old_value: old,
                    new_value: new_val,
                    reason: "Borderline threshold signals causing losses".to_owned(),
                    evidence: format!(
                        "{:.0}% of losses were borderline; win_rate={win_rate:.2}",
                        frac2 * 100.0
                    ),
                    win_rate_at_time: win_rate,
                    n_samples_used: n,
                };
                self.adjustments.push(adj.clone());
                new_adjustments.push(adj);
            }
        }

        // Pattern 3: Low confidence losses.
        let low_conf_thresh = params.confidence_min + 0.05;
        let low_conf_losses = losses
            .iter()
            .filter(|r| r.confidence < low_conf_thresh)
            .count();
        let frac3 = low_conf_losses as f64 / losses.len() as f64;
        if frac3 > 0.60 {
            let old = params.confidence_min;
            let new_val = clamp(
                old + lr * 0.08,
                self.config.bounds.confidence_min[0],
                self.config.bounds.confidence_min[1],
            );
            if (new_val - old).abs() > 1e-6 {
                params.confidence_min = new_val;
                let adj = ParameterAdjustment {
                    adjustment_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    parameter: "confidence_min".to_owned(),
                    old_value: old,
                    new_value: new_val,
                    reason: "Low-confidence signals underperforming".to_owned(),
                    evidence: format!(
                        "{:.0}% of losses had confidence < {low_conf_thresh:.2}",
                        frac3 * 100.0
                    ),
                    win_rate_at_time: win_rate,
                    n_samples_used: n,
                };
                self.adjustments.push(adj.clone());
                new_adjustments.push(adj);
            }
        }

        new_adjustments
    }

    /// Apply emergency tightening to all parameters.
    ///
    /// Takes explicit field refs so callers can hold other field borrows (e.g.
    /// `&self.records`) simultaneously without triggering a borrow conflict on
    /// `&mut self`.
    fn do_adjust(
        config: &AdaptiveConfig,
        adjustments_log: &mut Vec<ParameterAdjustment>,
        params: &mut DetectorParams,
        pattern: &str,
        win_rate: f64,
        n: usize,
    ) -> Vec<ParameterAdjustment> {
        let lr = config.learning_rate;
        let mut out = Vec::new();

        // Tighten threshold.
        {
            let old = params.imbalance_threshold;
            let new_val = clamp(
                old + lr * 2.0 * 0.10,
                config.bounds.imbalance_threshold[0],
                config.bounds.imbalance_threshold[1],
            );
            if (new_val - old).abs() > 1e-6 {
                params.imbalance_threshold = new_val;
                let adj = ParameterAdjustment {
                    adjustment_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    parameter: "imbalance_threshold".to_owned(),
                    old_value: old,
                    new_value: new_val,
                    reason: format!("{pattern}: emergency tighten"),
                    evidence: format!("win_rate={win_rate:.2} n={n}"),
                    win_rate_at_time: win_rate,
                    n_samples_used: n,
                };
                adjustments_log.push(adj.clone());
                out.push(adj);
            }
        }

        // Increase min_volume.
        {
            let old = params.min_volume_usdc;
            let new_val = clamp(
                old * 1.30,
                config.bounds.min_volume_usdc[0],
                config.bounds.min_volume_usdc[1],
            );
            if (new_val - old).abs() > 1e-3 {
                params.min_volume_usdc = new_val;
                let adj = ParameterAdjustment {
                    adjustment_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    parameter: "min_volume_usdc".to_owned(),
                    old_value: old,
                    new_value: new_val,
                    reason: format!("{pattern}: emergency tighten"),
                    evidence: format!("win_rate={win_rate:.2} n={n}"),
                    win_rate_at_time: win_rate,
                    n_samples_used: n,
                };
                adjustments_log.push(adj.clone());
                out.push(adj);
            }
        }

        // Increase confidence_min.
        {
            let old = params.confidence_min;
            let new_val = clamp(
                old + lr * 2.0 * 0.08,
                config.bounds.confidence_min[0],
                config.bounds.confidence_min[1],
            );
            if (new_val - old).abs() > 1e-6 {
                params.confidence_min = new_val;
                let adj = ParameterAdjustment {
                    adjustment_id: Uuid::new_v4().to_string(),
                    timestamp: now_secs(),
                    parameter: "confidence_min".to_owned(),
                    old_value: old,
                    new_value: new_val,
                    reason: format!("{pattern}: emergency tighten"),
                    evidence: format!("win_rate={win_rate:.2} n={n}"),
                    win_rate_at_time: win_rate,
                    n_samples_used: n,
                };
                adjustments_log.push(adj.clone());
                out.push(adj);
            }
        }

        out
    }

    /// Return a summary of current learner state.
    pub fn get_status(&self) -> LearnerStatus {
        let n = self.records.len();
        let wins = self.records.iter().filter(|r| r.outcome == "win").count();
        let win_rate = if n > 0 { wins as f64 / n as f64 } else { 0.0 };
        let next = if self.trades_since_optimize >= self.config.optimization_interval {
            0
        } else {
            self.config.optimization_interval - self.trades_since_optimize
        };

        LearnerStatus {
            total_records: n,
            win_rate,
            last_optimized_at: self.last_optimized_at,
            adjustments_made: self.adjustments.len(),
            lessons_learned: self.lessons.len(),
            next_optimization_in: next,
        }
    }

    pub fn get_adjustments(&self) -> &[ParameterAdjustment] {
        &self.adjustments
    }

    /// Persist lessons to a JSON file.
    pub fn save_lessons(&self, path: &str) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(&self.lessons)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}
