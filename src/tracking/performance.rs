use crate::signals::predictor::TradeRecommendation;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// A complete snapshot of trading performance metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    pub timestamp: f64,
    pub total_signals: usize,
    pub total_resolved: usize,
    pub wins: usize,
    pub losses: usize,
    pub breakevens: usize,
    pub expired: usize,
    pub win_rate: f64,
    pub win_rate_10: f64,
    pub win_rate_20: f64,
    pub win_rate_50: f64,
    pub gross_pnl: f64,
    pub pnl_10: f64,
    pub pnl_20: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub profit_factor: f64,
    pub sharpe: f64,
    /// Positive integer = win streak; negative = loss streak.
    pub current_streak: i32,
    pub max_win_streak: i32,
    pub max_loss_streak: i32,
    pub long_win_rate: f64,
    pub short_win_rate: f64,
    /// Win rate when confidence > 0.75.
    pub high_conf_win_rate: f64,
    /// Win rate when confidence is 0.60–0.75.
    pub med_conf_win_rate: f64,
    /// Win rate when confidence < 0.60.
    pub low_conf_win_rate: f64,
    /// Last 50 cumulative PnL values for charting.
    pub pnl_curve: Vec<f64>,
}

impl Default for PerformanceSnapshot {
    fn default() -> Self {
        Self {
            timestamp: 0.0,
            total_signals: 0,
            total_resolved: 0,
            wins: 0,
            losses: 0,
            breakevens: 0,
            expired: 0,
            win_rate: 0.0,
            win_rate_10: 0.0,
            win_rate_20: 0.0,
            win_rate_50: 0.0,
            gross_pnl: 0.0,
            pnl_10: 0.0,
            pnl_20: 0.0,
            avg_win: 0.0,
            avg_loss: 0.0,
            profit_factor: 0.0,
            sharpe: 0.0,
            current_streak: 0,
            max_win_streak: 0,
            max_loss_streak: 0,
            long_win_rate: 0.0,
            short_win_rate: 0.0,
            high_conf_win_rate: 0.0,
            med_conf_win_rate: 0.0,
            low_conf_win_rate: 0.0,
            pnl_curve: Vec::new(),
        }
    }
}

/// Stores a minimal record of each resolved trade for analytics.
#[derive(Debug, Clone)]
struct Record {
    outcome: String,
    pnl: f64,
    direction: String,
    confidence: f64,
}

/// Accumulates resolved trade outcomes and computes performance metrics.
pub struct PerformanceTracker {
    total_signals: usize,
    records: VecDeque<Record>,
    cumulative_pnl: f64,
    /// Last 50 cumulative PnL checkpoints.
    pnl_curve: VecDeque<f64>,
    current_streak: i32,
    max_win_streak: i32,
    max_loss_streak: i32,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        Self {
            total_signals: 0,
            records: VecDeque::new(),
            cumulative_pnl: 0.0,
            pnl_curve: VecDeque::new(),
            current_streak: 0,
            max_win_streak: 0,
            max_loss_streak: 0,
        }
    }

    /// Increment the signal counter (called on every signal, not just resolved ones).
    pub fn increment_signals(&mut self) {
        self.total_signals += 1;
    }

    /// Record a resolved `TradeRecommendation`.
    pub fn record(&mut self, rec: &TradeRecommendation) {
        let outcome = rec.outcome.clone().unwrap_or_else(|| "expired".to_owned());
        let pnl = rec.pnl_usdc.unwrap_or(0.0);
        let direction = format!("{:?}", rec.direction);
        let confidence = rec.confidence;

        self.cumulative_pnl += pnl;

        // Update streak.
        if outcome == "win" {
            if self.current_streak >= 0 {
                self.current_streak += 1;
            } else {
                self.current_streak = 1;
            }
            if self.current_streak > self.max_win_streak {
                self.max_win_streak = self.current_streak;
            }
        } else if outcome == "loss" {
            if self.current_streak <= 0 {
                self.current_streak -= 1;
            } else {
                self.current_streak = -1;
            }
            if self.current_streak.abs() > self.max_loss_streak {
                self.max_loss_streak = self.current_streak.abs();
            }
        }

        self.records.push_back(Record {
            outcome,
            pnl,
            direction,
            confidence,
        });

        // Keep rolling window size manageable.
        if self.records.len() > 1000 {
            self.records.pop_front();
        }

        // Append to PnL curve (keep last 50).
        if self.pnl_curve.len() >= 50 {
            self.pnl_curve.pop_front();
        }
        self.pnl_curve.push_back(self.cumulative_pnl);
    }

    /// Compute a full performance snapshot from accumulated records.
    pub fn snapshot(&self) -> PerformanceSnapshot {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        let all: Vec<&Record> = self.records.iter().collect();
        let total_resolved = all.len();
        let wins: Vec<&Record> = all.iter().filter(|r| r.outcome == "win").copied().collect();
        let losses: Vec<&Record> = all.iter().filter(|r| r.outcome == "loss").copied().collect();
        let breakevens = all.iter().filter(|r| r.outcome == "breakeven").count();
        let expired = all.iter().filter(|r| r.outcome == "expired").count();

        let win_rate = if total_resolved > 0 {
            wins.len() as f64 / total_resolved as f64
        } else {
            0.0
        };

        let win_rate_10 = rolling_win_rate(&all, 10);
        let win_rate_20 = rolling_win_rate(&all, 20);
        let win_rate_50 = rolling_win_rate(&all, 50);

        let gross_pnl = self.cumulative_pnl;
        let pnl_10 = all.iter().rev().take(10).map(|r| r.pnl).sum();
        let pnl_20 = all.iter().rev().take(20).map(|r| r.pnl).sum();

        let avg_win = if wins.is_empty() {
            0.0
        } else {
            wins.iter().map(|r| r.pnl).sum::<f64>() / wins.len() as f64
        };

        let avg_loss = if losses.is_empty() {
            0.0
        } else {
            losses.iter().map(|r| r.pnl).sum::<f64>() / losses.len() as f64
        };

        let gross_profit: f64 = wins.iter().map(|r| r.pnl).sum();
        let gross_loss: f64 = losses.iter().map(|r| r.pnl.abs()).sum();
        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let sharpe = compute_sharpe(&all);

        // Direction breakdown.
        let long_win_rate = directional_win_rate(&all, "Long");
        let short_win_rate = directional_win_rate(&all, "Short");

        // Confidence tiers.
        let high_conf_win_rate = conf_win_rate(&all, 0.75, 1.01);
        let med_conf_win_rate = conf_win_rate(&all, 0.60, 0.75);
        let low_conf_win_rate = conf_win_rate(&all, 0.0, 0.60);

        PerformanceSnapshot {
            timestamp: now,
            total_signals: self.total_signals,
            total_resolved,
            wins: wins.len(),
            losses: losses.len(),
            breakevens,
            expired,
            win_rate,
            win_rate_10,
            win_rate_20,
            win_rate_50,
            gross_pnl,
            pnl_10,
            pnl_20,
            avg_win,
            avg_loss,
            profit_factor,
            sharpe,
            current_streak: self.current_streak,
            max_win_streak: self.max_win_streak,
            max_loss_streak: self.max_loss_streak,
            long_win_rate,
            short_win_rate,
            high_conf_win_rate,
            med_conf_win_rate,
            low_conf_win_rate,
            pnl_curve: self.pnl_curve.iter().copied().collect(),
        }
    }

    /// Persist the latest snapshot to a JSON file.
    pub fn save(&self, path: &str) -> anyhow::Result<()> {
        let snap = self.snapshot();
        let json = serde_json::to_string_pretty(&snap)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

impl Default for PerformanceTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn rolling_win_rate(all: &[&Record], n: usize) -> f64 {
    let slice: Vec<&&Record> = all.iter().rev().take(n).collect();
    if slice.is_empty() {
        return 0.0;
    }
    let wins = slice.iter().filter(|r| r.outcome == "win").count();
    wins as f64 / slice.len() as f64
}

fn directional_win_rate(all: &[&Record], dir: &str) -> f64 {
    let subset: Vec<&Record> = all
        .iter()
        .filter(|r| r.direction == dir)
        .copied()
        .collect();
    if subset.is_empty() {
        return 0.0;
    }
    let wins = subset.iter().filter(|r| r.outcome == "win").count();
    wins as f64 / subset.len() as f64
}

fn conf_win_rate(all: &[&Record], lo: f64, hi: f64) -> f64 {
    let subset: Vec<&Record> = all
        .iter()
        .filter(|r| r.confidence >= lo && r.confidence < hi)
        .copied()
        .collect();
    if subset.is_empty() {
        return 0.0;
    }
    let wins = subset.iter().filter(|r| r.outcome == "win").count();
    wins as f64 / subset.len() as f64
}

fn compute_sharpe(all: &[&Record]) -> f64 {
    if all.len() < 2 {
        return 0.0;
    }
    let pnls: Vec<f64> = all.iter().map(|r| r.pnl).collect();
    let mean = pnls.iter().sum::<f64>() / pnls.len() as f64;
    let variance =
        pnls.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / (pnls.len() - 1) as f64;
    let std_dev = variance.sqrt();
    if std_dev < 1e-9 {
        return 0.0;
    }
    mean / std_dev
}
