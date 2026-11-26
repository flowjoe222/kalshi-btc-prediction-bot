use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tracing::warn;

/// Configurable risk limits (loaded from config.yaml, never from env).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskParams {
    /// Maximum cost per single trade in cents.  Default: 200 (= $2.00).
    pub max_position_cents: i64,
    /// Maximum cumulative daily loss in cents before halting.  Default: 1000 (= $10.00).
    pub max_daily_loss_cents: i64,
    /// Maximum number of positions open simultaneously.  Default: 3.
    pub max_concurrent_positions: usize,
    /// Minimum seconds between consecutive trades.  Default: 30.
    pub cooldown_seconds: u64,
    /// Minimum signal confidence required to attempt execution.  Default: 0.70.
    pub confidence_threshold: f64,
    /// Maximum aggregate exposure across all open positions in cents.
    /// Default: 1500 (= $15.00). The per-loop sizing logic in `main.rs` already
    /// caps each strategy individually — this is the GLOBAL ceiling enforced
    /// inside `RiskManager::check()` so it cannot be bypassed by any single
    /// strategy.
    #[serde(default = "default_max_total_exposure_cents")]
    pub max_total_exposure_cents: i64,
}

fn default_max_total_exposure_cents() -> i64 {
    1_500
}

impl Default for RiskParams {
    fn default() -> Self {
        Self {
            max_position_cents: 200,
            max_daily_loss_cents: 1_000,
            max_concurrent_positions: 3,
            cooldown_seconds: 30,
            confidence_threshold: 0.70,
            max_total_exposure_cents: default_max_total_exposure_cents(),
        }
    }
}

/// A lightweight record of an open Kalshi position for internal tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub order_id: String,
    pub ticker: String,
    /// "yes" or "no"
    pub side: String,
    pub count: i64,
    pub price_cents: i64,
    /// Total cost in cents (count × price_cents).
    pub cost_cents: i64,
    pub opened_at: f64,
    pub signal_id: String,
    /// Set once the position is closed.
    pub closed_at: Option<f64>,
    pub pnl_cents: Option<i64>,
}

/// Why a trade was blocked by the risk manager.
#[derive(Debug)]
pub enum RiskCheckResult {
    Allowed,
    Blocked(String),
}

/// Enforces all risk controls before a trade is submitted.
///
/// All methods are synchronous — wrap in `tokio::sync::Mutex` for shared access.
pub struct RiskManager {
    pub params: RiskParams,
    /// When `true`, no new orders will be placed.
    kill_switch: bool,
    /// Cumulative P&L today in cents (positive = profit, negative = loss).
    daily_pnl_cents: i64,
    /// Currently open positions.
    open_positions: Vec<OpenPosition>,
    /// Timestamp of the most recent trade.
    last_trade_at: Option<Instant>,
    /// When the current trading day started (resets after 24 h).
    day_start: Instant,
}

impl RiskManager {
    pub fn new(params: RiskParams) -> Self {
        Self {
            params,
            kill_switch: false,
            daily_pnl_cents: 0,
            open_positions: Vec::new(),
            last_trade_at: None,
            day_start: Instant::now(),
        }
    }

    // ---- Kill switch ----

    pub fn set_kill_switch(&mut self, active: bool) {
        if self.kill_switch == active { return; }
        self.kill_switch = active;
        if active {
            warn!("Risk: kill switch ACTIVATED — no new trades");
        } else {
            warn!("Risk: kill switch deactivated");
        }
    }

    pub fn kill_switch_active(&self) -> bool {
        self.kill_switch
    }

    // ---- Daily reset ----

    fn maybe_reset_daily(&mut self) {
        if self.day_start.elapsed() >= Duration::from_secs(86_400) {
            self.daily_pnl_cents = 0;
            self.day_start = Instant::now();
        }
    }

    // ---- Risk check ----

    /// Check whether a new trade with `cost_cents` is allowed.
    /// Returns `Allowed` or `Blocked(reason)`.
    pub fn check(&mut self, cost_cents: i64, confidence: f64) -> RiskCheckResult {
        self.maybe_reset_daily();

        if self.kill_switch {
            return RiskCheckResult::Blocked("kill switch is active".to_owned());
        }

        if confidence < self.params.confidence_threshold {
            return RiskCheckResult::Blocked(format!(
                "confidence {:.2} below threshold {:.2}",
                confidence, self.params.confidence_threshold
            ));
        }

        if cost_cents > self.params.max_position_cents {
            return RiskCheckResult::Blocked(format!(
                "cost {}¢ exceeds max {}¢ per trade",
                cost_cents, self.params.max_position_cents
            ));
        }

        // Aggregate exposure cap — must be enforced inside `check()` so it
        // can't be bypassed by per-loop sizing logic in any strategy.
        let current_exposure = self.total_exposure_cents();
        let projected = current_exposure.saturating_add(cost_cents);
        if projected > self.params.max_total_exposure_cents {
            return RiskCheckResult::Blocked(format!(
                "total exposure {}¢ + new {}¢ = {}¢ exceeds max {}¢",
                current_exposure,
                cost_cents,
                projected,
                self.params.max_total_exposure_cents
            ));
        }

        // Remaining daily loss budget.
        let daily_loss = (-self.daily_pnl_cents).max(0);
        if daily_loss + cost_cents > self.params.max_daily_loss_cents {
            return RiskCheckResult::Blocked(format!(
                "daily loss budget exhausted (loss so far {}¢, max {}¢)",
                daily_loss, self.params.max_daily_loss_cents
            ));
        }

        if self.open_positions.len() >= self.params.max_concurrent_positions {
            return RiskCheckResult::Blocked(format!(
                "max concurrent positions reached ({}/{})",
                self.open_positions.len(),
                self.params.max_concurrent_positions
            ));
        }

        if let Some(last) = self.last_trade_at {
            let elapsed = last.elapsed().as_secs();
            if elapsed < self.params.cooldown_seconds {
                return RiskCheckResult::Blocked(format!(
                    "cooldown active ({}s remaining)",
                    self.params.cooldown_seconds - elapsed
                ));
            }
        }

        RiskCheckResult::Allowed
    }

    // ---- State mutations ----

    /// Record that a new order was placed.
    pub fn record_open(&mut self, position: OpenPosition) {
        self.last_trade_at = Some(Instant::now());
        self.open_positions.push(position);
    }

    /// Record realised P&L when a position closes (positive = profit).
    pub fn record_close(&mut self, order_id: &str, pnl_cents: i64, closed_at: f64) {
        if !self.open_positions.iter().any(|p| p.order_id == order_id && p.closed_at.is_none()) { return; }
        for pos in &mut self.open_positions {
            if pos.order_id == order_id {
                pos.closed_at = Some(closed_at);
                pos.pnl_cents = Some(pnl_cents);
                break;
            }
        }
        self.open_positions.retain(|p| p.closed_at.is_none());
        self.daily_pnl_cents += pnl_cents;
    }

    /// Remove a position without a P&L update (e.g., cancelled order).
    pub fn remove_position(&mut self, order_id: &str) {
        self.open_positions.retain(|p| p.order_id != order_id);
    }

    // ---- Accessors ----

    pub fn open_positions(&self) -> &[OpenPosition] {
        &self.open_positions
    }

    pub fn position_count(&self) -> usize {
        self.open_positions.len()
    }

    /// Cumulative P&L today in cents.
    pub fn daily_pnl_cents(&self) -> i64 {
        self.daily_pnl_cents
    }

    /// Total current exposure in cents (sum of open position costs).
    pub fn total_exposure_cents(&self) -> i64 {
        self.open_positions.iter().map(|p| p.cost_cents).sum()
    }
}
