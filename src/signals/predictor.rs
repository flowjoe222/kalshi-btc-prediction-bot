use crate::data::markets::{MarketDiscovery, PredictionMarket};
use crate::signals::detector::{Direction, ImbalanceSignal};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::debug;
use uuid::Uuid;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A single trade recommendation derived from an `ImbalanceSignal`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecommendation {
    pub recommendation_id: String,
    pub signal_id: String,
    pub timestamp: f64,
    pub market: PredictionMarket,
    /// `"YES"` or `"NO"`.
    pub side: String,
    pub direction: Direction,
    pub entry_price: f64,
    pub suggested_size_usdc: f64,
    pub confidence: f64,
    pub imbalance_delta: f64,
    pub volume_usdc: f64,
    pub btc_price_at_signal: Option<f64>,
    pub btc_price_at_expiry: Option<f64>,
    pub resolved_at: Option<f64>,
    /// `"win"` | `"loss"` | `"breakeven"` | `"expired"`
    pub outcome: Option<String>,
    pub pnl_usdc: Option<f64>,
}

/// Generates `TradeRecommendation`s from `ImbalanceSignal`s and resolves
/// pending recommendations once BTC price data becomes available.
pub struct SignalPredictor {
    markets: Arc<Mutex<MarketDiscovery>>,
    btc_price: Arc<Mutex<Option<f64>>>,
    suggested_size_usdc: f64,
    pending: Vec<TradeRecommendation>,
    /// Minimum BTC move (basis points) to count as a win or loss rather than
    /// breakeven.
    min_move_bps: f64,
    /// Seconds after which an unresolved recommendation expires.
    check_after_seconds: f64,
}

impl SignalPredictor {
    pub fn new(
        markets: Arc<Mutex<MarketDiscovery>>,
        btc_price: Arc<Mutex<Option<f64>>>,
        suggested_size_usdc: f64,
    ) -> Self {
        Self {
            markets,
            btc_price,
            suggested_size_usdc,
            pending: Vec::new(),
            min_move_bps: 5.0,
            check_after_seconds: 60.0,
        }
    }

    pub fn set_min_move_bps(&mut self, bps: f64) {
        self.min_move_bps = bps;
    }

    pub fn set_check_after_seconds(&mut self, secs: f64) {
        self.check_after_seconds = secs;
    }

    /// Generate a recommendation for the best available market given `signal`.
    ///
    /// Returns `None` if no suitable market exists.
    pub async fn generate_recommendation(
        &self,
        signal: &ImbalanceSignal,
    ) -> Option<TradeRecommendation> {
        let btc_price = *self.btc_price.lock().await;
        let markets_guard: tokio::sync::MutexGuard<MarketDiscovery> = self.markets.lock().await;
        let active = markets_guard.get_active_markets();

        if active.is_empty() {
            debug!("No active markets available for recommendation");
            return None;
        }

        // Pick the highest-scoring market. Clone each candidate so we can drop
        // the guard before using `market` (avoids borrow-after-move).
        let best = active
            .iter()
            .map(|m| {
                let score = markets_guard.score_market(m, btc_price, &signal.direction);
                ((*m).clone(), score)
            })
            .filter(|(_, s)| *s >= 0.0)
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let (market, _score) = best?;
        drop(markets_guard);

        // Map signal direction × market direction to the correct contract side.
        // "above" markets: Long→YES (BTC going up = YES wins), Short→NO
        // "below" markets: Long→NO (BTC going up = NO wins), Short→YES
        let is_above = market.direction == "above";
        let (side, entry_price) = match (signal.direction.clone(), is_above) {
            (Direction::Long, true) => ("YES".to_owned(), market.current_yes_price),
            (Direction::Long, false) => ("NO".to_owned(), market.current_no_price),
            (Direction::Short, true) => ("NO".to_owned(), market.current_no_price),
            (Direction::Short, false) => ("YES".to_owned(), market.current_yes_price),
            (Direction::Neutral, _) => return None,
        };

        if entry_price <= 0.0 || entry_price >= 1.0 {
            return None;
        }

        Some(TradeRecommendation {
            recommendation_id: Uuid::new_v4().to_string(),
            signal_id: signal.signal_id.clone(),
            timestamp: now_secs(),
            market: market.clone(),
            side,
            direction: signal.direction.clone(),
            entry_price,
            suggested_size_usdc: self.suggested_size_usdc,
            confidence: signal.confidence,
            imbalance_delta: signal.primary_delta,
            volume_usdc: signal.total_volume_usdc,
            btc_price_at_signal: btc_price,
            btc_price_at_expiry: None,
            resolved_at: None,
            outcome: None,
            pnl_usdc: None,
        })
    }

    /// Add a recommendation to the pending queue.
    pub fn add_pending(&mut self, rec: TradeRecommendation) {
        self.pending.push(rec);
    }

    /// Attempt to resolve pending recommendations.
    ///
    /// A recommendation is resolved when either:
    /// - It is older than `check_after_seconds`, or
    /// - The underlying market has expired.
    ///
    /// Returns all newly-resolved recommendations.
    pub fn resolve_pending(&mut self, btc_price: f64) -> Vec<TradeRecommendation> {
        let now = now_secs();
        let min_move = self.min_move_bps;
        let check_after = self.check_after_seconds;

        let mut resolved = Vec::new();
        let mut still_pending = Vec::new();

        for mut rec in self.pending.drain(..) {
            let age = now - rec.timestamp;
            let market_expired = rec.market.expiry_ts > 0.0 && now >= rec.market.expiry_ts;

            if age < check_after && !market_expired {
                still_pending.push(rec);
                continue;
            }

            // Mark expiry price.
            rec.btc_price_at_expiry = Some(btc_price);
            rec.resolved_at = Some(now);

            let entry_price = rec.entry_price;
            let size = rec.suggested_size_usdc;

            // Compute BTC move in basis points.
            let (outcome, pnl) = match rec.btc_price_at_signal {
                None => ("expired".to_owned(), 0.0),
                Some(price_at_signal) => {
                    let btc_move_bps =
                        (btc_price - price_at_signal) / price_at_signal * 10_000.0;

                    if btc_move_bps.abs() < min_move {
                        ("breakeven".to_owned(), 0.0)
                    } else {
                        let won = match rec.direction {
                            Direction::Long => btc_move_bps > 0.0,
                            Direction::Short => btc_move_bps < 0.0,
                            Direction::Neutral => false,
                        };
                        if won {
                            let pnl = size * (1.0 / entry_price - 1.0);
                            ("win".to_owned(), pnl)
                        } else {
                            ("loss".to_owned(), -size)
                        }
                    }
                }
            };

            rec.outcome = Some(outcome);
            rec.pnl_usdc = Some(pnl);
            resolved.push(rec);
        }

        self.pending = still_pending;
        resolved
    }

    /// Number of recommendations still awaiting resolution.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}
