use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// A single executed trade on a prediction market token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub trade_id: String,
    pub market_id: String,
    pub token_id: String,
    /// Price in [0, 1] — a prediction token price.
    pub price: f64,
    /// Notional USDC value of the trade.
    pub size_usdc: f64,
    /// `true` when the aggressor was a buyer.
    pub is_buy_initiated: bool,
    /// Unix timestamp in fractional seconds.
    pub timestamp: f64,
}

/// A single price level in an order book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

/// Full order-book state for one token at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub token_id: String,
    pub market_id: String,
    /// Bids sorted from highest price to lowest.
    pub bids: Vec<Level>,
    /// Asks sorted from lowest price to highest.
    pub asks: Vec<Level>,
    pub timestamp: f64,
}

impl BookSnapshot {
    /// Best (highest) bid price, if any.
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price)
    }

    /// Best (lowest) ask price, if any.
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price)
    }

    /// Mid price between best bid and best ask.
    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) / 2.0),
            _ => None,
        }
    }

    /// Bid–ask spread.
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some(a - b),
            _ => None,
        }
    }
}

/// Per-token order book that maintains a rolling trade window.
///
/// Keeps at most `MAX_TRADES` recent trades in memory.
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub token_id: String,
    pub market_id: String,
    bids: HashMap<String, f64>,
    asks: HashMap<String, f64>,
    trades: Vec<Trade>,
    pub last_snapshot: Option<BookSnapshot>,
}

const MAX_TRADES: usize = 10_000;

/// Encode a floating-point price as a map key with 8 decimal places of
/// precision, avoiding floating-point equality pitfalls.
fn price_key(price: f64) -> String {
    format!("{price:.8}")
}

impl OrderBook {
    pub fn new(token_id: &str, market_id: &str) -> Self {
        Self {
            token_id: token_id.to_owned(),
            market_id: market_id.to_owned(),
            bids: HashMap::new(),
            asks: HashMap::new(),
            trades: Vec::new(),
            last_snapshot: None,
        }
    }

    /// Replace the current order book state with a full snapshot.
    pub fn apply_snapshot(&mut self, snap: BookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        for level in &snap.bids {
            self.bids.insert(price_key(level.price), level.size);
        }
        for level in &snap.asks {
            self.asks.insert(price_key(level.price), level.size);
        }
        self.last_snapshot = Some(snap);
    }

    /// Apply a single level update.  `side` must be `"bid"` or `"ask"`.
    /// A size of `0.0` removes the level.
    pub fn apply_change(&mut self, price: f64, size: f64, side: &str) {
        let key = price_key(price);
        let map = if side == "bid" {
            &mut self.bids
        } else {
            &mut self.asks
        };
        if size == 0.0 {
            map.remove(&key);
        } else {
            map.insert(key, size);
        }
    }

    /// Add a trade to the rolling window, evicting the oldest if necessary.
    pub fn add_trade(&mut self, trade: Trade) {
        if self.trades.len() >= MAX_TRADES {
            self.trades.remove(0);
        }
        self.trades.push(trade);
    }

    /// Return trades that occurred within `seconds` of now.
    pub fn recent_trades(&self, seconds: f64) -> Vec<&Trade> {
        let cutoff = now_secs() - seconds;
        self.trades
            .iter()
            .filter(|t| t.timestamp >= cutoff)
            .collect()
    }

    /// Return the best bid from the internal map.
    pub fn best_bid(&self) -> Option<f64> {
        self.bids
            .keys()
            .filter_map(|k| k.parse::<f64>().ok())
            .reduce(f64::max)
    }

    /// Return the best ask from the internal map.
    pub fn best_ask(&self) -> Option<f64> {
        self.asks
            .keys()
            .filter_map(|k| k.parse::<f64>().ok())
            .reduce(f64::min)
    }

    /// Classify a trade as buy-initiated.
    ///
    /// If an explicit side is given, use it; otherwise use the Lee–Ready rule:
    /// price at or above best ask → buy, price at or below best bid → sell.
    /// Returns `None` if the trade cannot be classified (avoids directional bias).
    pub fn classify_trade(&self, price: f64, side: Option<&str>) -> Option<bool> {
        if let Some(s) = side {
            return Some(s == "BUY" || s == "buy" || s == "bid");
        }
        if let Some(ask) = self.best_ask() {
            if price >= ask {
                return Some(true);
            }
        }
        if let Some(bid) = self.best_bid() {
            if price <= bid {
                return Some(false);
            }
        }
        // Cannot classify — return None to avoid directional bias.
        None
    }
}

/// Aggregates `OrderBook` instances keyed by token ID.
#[derive(Debug, Default)]
pub struct OrderBookRegistry {
    pub books: HashMap<String, OrderBook>,
}

impl OrderBookRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the `OrderBook` for `token_id`, creating it if it does not yet
    /// exist.
    pub fn get_or_create(&mut self, token_id: &str, market_id: &str) -> &mut OrderBook {
        self.books
            .entry(token_id.to_owned())
            .or_insert_with(|| OrderBook::new(token_id, market_id))
    }

    /// Return all trades across every book that occurred within `seconds` of
    /// now, cloned into a single `Vec`.
    pub fn all_recent_trades(&self, seconds: f64) -> Vec<Trade> {
        let cutoff = now_secs() - seconds;
        self.books
            .values()
            .flat_map(|b| b.trades.iter())
            .filter(|t| t.timestamp >= cutoff)
            .cloned()
            .collect()
    }
}

/// Current unix time as fractional seconds.
pub fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
