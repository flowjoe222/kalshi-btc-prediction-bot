pub mod matcher;

use crate::execution::kalshi::{KalshiExecutor, KalshiMarket};
use matcher::{MatchedPair, MarketMatcher};

use anyhow::Result;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::Write as _;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Polymarket CLOB REST types
// ---------------------------------------------------------------------------

/// Top-level response from `GET https://clob.polymarket.com/markets`.
#[derive(Debug, Deserialize)]
pub struct PolyMarketsResponse {
    pub data: Vec<PolyMarketInfo>,
    pub next_cursor: Option<String>,
    pub limit: Option<u64>,
    pub count: Option<u64>,
}

/// A single Polymarket market as returned by the CLOB REST API.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolyMarketInfo {
    pub condition_id: String,
    pub question: String,
    pub tokens: Vec<PolyToken>,
    pub active: bool,
    pub closed: bool,
    pub market_slug: Option<String>,
    pub end_date_iso: Option<String>,
    pub category: Option<String>,
    pub description: Option<String>,
}

/// A YES or NO token belonging to a Polymarket market.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolyToken {
    pub token_id: String,
    pub outcome: String,
}

/// Response from `GET https://clob.polymarket.com/book?token_id=X`.
#[derive(Debug, Deserialize)]
pub struct PolyOrderbook {
    pub market: Option<String>,
    pub asset_id: Option<String>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: Option<String>,
}

/// A single price level in an orderbook.
#[derive(Debug, Deserialize)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

// ---------------------------------------------------------------------------
// Arbitrage signal types
// ---------------------------------------------------------------------------

/// Which side has the better price and should be bought.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendedAction {
    /// Kalshi YES is underpriced — buy Kalshi, fade Polymarket.
    BuyKalshiSellPoly,
    /// Polymarket YES is underpriced — buy Polymarket, fade Kalshi.
    BuyPolySellKalshi,
}

impl std::fmt::Display for RecommendedAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BuyKalshiSellPoly => write!(f, "BUY_KALSHI_SELL_POLY"),
            Self::BuyPolySellKalshi => write!(f, "BUY_POLY_SELL_KALSHI"),
        }
    }
}

/// A detected arbitrage opportunity between Kalshi and Polymarket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunity {
    pub id: String,
    /// Unix seconds.
    pub timestamp: f64,
    /// ISO-8601 datetime string.
    pub datetime: String,
    pub kalshi_ticker: String,
    pub kalshi_title: String,
    pub poly_condition_id: String,
    pub poly_question: String,
    /// Shared underlying strike price (e.g. 97500 = $97,500 BTC).
    pub strike_price: f64,
    /// "above" or "below"
    pub direction: String,
    /// Kalshi best YES bid in [0, 1].
    pub kalshi_bid: f64,
    /// Kalshi best YES ask in [0, 1].
    pub kalshi_ask: f64,
    /// Kalshi mid price in [0, 1].
    pub kalshi_mid: f64,
    /// Polymarket best YES bid in [0, 1].
    pub poly_bid: f64,
    /// Polymarket best YES ask in [0, 1].
    pub poly_ask: f64,
    /// Polymarket mid price in [0, 1].
    pub poly_mid: f64,
    /// |kalshi_mid - poly_mid| — the exploitable gap.
    pub gap_size: f64,
    pub recommended_action: RecommendedAction,
    /// "GREEN" ≥ 5 cents, "YELLOW" 3–5 cents, "MARGINAL" < 3 cents.
    pub severity: String,
    /// Set to `true` when an order was auto-placed on Kalshi.
    pub auto_executed: bool,
}

impl ArbitrageOpportunity {
    fn severity_label(gap: f64) -> &'static str {
        if gap >= 0.05 {
            "GREEN"
        } else if gap >= 0.03 {
            "YELLOW"
        } else {
            "MARGINAL"
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime configuration
// ---------------------------------------------------------------------------

/// Runtime configuration for `ArbitrageScanner`.
///
/// Constructed from `ArbitrageConfigRaw` (the YAML-deserialized form) via
/// `From` in `config.rs`.
#[derive(Debug, Clone)]
pub struct ArbitrageConfig {
    /// Master switch — set `false` to disable all scanning.
    pub enabled: bool,
    /// Minimum |gap| to record as an opportunity (default 0.03 = 3 cents).
    pub min_gap: f64,
    /// Seconds between scans (default 30).
    pub scan_interval_secs: u64,
    /// When `true` and gap ≥ min_gap, place the Kalshi side automatically.
    pub auto_execute: bool,
    /// Maximum dollar value per auto-executed Kalshi order.
    pub max_position_size_usd: f64,
    /// Path to the CSV file for logging all detected opportunities.
    pub opportunities_log_csv: String,
    /// Maximum number of Polymarket markets to fetch per scan.
    pub poly_max_markets: usize,
}

impl Default for ArbitrageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_gap: 0.03,
            scan_interval_secs: 30,
            auto_execute: false,
            max_position_size_usd: 50.0,
            opportunities_log_csv: "logs/arbitrage_opportunities.csv".to_owned(),
            poly_max_markets: 200,
        }
    }
}

// ---------------------------------------------------------------------------
// ArbitrageScanner
// ---------------------------------------------------------------------------

/// Scans Kalshi and Polymarket for equivalent markets with exploitable price
/// gaps, then fires `ArbitrageOpportunity` records for each gap above
/// `config.min_gap`.
pub struct ArbitrageScanner {
    http: Client,
    /// Optional — scanning is skipped on the Kalshi side when `None`.
    kalshi: Option<Arc<KalshiExecutor>>,
    matcher: MarketMatcher,
    pub config: ArbitrageConfig,
    /// Base URL for the Polymarket CLOB REST API.
    poly_base_url: String,
}

impl ArbitrageScanner {
    pub fn new(
        http: Client,
        kalshi: Option<Arc<KalshiExecutor>>,
        config: ArbitrageConfig,
        poly_base_url: String,
    ) -> Self {
        Self {
            http,
            kalshi,
            matcher: MarketMatcher::new(),
            config,
            poly_base_url,
        }
    }

    // ---- Public interface ----

    /// Run a single scan cycle.
    ///
    /// Returns a list of opportunities sorted by `gap_size` descending.
    /// Returns an empty vec when scanning is disabled or no Kalshi executor is
    /// available.
    pub async fn scan(&self) -> Vec<ArbitrageOpportunity> {
        if !self.config.enabled {
            return Vec::new();
        }

        // We need Kalshi to have something to compare against.
        let kalshi = match &self.kalshi {
            Some(k) => k,
            None => {
                debug!("Arbitrage: no Kalshi executor configured, skipping scan");
                return Vec::new();
            }
        };

        // 1. Fetch markets in parallel.
        let (poly_result, kalshi_result) = tokio::join!(
            self.fetch_poly_markets(),
            kalshi.list_btc_markets(),
        );

        let poly_markets = match poly_result {
            Ok(m) => m,
            Err(e) => {
                warn!("Arbitrage: Polymarket fetch failed: {e}");
                return Vec::new();
            }
        };

        let kalshi_markets = match kalshi_result {
            Ok(m) => m,
            Err(e) => {
                warn!("Arbitrage: Kalshi fetch failed: {e}");
                return Vec::new();
            }
        };

        if kalshi_markets.is_empty() || poly_markets.is_empty() {
            debug!(
                "Arbitrage: nothing to compare (kalshi={} poly={})",
                kalshi_markets.len(),
                poly_markets.len()
            );
            return Vec::new();
        }

        // 2. Match equivalent markets.
        let pairs = self.matcher.match_markets(&kalshi_markets, &poly_markets);
        debug!("Arbitrage: {} matched pairs", pairs.len());

        if pairs.is_empty() {
            return Vec::new();
        }

        // 3. Fetch Polymarket orderbooks and compute gaps.
        let mut opportunities: Vec<ArbitrageOpportunity> = Vec::new();
        for pair in &pairs {
            match self.analyse_pair(pair, &kalshi_markets).await {
                Ok(Some(opp)) if opp.gap_size >= self.config.min_gap => {
                    info!(
                        "Arb gap detected: {} | K={:.3} P={:.3} gap={:.3} ({})",
                        opp.kalshi_ticker,
                        opp.kalshi_mid,
                        opp.poly_mid,
                        opp.gap_size,
                        opp.recommended_action,
                    );
                    opportunities.push(opp);
                }
                Ok(_) => {}
                Err(e) => {
                    debug!("Arbitrage pair error ({}): {e}", pair.kalshi_ticker);
                }
            }
        }

        // Sort largest gap first.
        opportunities.sort_by(|a, b| {
            b.gap_size
                .partial_cmp(&a.gap_size)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        opportunities
    }

    /// Write detected opportunities to the CSV log.
    ///
    /// Creates the file and writes the header row on first call.
    pub fn log_to_csv(&self, opportunities: &[ArbitrageOpportunity]) {
        if opportunities.is_empty() {
            return;
        }

        let path = &self.config.opportunities_log_csv;
        let needs_header = !std::path::Path::new(path).exists();

        let file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(f) => f,
            Err(e) => {
                warn!("Arbitrage: cannot open CSV {path}: {e}");
                return;
            }
        };

        let mut w = std::io::BufWriter::new(file);

        if needs_header {
            let _ = writeln!(
                w,
                "id,datetime,kalshi_ticker,kalshi_title,poly_question,strike_price,direction,\
                 kalshi_bid,kalshi_ask,kalshi_mid,poly_bid,poly_ask,poly_mid,\
                 gap_size,recommended_action,severity,auto_executed"
            );
        }

        for opp in opportunities {
            let title = opp.kalshi_title.replace(',', ";");
            let question = opp.poly_question.replace(',', ";");
            let _ = writeln!(
                w,
                "{},{},{},{},{},{:.0},{},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{},{},{}",
                opp.id,
                opp.datetime,
                opp.kalshi_ticker,
                title,
                question,
                opp.strike_price,
                opp.direction,
                opp.kalshi_bid,
                opp.kalshi_ask,
                opp.kalshi_mid,
                opp.poly_bid,
                opp.poly_ask,
                opp.poly_mid,
                opp.gap_size,
                opp.recommended_action,
                opp.severity,
                opp.auto_executed,
            );
        }
    }

    // ---- Private helpers ----

    /// Fetch BTC-related markets from the Polymarket CLOB REST API.
    async fn fetch_poly_markets(&self) -> Result<Vec<PolyMarketInfo>> {
        let url = format!("{}/markets", self.poly_base_url);
        let resp = self
            .http
            .get(&url)
            .query(&[("limit", "100"), ("next_cursor", "MA==")])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Polymarket /markets HTTP {status}: {body}"
            ));
        }

        let body: PolyMarketsResponse = resp.json().await?;

        let btc_markets: Vec<PolyMarketInfo> = body
            .data
            .into_iter()
            .filter(|m| {
                !m.closed
                    && m.active
                    && (m.question.to_lowercase().contains("bitcoin")
                        || m.question.to_lowercase().contains("btc"))
            })
            .take(self.config.poly_max_markets)
            .collect();

        debug!(
            "Arbitrage: {} BTC Polymarket markets fetched",
            btc_markets.len()
        );
        Ok(btc_markets)
    }

    /// Fetch the orderbook for a single Polymarket YES token.
    async fn fetch_poly_orderbook(&self, token_id: &str) -> Result<PolyOrderbook> {
        let url = format!("{}/book", self.poly_base_url);
        let resp = self
            .http
            .get(&url)
            .query(&[("token_id", token_id)])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Polymarket /book HTTP {status}: {body}"
            ));
        }

        Ok(resp.json::<PolyOrderbook>().await?)
    }

    /// Compute the arbitrage opportunity for one matched pair, if any.
    async fn analyse_pair(
        &self,
        pair: &MatchedPair,
        kalshi_markets: &[KalshiMarket],
    ) -> Result<Option<ArbitrageOpportunity>> {
        // Retrieve the full Kalshi market struct for this pair.
        let km = kalshi_markets
            .iter()
            .find(|m| m.ticker == pair.kalshi_ticker)
            .ok_or_else(|| anyhow::anyhow!("Kalshi market not in slice"))?;

        // Kalshi prices are in cents (1–99); normalise to [0, 1].
        let k_bid = match km.yes_bid {
            Some(b) if b > 0 => b as f64 / 100.0,
            _ => return Ok(None),
        };
        let k_ask = match km.yes_ask {
            Some(a) if a > 0 => a as f64 / 100.0,
            _ => return Ok(None),
        };
        let k_mid = (k_bid + k_ask) / 2.0;

        // Fetch Polymarket orderbook.
        let book = self
            .fetch_poly_orderbook(&pair.poly_yes_token_id)
            .await?;

        let p_bid = match best_bid(&book.bids) {
            Some(b) => b,
            None => return Ok(None),
        };
        let p_ask = match best_ask(&book.asks) {
            Some(a) => a,
            None => return Ok(None),
        };
        let p_mid = (p_bid + p_ask) / 2.0;

        let gap = (k_mid - p_mid).abs();

        // Return None for sub-0.5-cent gaps (noise).
        if gap < 0.005 {
            return Ok(None);
        }

        let action = if k_mid < p_mid {
            RecommendedAction::BuyKalshiSellPoly
        } else {
            RecommendedAction::BuyPolySellKalshi
        };

        let now = Utc::now();
        Ok(Some(ArbitrageOpportunity {
            id: Uuid::new_v4().to_string(),
            timestamp: now.timestamp() as f64,
            datetime: now.to_rfc3339(),
            kalshi_ticker: pair.kalshi_ticker.clone(),
            kalshi_title: pair.kalshi_title.clone(),
            poly_condition_id: pair.poly_condition_id.clone(),
            poly_question: pair.poly_question.clone(),
            strike_price: pair.strike_price,
            direction: pair.direction.clone(),
            kalshi_bid: k_bid,
            kalshi_ask: k_ask,
            kalshi_mid: k_mid,
            poly_bid: p_bid,
            poly_ask: p_ask,
            poly_mid: p_mid,
            gap_size: gap,
            recommended_action: action,
            severity: ArbitrageOpportunity::severity_label(gap).to_owned(),
            auto_executed: false,
        }))
    }
}

// ---------------------------------------------------------------------------
// Orderbook helpers
// ---------------------------------------------------------------------------

fn best_bid(levels: &[PriceLevel]) -> Option<f64> {
    levels
        .iter()
        .filter_map(|l| l.price.parse::<f64>().ok())
        .fold(None, |acc, p| {
            Some(match acc {
                None => p,
                Some(best) => best.max(p),
            })
        })
}

fn best_ask(levels: &[PriceLevel]) -> Option<f64> {
    levels
        .iter()
        .filter_map(|l| l.price.parse::<f64>().ok())
        .fold(None, |acc, p| {
            Some(match acc {
                None => p,
                Some(best) => best.min(p),
            })
        })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_level(price: &str, size: &str) -> PriceLevel {
        PriceLevel {
            price: price.to_owned(),
            size: size.to_owned(),
        }
    }

    #[test]
    fn test_best_bid_ask() {
        let bids = vec![
            make_level("0.45", "100"),
            make_level("0.43", "200"),
            make_level("0.47", "50"),
        ];
        let asks = vec![
            make_level("0.52", "100"),
            make_level("0.55", "200"),
            make_level("0.50", "50"),
        ];

        assert_eq!(best_bid(&bids), Some(0.47));
        assert_eq!(best_ask(&asks), Some(0.50));
    }

    #[test]
    fn test_best_empty() {
        assert_eq!(best_bid(&[]), None);
        assert_eq!(best_ask(&[]), None);
    }

    #[test]
    fn test_severity_label() {
        assert_eq!(ArbitrageOpportunity::severity_label(0.06), "GREEN");
        assert_eq!(ArbitrageOpportunity::severity_label(0.04), "YELLOW");
        assert_eq!(ArbitrageOpportunity::severity_label(0.02), "MARGINAL");
        assert_eq!(ArbitrageOpportunity::severity_label(0.05), "GREEN");
        assert_eq!(ArbitrageOpportunity::severity_label(0.03), "YELLOW");
    }

    #[test]
    fn test_recommended_action_display() {
        assert_eq!(
            RecommendedAction::BuyKalshiSellPoly.to_string(),
            "BUY_KALSHI_SELL_POLY"
        );
        assert_eq!(
            RecommendedAction::BuyPolySellKalshi.to_string(),
            "BUY_POLY_SELL_KALSHI"
        );
    }
}
