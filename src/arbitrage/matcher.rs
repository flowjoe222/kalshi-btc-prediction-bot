use crate::execution::kalshi::KalshiMarket;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// Returns the compiled regex for extracting a Kalshi strike from a ticker.
///
/// The pattern matches the trailing `-T<digits>` segment, e.g. `-T97500`.
/// Compiled exactly once for the lifetime of the process.
fn kalshi_strike_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"-T(\d+)$").expect("kalshi strike regex is valid"))
}

/// Returns the compiled regex for extracting a dollar-denominated strike from
/// a Polymarket question string.
///
/// Matches `$` followed by digits and optional comma separators, e.g. `$97,500`.
/// Compiled exactly once for the lifetime of the process.
fn poly_strike_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\$([0-9][0-9,]*)").expect("poly strike regex is valid"))
}

use super::{PolyMarketInfo};

/// A matched pair of equivalent markets across Kalshi and Polymarket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedPair {
    pub kalshi_ticker: String,
    pub kalshi_title: String,
    /// Polymarket YES token ID to fetch orderbook for.
    pub poly_yes_token_id: String,
    pub poly_condition_id: String,
    pub poly_question: String,
    /// Matched strike price (e.g. 97500 for $97,500).
    pub strike_price: f64,
    /// "above" or "below"
    pub direction: String,
}

/// Maps Kalshi markets to equivalent Polymarket markets.
///
/// Matching strategy (in order of preference):
/// 1. Static overrides from `add_static_mapping()`.
/// 2. Strike-price fuzzy match: extract price from Kalshi ticker (`-T97500`)
///    and from Polymarket question (`$97,500`) within ±0.5% tolerance.
/// 3. Direction must agree ("above"/"below" parsed from both titles).
pub struct MarketMatcher {
    /// Explicit overrides: (kalshi_ticker_exact, poly_condition_id).
    static_mappings: Vec<(String, String)>,
    /// Max relative difference in strike price to still call a match.
    pub strike_tolerance: f64,
}

impl Default for MarketMatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketMatcher {
    pub fn new() -> Self {
        Self {
            static_mappings: Vec::new(),
            strike_tolerance: 0.005, // 0.5%
        }
    }

    /// Register a hard-coded mapping so the fuzzy logic is bypassed.
    pub fn add_static_mapping(&mut self, kalshi_ticker: impl Into<String>, poly_condition_id: impl Into<String>) {
        self.static_mappings.push((kalshi_ticker.into(), poly_condition_id.into()));
    }

    // ---- Price extraction helpers ----

    /// Extract BTC strike price from a Kalshi ticker.
    ///
    /// Format examples:
    /// - `KXBTC-25DEC3100-T97500` → 97500.0
    /// - `KXBTC-25MAR1500-T100000` → 100000.0
    pub fn extract_kalshi_strike(ticker: &str) -> Option<f64> {
        // Use the process-wide static regex — no allocation on repeated calls.
        let cap = kalshi_strike_re().captures(ticker)?;
        cap.get(1)?.as_str().parse::<f64>().ok()
    }

    /// Extract a dollar figure from a free-text market question.
    ///
    /// Handles `$97,500`, `$97500`, `97,500 USDT`, etc.
    /// Returns the first match (largest plausible BTC price).
    pub fn extract_poly_strike(question: &str) -> Option<f64> {
        // Use the process-wide static regex — no allocation on repeated calls.
        // Collect all matches, then pick the one most likely to be a BTC price.
        let candidates: Vec<f64> = poly_strike_re()
            .captures_iter(question)
            .filter_map(|cap| {
                cap.get(1)
                    .map(|m| m.as_str().replace(',', ""))
                    .and_then(|s| s.parse::<f64>().ok())
            })
            // BTC price is plausibly between $1,000 and $10,000,000.
            .filter(|&p| p >= 1_000.0 && p <= 10_000_000.0)
            .collect();

        // Prefer the candidate closest to a "round" BTC price (multiple of 500).
        candidates
            .into_iter()
            .min_by_key(|&p| (p % 500.0) as u64)
    }

    /// Infer direction from a free-text string.
    ///
    /// Returns `"above"` or `"below"`.
    pub fn infer_direction(text: &str) -> &'static str {
        let lower = text.to_lowercase();
        if lower.contains("below")
            || lower.contains("under")
            || lower.contains("less than")
            || lower.contains("< ")
        {
            "below"
        } else {
            // "above", "over", "higher than", "≥", ">", or ambiguous → treat as above.
            "above"
        }
    }

    // ---- Main matching ----

    /// Match Kalshi markets against Polymarket markets.
    ///
    /// Returns pairs where both sides can be priced for arbitrage comparison.
    pub fn match_markets(
        &self,
        kalshi_markets: &[KalshiMarket],
        poly_markets: &[PolyMarketInfo],
    ) -> Vec<MatchedPair> {
        let mut pairs: Vec<MatchedPair> = Vec::new();

        for km in kalshi_markets {
            if km.status != "open" {
                continue;
            }

            // Check static override first.
            let static_match = self
                .static_mappings
                .iter()
                .find(|(kt, _)| kt == &km.ticker)
                .and_then(|(_, cid)| poly_markets.iter().find(|pm| &pm.condition_id == cid));

            if let Some(pm) = static_match {
                if let Some(pair) = self.build_pair(km, pm) {
                    pairs.push(pair);
                    continue;
                }
            }

            // Fuzzy: extract strike + direction from Kalshi ticker/title.
            let k_strike = match Self::extract_kalshi_strike(&km.ticker) {
                Some(s) => s,
                None => continue,
            };
            let k_dir = Self::infer_direction(&km.title);

            for pm in poly_markets {
                if pm.closed || !pm.active {
                    continue;
                }

                let p_strike = match Self::extract_poly_strike(&pm.question) {
                    Some(s) => s,
                    None => continue,
                };

                // Strike must be within tolerance.
                let diff_pct = (k_strike - p_strike).abs() / k_strike;
                if diff_pct > self.strike_tolerance {
                    continue;
                }

                // Direction must agree.
                let p_dir = Self::infer_direction(&pm.question);
                if k_dir != p_dir {
                    continue;
                }

                if let Some(pair) = self.build_pair(km, pm) {
                    pairs.push(pair);
                    // Don't match the same Kalshi market to multiple Poly markets;
                    // take the first (best) match.
                    break;
                }
            }
        }

        pairs
    }

    fn build_pair(&self, km: &KalshiMarket, pm: &PolyMarketInfo) -> Option<MatchedPair> {
        // Polymarket must have a YES token to fetch an orderbook.
        let yes_token = pm
            .tokens
            .iter()
            .find(|t| t.outcome.to_lowercase() == "yes")
            .map(|t| t.token_id.clone())?;

        let strike = Self::extract_kalshi_strike(&km.ticker).unwrap_or(0.0);
        let direction = Self::infer_direction(&km.title).to_owned();

        Some(MatchedPair {
            kalshi_ticker: km.ticker.clone(),
            kalshi_title: km.title.clone(),
            poly_yes_token_id: yes_token,
            poly_condition_id: pm.condition_id.clone(),
            poly_question: pm.question.clone(),
            strike_price: strike,
            direction,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_kalshi_strike() {
        assert_eq!(MarketMatcher::extract_kalshi_strike("KXBTC-25DEC3100-T97500"), Some(97500.0));
        assert_eq!(MarketMatcher::extract_kalshi_strike("KXBTC-25MAR1500-T100000"), Some(100000.0));
        assert_eq!(MarketMatcher::extract_kalshi_strike("KXBTC-25JAN-T85000"), Some(85000.0));
        assert_eq!(MarketMatcher::extract_kalshi_strike("KXBTC-NODASH"), None);
    }

    #[test]
    fn test_extract_poly_strike() {
        assert_eq!(
            MarketMatcher::extract_poly_strike("Will BTC be above $97,500 at 3pm?"),
            Some(97500.0)
        );
        assert_eq!(
            MarketMatcher::extract_poly_strike("Bitcoin above $100000 end of week"),
            Some(100000.0)
        );
        // Should ignore tiny dollar amounts
        assert!(MarketMatcher::extract_poly_strike("Pay $5 fee").is_none());
    }

    #[test]
    fn test_infer_direction() {
        assert_eq!(MarketMatcher::infer_direction("Will BTC be above $97,500"), "above");
        assert_eq!(MarketMatcher::infer_direction("Will BTC be below $80,000"), "below");
        assert_eq!(MarketMatcher::infer_direction("BTC under $75k"), "below");
        assert_eq!(MarketMatcher::infer_direction("BTC over $100k by Friday"), "above");
    }
}
