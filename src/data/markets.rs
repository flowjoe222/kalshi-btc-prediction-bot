use crate::config::MarketsConfig;
use crate::signals::detector::Direction;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A Polymarket prediction-market contract for a BTC price target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictionMarket {
    pub condition_id: String,
    pub question: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    /// Strike price in USD.
    pub strike_price: f64,
    /// `"above"` or `"below"`.
    pub direction: String,
    pub expiry_ts: f64,
    pub current_yes_price: f64,
    pub current_no_price: f64,
    pub volume_24h_usdc: f64,
    pub is_active: bool,
}

impl PredictionMarket {
    /// Minutes remaining until this market expires.
    pub fn minutes_to_expiry(&self) -> f64 {
        (self.expiry_ts - now_secs()) / 60.0
    }

    /// Implied probability of the YES outcome (equal to the YES token price
    /// in a well-functioning prediction market).
    pub fn implied_probability(&self) -> f64 {
        self.current_yes_price
    }
}

/// Raw Gamma API response for a single market.
#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId", default)]
    condition_id: String,
    question: Option<String>,
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: Option<Vec<String>>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
    #[serde(rename = "volume24hr", default)]
    volume_24hr: Option<f64>,
    active: Option<bool>,
    closed: Option<bool>,
    #[serde(rename = "outcomePrices", default)]
    outcome_prices: Option<Vec<String>>,
}

/// Discovers and scores relevant BTC prediction markets.
pub struct MarketDiscovery {
    config: MarketsConfig,
    http: reqwest::Client,
    markets: Vec<PredictionMarket>,
}

impl MarketDiscovery {
    pub fn new(config: MarketsConfig, http_client: reqwest::Client) -> Self {
        Self {
            config,
            http: http_client,
            markets: Vec::new(),
        }
    }

    /// Fetch active BTC prediction markets from the Gamma API.
    ///
    /// Searches for markets whose question contains any of `keywords`.
    pub async fn fetch_markets(
        &mut self,
        keywords: &[&str],
        btc_price: Option<f64>,
    ) -> Result<()> {
        let url = "https://gamma-api.polymarket.com/markets";
        let resp = self
            .http
            .get(url)
            .query(&[("active", "true"), ("closed", "false"), ("limit", "200")])
            .send()
            .await;

        let raw: Vec<GammaMarket> = match resp {
            Ok(r) => r.json().await.unwrap_or_default(),
            Err(e) => {
                warn!("Failed to fetch markets from Gamma API: {e}");
                return Ok(());
            }
        };

        debug!("Gamma API returned {} markets", raw.len());

        let mut found: Vec<PredictionMarket> = Vec::new();

        for gm in raw {
            let question = match &gm.question {
                Some(q) if !q.is_empty() => q.clone(),
                _ => continue,
            };

            // Filter by keywords.
            let matches = keywords
                .iter()
                .any(|kw| question.to_lowercase().contains(&kw.to_lowercase()));
            if !matches {
                continue;
            }

            if gm.active != Some(true) || gm.closed == Some(true) {
                continue;
            }

            let tokens = match &gm.clob_token_ids {
                Some(t) if t.len() >= 2 => t.clone(),
                _ => continue,
            };

            // Parse expiry.
            let expiry_ts = match &gm.end_date {
                Some(d) => parse_iso8601(d).unwrap_or(0.0),
                None => 0.0,
            };

            // Parse outcome prices.
            let (yes_price, no_price) = match &gm.outcome_prices {
                Some(p) if p.len() >= 2 => {
                    let y = p[0].parse::<f64>().unwrap_or(0.5);
                    let n = p[1].parse::<f64>().unwrap_or(0.5);
                    (y, n)
                }
                _ => (0.5, 0.5),
            };

            // Attempt to parse a strike price from the question.
            let (strike, dir) = extract_strike_and_direction(&question);

            // Filter by strike distance if we have a BTC price.
            if let (Some(btc), Some(sp)) = (btc_price, strike) {
                if sp > 0.0 {
                    let dist = (sp - btc).abs() / btc;
                    if dist > self.config.max_strike_distance_pct {
                        continue;
                    }
                }
            }

            found.push(PredictionMarket {
                condition_id: gm.condition_id.clone(),
                question,
                yes_token_id: tokens[0].clone(),
                no_token_id: tokens[1].clone(),
                strike_price: strike.unwrap_or(0.0),
                direction: dir,
                expiry_ts,
                current_yes_price: yes_price,
                current_no_price: no_price,
                volume_24h_usdc: gm.volume_24hr.unwrap_or(0.0),
                is_active: true,
            });
        }

        self.markets = found;
        debug!("Retained {} BTC markets after filtering", self.markets.len());
        Ok(())
    }

    /// Return references to all currently active markets.
    pub fn get_active_markets(&self) -> Vec<&PredictionMarket> {
        self.markets.iter().filter(|m| m.is_active).collect()
    }

    /// Return all YES and NO token IDs from active markets for WS subscription.
    pub fn get_all_token_ids(&self) -> Vec<String> {
        self.markets
            .iter()
            .filter(|m| m.is_active)
            .flat_map(|m| vec![m.yes_token_id.clone(), m.no_token_id.clone()])
            .collect()
    }

    /// Score a market for relevance given the current BTC price and signal
    /// direction.  Higher is better.
    pub fn score_market(
        &self,
        market: &PredictionMarket,
        btc_price: Option<f64>,
        direction: &Direction,
    ) -> f64 {
        let mut score = 0.0_f64;
        let mins = market.minutes_to_expiry();

        if mins > 1.0 && mins < 5.0 {
            score += 30.0;
        } else if mins < 15.0 {
            score += 15.0;
        }

        if let Some(btc) = btc_price {
            if market.strike_price > 0.0 {
                let dist = (market.strike_price - btc).abs() / btc;
                if dist > self.config.max_strike_distance_pct {
                    return -1.0; // disqualified
                }
                let proximity = (0.03 - dist) / 0.03 * 30.0;
                score += proximity.max(0.0);
            }
        }

        // Volume score (capped at 20 pts).
        score += (market.volume_24h_usdc / 10_000.0).min(20.0);

        // Direction alignment.
        match direction {
            Direction::Long => {
                score += (1.0 - (market.current_yes_price - 0.5).abs()) * 10.0;
            }
            Direction::Short => {
                score += (1.0 - (market.current_no_price - 0.5).abs()) * 10.0;
            }
            Direction::Neutral => {}
        }

        score
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse an ISO-8601 date/time string into a unix timestamp (seconds).
/// Handles formats like "2024-11-15T14:00:00Z".
fn parse_iso8601(s: &str) -> Option<f64> {
    // Use chrono if available, otherwise do a naive parse.
    use chrono::DateTime;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp() as f64)
        .or_else(|| {
            // Try without timezone suffix.
            let with_z = if s.ends_with('Z') {
                s.to_owned()
            } else {
                format!("{s}Z")
            };
            DateTime::parse_from_rfc3339(&with_z)
                .ok()
                .map(|dt| dt.timestamp() as f64)
        })
}

/// Returns the compiled regex used by `extract_strike_and_direction`.
///
/// Matches optional `$`, digits with optional commas, optional decimal part,
/// and optional trailing `k` — e.g. `$70,000`, `70000`, `70k`.
/// Compiled exactly once for the lifetime of the process.
fn strike_re() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(r"\$?([\d,]+(?:\.\d+)?)k?")
            .expect("strike regex is valid")
    })
}

/// Extract a strike price and direction hint from a market question string.
///
/// Examples:
/// - `"Will BTC be above $70,000 by ..."` → `(Some(70_000.0), "above")`
/// - `"Will BTC close below $65000?"`      → `(Some(65_000.0), "below")`
fn extract_strike_and_direction(question: &str) -> (Option<f64>, String) {
    let lower = question.to_lowercase();
    let direction = if lower.contains("above") || lower.contains("over") {
        "above"
    } else if lower.contains("below") || lower.contains("under") {
        "below"
    } else {
        "above"
    };

    // Use the process-wide static regex — no allocation on repeated calls.
    let strike = strike_re().captures_iter(question).find_map(|cap| {
        let raw = cap[1].replace(',', "");
        raw.parse::<f64>().ok().filter(|&v| v > 1_000.0)
    });

    (strike, direction.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_strike_above() {
        let (strike, dir) = extract_strike_and_direction("Will BTC be above $70,000 by end of day?");
        assert!(strike.is_some());
        assert_eq!(dir, "above");
    }

    #[test]
    fn test_extract_strike_below() {
        let (strike, dir) = extract_strike_and_direction("Will BTC close below $65000?");
        assert!(strike.is_some());
        assert_eq!(dir, "below");
    }
}
