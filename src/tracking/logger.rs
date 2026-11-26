use crate::signals::detector::ImbalanceSignal;
use crate::signals::predictor::TradeRecommendation;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::Write as IoWrite;
use std::path::Path;

/// Writes trade signals and outcomes to CSV and JSONL files.
pub struct TradeLogger {
    csv_path: String,
    jsonl_path: String,
}

impl TradeLogger {
    /// Create a new logger.  The CSV header is written only if the file does
    /// not already exist.
    pub fn new(csv_path: &str, jsonl_path: &str) -> Result<Self> {
        // Ensure parent directories exist.
        if let Some(parent) = Path::new(csv_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        if let Some(parent) = Path::new(jsonl_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Write CSV header if the file is new.
        if !Path::new(csv_path).exists() {
            let mut f = File::create(csv_path)?;
            writeln!(
                f,
                "timestamp,datetime,signal_id,direction,market_question,strike_price,side,\
                 entry_price,suggested_size_usdc,confidence,imbalance_delta,volume_usdc,\
                 btc_at_signal,btc_at_expiry,btc_move_bps,outcome,pnl_usdc,threshold_used,\
                 window_used"
            )?;
        }

        Ok(Self {
            csv_path: csv_path.to_owned(),
            jsonl_path: jsonl_path.to_owned(),
        })
    }

    /// Append the signal/recommendation pair to both files.
    pub fn log_signal(
        &mut self,
        signal: &ImbalanceSignal,
        rec: &TradeRecommendation,
    ) -> Result<()> {
        let dt: DateTime<Utc> = DateTime::from_timestamp(signal.timestamp as i64, 0)
            .unwrap_or_default();

        // CSV row — outcome fields are empty at signal time.
        let mut csv = OpenOptions::new()
            .append(true)
            .open(&self.csv_path)?;
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{:.4},{:.2},{:.4},{:.4},{:.2},{},{},,,,,{:.4},{:.1}",
            signal.timestamp,
            dt.format("%Y-%m-%dT%H:%M:%SZ"),
            signal.signal_id,
            format!("{:?}", signal.direction),
            csv_escape(&rec.market.question),
            format!("{:.2}", rec.market.strike_price),
            rec.side,
            rec.entry_price,
            rec.suggested_size_usdc,
            signal.confidence,
            signal.primary_delta,
            signal.total_volume_usdc,
            rec.btc_price_at_signal
                .map(|p| format!("{p:.2}"))
                .unwrap_or_default(),
            "",                  // btc_at_expiry
            signal.threshold_used,
            signal.window_used,
        )?;

        // JSONL record.
        let obj = json!({
            "event": "signal",
            "timestamp": signal.timestamp,
            "datetime": dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "signal_id": signal.signal_id,
            "direction": format!("{:?}", signal.direction),
            "market_question": rec.market.question,
            "strike_price": rec.market.strike_price,
            "side": rec.side,
            "entry_price": rec.entry_price,
            "suggested_size_usdc": rec.suggested_size_usdc,
            "confidence": signal.confidence,
            "imbalance_delta": signal.primary_delta,
            "volume_usdc": signal.total_volume_usdc,
            "btc_at_signal": rec.btc_price_at_signal,
        });

        let mut jsonl = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.jsonl_path)?;
        writeln!(jsonl, "{}", serde_json::to_string(&obj)?)?;

        Ok(())
    }

    /// Update the CSV and JSONL files with the resolved outcome.
    pub fn log_outcome(&mut self, rec: &TradeRecommendation) -> Result<()> {
        let dt: DateTime<Utc> = DateTime::from_timestamp(rec.timestamp as i64, 0)
            .unwrap_or_default();

        let btc_move_bps = match (rec.btc_price_at_signal, rec.btc_price_at_expiry) {
            (Some(entry), Some(exit)) => (exit - entry) / entry * 10_000.0,
            _ => 0.0,
        };

        // Append outcome row to CSV.
        let mut csv = OpenOptions::new()
            .append(true)
            .open(&self.csv_path)?;
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{:.4},{:.2},{:.4},{:.4},{:.2},{},{},{:.2},{},{:.2},{:.4},{:.1}",
            rec.timestamp,
            dt.format("%Y-%m-%dT%H:%M:%SZ"),
            rec.signal_id,
            format!("{:?}", rec.direction),
            csv_escape(&rec.market.question),
            format!("{:.2}", rec.market.strike_price),
            rec.side,
            rec.entry_price,
            rec.suggested_size_usdc,
            rec.confidence,
            rec.imbalance_delta,
            rec.volume_usdc,
            rec.btc_price_at_signal
                .map(|p| format!("{p:.2}"))
                .unwrap_or_default(),
            rec.btc_price_at_expiry
                .map(|p| format!("{p:.2}"))
                .unwrap_or_default(),
            btc_move_bps,
            rec.outcome.as_deref().unwrap_or(""),
            rec.pnl_usdc.unwrap_or(0.0),
            rec.confidence,  // threshold_used approximation
            10.0_f64,        // window_used approximation
        )?;

        // JSONL record.
        let obj = json!({
            "event": "outcome",
            "timestamp": rec.timestamp,
            "datetime": dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "signal_id": rec.signal_id,
            "recommendation_id": rec.recommendation_id,
            "direction": format!("{:?}", rec.direction),
            "market_question": rec.market.question,
            "strike_price": rec.market.strike_price,
            "side": rec.side,
            "entry_price": rec.entry_price,
            "suggested_size_usdc": rec.suggested_size_usdc,
            "confidence": rec.confidence,
            "imbalance_delta": rec.imbalance_delta,
            "volume_usdc": rec.volume_usdc,
            "btc_at_signal": rec.btc_price_at_signal,
            "btc_at_expiry": rec.btc_price_at_expiry,
            "btc_move_bps": btc_move_bps,
            "outcome": rec.outcome,
            "pnl_usdc": rec.pnl_usdc,
        });

        let mut jsonl = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.jsonl_path)?;
        writeln!(jsonl, "{}", serde_json::to_string(&obj)?)?;

        Ok(())
    }
}

/// Escape a string for safe inclusion in a CSV cell by wrapping it in quotes
/// and escaping any embedded quotes.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}
