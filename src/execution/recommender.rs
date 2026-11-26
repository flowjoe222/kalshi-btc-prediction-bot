use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A limit-order payload formatted for the Glint execution layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlintOrderPayload {
    pub market_id: String,
    pub token_id: String,
    /// Always `"BUY"` — prediction markets use directional token selection
    /// rather than separate buy/sell sides.
    pub side: String,
    pub price: f64,
    pub size_usdc: f64,
    pub order_type: String,
    pub time_in_force: String,
    pub signal_id: String,
    pub recommendation_id: String,
    pub created_at: f64,
}

impl GlintOrderPayload {
    /// Create a new order payload.
    pub fn new(
        market_id: String,
        token_id: String,
        price: f64,
        size_usdc: f64,
        signal_id: String,
        recommendation_id: String,
    ) -> Self {
        Self {
            market_id,
            token_id,
            side: "BUY".to_owned(),
            price,
            size_usdc,
            order_type: "LIMIT".to_owned(),
            time_in_force: "GTC".to_owned(),
            signal_id,
            recommendation_id,
            created_at: now_secs(),
        }
    }

    /// Serialise the payload to a JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// Write the payload to a file at `path`.
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, self.to_json())?;
        Ok(())
    }
}
