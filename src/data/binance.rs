use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

/// Coinbase WebSocket match message (real-time trades).
#[derive(Debug, Deserialize)]
struct CoinbaseMatch {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    product_id: Option<String>,
    price: Option<String>,
    time: Option<String>,
}

/// Shared multi-asset price store with expiry.
#[derive(Debug, Default)]
pub struct CryptoPrices {
    prices: HashMap<String, (f64, f64)>,
}

impl CryptoPrices {
    pub fn new() -> Self {
        Self { prices: HashMap::new() }
    }

    pub fn get(&self, symbol: &str) -> Option<f64> {
        self.prices.get(symbol).and_then(|(price, ts)| {
            let age = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64() - ts;
            if (0.0..30.0).contains(&age) && price.is_finite() && *price > 0.0 { Some(*price) } else { None }
        })
    }

    fn set(&mut self, symbol: &str, price: f64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        self.prices.insert(symbol.to_uppercase(), (price, now));
    }
}

/// Real-time crypto price feed via Coinbase WebSocket.
///
/// Streams BTC, ETH, SOL, XRP prices with sub-100ms latency.
/// Falls back to HTTP polling if WebSocket fails.
pub struct BinancePriceClient {
    #[allow(dead_code)]
    ws_url: String,
    price_tx: watch::Sender<f64>,
    pub crypto_prices: Arc<RwLock<CryptoPrices>>,
}

impl BinancePriceClient {
    pub fn new(ws_url: String, price_tx: watch::Sender<f64>) -> Self {
        Self {
            ws_url,
            price_tx,
            crypto_prices: Arc::new(RwLock::new(CryptoPrices::new())),
        }
    }

    pub async fn run(&self) {
        let mut delay_secs = 1u64;

        loop {
            info!("Crypto price feed: connecting to Coinbase WebSocket (real-time)...");
            match self.run_websocket().await {
                Ok(()) => {
                    info!("Coinbase WS disconnected cleanly, reconnecting...");
                    delay_secs = 1;
                }
                Err(e) => {
                    warn!("Coinbase WS error: {e}, falling back to HTTP for {delay_secs}s then retry WS");
                    // Fall back to HTTP polling briefly while WS reconnects.
                    self.poll_http(delay_secs).await;
                    delay_secs = (delay_secs * 2).min(30);
                }
            }
        }
    }

    /// Real-time WebSocket feed from Coinbase Exchange.
    async fn run_websocket(&self) -> Result<()> {
        let url = "wss://ws-feed.exchange.coinbase.com";
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to real-time matches for BTC, ETH, SOL, XRP.
        let subscribe = json!({
            "type": "subscribe",
            "channels": [{
                "name": "matches",
                "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]
            }]
        });
        write.send(Message::Text(subscribe.to_string())).await?;
        info!("Coinbase WS: subscribed to BTC, ETH, SOL, XRP real-time matches");

        let mut tick_count: u64 = 0;

        while let Some(msg) = tokio::time::timeout(Duration::from_secs(30), read.next()).await
            .map_err(|_| anyhow::anyhow!("Coinbase stream idle for 30s"))? {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(m) = serde_json::from_str::<CoinbaseMatch>(&text) {
                        if m.msg_type.as_deref() != Some("match")
                            && m.msg_type.as_deref() != Some("last_match") {
                            continue;
                        }

                        if m.time.as_deref().and_then(|t|chrono::DateTime::parse_from_rfc3339(t).ok())
                            .map_or(true, |t| (chrono::Utc::now().timestamp() - t.timestamp()).abs() > 30) { continue; }

                        let product = match &m.product_id {
                            Some(p) => p.as_str(),
                            None => continue,
                        };
                        let price: f64 = match m.price.as_deref().and_then(|s| s.parse::<f64>().ok()) {
                            Some(p) if p.is_finite() && p > 0.0 => p,
                            _ => continue,
                        };

                        // Map product_id to symbol.
                        let symbol = match product {
                            "BTC-USD" => "BTC",
                            "ETH-USD" => "ETH",
                            "SOL-USD" => "SOL",
                            "XRP-USD" => "XRP",
                            "DOGE-USD" => "DOGE",
                            _ => continue,
                        };

                        self.crypto_prices.write().set(symbol, price);

                        // BTC also goes to the legacy watch channel.
                        if symbol == "BTC" {
                            let _ = self.price_tx.send(price);
                        }

                        tick_count += 1;
                        if tick_count % 500 == 0 {
                            let p = self.crypto_prices.read();
                            info!(
                                "Price feed [WS]: BTC=${:.0} ETH=${:.0} SOL=${:.2} XRP=${:.4} ({} ticks)",
                                p.get("BTC").unwrap_or(0.0),
                                p.get("ETH").unwrap_or(0.0),
                                p.get("SOL").unwrap_or(0.0),
                                p.get("XRP").unwrap_or(0.0),
                                tick_count,
                            );
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("Coinbase WS: server closed connection");
                    return Ok(());
                }
                Err(e) => {
                    warn!("Coinbase WS read error: {e}");
                    return Err(e.into());
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// HTTP polling fallback when WebSocket is down.
    async fn poll_http(&self, seconds: u64) {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);

        while tokio::time::Instant::now() < deadline {
            for (symbol, pair) in &[("BTC", "BTC-USD"), ("ETH", "ETH-USD"), ("SOL", "SOL-USD"), ("XRP", "XRP-USD"), ("DOGE", "DOGE-USD")] {
                if let Ok(resp) = http.get(format!("https://api.coinbase.com/v2/prices/{pair}/spot")).send().await {
                    if let Ok(data) = resp.json::<serde_json::Value>().await {
                        if let Some(price) = data["data"]["amount"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                            self.crypto_prices.write().set(symbol, price);
                            if *symbol == "BTC" {
                                let _ = self.price_tx.send(price);
                            }
                        }
                    }
                }
            }
            sleep(Duration::from_secs(2)).await;
        }
    }
}

#[cfg(test)]
mod freshness_tests {
    use super::*;
    #[test]
    fn stale_future_and_invalid_prices_are_not_tradeable() {
        let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
        for (price,ts) in [(100.0,now-31.0),(100.0,now+60.0),(f64::NAN,now),(0.0,now),(-1.0,now)] {
            let mut p=CryptoPrices::new();p.prices.insert("BTC".into(),(price,ts));assert_eq!(p.get("BTC"),None);
        }
        let mut p=CryptoPrices::new();p.set("BTC",100.0);assert_eq!(p.get("BTC"),Some(100.0));
    }
}
