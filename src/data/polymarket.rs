use crate::data::orderbook::{BookSnapshot, Level, OrderBookRegistry, Trade, now_secs};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Raw WebSocket event from the Polymarket market feed.
#[derive(Debug, Deserialize)]
struct WsEvent {
    #[serde(rename = "event_type", default)]
    event_type: Option<String>,
    #[serde(default)]
    asset_id: Option<String>,
    #[serde(default)]
    market: Option<String>,
    // book snapshot
    #[serde(default)]
    bids: Option<Vec<RawLevel>>,
    #[serde(default)]
    asks: Option<Vec<RawLevel>>,
    // price change
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    size: Option<String>,
    // trade
    #[serde(default)]
    maker_orders: Option<Vec<Value>>,
}

#[derive(Debug, Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

/// Polymarket WebSocket client with auto-reconnect.
///
/// Token IDs are received via a `watch` channel so the market refresh task
/// can push newly discovered tokens without restarting the WebSocket.
pub struct PolymarketWsClient {
    ws_url: String,
    token_rx: tokio::sync::watch::Receiver<Vec<String>>,
    registry: Arc<RwLock<OrderBookRegistry>>,
    trade_tx: mpsc::Sender<Trade>,
    max_reconnect_delay: u64,
}

impl PolymarketWsClient {
    pub fn new(
        ws_url: String,
        token_rx: tokio::sync::watch::Receiver<Vec<String>>,
        registry: Arc<RwLock<OrderBookRegistry>>,
        trade_tx: mpsc::Sender<Trade>,
    ) -> Self {
        Self {
            ws_url,
            token_rx,
            registry,
            trade_tx,
            max_reconnect_delay: 60,
        }
    }

    /// Run the client until cancelled.  Reconnects with exponential backoff on
    /// any disconnection or error.  Waits for at least one token before connecting.
    pub async fn run(&self) {
        // Wait until we have at least one token to subscribe to.
        let mut rx = self.token_rx.clone();
        loop {
            let tokens = rx.borrow().clone();
            if !tokens.is_empty() {
                break;
            }
            info!("Polymarket WS: waiting for market discovery to provide token IDs...");
            if rx.changed().await.is_err() {
                error!("Polymarket WS: token channel closed before any tokens arrived");
                return;
            }
        }

        let mut delay_secs = 1u64;

        loop {
            info!("Connecting to Polymarket WS: {}", self.ws_url);
            match self.connect_once().await {
                Ok(()) => {
                    info!("Polymarket WS disconnected cleanly; reconnecting");
                    delay_secs = 1;
                }
                Err(e) => {
                    error!("Polymarket WS error: {e}; reconnecting in {delay_secs}s");
                    sleep(Duration::from_secs(delay_secs)).await;
                    delay_secs = (delay_secs * 2).min(self.max_reconnect_delay);
                }
            }
        }
    }

    async fn connect_once(&self) -> Result<()> {
        let current_tokens = self.token_rx.borrow().clone();
        if current_tokens.is_empty() {
            warn!("Polymarket WS: no tokens to subscribe to, skipping connection");
            tokio::time::sleep(Duration::from_secs(10)).await;
            return Ok(());
        }

        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to market channels with currently known tokens.
        let sub_msg = json!([{
            "assets_ids": current_tokens,
            "type": "Market"
        }]);
        write
            .send(Message::Text(sub_msg.to_string()))
            .await?;

        info!("Polymarket WS: subscribed to {} tokens", current_tokens.len());

        let ping_interval = Duration::from_secs(30);
        let mut interval = tokio::time::interval(ping_interval);
        let mut token_rx = self.token_rx.clone();

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_message(&text);
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("Polymarket WS: server closed connection");
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            warn!("Polymarket WS read error: {e}");
                            return Err(e.into());
                        }
                        None => return Ok(()),
                        _ => {}
                    }
                }
                _ = interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        warn!("Polymarket WS ping failed: {e}");
                        return Err(e.into());
                    }
                }
                // Reconnect when the token list changes (new markets discovered).
                Ok(()) = token_rx.changed() => {
                    let new_tokens = token_rx.borrow().clone();
                    if new_tokens != current_tokens {
                        info!("Polymarket WS: token list changed ({} → {}), reconnecting", current_tokens.len(), new_tokens.len());
                        return Ok(());
                    }
                }
            }
        }
    }

    fn handle_message(&self, text: &str) {
        let events: Vec<Value> = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                debug!("Polymarket WS parse error: {e} — raw: {text}");
                return;
            }
        };

        for event in events {
            let ev: WsEvent = match serde_json::from_value(event) {
                Ok(e) => e,
                Err(_) => continue,
            };

            let token_id = match &ev.asset_id {
                Some(id) => id.clone(),
                None => continue,
            };
            let market_id = ev.market.clone().unwrap_or_default();

            match ev.event_type.as_deref() {
                Some("book") => {
                    let bids = ev
                        .bids
                        .unwrap_or_default()
                        .iter()
                        .filter_map(parse_level)
                        .collect::<Vec<_>>();
                    let mut asks = ev
                        .asks
                        .unwrap_or_default()
                        .iter()
                        .filter_map(parse_level)
                        .collect::<Vec<_>>();
                    let mut sorted_bids = bids;
                    sorted_bids.sort_by(|a, b| {
                        b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    asks.sort_by(|a, b| {
                        a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal)
                    });

                    let snap = BookSnapshot {
                        token_id: token_id.clone(),
                        market_id: market_id.clone(),
                        bids: sorted_bids,
                        asks,
                        timestamp: now_secs(),
                    };

                    let mut reg = self.registry.write();
                    reg.get_or_create(&token_id, &market_id)
                        .apply_snapshot(snap);
                }
                Some("price_change") => {
                    let price = ev
                        .price
                        .as_deref()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let size = ev
                        .size
                        .as_deref()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let side = ev.side.as_deref().unwrap_or("bid");

                    let mut reg = self.registry.write();
                    reg.get_or_create(&token_id, &market_id)
                        .apply_change(price, size, side);
                }
                Some("trade") => {
                    if let Some(orders) = ev.maker_orders {
                        for order in orders {
                            let price = order["price"]
                                .as_str()
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(0.0);
                            let size = order["size"]
                                .as_str()
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(0.0);
                            if price <= 0.0 || size <= 0.0 {
                                continue;
                            }

                            let side = order["side"].as_str();
                            let is_buy = {
                                let reg = self.registry.read();
                                if let Some(book) = reg.books.get(&token_id) {
                                    match book.classify_trade(price, side) {
                                        Some(b) => b,
                                        None => continue, // Skip unclassifiable trades
                                    }
                                } else {
                                    // No book data — skip rather than assume buy.
                                    continue;
                                }
                            };

                            let trade = Trade {
                                trade_id: Uuid::new_v4().to_string(),
                                market_id: market_id.clone(),
                                token_id: token_id.clone(),
                                price,
                                size_usdc: size,
                                is_buy_initiated: is_buy,
                                timestamp: now_secs(),
                            };

                            // Send to the central ingestion task — do NOT also write
                            // to the registry here, or every trade gets double-counted.
                            if self.trade_tx.try_send(trade).is_err() {
                                tracing::warn!("Trade channel full — trade dropped");
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

fn parse_level(raw: &RawLevel) -> Option<Level> {
    let price = raw.price.parse::<f64>().ok()?;
    let size = raw.size.parse::<f64>().ok()?;
    Some(Level { price, size })
}
