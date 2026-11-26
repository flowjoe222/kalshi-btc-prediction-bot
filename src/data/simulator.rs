use crate::config::SimulationConfig;
use crate::data::orderbook::{Trade, now_secs};
use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand_distr::{Distribution, LogNormal, Normal};
use std::f64::consts::E;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, sleep};
use tracing::debug;
use uuid::Uuid;

/// GBM-based tick simulator that generates synthetic order-flow data
/// resembling a real prediction market.
pub struct Simulator {
    config: SimulationConfig,
    /// A fake market/token to attach to generated trades.
    market_id: String,
    token_id: String,
}

impl Simulator {
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            config,
            market_id: "sim-market".to_owned(),
            token_id: "sim-token".to_owned(),
        }
    }

    /// Run the simulator indefinitely.
    ///
    /// On each tick, the simulator:
    /// 1. Decides whether a signal window has begun.
    /// 2. Sets a biased buy-fraction for the window.
    /// 3. Generates 2–8 lognormally-sized trades.
    /// 4. Advances the BTC price via GBM.
    /// 5. Publishes the price and all trades.
    pub async fn run(
        &self,
        trade_tx: mpsc::Sender<Trade>,
        price_tx: watch::Sender<f64>,
    ) -> Result<()> {
        let cfg = &self.config;
        let dt = cfg.tick_interval_seconds / (365.0 * 24.0 * 3600.0); // dt in years
        let drift_dt = (cfg.annual_drift - 0.5 * cfg.annual_volatility.powi(2)) * dt;
        let vol_dt = cfg.annual_volatility * dt.sqrt();

        // StdRng is Send (unlike ThreadRng), allowing the future to be spawned.
        let mut rng = StdRng::from_entropy();
        let normal = Normal::new(0.0_f64, 1.0_f64).expect("valid normal");
        let log_normal = LogNormal::new(5.0_f64, 1.0_f64).expect("valid lognormal");

        let mut btc_price = cfg.starting_price;
        let mut ticks_since_signal = 0_u32;
        let mut pending_move: Option<(&'static str, u32)> = None; // (direction, ticks_remaining)
        let mut yes_price = 0.5_f64;

        loop {
            ticks_since_signal += 1;

            // Decide if a signal starts this tick.
            let signal_prob = cfg.signal_quality / 20.0;
            if ticks_since_signal > 15 && rng.gen::<f64>() < signal_prob && pending_move.is_none() {
                let direction = if rng.gen::<bool>() { "up" } else { "down" };
                let duration = rng.gen_range(8..=15);
                pending_move = Some((direction, duration));
                ticks_since_signal = 0;
                debug!("Simulator: signal started — {direction} for {duration} ticks");
            }

            // Update pending move.
            let buy_fraction = if let Some((dir, ref mut remaining)) = pending_move {
                *remaining = remaining.saturating_sub(1);
                let base = if dir == "up" { 0.72 } else { 0.28 };
                let noise = Normal::new(0.0_f64, 0.05_f64).unwrap().sample(&mut rng);
                (base + noise).clamp(0.05, 0.95)
            } else {
                let noise = Normal::new(0.0_f64, 0.08_f64).unwrap().sample(&mut rng);
                (0.50 + noise).clamp(0.05, 0.95)
            };

            // Clear expired signal.
            if let Some((_, 0)) = pending_move {
                pending_move = None;
            }

            // Generate 2-8 trades.
            let n_trades = rng.gen_range(2..=8_usize);
            for i in 0..n_trades {
                let size_raw = log_normal.sample(&mut rng) as f64;
                let size_usdc = size_raw.clamp(10.0, 5000.0);
                let is_buy = rng.gen::<f64>() < buy_fraction;

                // Price around YES token mid-price ± small spread.
                let price_noise = Normal::new(0.0_f64, 0.005_f64).unwrap().sample(&mut rng);
                let trade_price = (yes_price + price_noise).clamp(0.01, 0.99);

                let trade = Trade {
                    trade_id: format!("sim-{}-{i}", Uuid::new_v4()),
                    market_id: self.market_id.clone(),
                    token_id: self.token_id.clone(),
                    price: trade_price,
                    size_usdc,
                    is_buy_initiated: is_buy,
                    timestamp: now_secs(),
                };

                let _ = trade_tx.try_send(trade);
            }

            // Advance BTC price via GBM.
            let flow_bias = (buy_fraction - 0.5) * 0.001; // small directional tilt
            let z = normal.sample(&mut rng);
            btc_price *= E.powf(drift_dt + vol_dt * z + flow_bias);

            // Update YES token price.
            let spread = btc_price - cfg.starting_price;
            let raw_yes = 0.5 + spread / (cfg.starting_price * 0.05);
            yes_price = raw_yes.clamp(0.05, 0.95);

            // Publish BTC price.
            let _ = price_tx.send(btc_price);

            sleep(Duration::from_secs_f64(cfg.tick_interval_seconds)).await;
        }
    }
}
