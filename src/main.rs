mod adaptive;
mod arbitrage;
mod config;
mod dashboard;
mod data;
mod execution;
mod signals;
mod tracking;

use adaptive::learner::AdaptiveLearner;
use arbitrage::ArbitrageScanner;
use config::Config;
use dashboard::routes::{
    FlowHistoryPoint, FvSignal, SharedState, SignalRecord, SsePayload, router,
};
use data::binance::BinancePriceClient;
use data::markets::MarketDiscovery;
use data::orderbook::{OrderBookRegistry, now_secs};
use data::polymarket::PolymarketWsClient;
use data::simulator::Simulator;
use execution::kalshi::KalshiExecutor;
use execution::risk::{OpenPosition, RiskCheckResult, RiskManager};
use parking_lot::RwLock;
use signals::detector::{DetectorParams, Direction, ImbalanceDetector};
use signals::orderflow::OrderFlowCalculator;
use signals::predictor::SignalPredictor;
use tracking::logger::TradeLogger;
use tracking::performance::PerformanceTracker;

use anyhow::Result;
use clap::Parser;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tracing::{debug, error, info, warn};

/// Bitcoin prediction bot — Rust edition.
#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Run in simulation (test) mode rather than connecting to live WebSockets.
    #[arg(long, default_value_t = false)]
    test_mode: bool,

    /// Tracing log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Verify one 1-cent demo order and immediately cancel it, then exit.
    #[arg(long, default_value_t = false)]
    demo_smoke: bool,

    /// Attempt one bounded demo fill through the durable execution coordinator.
    #[arg(long, default_value_t = false)]
    demo_fill_probe: bool,

    /// Path to the YAML configuration file.
    #[arg(long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialise structured logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.as_str().into()),
        )
        .init();

    info!("Starting Bitcoin Prediction Bot (Rust edition)");

    // Load .env if present (Kalshi credentials, etc.). Path is overrideable
    // so deployments can point at a different file (e.g. systemd EnvironmentFile).
    let env_path = std::env::var("KALSHI_ENV_PATH").unwrap_or_else(|_| ".env".to_string());
    match dotenvy::from_path(&env_path) {
        Ok(()) => info!("Loaded environment from {env_path}"),
        Err(e) => info!("No .env loaded from {env_path} ({e}); relying on shell env"),
    }

    // This working copy is restricted to the requested demo deployment.
    if std::env::var("KALSHI_MODE").unwrap_or_else(|_| "demo".into()) != "demo" {
        anyhow::bail!("This deployment only supports KALSHI_MODE=demo");
    }

    // ---- Configuration ----
    let cfg = Config::load_or_default(&args.config);
    let test_mode = args.test_mode || cfg.simulation.test_mode;
    info!("Mode: {}", if test_mode { "SIMULATION" } else { "DEMO EXCHANGE" });

    // Fail fast in LIVE mode if no Kalshi API key is configured. Continuing
    // silently used to mean "fall back to unauth requests that 401 forever";
    // panicking with a clear message saves an hour of head-scratching.
    if !test_mode && std::env::var("KALSHI_API_KEY").ok().filter(|s| !s.is_empty()).is_none() {
        panic!(
            "KALSHI_API_KEY is not set in env after loading {env_path}. \
             Add it to {env_path} or export it before launching in LIVE mode. \
             Refusing to continue."
        );
    }

    // Ensure logs directory exists.
    std::fs::create_dir_all("logs").ok();

    // ---- Core components ----
    let registry: Arc<RwLock<OrderBookRegistry>> =
        Arc::new(RwLock::new(OrderBookRegistry::new()));

    let calc = Arc::new(OrderFlowCalculator::new(
        Arc::clone(&registry),
        cfg.signal.large_trade_threshold_usdc,
        cfg.signal.large_trade_multiplier,
    ));

    let initial_params: DetectorParams = cfg.signal.clone().into();
    let params_arc = Arc::new(Mutex::new(initial_params.clone()));

    let adaptive_cfg: adaptive::AdaptiveConfig = cfg.adaptive.clone().into();
    let mut learner = AdaptiveLearner::new(adaptive_cfg, Arc::clone(&params_arc));

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let markets = Arc::new(Mutex::new(MarketDiscovery::new(
        cfg.markets.clone(),
        http.clone(),
    )));

    // BTC price — shared via watch channel.
    let (price_tx, price_rx) = watch::channel(cfg.simulation.starting_price);
    let btc_price_arc: Arc<Mutex<Option<f64>>> = Arc::new(Mutex::new(None));

    // Trade channel (simulator or Polymarket → registry).
    let (trade_tx, mut trade_rx) = mpsc::channel::<data::orderbook::Trade>(4096);

    // SSE broadcast channel.
    let (sse_tx, _) = broadcast::channel::<SsePayload>(200);

    // Shared dashboard state.
    let shared_state = Arc::new(RwLock::new(SharedState::new(
        if test_mode {
            "simulation".to_owned()
        } else {
            "live".to_owned()
        },
        initial_params.clone(),
    )));

    // ---- Kalshi executor (optional — requires credentials in env) ----
    let kalshi_max_cents = cfg.kalshi.max_position_cents;
    let risk_manager = Arc::new(Mutex::new(RiskManager::new(cfg.kalshi.clone().into())));
    let kalshi_executor: Option<Arc<KalshiExecutor>> = if test_mode { None } else { match KalshiExecutor::from_env() {
        Ok(exec) => {
            if !test_mode && exec.is_paper() {
                anyhow::bail!("Demo startup requires KALSHI_PAPER=0; simulated fills are not demo exchange fills");
            }
            let initial_balance = if !test_mode { Some(exec.warm_up().await?) } else { None };
            let mode_str = if exec.is_demo() { "demo" } else { "live" };
            {
                let mut s = shared_state.write();
                s.kalshi_enabled = true;
                s.kalshi_balance_cents = initial_balance;
                s.kalshi_mode = mode_str.to_owned();
            }
            info!("Kalshi executor ready ({} mode)", mode_str.to_uppercase());
            Some(Arc::new(exec))
        }
        Err(e) => {
            if !test_mode { return Err(e); }
            info!("Kalshi execution disabled: {e}");
            None
        }
    }};

    if !test_mode {
        return execution::runtime::run(cfg, kalshi_executor.unwrap(), shared_state, args.demo_smoke, args.demo_fill_probe).await;
    }

    // ---- Signal processing components ----
    let predictor = Arc::new(Mutex::new(SignalPredictor::new(
        Arc::clone(&markets),
        Arc::clone(&btc_price_arc),
        50.0, // suggested_size_usdc
    )));
    {
        let mut p = predictor.lock().await;
        p.set_min_move_bps(cfg.resolution.min_move_bps);
        p.set_check_after_seconds(cfg.resolution.check_after_seconds);
    }

    let mut detector = {
        let params = params_arc.lock().await.clone();
        ImbalanceDetector::new(params, Arc::clone(&calc))
    };

    let performance = Arc::new(Mutex::new(PerformanceTracker::new()));

    // Wrap in Arc<Mutex> so both the signal loop and resolution loop can share it.
    let logger = Arc::new(Mutex::new(TradeLogger::new(
        &cfg.logging.trade_log_csv,
        &cfg.logging.signal_log_jsonl,
    )?));

    // Shared crypto price store — populated by the live price client.
    let mut shared_crypto_prices = Arc::new(parking_lot::RwLock::new(data::binance::CryptoPrices::new()));

    // ---- Spawn data source tasks ----
    if test_mode {
        let sim = Simulator::new(cfg.simulation.clone());
        let trade_tx2 = trade_tx.clone();
        let price_tx2 = price_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = sim.run(trade_tx2, price_tx2).await {
                error!("Simulator error: {e}");
            }
        });
        info!("Simulator started");
    } else {
        // Polymarket WebSocket — subscribe to tokens via watch channel.
        let registry2 = Arc::clone(&registry);
        let trade_tx2 = trade_tx.clone();
        let pm_url = cfg.polymarket.clob_ws_url.clone();
        let (_token_tx, token_rx) = watch::channel::<Vec<String>>(vec![]);
        tokio::spawn(async move {
            let client = PolymarketWsClient::new(pm_url, token_rx, registry2, trade_tx2);
            client.run().await;
        });

        // Binance price feed.
        let bn_url = cfg.binance.ws_url.clone();
        let price_tx2 = price_tx.clone();
        let price_client = BinancePriceClient::new(bn_url, price_tx2);
        shared_crypto_prices = price_client.crypto_prices.clone();
        tokio::spawn(async move {
            price_client.run().await;
        });

        info!("Live WebSocket clients started");
    }

    // ---- Trade ingestion task ----
    // Receives trades from the channel and inserts them into the registry.
    {
        let registry2 = Arc::clone(&registry);
        tokio::spawn(async move {
            while let Some(trade) = trade_rx.recv().await {
                let mut reg = registry2.write();
                reg.get_or_create(&trade.token_id, &trade.market_id)
                    .add_trade(trade);
            }
        });
    }

    // ---- BTC price sync task ----
    // Keeps `btc_price_arc` in sync with the watch channel.
    {
        let btc_arc = Arc::clone(&btc_price_arc);
        let mut rx = price_rx.clone();
        tokio::spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let price = *rx.borrow();
                *btc_arc.lock().await = Some(price);
            }
        });
    }

    // ---- Market refresh task ----
    {
        let markets2 = Arc::clone(&markets);
        let btc_arc2 = Arc::clone(&btc_price_arc);
        let keywords: Vec<String> = cfg.markets.search_keywords.clone();
        let interval_secs = cfg.polymarket.market_refresh_interval;
        tokio::spawn(async move {
            loop {
                let btc = *btc_arc2.lock().await;
                let kw_refs: Vec<&str> = keywords.iter().map(|s| s.as_str()).collect();
                let mut mg = markets2.lock().await;
                if let Err(e) = mg.fetch_markets(&kw_refs, btc).await {
                    warn!("Market refresh error: {e}");
                }
                drop(mg);
                tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            }
        });
    }

    // ---- State update task ----
    // Periodically syncs flow metrics and BTC price into SharedState.
    {
        let shared2 = Arc::clone(&shared_state);
        let calc2 = Arc::clone(&calc);
        let btc_arc2 = Arc::clone(&btc_price_arc);
        let markets2 = Arc::clone(&markets);
        let params2 = Arc::clone(&params_arc);
        let sse_tx2 = sse_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                let flow = calc2.compute(10.0);
                let btc = *btc_arc2.lock().await;
                let params_snap = params2.lock().await.clone();
                let active_markets = markets2.lock().await.get_active_markets().len();

                {
                    let mut s = shared2.write();
                    s.status = "running".to_owned();
                    s.btc_price = btc;
                    s.flow_delta = flow.delta();
                    s.flow_volume = flow.total_volume;
                    s.flow_buy_pct = flow.buy_fraction();
                    s.current_params = params_snap;
                    s.active_markets_count = active_markets;
                    s.push_flow_point(FlowHistoryPoint {
                        timestamp: now_secs(),
                        delta: flow.delta(),
                        volume: flow.total_volume,
                        buy_pct: flow.buy_fraction(),
                    });
                }

                // Heartbeat SSE event.
                let _ = sse_tx2.send(SsePayload::Heartbeat {
                    timestamp: now_secs(),
                    btc_price: btc,
                    flow_delta: flow.delta(),
                });
            }
        });
    }

    // ---- Dashboard server ----
    {
        let shared2 = Arc::clone(&shared_state);
        let sse_tx2 = sse_tx.clone();
        let host = cfg.dashboard.host.clone();
        let port = std::env::var("DASHBOARD_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(cfg.dashboard.port);

        // Read auth + CORS configuration from env. Missing token = dev mode (warned).
        let dashboard_token = std::env::var("DASHBOARD_TOKEN").ok();
        if dashboard_token.is_none() {
            warn!(
                "DASHBOARD_TOKEN is not set — dashboard /api/* endpoints are UNAUTHENTICATED. \
                 Set DASHBOARD_TOKEN in .env for production use."
            );
        }
        let dashboard_origin = std::env::var("DASHBOARD_ORIGIN")
            .unwrap_or_else(|_| "http://localhost:3000".to_string());

        tokio::spawn(async move {
            // Restrict CORS to the configured Next.js dashboard origin.
            let cors = match dashboard_origin.parse::<axum::http::HeaderValue>() {
                Ok(origin) => tower_http::cors::CorsLayer::new()
                    .allow_origin(origin)
                    .allow_methods([
                        axum::http::Method::GET,
                        axum::http::Method::POST,
                        axum::http::Method::PUT,
                        axum::http::Method::OPTIONS,
                    ])
                    .allow_headers([
                        axum::http::header::CONTENT_TYPE,
                        axum::http::header::AUTHORIZATION,
                    ]),
                Err(e) => {
                    warn!(
                        "Invalid DASHBOARD_ORIGIN {dashboard_origin:?}: {e} — \
                         falling back to no CORS (same-origin only)"
                    );
                    tower_http::cors::CorsLayer::new()
                }
            };

            // Build the router and apply the bearer-token auth middleware.
            let app = dashboard::auth::with_auth(
                router(shared2, sse_tx2),
                dashboard_token,
            )
            .layer(cors);

            // Don't unwrap on parse — log and bail so a misconfigured host
            // can't panic-kill this background task.
            let addr: SocketAddr = match format!("{host}:{port}").parse() {
                Ok(a) => a,
                Err(e) => {
                    error!(
                        "Dashboard bind addr parse failed for {host}:{port}: {e} — \
                         dashboard disabled"
                    );
                    return;
                }
            };
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!("Dashboard listening on http://{addr}");
                    if let Err(e) = axum::serve(listener, app).await {
                        warn!("Dashboard server error: {e}");
                    }
                }
                Err(e) => {
                    warn!("Dashboard bind failed on {addr}: {e} — trading continues without dashboard");
                }
            }
        });
    }

    // ---- Kalshi background state refresh (every 30 s) ----
    if let Some(exec) = kalshi_executor.clone() {
        let shared_k = Arc::clone(&shared_state);
        let risk_k = Arc::clone(&risk_manager);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;

                // Fetch all values asynchronously first (no locks held).
                let balance = exec.get_balance().await.ok();
                let positions = exec.get_positions().await.ok();
                let markets = exec.list_btc_markets().await.ok();
                let daily_pnl = risk_k.lock().await.daily_pnl_cents();
                let rm_pos_count = risk_k.lock().await.position_count();

                // Warn on low balance before writing to shared state.
                if let Some(bal) = balance {
                    KalshiExecutor::warn_low_balance(bal, daily_pnl);
                }

                // Update SharedState without holding any async lock.
                {
                    let mut s = shared_k.write();
                    if let Some(bal) = balance {
                        s.kalshi_balance_cents = Some(bal);
                    }
                    if let Some(pos) = positions {
                        s.kalshi_open_position_count = pos.len().max(rm_pos_count);
                        s.kalshi_positions = pos;
                    }
                    if let Some(mkts) = markets {
                        s.kalshi_btc_markets = mkts;
                    }
                    s.kalshi_daily_pnl_cents = daily_pnl;
                }
            }
        });
    }

    // ---- Arbitrage scan task ----
    // Tries to connect to Kalshi using env credentials; gracefully degrades if
    // credentials are absent (Kalshi side is skipped but scanner still runs).
    {
        let shared2 = Arc::clone(&shared_state);
        let http2 = http.clone();
        let arb_cfg: arbitrage::ArbitrageConfig = cfg.arbitrage.clone().into();
        let poly_base = cfg.polymarket.clob_rest_url.clone();
        let scan_interval = arb_cfg.scan_interval_secs;
        let auto_execute = arb_cfg.auto_execute && !test_mode;

        let kalshi_arc: Option<Arc<KalshiExecutor>> = kalshi_executor.clone();

        tokio::spawn(async move {
            let scanner = ArbitrageScanner::new(http2, kalshi_arc.clone(), arb_cfg, poly_base);
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(scan_interval));

            loop {
                ticker.tick().await;

                let mut opportunities = scanner.scan().await;

                // DISABLED: arb auto-execution was placing uncontrolled trades.
                // Only the direct trading loop should place orders.
                let auto_execute = false;
                if auto_execute {
                    if let Some(ref kalshi) = kalshi_arc {
                        for opp in &mut opportunities {
                            if opp.recommended_action
                                == arbitrage::RecommendedAction::BuyKalshiSellPoly
                            {
                                // Buy YES on Kalshi at the ask price.
                                let price_cents =
                                    (opp.kalshi_ask * 100.0).round() as i64;
                                let max_cents =
                                    (scanner.config.max_position_size_usd * 100.0) as i64;
                                let count =
                                    KalshiExecutor::size_from_confidence(
                                        opp.gap_size.min(1.0),
                                        price_cents,
                                        max_cents,
                                    );
                                match kalshi
                                    .place_order(
                                        &opp.kalshi_ticker,
                                        "buy",
                                        "yes",
                                        price_cents,
                                        count,
                                    )
                                    .await
                                {
                                    Ok(order) => {
                                        info!(
                                            "Arb auto-executed: {} ×{} @ {}¢ (order {})",
                                            opp.kalshi_ticker,
                                            count,
                                            price_cents,
                                            order.order_id
                                        );
                                        opp.auto_executed = true;
                                    }
                                    Err(e) => {
                                        warn!("Arb auto-execute failed: {e}");
                                    }
                                }
                            }
                        }
                    }
                }

                if !opportunities.is_empty() {
                    info!(
                        "Arbitrage scan: {} opportunities (top gap={:.3})",
                        opportunities.len(),
                        opportunities[0].gap_size
                    );
                    scanner.log_to_csv(&opportunities);
                }

                shared2.write().update_arbitrage(opportunities);
            }
        });
    }

    // Shared cross-bot ticker lock: prevents two bots from entering the same contract.
    let reserved_tickers: Arc<Mutex<std::collections::HashSet<String>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));

    // GLOBAL ENTRY GATE: only 1 trade per 15-min window across ALL bots.
    // This fixes the win-lose-win-lose pattern where 1 wins and 1-2 expire.
    // The BEST setup claims the window. Everything else skips.
    let global_window_entry: Arc<Mutex<(u64, bool)>> = // (window_id, has_entry)
        Arc::new(Mutex::new((0, false)));

    // ---- Direct trading loop (simple profitable strategy) ----
    // Scans Kalshi 15-min contracts every 5 seconds.
    // Above strike → YES. Below strike → NO. Max 50¢.
    if let Some(exec_direct) = kalshi_executor.clone() {
        let risk_direct = Arc::clone(&risk_manager);
        let shared_direct = Arc::clone(&shared_state);
        let btc_direct = Arc::clone(&btc_price_arc);
        let reserved_direct = Arc::clone(&reserved_tickers);
        let global_gate_hc = Arc::clone(&global_window_entry);
        // Get crypto prices for all assets (BTC uses watch channel, others use this).
        let crypto_prices_direct = shared_crypto_prices.clone();
        tokio::spawn(async move {
            let mut scan_count: u64 = 0;
            let mut last_trade_ticker: Option<String> = None;
            // Track tickers we've already traded (including scalped out).
            // Don't re-enter a contract after taking profit on it.
            let mut traded_tickers: std::collections::HashSet<String> = std::collections::HashSet::new();
            // Max 2 assets per contract window. Prevents blowing bankroll on 4 simultaneous entries.
            let mut assets_this_window: u32 = 0;
            let mut last_window_clear: u64 = 0;
            // Track contract prices over time to detect dips.
            // Key: ticker, Value: (lowest_price_seen, last_price, scan_count_when_first_seen)
            let mut contract_prices: std::collections::HashMap<String, (i64, i64, u64)> = std::collections::HashMap::new();

            // ---- LIVE LEARNING ----
            // The bot gets smarter with every trade. Tracks what works.
            let mut max_entry: i64 = 45; // Opt#3: data flipped — expensive 77% vs cheap 67%. Allow 45¢.
            let mut consecutive_losses: i32 = 0;
            let mut total_wins: u32 = 0;
            let mut total_losses: u32 = 0;
            let mut last_known_balance: i64 = shared_direct.read()
                .kalshi_balance_cents.unwrap_or(2152);
            let mut avg_winning_entry: i64 = 40; // Tracks what entry prices win.
            let mut avg_losing_entry: i64 = 50; // Tracks what entry prices lose.
            let mut best_scalp_target: i64 = 15; // Adjusts based on actual fills.

            // ---- Per-Asset Direction Prediction Engine ----
            use std::collections::HashMap as PredMap;

            // Multi-timeframe trend analyzer.
            // Tracks 1-min, 5-min, and 15-min EMAs to confirm direction
            // across ALL timeframes before entering a trade.
            struct AssetPredictor {
                prices: std::collections::VecDeque<f64>,
                // 1-minute EMAs (fast signals, entry timing)
                ema_20: Option<f64>,    // ~20 seconds
                ema_60: Option<f64>,    // ~1 minute
                // 5-minute EMAs (medium trend, must agree with trade)
                ema_300: Option<f64>,   // ~5 minutes
                // 15-minute EMA (session trend, strongest signal)
                ema_900: Option<f64>,   // ~15 minutes
            }
            impl AssetPredictor {
                fn new() -> Self {
                    Self {
                        prices: std::collections::VecDeque::new(),
                        ema_20: None, ema_60: None, ema_300: None, ema_900: None,
                    }
                }
                fn update(&mut self, price: f64) {
                    self.prices.push_back(price);
                    if self.prices.len() > 1200 { self.prices.pop_front(); } // Keep 20 min of data
                    let a20 = 2.0 / 21.0;
                    let a60 = 2.0 / 61.0;
                    let a300 = 2.0 / 301.0;
                    let a900 = 2.0 / 901.0;
                    self.ema_20 = Some(match self.ema_20 { Some(p) => p + a20 * (price - p), None => price });
                    self.ema_60 = Some(match self.ema_60 { Some(p) => p + a60 * (price - p), None => price });
                    self.ema_300 = Some(match self.ema_300 { Some(p) => p + a300 * (price - p), None => price });
                    self.ema_900 = Some(match self.ema_900 { Some(p) => p + a900 * (price - p), None => price });
                }

                /// 1-minute trend: fast EMA vs 1-min EMA. +1 bull, -1 bear, 0 neutral.
                fn trend_1m(&self) -> i32 {
                    let ef = self.ema_20.unwrap_or(0.0);
                    let es = self.ema_60.unwrap_or(0.0);
                    if ef <= 0.0 || es <= 0.0 { return 0; }
                    let spread = (ef - es) / es;
                    if spread > 0.0001 { 1 } else if spread < -0.0001 { -1 } else { 0 }
                }

                /// 5-minute trend: price vs 5-min EMA. +1 bull, -1 bear, 0 neutral.
                fn trend_5m(&self) -> i32 {
                    if self.prices.len() < 30 { return 0; } // Fast: 1 min warmup
                    // Defensive: skip rather than panic if `prices` is empty.
                    let Some(&price) = self.prices.back() else { return 0; };
                    let e300 = self.ema_300.unwrap_or(0.0);
                    if e300 <= 0.0 { return 0; }
                    let spread = (price - e300) / e300;
                    if spread > 0.0002 { 1 } else if spread < -0.0002 { -1 } else { 0 }
                }

                /// 15-minute trend: 5-min EMA vs 15-min EMA. +1 bull, -1 bear, 0 neutral.
                fn trend_15m(&self) -> i32 {
                    let e300 = self.ema_300.unwrap_or(0.0);
                    let e900 = self.ema_900.unwrap_or(0.0);
                    if e300 <= 0.0 || e900 <= 0.0 { return 0; }
                    let spread = (e300 - e900) / e900;
                    if spread > 0.0001 { 1 } else if spread < -0.0001 { -1 } else { 0 }
                }

                /// Multi-timeframe prediction.
                ///
                /// Hierarchy:
                ///   1-min = OBSERVER — sees the action, used for timing only
                ///   5-min = THE SIGNAL — must be strictly directional to enter
                ///   15-min = THE SETUP — if agrees with 5m, max conviction
                ///
                /// Returns +1 (bull) or -1 (bear) ONLY when 5-min confirms.
                fn predict(&self) -> i32 {
                    if self.prices.len() < 60 { return 0; }
                    let t5 = self.trend_5m();

                    // 5-min is THE gate. If flat, no trade.
                    if t5 == 0 { return 0; }

                    t5 // +1 bull, -1 bear — direction is set by 5-min
                }

                /// Conviction level: how many timeframes agree.
                /// 3 = all agree (1m + 5m + 15m), 2 = 5m + one other, 1 = only 5m
                fn conviction(&self) -> i32 {
                    let t1 = self.trend_1m();
                    let t5 = self.trend_5m();
                    let t15 = self.trend_15m();
                    if t5 == 0 { return 0; }
                    let mut c = 1; // 5m confirms = base conviction
                    if t1 == t5 { c += 1; } // 1m agrees = timing confirmation
                    if t15 == t5 { c += 1; } // 15m agrees = setup confirmation
                    c
                }

                /// Is this a high-conviction setup? (5m + 15m agree)
                fn is_high_conviction(&self) -> bool {
                    let t5 = self.trend_5m();
                    let t15 = self.trend_15m();
                    t5 != 0 && t5 == t15
                }

                /// PREDICT WHERE PRICE WILL BE at contract expiry.
                /// Uses momentum projection + EMA alignment + distance from strike.
                /// Returns (side, confidence):
                ///   side: "yes" if price predicted above strike, "no" if below
                ///   confidence: 0.0-1.0 (only trade at 0.55+)
                fn predict_at_expiry(&self, strike: f64) -> (i32, f64) {
                    if self.prices.len() < 150 { return (0, 0.0); } // 5-min warmup
                    // Defensive: skip rather than panic if `prices` is empty.
                    let Some(&price) = self.prices.back() else { return (0, 0.0); };
                    let e20 = self.ema_20.unwrap_or(price);
                    let e60 = self.ema_60.unwrap_or(price);
                    let e300 = self.ema_300.unwrap_or(price);
                    let e900 = self.ema_900.unwrap_or(price);

                    let mut score: f64 = 0.0;

                    // 1. MOMENTUM PROJECTION: where will price be in 15 min?
                    //    Use 1-min momentum (60 ticks) and project forward.
                    //    Dampen by 0.5 (momentum decays over time).
                    if self.prices.len() >= 60 {
                        let old = self.prices[self.prices.len() - 60];
                        let mom_1m = (price - old) / old; // 1-min change as %
                        // Project: dampened 1.5× (was 7.5× which gave 100% confidence on 50/50 contracts)
                        let projected = price * (1.0 + mom_1m * 1.5);
                        let proj_dist = (projected - strike) / strike;
                        score += proj_dist * 2000.0; // Strong weight on projection
                    }

                    // 2. CURRENT DISTANCE from strike (the #1 signal)
                    let distance = (price - strike) / strike;
                    score += distance * 3000.0; // Already above/below = big advantage

                    // 3. EMA ALIGNMENT (1-min confirmation)
                    //    All EMAs stacked = strong trend that will continue.
                    if price > e20 && e20 > e60 { score += 0.5; }         // 1-min bullish
                    else if price < e20 && e20 < e60 { score -= 0.5; }    // 1-min bearish

                    // 4. 15-MIN SETUP (longer trend context)
                    if e300 > e900 { score += 0.3; }                       // 15-min bullish
                    else if e300 < e900 { score -= 0.3; }                  // 15-min bearish

                    // 5. 5-MIN as secondary confirmation
                    if price > e300 { score += 0.2; }
                    else if price < e300 { score -= 0.2; }

                    // 6. TICK DIRECTION (are recent ticks confirming?)
                    if self.prices.len() >= 20 {
                        let recent: Vec<&f64> = self.prices.iter().rev().take(20).collect();
                        let ups = recent.windows(2).filter(|w| w[0] > w[1]).count();
                        let tick_bias = (ups as f64 - 10.0) / 10.0; // -1.0 to +1.0
                        score += tick_bias * 0.3;
                    }

                    // Direction and confidence
                    let direction = if score > 0.0 { 1 } else if score < 0.0 { -1 } else { 0 };
                    let confidence = (score.abs() / 3.0).min(1.0); // Normalize to 0-1

                    (direction, confidence)
                }

                /// Raw score for bots that need finer granularity.
                fn score(&self) -> i32 {
                    if self.prices.len() < 30 { return 0; }
                    let t1 = self.trend_1m();
                    let t5 = self.trend_5m();
                    t1 + t5 * 2 + self.trend_15m()
                }
            }

            let mut predictors: PredMap<String, AssetPredictor> = PredMap::new();
            for asset in &["BTC", "ETH", "SOL", "XRP", "DOGE"] {
                predictors.insert(asset.to_string(), AssetPredictor::new());
            }
            let max_history = 600;
            // Track trend consistency — only trade when trend holds for 30+ seconds.
            let mut trend_hold_count: u32 = 0;
            let mut last_trend_direction: i32 = 0;

            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                scan_count += 1;

                // Fetch markets every 2 seconds (fast but under rate limit).
                if false { // Scan every tick — market cache prevents API spam
                    continue;
                }

                // Log balance snapshot every 60 seconds for dashboard charts.
                if scan_count % 60 == 0 {
                    let bal = shared_direct.read().kalshi_balance_cents.unwrap_or(0);
                    let pos = risk_direct.lock().await.position_count();
                    use std::io::Write;
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true).append(true).open("logs/balance_history.csv")
                    {
                        if f.metadata().map(|m| m.len() == 0).unwrap_or(true) {
                            let _ = writeln!(f, "timestamp,balance_cents,open_positions");
                        }
                        let _ = writeln!(f, "{},{},{}", chrono::Utc::now().to_rfc3339(), bal, pos);
                    }
                }

                // Auto-reconcile every 60 seconds: check Kalshi for actual positions
                // and clear any stale ones from risk manager so we don't get stuck.
                // SKIP in paper mode: Kalshi holds no real positions there, so this
                // would falsely "settle" every simulated position as EXPIRED.
                if !exec_direct.is_paper() && scan_count % 60 == 0 {
                    match exec_direct.get_positions().await {
                        Ok(live_pos) => {
                            let tracked_list = risk_direct.lock().await.open_positions().to_vec();
                            let now = data::orderbook::now_secs();
                            let mut cleared = 0;

                            for pos in &tracked_list {
                                // Check if this specific ticker still has an open position on Kalshi.
                                let still_open = live_pos.iter()
                                    .any(|lp| lp.ticker == pos.ticker && lp.position != 0.0);

                                if !still_open {
                                    // This position settled — close it in risk manager.
                                    let pnl = live_pos.iter()
                                        .find(|lp| lp.ticker == pos.ticker)
                                        .and_then(|lp| lp.realized_pnl)
                                        .unwrap_or(0);
                                    risk_direct.lock().await.record_close(&pos.order_id, pnl, now);
                                    cleared += 1;

                                    // ---- AUTO-LEARN: analyze expired positions ----
                                    // If no take-profit exit happened, this position expired.
                                    // Log what went wrong so the bot can learn.
                                    let age_secs = now - pos.opened_at;
                                    if pnl <= 0 {
                                        warn!(
                                            "TRADE-REVIEW: EXPIRED {} {} entry={}¢ cost={}¢ age={:.0}s signal={} — NEVER HIT TARGET",
                                            pos.ticker, pos.side, pos.price_cents, pos.cost_cents,
                                            age_secs, pos.signal_id
                                        );
                                        // Log to trade journal
                                        {
                                            use std::io::Write;
                                            let path = "logs/trade_journal.csv";
                                            let needs_header = !std::path::Path::new(path).exists()
                                                || std::fs::metadata(path).map(|m| m.len() == 0).unwrap_or(true);
                                            if let Ok(mut f) = std::fs::OpenOptions::new()
                                                .create(true).append(true).open(path)
                                            {
                                                if needs_header {
                                                    let _ = writeln!(f, "timestamp,ticker,side,result,entry_cents,exit_cents,profit_cents,peak_cents,contracts,signal_id");
                                                }
                                                let _ = writeln!(f, "{},{},{},EXPIRED,{},0,-{},0,{},{}",
                                                    chrono::Utc::now().to_rfc3339(),
                                                    pos.ticker, pos.side, pos.price_cents,
                                                    pos.cost_cents, pos.count, pos.signal_id
                                                );
                                            }
                                        }
                                    } else {
                                        info!(
                                            "TRADE-REVIEW: SETTLED-WIN {} {} entry={}¢ pnl=+{}¢ signal={}",
                                            pos.ticker, pos.side, pos.price_cents, pnl, pos.signal_id
                                        );
                                    }
                                }
                            }

                            if cleared > 0 {
                                // ---- LIVE LEARNING: gets smarter every trade ----
                                let new_balance = shared_direct.read()
                                    .kalshi_balance_cents.unwrap_or(0);
                                let balance_change = new_balance - last_known_balance;
                                let total = total_wins + total_losses;
                                let win_rate = if total > 0 { total_wins as f64 / total as f64 * 100.0 } else { 0.0 };

                                if balance_change > 0 {
                                    total_wins += cleared as u32;
                                    consecutive_losses = 0;
                                    // Winning — loosen slightly to trade more.
                                    max_entry = (max_entry + 1).min(50);
                                    info!(
                                        "LEARN: WIN +{}¢ | W:{} L:{} ({:.0}%) | max_entry→{}¢ | bal=${:.2}",
                                        balance_change, total_wins, total_losses,
                                        win_rate, max_entry, new_balance as f64 / 100.0
                                    );
                                } else if balance_change < 0 {
                                    total_losses += cleared as u32;
                                    consecutive_losses += cleared as i32;
                                    // Losing — tighten to avoid bad entries.
                                    max_entry = (max_entry - 3).max(40);
                                    info!(
                                        "LEARN: LOSS {}¢ | W:{} L:{} ({:.0}%) streak:{} | max_entry→{}¢ | bal=${:.2}",
                                        balance_change, total_wins, total_losses,
                                        win_rate, consecutive_losses, max_entry,
                                        new_balance as f64 / 100.0
                                    );

                                    // 3 consecutive losses = pause 3 minutes.
                                    if consecutive_losses >= 3 {
                                        warn!("LEARN: 3 losses in a row — pausing 3 min");
                                        tokio::time::sleep(std::time::Duration::from_secs(180)).await;
                                        consecutive_losses = 0;
                                    }
                                }

                                last_known_balance = new_balance;
                                let total = total_wins + total_losses;
                                let win_rate = if total > 0 {
                                    total_wins as f64 / total as f64 * 100.0
                                } else { 0.0 };
                                info!(
                                    "AUTO-LEARN: Win rate {:.0}% ({}/{}) | Balance ${:.2}",
                                    win_rate, total_wins, total, new_balance as f64 / 100.0
                                );

                                info!("AUTO-RECONCILE: Cleared {} settled positions", cleared);
                                last_trade_ticker = None;

                                // Clear reserved tickers for settled positions
                                // so other bots can trade new contracts on those tickers.
                                {
                                    let live_tickers: std::collections::HashSet<String> =
                                        risk_direct.lock().await.open_positions().iter()
                                        .map(|p| p.ticker.clone()).collect();
                                    let mut reserved = reserved_direct.lock().await;
                                    reserved.retain(|t| live_tickers.contains(t));
                                }
                            }
                        }
                        Err(e) => debug!("Auto-reconcile failed: {e}"),
                    }

                    // Also clear traded_tickers to allow new contract windows.
                    traded_tickers.clear();
                }

                let btc = match *btc_direct.lock().await {
                    Some(p) if p > 0.0 => p,
                    _ => continue,
                };

                // Feed ALL asset prices to their predictors.
                if let Some(p) = predictors.get_mut("BTC") { p.update(btc); }
                {
                    let cp = crypto_prices_direct.read();
                    if let Some(eth) = cp.get("ETH") { if let Some(p) = predictors.get_mut("ETH") { p.update(eth); } }
                    if let Some(sol) = cp.get("SOL") { if let Some(p) = predictors.get_mut("SOL") { p.update(sol); } }
                    if let Some(xrp) = cp.get("XRP") { if let Some(p) = predictors.get_mut("XRP") { p.update(xrp); } }
                }

                // Log BTC prediction every 15 seconds.
                if scan_count % 15 == 0 {
                    if let Some(p) = predictors.get("BTC") {
                        if p.prices.len() >= 30 {
                            let dir = match p.predict() { 1 => "▲ BULL", -1 => "▼ BEAR", _ => "— FLAT" };
                            let e20 = p.ema_20.unwrap_or(btc);
                            let e60 = p.ema_60.unwrap_or(btc);
                            let e300 = p.ema_300.unwrap_or(btc);
                            info!(
                                "{} BTC=${:.0} EMA20={:.0}/60={:.0}/300={:.0} 1m={} 5m={} 15m={} W:{} L:{}",
                                dir, btc, e20, e60, e300,
                                p.trend_1m(), p.trend_5m(), p.trend_15m(),
                                total_wins, total_losses
                            );
                        }
                    }
                }

                // Get 15-min markets.
                let markets = match exec_direct.list_btc_markets().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                let kill_switch = shared_direct.read().kalshi_kill_switch;
                let bankroll = shared_direct.read().kalshi_balance_cents.unwrap_or(2490);

                // Trade ALL assets equally — market regime determines profitability, not asset.
                // Bull market = YES on any asset. Bear market = NO on any asset.

                // Debug: log market count and filter breakdown every 60 scans
                if scan_count % 60 == 0 {
                    let liq_count = markets.iter().filter(|m| m.has_liquidity()).count();
                    let with_strike = markets.iter().filter(|m| m.floor_strike.map(|s| s > 0.0).unwrap_or(false)).count();
                    let is_15m = markets.iter().filter(|m| m.ticker.contains("15M") || m.ticker.contains("1H") || m.ticker.contains("60M")).count();
                    let has_bid_ask = markets.iter().filter(|m| m.yes_bid_cents().is_some() && m.yes_ask_cents().is_some()).count();
                    // Check time-to-close for the first liquid BTC 15M
                    let sample = markets.iter().find(|m| m.has_liquidity() && m.ticker.contains("15M") && m.ticker.contains("BTC"));
                    let sample_info = if let Some(m) = sample {
                        let ttc = m.close_time.as_deref().map(|ct| {
                            chrono::DateTime::parse_from_rfc3339(ct)
                                .map(|t| t.timestamp() as f64 - data::orderbook::now_secs())
                                .unwrap_or(-1.0)
                        }).unwrap_or(-1.0);
                        format!("{} bid={:?} ask={:?} ttc={:.0}s spread={}", m.ticker,
                            m.yes_bid_cents(), m.yes_ask_cents(), ttc,
                            m.yes_ask_cents().unwrap_or(0) - m.yes_bid_cents().unwrap_or(0))
                    } else { "no sample".to_string() };
                    info!("DEBUG-MARKETS: {} total, {} liq, {} strikes, {} 15m, {} bid/ask | {}",
                        markets.len(), liq_count, with_strike, is_15m, has_bid_ask, sample_info);
                }

                for market in &markets {
                    if !market.has_liquidity() { continue; }

                    // Allow bid=0 on demo (thin liquidity). At least one side must be quoted.
                    let yes_bid = market.yes_bid_cents().unwrap_or(0);
                    let yes_ask = market.yes_ask_cents().unwrap_or(0);
                    if yes_bid == 0 && yes_ask == 0 { continue; } // No quotes at all

                    // Skip wide spreads (relaxed for demo — liquidity is thin).
                    let spread = if yes_bid > 0 && yes_ask > 0 { yes_ask - yes_bid } else { 50 }; // Unknown spread = assume 50
                    if spread > 60 { continue; }

                    // Strike price: use floor_strike if available (live API),
                    // otherwise use current spot price as proxy (demo API has no strikes).
                    let has_real_strike = market.floor_strike.map(|s| s > 0.0).unwrap_or(false);
                    let strike = if has_real_strike {
                        market.floor_strike.unwrap()
                    } else {
                        // Demo mode: no strike — use spot price as proxy.
                        if market.ticker.to_uppercase().contains("KXBTC") { btc }
                        else if market.ticker.to_uppercase().contains("KXETH") {
                            match crypto_prices_direct.read().get("ETH") { Some(p) => p, None => continue }
                        } else if market.ticker.to_uppercase().contains("KXSOL") {
                            match crypto_prices_direct.read().get("SOL") { Some(p) => p, None => continue }
                        } else if market.ticker.to_uppercase().contains("KXXRP") {
                            match crypto_prices_direct.read().get("XRP") { Some(p) => p, None => continue }
                        } else if market.ticker.to_uppercase().contains("KXDOGE") {
                            match crypto_prices_direct.read().get("DOGE") { Some(p) => p, None => continue }
                        } else { continue }
                    };

                    let strike_type = market.strike_type.as_deref().unwrap_or("");
                    if has_real_strike && strike_type != "greater" && strike_type != "greater_or_equal" {
                        continue;
                    }

                    // Time check — need at least 2 min.
                    let close_time = market.close_time.as_deref().unwrap_or("");
                    let ttc = if !close_time.is_empty() {
                        chrono::DateTime::parse_from_rfc3339(close_time)
                            .map(|ct| ct.timestamp() as f64 - data::orderbook::now_secs())
                            .unwrap_or(600.0)
                    } else { 600.0 };
                    if ttc < 300.0 { continue; } // Need 5+ minutes — entering <5min is throwing money away

                    // ---- PROFITABLE STRATEGY (data-driven from 27 trades) ----
                    //
                    // WHAT LOST MONEY:
                    //   - Daily contracts (KXBTCD): 16 trades, -$19 spent, hedged both sides
                    //   - Entries at 43-50¢: coin flips that lose to fees
                    //   - Distance 0.00%: no directional edge
                    //
                    // WHAT MADE MONEY:
                    //   - 15-min contracts (KXBTC15M): morning YES trades at 30-37¢
                    //   - Cheap entries with BTC clearly above/below strike
                    //
                    // RULES:
                    //   1. 15-MIN CONTRACTS ONLY (no daily — they caused all the hedging)
                    //   2. Max 40¢ entry (cheap = high profit potential)
                    //   3. One trade per 15-min window (no rapid fire)
                    //   4. Never trade a ticker we already hold

                    // 15-min AND hourly contracts — more opportunities.
                    if !market.ticker.contains("15M") && !market.ticker.contains("1H") && !market.ticker.contains("60M") { continue; }

                    // Determine asset and get its spot price.
                    let ticker_upper = market.ticker.to_uppercase();
                    let spot_price = if ticker_upper.contains("KXBTC") {
                        continue; // BTC disabled on live — 4 losses (-$5.31) vs 0 wins today. Reverses too fast.
                    } else if ticker_upper.contains("KXETH") {
                        continue; // ETH disabled — lost money on both demo AND live
                    } else if ticker_upper.contains("KXSOL") {
                        match crypto_prices_direct.read().get("SOL") { Some(p) => p, None => continue }
                    } else if ticker_upper.contains("KXXRP") {
                        continue; // direct:XRP disabled 2026-06-19 (cycle9): +0.3c/277 ~= zero + recurring -EV recent; direct:SOL/DOGE kept
                    } else if ticker_upper.contains("KXDOGE") {
                        match crypto_prices_direct.read().get("DOGE") { Some(p) => p, None => continue }
                    } else {
                        continue;
                    };

                    let distance_pct = (spot_price - strike) / strike;

                    // No duplicate or re-entry after scalp.
                    if traded_tickers.contains(&market.ticker) { continue; }
                    let already_in = risk_direct.lock().await.open_positions()
                        .iter().any(|p| p.ticker == market.ticker);
                    if already_in { continue; }

                    // BET ON PREDICTED DIRECTION — but only for BTC.
                    // For other assets, only trade if spot is clearly directional vs strike.
                    let ticker_upper = market.ticker.to_uppercase();
                    let is_btc = ticker_upper.contains("KXBTC");

                    // Get THIS asset's prediction (not BTC's prediction for all assets).
                    let asset_name = if is_btc { "BTC" }
                        else if ticker_upper.contains("KXETH") { "ETH" }
                        else if ticker_upper.contains("KXSOL") { "SOL" }
                        else if ticker_upper.contains("KXXRP") { "XRP" }
                        else if ticker_upper.contains("KXDOGE") { "DOGE" }
                        else { continue; };

                    // MULTI-TIMEFRAME STRATEGY:
                    //
                    // Layer 1: TREND — 1-min, 5-min, 15-min trends must align.
                    //   Never enter against the 5-min trend. Period.
                    //
                    // Layer 2: MISPRICING — strike distance confirms the trade.
                    //   Price above strike + bullish trend = YES.
                    //   Price below strike + bearish trend = NO.
                    //
                    // Layer 3: ENTRY — max_entry cap, self-learning adjusts.

                    // Get multi-timeframe trends for this asset.
                    let predictor = match predictors.get(asset_name) {
                        Some(p) => p,
                        None => continue,
                    };

                    // THE ORIGINAL MODEL THAT MADE +$14.19 ($20→$35)
                    // Simple. Fast. No overthinking.
                    // Above strike + EMA bullish = YES
                    // Below strike + EMA bearish = NO
                    // Sell at entry+15¢ IMMEDIATELY
                    // That's it.

                    // EMA score: price vs fast vs slow
                    let ema_score = predictor.score(); // uses trend_1m + trend_5m

                    // Debug: log BEFORE score filter so we can see what's blocking
                    if scan_count % 60 == 0 && asset_name == "BTC" {
                        info!(
                            "ENTRY-CHECK: {} score={} ticks={} dist={:.4}% strike={} bid={}¢ ask={}¢",
                            market.ticker, ema_score, predictor.prices.len(),
                            distance_pct * 100.0,
                            if has_real_strike { "real" } else { "none" },
                            yes_bid, yes_ask,
                        );
                    }

                    // REGIME GATE (2026-04-17): direct bot is now BULL-ONLY.
                    // Bearish signals → bear bot. Sideways → scalp bot. HC → skipped entirely.
                    // This partitions the three bots into mutually-exclusive regimes so they
                    // never compete for the same trade window.
                    //   - Historical loss driver: `hc-` signals, -$22.29 net over 72 trades (43% win).
                    //   - Historical winner: `bear-` on downtrends (+$1.30 net, 83% win).
                    //   - Historical winner: `scalp-` in ranging markets (+$0.53 net, 68% win).
                    // HC signals were the historical loss driver (-$22.29 over 72 trades, 43% win)
                    // so we continue to block them. Low-conviction `direct-` signals are allowed.
                    if predictor.is_high_conviction() { continue; }

                    // Score ≥1 = any directional signal
                    // When no strike (distance=0), use EMA direction alone
                    let has_strike = distance_pct.abs() > 0.00001;
                    let (side, raw_price) = if has_strike && distance_pct > 0.0 && ema_score >= 1 {
                            if yes_ask > 60 || yes_ask < 5 { continue; }
                            ("yes", yes_ask)
                        } else if has_strike && distance_pct < 0.0 && ema_score <= -1 {
                            let no_price = if yes_bid > 0 { 100 - yes_bid } else { 100 - yes_ask };
                            if no_price > 60 || no_price < 5 { continue; }
                            ("no", no_price)
                        } else if !has_strike && ema_score >= 1 {
                            // No strike — pure directional: bullish = YES
                            if yes_ask > 60 || yes_ask < 5 { continue; }
                            ("yes", yes_ask)
                        } else if !has_strike && ema_score <= -1 {
                            // No strike — pure directional: bearish = NO
                            let no_price = if yes_bid > 0 { 100 - yes_bid } else { 100 - yes_ask };
                            if no_price > 60 || no_price < 5 { continue; }
                            ("no", no_price)
                        } else {
                            continue;
                        };

                    // ---- DIP DETECTION: wait for pullback, buy the recovery ----
                    // Track this contract's price history. Only buy when:
                    // 1. We've seen it for at least 3 scans (know the range)
                    // 2. Current price is near the LOW (within 5¢ of lowest seen)
                    // 3. Price is recovering (current > lowest = bouncing)
                    let price_entry = contract_prices.entry(market.ticker.clone())
                        .or_insert((raw_price, raw_price, scan_count));
                    let (ref mut lowest, ref mut last, ref first_seen) = *price_entry;

                    if raw_price < *lowest { *lowest = raw_price; }
                    let prev = *last;
                    *last = raw_price;
                    let scans_watched = scan_count - first_seen;

                    // Enter when direction confirms. No dip-buy wait.

                    let price = raw_price;

                    // Depth check — relaxed: use ask size if bid size is 0 (demo has one-sided books).
                    let bid_size: f64 = market.yes_bid_size.as_deref()
                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let ask_size: f64 = market.yes_ask_size.as_deref()
                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let best_size = bid_size.max(ask_size);
                    if best_size < 1.0 { continue; } // Need at least 1 contract available

                    // Dynamic position sizing — scale up as bankroll grows.
                    // <$50: 15% per trade, max 3 contracts
                    // $50-100: 12% per trade, max 5 contracts
                    // ORIGINAL SIZING: 15% bankroll, max 3 contracts
                    let size_pct = 0.15_f64;
                    let max_contracts = 3_i64;
                    let max_cost = (bankroll as f64 * size_pct).round() as i64;
                    if price > max_cost { continue; }
                    let count = (max_cost / price).min(max_contracts).max(1);
                    let cost = price * count;
                    // Exposure check uses TOTAL cost (price × count), not just price.
                    let exposure = risk_direct.lock().await.total_exposure_cents();
                    if exposure + cost > (bankroll as f64 * 0.50).round() as i64 {
                        continue; // Would exceed 50% of bankroll
                    }

                    // Risk check.
                    let allowed = {
                        let mut rm = risk_direct.lock().await;
                        rm.set_kill_switch(kill_switch);
                        matches!(rm.check(cost, 0.75), execution::risk::RiskCheckResult::Allowed)
                    };
                    if !allowed { continue; }

                    // MAX 1 ASSET PER WINDOW — the #1 profitability fix.
                    // With 3-4 assets: 1-2 hit take-profit, rest expire = net loss.
                    // With 1 asset: pick the best, no wasted capital on losers.
                    if scan_count - last_window_clear > 450 {
                        assets_this_window = 0;
                        last_window_clear = scan_count;
                    }
                    if assets_this_window >= 4 { continue; } // Scalp ALL assets per window

                    // RAPID SCALP: allow up to 4 entries per window.
                    // +8¢ target hits fast. Volume × small wins = compound growth.
                    if assets_this_window >= 4 { continue; }

                    // Cross-bot dedup
                    {
                        let mut reserved = reserved_direct.lock().await;
                        if reserved.contains(&market.ticker) { continue; }
                        if reserved.contains(asset_name) { continue; }
                        reserved.insert(market.ticker.clone());
                        reserved.insert(asset_name.to_string());
                    }

                    // Place trade.
                    match exec_direct.place_order(&market.ticker, "buy", side, price, count).await {
                        Ok(order) => {
                            // ---- FILL VERIFICATION ----
                            // Only record as open position if the order actually filled.
                            // "resting" = unfilled limit on the book — not a real position.
                            if !KalshiExecutor::is_order_filled(&order.status) {
                                warn!(
                                    "HC order {} status='{}' (not filled) — cancelling",
                                    order.order_id, order.status
                                );
                                let _ = exec_direct.cancel_order(&order.order_id).await;
                                reserved_direct.lock().await.remove(&market.ticker);
                                reserved_direct.lock().await.remove(asset_name);
                                continue;
                            }

                            let trend_str = format!("score={}", ema_score);
                            info!(
                                "DIRECT: {} {} {} ×{} @ {}¢ dist={:.2}% {} → {} [FILLED]",
                                market.ticker, "buy", side, count, price,
                                distance_pct * 100.0, trend_str, order.order_id
                            );

                            // Log trade to CSV for dashboard tracking.
                            {
                                use std::io::Write;
                                let csv_path = "logs/trades_history.csv";
                                let needs_header = !std::path::Path::new(csv_path).exists();
                                if let Ok(mut f) = std::fs::OpenOptions::new()
                                    .create(true).append(true).open(csv_path)
                                {
                                    if needs_header {
                                        let _ = writeln!(f, "timestamp,order_id,ticker,side,count,price_cents,cost_cents,btc_price,strike,distance_pct,balance_cents");
                                    }
                                    let _ = writeln!(f, "{},{},{},{},{},{},{},{:.2},{:.2},{:.4},{}",
                                        chrono::Utc::now().to_rfc3339(),
                                        order.order_id, market.ticker, side, count,
                                        price, cost, btc, strike, distance_pct,
                                        bankroll
                                    );
                                }
                            }

                            let open_pos = execution::risk::OpenPosition {
                                order_id: order.order_id.clone(),
                                ticker: market.ticker.clone(),
                                side: side.to_owned(),
                                count,
                                price_cents: price,
                                cost_cents: cost,
                                opened_at: data::orderbook::now_secs(),
                                signal_id: if predictor.is_high_conviction() {
                                    format!("hc-{}", market.ticker)
                                } else {
                                    format!("direct-{}", market.ticker)
                                },
                                closed_at: None,
                                pnl_cents: None,
                            };
                            risk_direct.lock().await.record_open(open_pos);
                            shared_direct.write().push_kalshi_order(order);
                            last_trade_ticker = Some(market.ticker.clone());
                            traded_tickers.insert(market.ticker.clone());
                            assets_this_window += 1;
                            // Mark global window as taken — no other bot can enter
                            { let mut g = global_gate_hc.lock().await; g.1 = true; }
                            break; // One trade per cycle, one per ticker.
                        }
                        Err(e) => {
                            warn!("Direct trade failed: {e}");
                            reserved_direct.lock().await.remove(&market.ticker);
                        }
                    }
                }
            }
        });
        info!("High conviction + morning scalp model running");
    }

    // ---- FAIR-VALUE BOT (4th strategy: Black-Scholes digital mispricing) ----
    // Prices each near-expiry crypto binary from live spot + realized vol +
    // time-to-expiry (a driftless BS digital with a 60s-average settlement
    // correction), and takes the quote ONLY when fee+slippage-adjusted edge
    // clears a hard buffer. Prioritizes the deep ITM/OTM convergence bucket and
    // holds those to fee-free settlement. signal_id prefix `fv-` / `fv-hold-`.
    // Runs alongside the 3 heuristic bots so it is A/B-comparable by prefix.
    if cfg.kalshi.fv_enabled {
        if let Some(exec_fv) = kalshi_executor.clone() {
            let risk_fv = Arc::clone(&risk_manager);
            let shared_fv = Arc::clone(&shared_state);
            let btc_fv = Arc::clone(&btc_price_arc);
            let reserved_fv = Arc::clone(&reserved_tickers);
            let crypto_fv = shared_crypto_prices.clone();
            let fv_min_edge = cfg.kalshi.fv_min_edge_cents;
            let fv_slip = cfg.kalshi.fv_slippage_cents;
            let fv_size_pct = cfg.kalshi.fv_size_pct;
            let fv_max_contracts = cfg.kalshi.fv_max_contracts;
            let fv_hold = cfg.kalshi.fv_hold_to_settlement;
            let fv_daily_stop = cfg.kalshi.fv_daily_stop_cents;
            let fv_vol_floor = cfg.kalshi.fv_vol_floor_per_sec;
            let fv_max_ttc = cfg.kalshi.fv_max_ttc_secs;
            let fv_is_paper = exec_fv.is_paper();
            tokio::spawn(async move {
                use signals::fair_value as fv;
                use std::collections::{HashMap, VecDeque};

                #[derive(Clone)]
                struct FvCand {
                    net: i64,
                    ticker: String,
                    asset: String,
                    side: &'static str,
                    price: i64,
                    fair: i64,
                    ttc: f64,
                    bucket: &'static str,
                    conf: f64,
                    depth: f64,
                }

                // Per-asset spot history for realized-vol estimation (own buffers,
                // since the DIRECT bot's AssetPredictor is private to its closure).
                let mut hist: HashMap<String, VecDeque<f64>> = HashMap::new();
                let mut scan: u64 = 0;
                let mut fv_day_pnl: i64 = 0; // today's realized fv- P&L (cents), from journal
                let mut last_fv_window: i64 = -1; // FV self-limits to 1 entry / 15-min window
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));

                loop {
                    ticker.tick().await;
                    scan += 1;

                    // --- update spot history for vol ---
                    let btc = match *btc_fv.lock().await {
                        Some(p) if p > 0.0 => p,
                        _ => continue,
                    };
                    {
                        let b = hist.entry("BTC".to_string()).or_default();
                        b.push_back(btc);
                        if b.len() > 600 { b.pop_front(); }
                    }
                    {
                        let cp = crypto_fv.read();
                        for a in ["ETH", "SOL", "XRP", "DOGE"] {
                            if let Some(v) = cp.get(a) {
                                if v > 0.0 {
                                    let buf = hist.entry(a.to_string()).or_default();
                                    buf.push_back(v);
                                    if buf.len() > 600 { buf.pop_front(); }
                                }
                            }
                        }
                    }

                    // --- daily stop: recompute today's fv- realized P&L from the journal every 30s ---
                    if fv_daily_stop > 0 && scan % 30 == 0 {
                        if let Ok(j) = std::fs::read_to_string("logs/trade_journal.csv") {
                            let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
                            let mut net = 0i64;
                            for line in j.lines().skip(1) {
                                let p: Vec<&str> = line.split(',').collect();
                                if p.len() < 10 { continue; }
                                if !p[0].starts_with(&today) || !p[9].starts_with("fv-") { continue; }
                                let e: i64 = p[4].parse().unwrap_or(0);
                                let x: i64 = p[5].parse().unwrap_or(0);
                                let c: i64 = p[8].parse().unwrap_or(1);
                                net += (x - e) * c;
                            }
                            fv_day_pnl = net;
                        }
                    }
                    let day_stopped = fv_daily_stop > 0 && fv_day_pnl <= -fv_daily_stop;

                    let kill_switch = shared_fv.read().kalshi_kill_switch;
                    let bankroll = shared_fv.read().kalshi_balance_cents.unwrap_or(2490);

                    let markets = match exec_fv.list_all_crypto_15m().await {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    // --- evaluate every candidate; track best-for-display and best-tradeable ---
                    let mut disp: Option<FvCand> = None;
                    let mut trade: Option<FvCand> = None;

                    for market in &markets {
                        if !market.has_liquidity() { continue; }
                        let tk = market.ticker.to_uppercase();
                        if !(tk.contains("15M") || tk.contains("1H") || tk.contains("60M")) { continue; }
                        // Only "greater"/"greater_or_equal" strikes → P_above == P(YES).
                        match market.strike_type.as_deref() {
                            Some("greater") | Some("greater_or_equal") => {}
                            _ => continue,
                        }
                        // Real strike required — never fall back to spot-as-strike.
                        let strike = match market.floor_strike {
                            Some(s) if s > 0.0 => s,
                            _ => continue,
                        };
                        // Seconds to expiry.
                        let ttc = match market.close_time.as_deref() {
                            Some(ct) => chrono::DateTime::parse_from_rfc3339(ct)
                                .map(|t| t.timestamp() as f64 - now_secs())
                                .unwrap_or(-1.0),
                            None => -1.0,
                        };
                        if ttc < 300.0 || ttc > fv_max_ttc { continue; }
                        // Asset + spot.
                        let asset = if tk.contains("BTC") { "BTC" }
                            else if tk.contains("ETH") { "ETH" }
                            else if tk.contains("SOL") { "SOL" }
                            else if tk.contains("XRP") { "XRP" }
                            else if tk.contains("DOGE") { "DOGE" }
                            else { continue };
                        let spot = if asset == "BTC" { btc } else {
                            match crypto_fv.read().get(asset) {
                                Some(p) if p > 0.0 => p,
                                _ => continue,
                            }
                        };
                        // Realized vol (need >=120 ticks of history).
                        let prices: Vec<f64> = match hist.get(asset) {
                            Some(b) if b.len() >= 120 => b.iter().copied().collect(),
                            _ => continue,
                        };
                        let sigma = match fv::realized_vol_per_step(&prices, 300) {
                            Some(s) => s.max(fv_vol_floor),
                            None => continue,
                        };
                        // Fair value.
                        let p_above = fv::fair_prob_above(spot, strike, sigma, ttc);
                        let fair_cents = fv::fair_cents_yes(p_above);
                        let yes_ask = market.yes_ask_cents().unwrap_or(0);
                        let yes_bid = market.yes_bid_cents().unwrap_or(0);

                        // Best of {buy YES at ask, buy NO at 100-yes_bid}.
                        let mut cand_net = i64::MIN;
                        let mut cand_side: &'static str = "yes";
                        let mut cand_price = 0i64;
                        if yes_ask > 0 && yes_ask < 100 {
                            let net = fv::net_edge_cents(fair_cents, yes_ask, fv_slip, 1);
                            if net > cand_net { cand_net = net; cand_side = "yes"; cand_price = yes_ask; }
                        }
                        if yes_bid > 0 && yes_bid < 100 {
                            let no_ask = 100 - yes_bid;
                            let no_fair = 100 - fair_cents;
                            let net = fv::net_edge_cents(no_fair, no_ask, fv_slip, 1);
                            if net > cand_net { cand_net = net; cand_side = "no"; cand_price = no_ask; }
                        }
                        if cand_price <= 0 { continue; }

                        let convergence = fair_cents >= 90 || fair_cents <= 10;
                        let min_e = if convergence { (fv_min_edge / 2).max(2) } else { fv_min_edge };
                        let bid_size: f64 = market.yes_bid_size.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let ask_size: f64 = market.yes_ask_size.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        // Side-aware depth: a YES buy lifts the yes-ask; a NO buy lifts the
                        // yes-bid (NO ask == 100 - yes_bid). Sizing off the wrong side over-
                        // states available liquidity.
                        let depth = if cand_side == "yes" { ask_size } else { bid_size };
                        let conf = ((p_above - 0.5).abs() * 2.0).clamp(0.0, 1.0);

                        let cand = FvCand {
                            net: cand_net,
                            ticker: market.ticker.clone(),
                            asset: asset.to_string(),
                            side: cand_side,
                            price: cand_price,
                            fair: fair_cents,
                            ttc,
                            bucket: if convergence { "conv" } else { "mid" },
                            conf,
                            depth,
                        };
                        // Best-for-display tracks the highest net regardless of gates.
                        if disp.as_ref().map_or(true, |d| cand.net > d.net) {
                            disp = Some(cand.clone());
                        }
                        // Best-tradeable must clear the bucket edge gate and have depth.
                        if cand.net >= min_e && depth >= 1.0
                            && trade.as_ref().map_or(true, |t| cand.net > t.net)
                        {
                            trade = Some(cand);
                        }
                    }

                    // --- attempt the best tradeable candidate ---
                    let mut traded = false;
                    let win = (now_secs() as i64) / 900;
                    if let Some(c) = trade.clone() {
                        if !kill_switch && !day_stopped && win != last_fv_window {
                            let max_cost = (bankroll as f64 * fv_size_pct).round() as i64;
                            if c.price <= max_cost {
                                let mut count = (max_cost / c.price).min(fv_max_contracts).max(1);
                                if (c.depth as i64) < count { count = (c.depth as i64).max(1); }
                                let cost = c.price * count;

                                let exposure = risk_fv.lock().await.total_exposure_cents();
                                let within_exposure =
                                    exposure + cost <= (bankroll as f64 * 0.50).round() as i64;

                                let allowed = within_exposure && {
                                    let mut rm = risk_fv.lock().await;
                                    rm.set_kill_switch(kill_switch);
                                    matches!(rm.check(cost, c.conf), RiskCheckResult::Allowed)
                                };

                                // Cross-bot dedup: don't double-up on a ticker/asset another bot holds.
                                let reserved_ok = if allowed {
                                    let mut reserved = reserved_fv.lock().await;
                                    if reserved.contains(&c.ticker) || reserved.contains(&c.asset) {
                                        false
                                    } else {
                                        reserved.insert(c.ticker.clone());
                                        reserved.insert(c.asset.clone());
                                        true
                                    }
                                } else { false };

                                if reserved_ok {
                                    match exec_fv.place_order(&c.ticker, "buy", c.side, c.price, count).await {
                                        Ok(order) => {
                                            if !KalshiExecutor::is_order_filled(&order.status) {
                                                let _ = exec_fv.cancel_order(&order.order_id).await;
                                                let mut reserved = reserved_fv.lock().await;
                                                reserved.remove(&c.ticker);
                                                reserved.remove(&c.asset);
                                            } else {
                                                let prefix = if fv_hold && c.bucket == "conv" { "fv-hold-" } else { "fv-" };
                                                let signal_id = format!("{}{}", prefix, c.ticker);
                                                info!(
                                                    "FV: {} buy {} ×{} @ {}¢ fair={}¢ net=+{}¢ ttc={:.0}s [{}] → {} [FILLED]",
                                                    c.ticker, c.side, count, c.price, c.fair, c.net, c.ttc, c.bucket, order.order_id
                                                );
                                                {
                                                    use std::io::Write;
                                                    let log_spot = if c.asset == "BTC" { btc }
                                                        else { crypto_fv.read().get(c.asset.as_str()).unwrap_or(0.0) };
                                                    let csv_path = "logs/trades_history.csv";
                                                    let needs_header = !std::path::Path::new(csv_path).exists();
                                                    if let Ok(mut f) = std::fs::OpenOptions::new()
                                                        .create(true).append(true).open(csv_path)
                                                    {
                                                        if needs_header {
                                                            let _ = writeln!(f, "timestamp,order_id,ticker,side,count,price_cents,cost_cents,btc_price,strike,distance_pct,balance_cents");
                                                        }
                                                        let _ = writeln!(f, "{},{},{},{},{},{},{},{:.2},{:.2},{:.4},{}",
                                                            chrono::Utc::now().to_rfc3339(), order.order_id, c.ticker, c.side,
                                                            count, c.price, cost, log_spot, 0.0, 0.0, bankroll);
                                                    }
                                                }
                                                let open_pos = OpenPosition {
                                                    order_id: order.order_id.clone(),
                                                    ticker: c.ticker.clone(),
                                                    side: c.side.to_owned(),
                                                    count,
                                                    price_cents: c.price,
                                                    cost_cents: cost,
                                                    opened_at: now_secs(),
                                                    signal_id,
                                                    closed_at: None,
                                                    pnl_cents: None,
                                                };
                                                risk_fv.lock().await.record_open(open_pos);
                                                shared_fv.write().push_kalshi_order(order);
                                                last_fv_window = win;
                                                traded = true;
                                            }
                                        }
                                        Err(e) => {
                                            warn!("FV trade failed: {e}");
                                            let mut reserved = reserved_fv.lock().await;
                                            reserved.remove(&c.ticker);
                                            reserved.remove(&c.asset);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // --- surface the model's view to the dashboard (throttled) ---
                    if scan % 3 == 0 {
                        if let Some(d) = disp {
                            let min_e = if d.bucket == "conv" { (fv_min_edge / 2).max(2) } else { fv_min_edge };
                            let decision = if traded { "TRADE" }
                                else if day_stopped { "skip:daily-stop" }
                                else if d.net >= min_e { "skip:gate" }
                                else { "watch" };
                            shared_fv.write().push_fv_signal(FvSignal {
                                ts: chrono::Utc::now().to_rfc3339(),
                                ticker: d.ticker,
                                asset: d.asset,
                                side: d.side.to_string(),
                                fair_cents: d.fair,
                                market_cents: d.price,
                                net_edge_cents: d.net,
                                ttc_secs: d.ttc,
                                bucket: d.bucket.to_string(),
                                decision: decision.to_string(),
                            });
                        }
                    }
                }
            });
            info!("Fair-value bot started (BS digital edge, prefix fv-/fv-hold-)");
            if !fv_is_paper {
                warn!("FV bot is LIVE but has KNOWN UNFIXED live-mode gaps: partial fills are \
                       cancel-and-forgotten (untracked exposure), fv-hold settlements aren't \
                       journaled correctly, and check→place→record is non-atomic. Validate in \
                       PAPER and fix the go-live checklist before trusting live fills.");
            }
        }
    }

    // ---- SIDEWAYS SCALP BOT (second strategy, runs in parallel) ----
    // The most profitable model: simple direction + cheap entry + quick scalp.
    // This made $10+ in one session. Runs alongside the bullish/bearish models.
    // RE-ENABLED 2026-04-17 as the SIDEWAYS specialist in the three-bot regime split.
    // Fires ONLY when 5-min trend is FLAT (scalp_trend == 0). Bull/Bear are handled
    // by the direct bot and bear bot respectively — see regime gate above.
    if let Some(exec_scalp) = kalshi_executor.clone() {
        let global_gate_scalp = Arc::clone(&global_window_entry);
        let risk_scalp = Arc::clone(&risk_manager);
        let shared_scalp = Arc::clone(&shared_state);
        let btc_scalp = Arc::clone(&btc_price_arc);
        let crypto_scalp = shared_crypto_prices.clone();
        let reserved_scalp = Arc::clone(&reserved_tickers);
        // Scalp bot gets its own trend trackers for 5-min gate.
        tokio::spawn(async move {
            // Offset by 1 second so it doesn't collide with the main loop.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let mut scan_count: u64 = 0;
            let mut last_scalp_ticker: Option<String> = None;
            let mut scalp_entries_window: u32 = 0;
            let mut scalp_window_clear: u64 = 0;

            // Per-asset EMA trackers for 5-min trend gate.
            // Uses EMA-150 (~5 min at 2s updates) for trend confirmation.
            let mut scalp_emas: std::collections::HashMap<String, (Option<f64>, Option<f64>, u32)> =
                std::collections::HashMap::new(); // (ema_fast, ema_slow, tick_count)
            for a in &["BTC", "ETH", "SOL", "XRP", "DOGE"] {
                scalp_emas.insert(a.to_string(), (None, None, 0));
            }

            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                scan_count += 1;
                if scan_count % 3 != 0 { continue; } // Every 3 seconds

                let btc = match *btc_scalp.lock().await {
                    Some(p) if p > 0.0 => p,
                    _ => continue,
                };

                // Update all asset EMAs for trend gating.
                {
                    let af = 2.0 / 21.0;  // ~20 period (fast)
                    let as_ = 2.0 / 151.0; // ~150 period (~5 min at 3s intervals)
                    let update_ema = |entry: &mut (Option<f64>, Option<f64>, u32), price: f64| {
                        entry.0 = Some(match entry.0 { Some(p) => p + af * (price - p), None => price });
                        entry.1 = Some(match entry.1 { Some(p) => p + as_ * (price - p), None => price });
                        entry.2 += 1;
                    };
                    if let Some(e) = scalp_emas.get_mut("BTC") { update_ema(e, btc); }
                    if let Some(eth) = crypto_scalp.read().get("ETH") {
                        if let Some(e) = scalp_emas.get_mut("ETH") { update_ema(e, eth); }
                    }
                    if let Some(sol) = crypto_scalp.read().get("SOL") {
                        if let Some(e) = scalp_emas.get_mut("SOL") { update_ema(e, sol); }
                    }
                    if let Some(xrp) = crypto_scalp.read().get("XRP") {
                        if let Some(e) = scalp_emas.get_mut("XRP") { update_ema(e, xrp); }
                    }
                }

                let markets = match exec_scalp.list_all_crypto_15m().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                let kill_switch = shared_scalp.read().kalshi_kill_switch;
                let bankroll = shared_scalp.read().kalshi_balance_cents.unwrap_or(0);

                for market in &markets {
                    if !market.has_liquidity() { continue; }
                    // 15M ONLY — exit manager only monitors 15M markets.
                    // 5M positions would get zero trailing stop protection.
                    if !market.ticker.contains("15M") && !market.ticker.contains("1H") && !market.ticker.contains("60M") { continue; }

                    // Don't trade same ticker twice.
                    // MAX 1 SCALP ENTRY PER WINDOW
                    if scan_count - scalp_window_clear > 300 {
                        scalp_entries_window = 0;
                        scalp_window_clear = scan_count;
                    }
                    if scalp_entries_window >= 3 { continue; } // 3 scalps per window

                    if let Some(ref last) = last_scalp_ticker {
                        if &market.ticker == last { continue; }
                    }
                    let already_in = risk_scalp.lock().await.open_positions()
                        .iter().any(|p| p.ticker == market.ticker);
                    if already_in { continue; }

                    let yes_bid = match market.yes_bid_cents() { Some(b) if b > 0 => b, _ => continue };
                    let yes_ask = match market.yes_ask_cents() { Some(a) if a > 0 => a, _ => continue };
                    if yes_ask - yes_bid > 12 { continue; }

                    let strike = match market.floor_strike { Some(s) if s > 0.0 => s, _ => continue };
                    let st = market.strike_type.as_deref().unwrap_or("");
                    if st != "greater" && st != "greater_or_equal" { continue; }

                    // Time check.
                    let close_time = market.close_time.as_deref().unwrap_or("");
                    let ttc = if !close_time.is_empty() {
                        chrono::DateTime::parse_from_rfc3339(close_time)
                            .map(|ct| ct.timestamp() as f64 - data::orderbook::now_secs())
                            .unwrap_or(600.0)
                    } else { 600.0 };
                    if ttc < 300.0 { continue; }

                    // Get spot price for this asset.
                    let ticker_upper = market.ticker.to_uppercase();
                    let spot = if ticker_upper.contains("KXBTC") { btc }
                        else if ticker_upper.contains("KXETH") { match crypto_scalp.read().get("ETH") { Some(p) => p, None => continue } }
                        else if ticker_upper.contains("KXSOL") { match crypto_scalp.read().get("SOL") { Some(p) => p, None => continue } }
                        else if ticker_upper.contains("KXXRP") { match crypto_scalp.read().get("XRP") { Some(p) => p, None => continue } }
                        else if ticker_upper.contains("KXDOGE") { match crypto_scalp.read().get("DOGE") { Some(p) => p, None => continue } }
                        else { continue };

                    let dist = (spot - strike) / strike;

                    // SCALP MODEL: 5-min trend gate + direction + cheap entry.
                    // The scalp bot MUST respect the 5-min trend — no more entering
                    // against momentum just because the contract is cheap.
                    let asset_name = if ticker_upper.contains("KXBTC") { "BTC" }
                        else if ticker_upper.contains("KXETH") { "ETH" }
                        else if ticker_upper.contains("KXSOL") { "SOL" }
                        else if ticker_upper.contains("KXXRP") { "XRP" }
                        else if ticker_upper.contains("KXDOGE") { "DOGE" }
                        else { continue };

                    // Check 5-min trend for this asset.
                    let scalp_trend = match scalp_emas.get(asset_name) {
                        Some((Some(fast), Some(slow), ticks)) if *ticks >= 40 => {
                            // Need 100+ ticks (~5 min at 3s) before EMA is meaningful
                            let spread = (fast - slow) / slow;
                            if spread > 0.0008 { 1 } else if spread < -0.0008 { -1 } else { 0 }
                        }
                        _ => 0, // Not enough data
                    };

                    // REGIME GATE (2026-04-17): scalp bot fires ONLY when 5-min trend is FLAT.
                    // In a sideways market, price oscillates around the strike and cheap contracts
                    // on either side can resolve profitably. Bull/bear regimes belong to the
                    // direct bot and bear bot respectively.
                    if scalp_trend != 0 { continue; }
                    let (side, price) = if dist > 0.0 {
                        if yes_ask > 42 || yes_ask < 25 { continue; } // Cheap YES above strike
                        ("yes", yes_ask)
                    } else if dist < 0.0 {
                        let no_price = 100 - yes_bid;
                        if no_price > 42 || no_price < 25 { continue; } // Cheap NO below strike
                        ("no", no_price)
                    } else { continue };

                    // Sizing: 8% bankroll, max 2 contracts.
                    let max_cost = (bankroll as f64 * 0.06).round() as i64;
                    if price > max_cost { continue; }
                    // ---- EDGE-SCALED SIZING (improvement 2026-06-03) ----
                    // The scalp model is the proven winner (79% win, +$2.74 net). Press the
                    // edge: take 2 contracts on premium setups — a very tight spread AND price
                    // hugging the strike in this confirmed-flat regime — and 1 contract
                    // otherwise. Still bounded by the 6% per-trade cap and the 50% exposure cap.
                    let spread = yes_ask - yes_bid;
                    let premium = spread <= 4 && dist.abs() < 0.0008;
                    let mut count = if premium { 2_i64 } else { 1_i64 };
                    while count > 1 && price * count > max_cost { count -= 1; }
                    let cost = price * count;
                    let exposure = risk_scalp.lock().await.total_exposure_cents();
                    if exposure + cost > (bankroll as f64 * 0.50).round() as i64 { continue; }

                    let allowed = {
                        let mut rm = risk_scalp.lock().await;
                        rm.set_kill_switch(kill_switch);
                        matches!(rm.check(cost, 0.75), execution::risk::RiskCheckResult::Allowed)
                    };
                    if !allowed { continue; }

                    // GLOBAL GATE
                    {
                        let window_id = (data::orderbook::now_secs() / 900.0) as u64;
                        let mut gate = global_gate_scalp.lock().await;
                        if gate.0 == window_id && gate.1 { continue; }
                        if gate.0 != window_id { *gate = (window_id, false); }
                    }
                    {
                        let mut reserved = reserved_scalp.lock().await;
                        if reserved.contains(&market.ticker) { continue; }
                        if reserved.contains(asset_name) { continue; }
                        reserved.insert(market.ticker.clone());
                        reserved.insert(asset_name.to_string());
                    }

                    match exec_scalp.place_order(&market.ticker, "buy", side, price, count).await {
                        Ok(order) => {
                            // ---- FILL VERIFICATION ----
                            if !KalshiExecutor::is_order_filled(&order.status) {
                                warn!(
                                    "SCALP order {} status='{}' (not filled) — cancelling",
                                    order.order_id, order.status
                                );
                                let _ = exec_scalp.cancel_order(&order.order_id).await;
                                reserved_scalp.lock().await.remove(&market.ticker);
                                reserved_scalp.lock().await.remove(asset_name);
                                continue;
                            }

                            info!(
                                "SCALP-BOT: {} {} {} ×{} @ {}¢ dist={:.2}% → {} [FILLED]",
                                market.ticker, "buy", side, count, price,
                                dist * 100.0, order.order_id
                            );
                            let open_pos = execution::risk::OpenPosition {
                                order_id: order.order_id.clone(),
                                ticker: market.ticker.clone(),
                                side: side.to_owned(),
                                count, price_cents: price, cost_cents: cost,
                                opened_at: data::orderbook::now_secs(),
                                signal_id: format!("scalp-{}", market.ticker),
                                closed_at: None, pnl_cents: None,
                            };
                            risk_scalp.lock().await.record_open(open_pos);
                            shared_scalp.write().push_kalshi_order(order);
                            last_scalp_ticker = Some(market.ticker.clone());
                            scalp_entries_window += 1;
                            { let mut g = global_gate_scalp.lock().await; g.1 = true; }
                            break;
                        }
                        Err(e) => {
                            warn!("Scalp-bot trade failed: {e}");
                            reserved_scalp.lock().await.remove(&market.ticker);
                        }
                    }
                }
            }
        });
        info!("Morning scalp bot started (parallel strategy)");
    }

    // ---- CHOP SCALPER — DISABLED (caused losses by entering against macro trend) ----
    // When EMAs are tangled and market is choppy, price bounces in a range.
    // Buy the dip, sell the bounce. Repeat.
    if let Some(exec_chop) = None::<std::sync::Arc<execution::kalshi::KalshiExecutor>> {
        let risk_chop = Arc::clone(&risk_manager);
        let shared_chop = Arc::clone(&shared_state);
        let btc_chop = Arc::clone(&btc_price_arc);
        let reserved_chop = Arc::clone(&reserved_tickers);
        let crypto_chop = shared_crypto_prices.clone();
        tokio::spawn(async move {
            let mut scan_count: u64 = 0;
            // Track price range per contract to detect bounces.
            let mut contract_range: std::collections::HashMap<String, (i64, i64, i64)> = std::collections::HashMap::new(); // (low, high, last)
            let mut chop_traded: std::collections::HashSet<String> = std::collections::HashSet::new();

            // Macro trend tracker: prevents buying YES in bear trends, NO in bull trends.
            let mut chop_ema_fast: Option<f64> = None;
            let mut chop_ema_slow: Option<f64> = None;
            let mut chop_ticks: u32 = 0;

            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                scan_count += 1;
                if scan_count % 4 != 0 { continue; } // Every 4 seconds

                let btc = match *btc_chop.lock().await {
                    Some(p) if p > 0.0 => p,
                    _ => continue,
                };

                let markets = match exec_chop.list_all_crypto_15m().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                let kill_switch = shared_chop.read().kalshi_kill_switch;
                let bankroll = shared_chop.read().kalshi_balance_cents.unwrap_or(0);

                for market in &markets {
                    if !market.has_liquidity() { continue; }
                    if !market.ticker.contains("15M") && !market.ticker.contains("1H") && !market.ticker.contains("60M") { continue; }
                    if chop_traded.contains(&market.ticker) { continue; }

                    let yes_ask = match market.yes_ask_cents() { Some(a) if a > 0 => a, _ => continue };
                    let yes_bid = match market.yes_bid_cents() { Some(b) if b > 0 => b, _ => continue };

                    // Track this contract's price range.
                    let entry = contract_range.entry(market.ticker.clone())
                        .or_insert((yes_ask, yes_ask, yes_ask));
                    let (ref mut low, ref mut high, ref mut last) = *entry;
                    if yes_ask < *low { *low = yes_ask; }
                    if yes_ask > *high { *high = yes_ask; }
                    let prev = *last;
                    *last = yes_ask;

                    // Need to see a range of at least 10¢ (bouncing market).
                    let range = *high - *low;
                    if range < 10 { continue; }

                    // Update macro trend EMA using BTC as proxy for overall market direction.
                    {
                        let af = 2.0 / 21.0;
                        let as_ = 2.0 / 151.0;
                        chop_ema_fast = Some(match chop_ema_fast { Some(p) => p + af * (btc - p), None => btc });
                        chop_ema_slow = Some(match chop_ema_slow { Some(p) => p + as_ * (btc - p), None => btc });
                        chop_ticks += 1;
                    }

                    // Macro trend: is the overall market trending?
                    let macro_trend = if chop_ticks < 75 { 0 } else { // ~5 min warmup
                        let ef = chop_ema_fast.unwrap_or(0.0);
                        let es = chop_ema_slow.unwrap_or(0.0);
                        if es <= 0.0 { 0 }
                        else {
                            let spread = (ef - es) / es;
                            if spread > 0.0003 { 1 } else if spread < -0.0003 { -1 } else { 0 }
                        }
                    };

                    // CHOP SCALP: buy near edges of the range.
                    // Near LOW + bouncing up → buy YES (only if macro NOT bearish)
                    // Near HIGH + dropping down → buy NO (only if macro NOT bullish)
                    let near_low = yes_ask <= *low + 3;
                    let near_high = yes_ask >= *high - 3;
                    let bouncing_up = yes_ask > prev;
                    let dropping_down = yes_ask < prev;

                    let already_in = risk_chop.lock().await.open_positions()
                        .iter().any(|p| p.ticker == market.ticker);
                    if already_in { continue; }

                    // Determine side — with macro trend guard:
                    // Don't buy YES when macro is bearish (price will keep falling)
                    // Don't buy NO when macro is bullish (price will keep rising)
                    // macro_trend must be STRICTLY non-bearish for YES, STRICTLY non-bullish for NO.
                    // Flat (0) during warmup = sit out. Don't trade when trend is unknown.
                    let (chop_side, chop_price) = if near_low && bouncing_up && yes_ask <= 48 && yes_ask >= 15 && macro_trend > 0 {
                        ("yes", yes_ask)
                    } else if near_high && dropping_down && macro_trend < 0 {
                        let no_price = 100 - yes_bid;
                        if no_price < 15 || no_price > 48 { continue; }
                        ("no", no_price)
                    } else {
                        continue;
                    };

                    {
                        let exposure = risk_chop.lock().await.total_exposure_cents();
                        if exposure + chop_price > (bankroll as f64 * 0.50).round() as i64 { continue; }

                        let count = 1_i64; // Small size for chop scalps.
                        let cost = chop_price * count;
                        let allowed = {
                            let mut rm = risk_chop.lock().await;
                            rm.set_kill_switch(kill_switch);
                            matches!(rm.check(cost, 0.75), execution::risk::RiskCheckResult::Allowed)
                        };
                        if !allowed { continue; }

                        // Cross-bot dedup
                        {
                            let mut reserved = reserved_chop.lock().await;
                            if reserved.contains(&market.ticker) { continue; }
                            reserved.insert(market.ticker.clone());
                        }

                        match exec_chop.place_order(&market.ticker, "buy", chop_side, chop_price, count).await {
                            Ok(order) => {
                                info!(
                                    "CHOP-SCALP: {} buy {} x{} @ {}¢ range={}¢ → {}",
                                    market.ticker, chop_side, count, chop_price, range, order.order_id
                                );
                                let open_pos = execution::risk::OpenPosition {
                                    order_id: order.order_id.clone(),
                                    ticker: market.ticker.clone(),
                                    side: chop_side.to_owned(),
                                    count, price_cents: chop_price, cost_cents: cost,
                                    opened_at: data::orderbook::now_secs(),
                                    signal_id: format!("chop-{}", market.ticker),
                                    closed_at: None, pnl_cents: None,
                                };
                                risk_chop.lock().await.record_open(open_pos);
                                shared_chop.write().push_kalshi_order(order);
                                chop_traded.insert(market.ticker.clone());
                                break;
                            }
                            Err(e) => {
                                warn!("Chop scalp failed: {e}");
                                reserved_chop.lock().await.remove(&market.ticker);
                            }
                        }
                    }
                }

                // Clear traded tickers every 15 minutes (new contracts).
                if scan_count % 900 == 0 {
                    chop_traded.clear();
                    contract_range.clear();
                }
            }
        });
        info!("Chop scalper started (sideways market strategy, YES+NO)");
    } // chop disabled — None always fails

    // ---- BEAR MOMENTUM BOT (profits from sustained downtrends) ----
    // Re-enabled per user request. Has BTC macro check to prevent NO in bull market.
    // When EMAs stack bearish (price < EMA10 < EMA30), aggressively buy NO.
    // In a downtrend, price stays below strike = NO wins at $1.
    // Also fades bounces: when price spikes up in a downtrend, NO gets cheaper → buy.
    // BEAR BOT RE-ENABLED 2026-04-17 as the BEARISH specialist in the three-bot regime split.
    // The direct bot is now gated bull-only (ema_score >= 1); bear handles downtrends exclusively.
    // Historical winner: +$1.30 net on 18 trades, 83% win rate — highest-EV strategy by far.
    if let Some(exec_bear) = kalshi_executor.clone() {
        let risk_bear = Arc::clone(&risk_manager);
        let reserved_bear = Arc::clone(&reserved_tickers);
        let global_gate_bear = Arc::clone(&global_window_entry);
        let shared_bear = Arc::clone(&shared_state);
        let btc_bear = Arc::clone(&btc_price_arc);
        let crypto_prices_bear = shared_crypto_prices.clone();
        tokio::spawn(async move {
            let mut scan_count: u64 = 0;
            let mut bear_traded: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut bear_entries_this_window: u32 = 0;
            let mut bear_last_window_clear: u64 = 0;

            // Per-asset bear trend tracker: needs its own EMAs to detect macro trend.
            struct BearTracker {
                prices: std::collections::VecDeque<f64>,
                ema_fast: Option<f64>,  // 10-period
                ema_slow: Option<f64>,  // 30-period
            }
            impl BearTracker {
                fn new() -> Self {
                    Self { prices: std::collections::VecDeque::new(), ema_fast: None, ema_slow: None }
                }
                fn update(&mut self, price: f64) {
                    self.prices.push_back(price);
                    if self.prices.len() > 300 { self.prices.pop_front(); }
                    let af = 2.0 / 11.0;
                    let as_ = 2.0 / 31.0;
                    self.ema_fast = Some(match self.ema_fast { Some(p) => p + af * (price - p), None => price });
                    self.ema_slow = Some(match self.ema_slow { Some(p) => p + as_ * (price - p), None => price });
                }
                fn is_bearish(&self) -> bool {
                    if self.prices.len() < 30 { return false; }
                    // Convert unwrap → graceful skip (return false) so a stale
                    // tracker can't panic the bear-trading task.
                    let Some(&price) = self.prices.back() else { return false; };
                    let ef = self.ema_fast.unwrap_or(price);
                    let es = self.ema_slow.unwrap_or(price);
                    // Bear stack: price < fast EMA < slow EMA
                    price < ef && ef < es
                }
                fn momentum_60s(&self) -> f64 {
                    if self.prices.len() < 60 { return 0.0; }
                    let Some(&now) = self.prices.back() else { return 0.0; };
                    let old = self.prices[self.prices.len() - 60];
                    if old == 0.0 { return 0.0; }
                    (now - old) / old
                }
                fn is_bouncing(&self) -> bool {
                    // Short-term bounce in a downtrend: last 5 ticks going up
                    if self.prices.len() < 10 { return false; }
                    let recent: Vec<f64> = self.prices.iter().rev().take(5).copied().collect();
                    let ups = recent.windows(2).filter(|w| w[0] > w[1]).count();
                    ups >= 3  // 3+ of last 5 ticks are up = temporary bounce
                }
            }

            let mut trackers: std::collections::HashMap<String, BearTracker> = std::collections::HashMap::new();
            for asset in &["BTC", "ETH", "SOL", "XRP", "DOGE"] {
                trackers.insert(asset.to_string(), BearTracker::new());
            }

            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                scan_count += 1;
                if scan_count % 3 != 0 { continue; } // Every 3 seconds

                let btc = match *btc_bear.lock().await {
                    Some(p) if p > 0.0 => p,
                    _ => continue,
                };

                // Update all asset trackers with latest prices.
                // Use `if let Some(...)` instead of unwrap so a missing
                // tracker (e.g. asset list edited at runtime) skips this
                // tick rather than panicking the whole trading task.
                if let Some(t) = trackers.get_mut("BTC") {
                    t.update(btc);
                }
                if let Some(eth) = crypto_prices_bear.read().get("ETH") {
                    if let Some(t) = trackers.get_mut("ETH") {
                        t.update(eth);
                    }
                }
                if let Some(sol) = crypto_prices_bear.read().get("SOL") {
                    if let Some(t) = trackers.get_mut("SOL") {
                        t.update(sol);
                    }
                }
                if let Some(xrp) = crypto_prices_bear.read().get("XRP") {
                    if let Some(t) = trackers.get_mut("XRP") {
                        t.update(xrp);
                    }
                }

                let markets = match exec_bear.list_all_crypto_15m().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                let kill_switch = shared_bear.read().kalshi_kill_switch;
                let bankroll = shared_bear.read().kalshi_balance_cents.unwrap_or(0);

                for market in &markets {
                    if !market.has_liquidity() { continue; }
                    if !market.ticker.contains("15M") && !market.ticker.contains("1H") && !market.ticker.contains("60M") { continue; }
                    if bear_traded.contains(&market.ticker) { continue; }

                    let yes_bid = match market.yes_bid_cents() { Some(b) if b > 0 => b, _ => continue };
                    let yes_ask = match market.yes_ask_cents() { Some(a) if a > 0 => a, _ => continue };
                    if yes_ask - yes_bid > 12 { continue; } // Spread too wide

                    let strike = match market.floor_strike { Some(s) if s > 0.0 => s, _ => continue };
                    let st = market.strike_type.as_deref().unwrap_or("");
                    if st != "greater" && st != "greater_or_equal" { continue; }

                    // Time check: need 5+ minutes.
                    let close_time = market.close_time.as_deref().unwrap_or("");
                    let ttc = if !close_time.is_empty() {
                        chrono::DateTime::parse_from_rfc3339(close_time)
                            .map(|ct| ct.timestamp() as f64 - data::orderbook::now_secs())
                            .unwrap_or(600.0)
                    } else { 600.0 };
                    if ttc < 300.0 { continue; }

                    // Identify which asset this contract is for.
                    let ticker_upper = market.ticker.to_uppercase();
                    let asset_name = if ticker_upper.contains("KXBTC") { "BTC" }
                        else if ticker_upper.contains("KXETH") { "ETH" }
                        else if ticker_upper.contains("KXSOL") { "SOL" }
                        else if ticker_upper.contains("KXXRP") { "XRP" }
                        else if ticker_upper.contains("KXDOGE") { "DOGE" }
                        else { continue };

                    let tracker = match trackers.get(asset_name) {
                        Some(t) => t,
                        None => continue,
                    };

                    // WARMUP: need 60+ ticks (~3 min at 3s updates) before bear EMAs are meaningful.
                    if tracker.prices.len() < 30 { continue; } // ~1.5 min warmup

                    // BEAR CHECK: EMA stack must be bearish for THIS asset.
                    if !tracker.is_bearish() { continue; }

                    // MACRO CHECK: BTC is the market leader. If BTC is bullish,
                    // don't enter NO on ANY crypto — they all correlate.
                    // This prevented the XRP NO loss during a BTC bull rally.
                    let btc_tracker = match trackers.get("BTC") {
                        Some(t) => t,
                        None => continue,
                    };
                    if btc_tracker.prices.len() >= 60 {
                        let btc_ef = btc_tracker.ema_fast.unwrap_or(0.0);
                        let btc_es = btc_tracker.ema_slow.unwrap_or(0.0);
                        if btc_es > 0.0 && (btc_ef - btc_es) / btc_es > 0.0002 {
                            continue; // BTC is bullish — don't enter NO on any asset
                        }
                    }

                    // 60s momentum must be negative (still dropping).
                    let mom = tracker.momentum_60s();
                    if mom >= 0.0 { continue; }

                    // Get spot price.
                    let spot = if asset_name == "BTC" { btc }
                        else if let Some(p) = crypto_prices_bear.read().get(asset_name) { p }
                        else { continue };

                    let dist = (spot - strike) / strike;

                    // BEAR MOMENTUM: price below strike → buy NO.
                    // In a downtrend, NO is the winning side.
                    if dist >= 0.0 { continue; } // Price above strike — skip, not a bear play

                    let no_price = 100 - yes_bid;
                    if no_price > 50 || no_price < 40 { continue; } // 42¢ max

                    // BOUNCE BONUS: if price is temporarily bouncing up, NO is even cheaper.
                    // This is the "fade the bounce" entry — buy NO during dead cat bounces.
                    let is_bounce_entry = tracker.is_bouncing();

                    // Bear bot sizing — reduce contracts on expensive entries.
                    // Data: 49¢ × 4 lost -$1.60. Cheap entries (≤40¢) win more.
                    let bear_max = 2; // Max 2 contracts — risk $0.80, reward $0.40+
                    let max_cost = (bankroll as f64 * 0.10).round() as i64;
                    if no_price > max_cost { continue; }
                    let count = (max_cost / no_price).min(bear_max).max(1);
                    let cost = no_price * count;
                    let exposure = risk_bear.lock().await.total_exposure_cents();
                    if exposure + cost > (bankroll as f64 * 0.50).round() as i64 { continue; }

                    // MAX 1 BEAR ENTRY PER WINDOW
                    if scan_count - bear_last_window_clear > 300 {
                        bear_entries_this_window = 0;
                        bear_last_window_clear = scan_count;
                    }
                    if bear_entries_this_window >= 3 { continue; } // 3 bear entries per window

                    let allowed = {
                        let mut rm = risk_bear.lock().await;
                        rm.set_kill_switch(kill_switch);
                        matches!(rm.check(cost, 0.75), execution::risk::RiskCheckResult::Allowed)
                    };
                    if !allowed { continue; }

                    // GLOBAL GATE
                    {
                        let window_id = (data::orderbook::now_secs() / 900.0) as u64;
                        let mut gate = global_gate_bear.lock().await;
                        if gate.0 == window_id && gate.1 { continue; }
                        if gate.0 != window_id { *gate = (window_id, false); }
                    }
                    {
                        let mut reserved = reserved_bear.lock().await;
                        if reserved.contains(&market.ticker) { continue; }
                        if reserved.contains(asset_name) { continue; }
                        reserved.insert(market.ticker.clone());
                        reserved.insert(asset_name.to_string());
                    }

                    match exec_bear.place_order(&market.ticker, "buy", "no", no_price, count).await {
                        Ok(order) => {
                            // ---- FILL VERIFICATION ----
                            if !KalshiExecutor::is_order_filled(&order.status) {
                                warn!(
                                    "BEAR order {} status='{}' (not filled) — cancelling",
                                    order.order_id, order.status
                                );
                                let _ = exec_bear.cancel_order(&order.order_id).await;
                                reserved_bear.lock().await.remove(&market.ticker);
                                reserved_bear.lock().await.remove(asset_name);
                                continue;
                            }

                            let bounce_tag = if is_bounce_entry { " [FADE-BOUNCE]" } else { "" };
                            info!(
                                "BEAR-MOMENTUM: {} buy no ×{} @ {}¢ dist={:.2}% mom={:.4}%{} → {} [FILLED]",
                                market.ticker, count, no_price,
                                dist * 100.0, mom * 100.0, bounce_tag, order.order_id
                            );
                            let open_pos = execution::risk::OpenPosition {
                                order_id: order.order_id.clone(),
                                ticker: market.ticker.clone(),
                                side: "no".to_owned(),
                                count, price_cents: no_price, cost_cents: cost,
                                opened_at: data::orderbook::now_secs(),
                                signal_id: format!("bear-{}", market.ticker),
                                closed_at: None, pnl_cents: None,
                            };
                            risk_bear.lock().await.record_open(open_pos);
                            shared_bear.write().push_kalshi_order(order);
                            bear_traded.insert(market.ticker.clone());
                            bear_entries_this_window += 1;
                            { let mut g = global_gate_bear.lock().await; g.1 = true; }
                            break;
                        }
                        Err(e) => {
                            warn!("Bear momentum trade failed: {e}");
                            reserved_bear.lock().await.remove(&market.ticker);
                        }
                    }
                }

                // Clear traded tickers every 15 minutes (new contracts).
                if scan_count % 900 == 0 {
                    bear_traded.clear();
                }
            }
        });
        info!("Bear momentum bot started (downtrend strategy, NO-only)");
    }

    // ---- Position exit manager ----
    // Every 10 seconds, checks open positions and exits early:
    // - TAKE PROFIT: if contract is now worth 80¢+ (we bought at <42¢, sell for 80¢ = lock in profit)
    // - CUT LOSS: if contract dropped to <10¢ (cut the loss instead of waiting for full expiry)
    if let Some(exec_exit) = kalshi_executor.clone() {
        let risk_exit = Arc::clone(&risk_manager);
        let shared_exit = Arc::clone(&shared_state);
        let reserved_exit = Arc::clone(&reserved_tickers);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            // Trailing stop: track peak value per position once it hits scalp target.
            // Sell when price drops 5¢ from peak, but never below the scalp target floor.
            let mut peak_values: HashMap<String, i64> = HashMap::new();

            // ---- EXIT TUNING (2026-06-03 upgrade) ----
            // The bot's net loss came from payoff asymmetry: winners were hard-capped at
            // +15¢ while losers rode to expiry (~-45¢). These three knobs fix that:
            const STOP_LOSS_CENTS: i64 = 12; // cut a loser here instead of riding to expiry
            const TRAIL_ARM_CENTS: i64 = 15; // arm the trailing take-profit at +15¢ (old hard target)
            const TRAIL_GAP_CENTS: i64 = 6;  // give back at most 6¢ from the peak before selling
            loop {
                ticker.tick().await;

                // Get our tracked open positions.
                let positions = risk_exit.lock().await.open_positions().to_vec();

                // ---- RELEASE RESERVATIONS FOR CLOSED POSITIONS ----
                // Entry bots reserve BOTH the contract ticker AND the bare asset name
                // (e.g. "BTC") to block double-entries. Positions are closed HERE in the
                // exit manager — the only close path that runs in paper mode — so this is
                // the only correct place to free those reservations. Without it the
                // reserved set leaks and the bare-asset key permanently locks the entire
                // bot fleet out of that asset. Retain a key only while a still-open
                // position matches it (by ticker or asset); free everything fully closed.
                {
                    let asset_of = |t: &str| -> Option<&'static str> {
                        let u = t.to_uppercase();
                        if u.contains("BTC") { Some("BTC") }
                        else if u.contains("ETH") { Some("ETH") }
                        else if u.contains("SOL") { Some("SOL") }
                        else if u.contains("XRP") { Some("XRP") }
                        else if u.contains("DOGE") { Some("DOGE") }
                        else { None }
                    };
                    let open_tickers: std::collections::HashSet<String> =
                        positions.iter().map(|p| p.ticker.clone()).collect();
                    let open_assets: std::collections::HashSet<String> = positions
                        .iter()
                        .filter_map(|p| asset_of(&p.ticker).map(|s| s.to_string()))
                        .collect();
                    let mut reserved = reserved_exit.lock().await;
                    reserved.retain(|k| open_tickers.contains(k) || open_assets.contains(k));
                }

                if positions.is_empty() {
                    peak_values.clear(); // No positions → reset peaks
                    continue;
                }

                // Clean up peaks for positions that no longer exist
                let active_ids: std::collections::HashSet<String> =
                    positions.iter().map(|p| p.order_id.clone()).collect();
                peak_values.retain(|id, _| active_ids.contains(id));

                // Get current market data for each position.
                let markets = match exec_exit.list_all_crypto_15m().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                for pos in &positions {
                    // Find the market for this position.
                    let market = match markets.iter().find(|m| m.ticker == pos.ticker) {
                        Some(m) => m,
                        None => continue, // Market might have settled already.
                    };

                    // What's the current value of our position?
                    let current_value = if pos.side == "yes" {
                        market.yes_bid_cents().unwrap_or(0)
                    } else {
                        // NO value = 100 - yes_ask
                        100 - market.yes_ask_cents().unwrap_or(100)
                    };

                    let entry = pos.price_cents;

                    // ---- PEAK TRACKING (feeds the trailing take-profit below) ----
                    // Only meaningful once a position is in profit.
                    if current_value > entry {
                        let p = peak_values.entry(pos.order_id.clone()).or_insert(current_value);
                        if current_value > *p { *p = current_value; }
                    }
                    let peak_val = peak_values.get(&pos.order_id).copied().unwrap_or(current_value);

                    // ---- PAPER SETTLEMENT (paper mode only) ----
                    // Near expiry, flatten at the live market price so the position settles
                    // at its real outcome (winner ~95-99¢, loser ~1-5¢) and never orphans
                    // when the market drops off the feed. This is what makes the paper engine
                    // produce real, watchable win/loss outcomes.
                    if exec_exit.is_paper() {
                        let close_time = market.close_time.as_deref().unwrap_or("");
                        let tleft = if !close_time.is_empty() {
                            chrono::DateTime::parse_from_rfc3339(close_time)
                                .map(|ct| ct.timestamp() as f64 - data::orderbook::now_secs())
                                .unwrap_or(600.0)
                        } else { 600.0 };
                        if tleft < 30.0 {
                            let sell_side = if pos.side == "yes" { "yes" } else { "no" };
                            let sell_price = current_value.max(1);
                            if let Ok(order) = exec_exit
                                .place_order(&pos.ticker, "sell", sell_side, sell_price, pos.count).await
                            {
                                let pnl = (sell_price - entry) * pos.count;
                                let result = if pnl >= 0 { "WIN" } else { "EXPIRED" };
                                info!(
                                    "PAPER-SETTLE: {} {} x{} @ {}¢ (entry {}¢, pnl {}{}¢) [{}] → {}",
                                    pos.ticker, sell_side, pos.count, sell_price, entry,
                                    if pnl >= 0 { "+" } else { "" }, pnl, result, order.order_id
                                );
                                risk_exit.lock().await.record_close(
                                    &pos.order_id, pnl, data::orderbook::now_secs(),
                                );
                                shared_exit.write().push_kalshi_order(order);
                                peak_values.remove(&pos.order_id);
                                use std::io::Write;
                                if let Ok(mut f) = std::fs::OpenOptions::new()
                                    .create(true).append(true).open("logs/trade_journal.csv")
                                {
                                    let _ = writeln!(f, "{},{},{},{},{},{},{},{},{},{}",
                                        chrono::Utc::now().to_rfc3339(), pos.ticker, pos.side,
                                        result, entry, sell_price, pnl, peak_val, pos.count, pos.signal_id);
                                }
                            }
                            continue;
                        }
                    }

                    // ---- HOLD-TO-SETTLEMENT (fv-hold-) ----
                    // Deep ITM/OTM convergence trades are taken at a fair value that is
                    // pinned near 0/100; their entire edge is paying ONE taker leg in and
                    // settling fee-free. Scalping them out would pay a second 0.07 leg and
                    // usually destroy the thin edge. So they skip stop/trail/time exits and
                    // ride to settlement (paper settles in the block above at <30s; live
                    // rides to actual Kalshi expiry).
                    if pos.signal_id.starts_with("fv-hold-") {
                        continue;
                    }

                    // ---- HARD STOP-LOSS (improvement 2026-06-03) ----
                    // The old engine let losers ride to expiry (~-45¢) while capping winners at
                    // +15¢ — that asymmetry was the bulk of the -$29 net loss. Cut every loser
                    // at -STOP_LOSS_CENTS so a typical loss is about the size of a typical win.
                    if current_value > 0 && current_value <= entry - STOP_LOSS_CENTS {
                        let sell_side = if pos.side == "yes" { "yes" } else { "no" };
                        let sell_price = (current_value - 1).max(1);
                        match exec_exit.place_order(&pos.ticker, "sell", sell_side, sell_price, pos.count).await {
                            Ok(order) => {
                                let loss = entry - sell_price;
                                warn!(
                                    "STOP-LOSS: {} sell {} x{} @ {}¢ (entered {}¢, loss -{}¢/contract) → {}",
                                    pos.ticker, sell_side, pos.count, sell_price, entry, loss, order.order_id
                                );
                                risk_exit.lock().await.record_close(
                                    &pos.order_id, -(loss * pos.count), data::orderbook::now_secs(),
                                );
                                shared_exit.write().push_kalshi_order(order);
                                peak_values.remove(&pos.order_id);
                                use std::io::Write;
                                if let Ok(mut f) = std::fs::OpenOptions::new()
                                    .create(true).append(true).open("logs/trade_journal.csv")
                                {
                                    let _ = writeln!(f, "{},{},{},STOP-LOSS,{},{},{},0,{},{}",
                                        chrono::Utc::now().to_rfc3339(),
                                        pos.ticker, pos.side, entry, sell_price,
                                        -(loss as i64), pos.count, pos.signal_id);
                                }
                            }
                            Err(e) => debug!("Stop-loss sell failed: {e}"),
                        }
                        continue;
                    }

                    // SMART TIME-BASED EXIT:
                    // - If price is FAR from strike (>0.1%) with <3 min left → sell early, salvage
                    // - If price is CLOSE to strike (<0.05%) → hold, could swing back
                    // - At <1 min left → sell whatever remains (last chance)
                    let close_time = market.close_time.as_deref().unwrap_or("");
                    let time_left = if !close_time.is_empty() {
                        chrono::DateTime::parse_from_rfc3339(close_time)
                            .map(|ct| ct.timestamp() as f64 - data::orderbook::now_secs())
                            .unwrap_or(600.0)
                    } else { 600.0 };

                    if current_value > 1 && current_value < entry {  // Was >5, missed deeply losing positions
                        // How far is current value from being profitable?
                        // Low value (under 15¢) = very unlikely to recover = sell early
                        // Medium value (15-35¢) = might recover if strike is close
                        let is_hopeless = current_value <= 15; // Under 15¢ = <15% chance
                        let is_unlikely = current_value <= 30;  // Under 30¢ = <30% chance
                        let clearly_losing = current_value <= entry - 10; // down 10¢+ from entry

                        let should_exit = if time_left < 60.0 && current_value > 1 {
                            true // <1 min left — last chance to recover ANYTHING
                        } else if time_left < 180.0 && is_hopeless {
                            true // <3 min + under 15¢ = unrealistic recovery
                        } else if time_left < 240.0 && is_unlikely && clearly_losing {
                            true // <4 min + under 30¢ + down 10¢+ = salvage partial value now
                                 // instead of riding a probable loser to a ~1¢ expiry (the -100% tail)
                        } else {
                            false // Hold — could still swing back
                        };

                        if should_exit {
                            let sell_side = if pos.side == "yes" { "yes" } else { "no" };
                            let sell_price = current_value - 1;
                            match exec_exit.place_order(&pos.ticker, "sell", sell_side, sell_price, pos.count).await {
                                Ok(order) => {
                                    let loss = entry - sell_price;
                                    let saved = sell_price * pos.count;
                                    warn!(
                                        "TIME-EXIT: {} sell {} x{} @ {}¢ (entered {}¢, loss -{}¢, saved {}¢ vs expiry, {:.0}s left) → {}",
                                        pos.ticker, sell_side, pos.count, sell_price,
                                        entry, loss, saved, time_left, order.order_id
                                    );
                                    risk_exit.lock().await.record_close(
                                        &pos.order_id, -(loss * pos.count), data::orderbook::now_secs(),
                                    );
                                    shared_exit.write().push_kalshi_order(order);
                                    {
                                        use std::io::Write;
                                        if let Ok(mut f) = std::fs::OpenOptions::new()
                                            .create(true).append(true).open("logs/trade_journal.csv")
                                        {
                                            let _ = writeln!(f, "{},{},{},TIME-EXIT,{},{},{},0,{},{}",
                                                chrono::Utc::now().to_rfc3339(),
                                                pos.ticker, pos.side, entry, sell_price,
                                                -(loss as i64), pos.count, pos.signal_id
                                            );
                                        }
                                    }
                                }
                                Err(e) => debug!("Time exit failed: {e}"),
                            }
                            continue;
                        }
                    }

                    // 95¢+ = near certain, let it ride to settlement.
                    if current_value >= 95 { continue; }

                    // ---- TRAILING TAKE-PROFIT (improvement 2026-06-03) ----
                    // The old engine hard-sold every winner at entry+15¢, leaving 40-56¢ on
                    // the table whenever a contract ran. Now: arm at +TRAIL_ARM_CENTS, then
                    // sell only once price gives back TRAIL_GAP_CENTS from its peak. This rides
                    // trends further while still protecting the +15¢ floor that made the scalps
                    // profitable. peak_val is maintained in the peak-tracking block above.
                    let armed = peak_val >= entry + TRAIL_ARM_CENTS;
                    if !(armed && current_value <= peak_val - TRAIL_GAP_CENTS) { continue; }

                    // TRAILING STOP TRIGGERED — SELL NOW.
                    let sell_side = if pos.side == "yes" { "yes" } else { "no" };
                    let sell_price = current_value - 1;
                    if sell_price <= entry { continue; } // never book a "winner" exit at a loss
                    match exec_exit.place_order(&pos.ticker, "sell", sell_side, sell_price, pos.count).await {
                        Ok(order) => {
                            let real_profit = sell_price - entry;
                                info!(
                                    "TAKE PROFIT: {} sell {} x{} @ {}¢ (entered {}¢, profit +{}¢/contract, peak {}¢) → {}",
                                    pos.ticker, sell_side, pos.count, sell_price,
                                    entry, real_profit, peak_val, order.order_id
                                );
                                risk_exit.lock().await.record_close(
                                    &pos.order_id,
                                    real_profit * pos.count,
                                    data::orderbook::now_secs(),
                                );
                                shared_exit.write().push_kalshi_order(order);
                                peak_values.remove(&pos.order_id);

                                // ---- AUTO-LEARN: review every completed trade ----
                                let return_pct = if entry > 0 { real_profit * 100 / entry } else { 0 };
                                let gap_from_peak = peak_val - sell_price;
                                info!(
                                    "TRADE-REVIEW: WIN {} {} entry={}¢ exit={}¢ profit=+{}¢ peak={}¢ gap={}¢ return={}% signal={}",
                                    pos.ticker, pos.side, entry, sell_price, real_profit,
                                    peak_val, gap_from_peak, return_pct, pos.signal_id
                                );
                                // Log to trade journal CSV
                                {
                                    use std::io::Write;
                                    let path = "logs/trade_journal.csv";
                                    let needs_header = !std::path::Path::new(path).exists()
                                        || std::fs::metadata(path).map(|m| m.len() == 0).unwrap_or(true);
                                    if let Ok(mut f) = std::fs::OpenOptions::new()
                                        .create(true).append(true).open(path)
                                    {
                                        if needs_header {
                                            let _ = writeln!(f, "timestamp,ticker,side,result,entry_cents,exit_cents,profit_cents,peak_cents,contracts,signal_id");
                                        }
                                        let _ = writeln!(f, "{},{},{},WIN,{},{},{},{},{},{}",
                                            chrono::Utc::now().to_rfc3339(),
                                            pos.ticker, pos.side, entry, sell_price,
                                            real_profit, peak_val, pos.count, pos.signal_id
                                        );
                                    }
                                }
                            }
                            Err(e) => debug!("Take profit failed: {e}"),
                        }

                    // Losers ride to expiry — handled above by skipping when current < entry.
                }
            }
        });
        info!("Position exit manager started (hard stop -12¢, trailing take-profit: arm +15¢ / trail 6¢ from peak)");
    }

    // ---- SELF-LEARNING ENGINE (runs every 15 minutes) ----
    // Reads the trade journal, analyzes wins vs losses, detects patterns,
    // and writes optimization insights to logs/self_learning.log.
    // The bot uses these insights to get smarter over time.
    {
        let shared_learn = Arc::clone(&shared_state);
        tokio::spawn(async move {
            // Wait 2 minutes for initial trades to happen before first analysis.
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(900)); // 15 min
            loop {
                ticker.tick().await;

                let balance = shared_learn.read().kalshi_balance_cents.unwrap_or(0);

                // Read the trade journal
                let journal = match std::fs::read_to_string("logs/trade_journal.csv") {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                let lines: Vec<&str> = journal.lines().collect();
                if lines.len() < 2 { continue; } // Need header + at least 1 trade

                let mut wins = 0_u32;
                let mut losses = 0_u32;
                let mut total_profit = 0_i64;
                let mut total_loss = 0_i64;
                let mut win_by_asset: std::collections::HashMap<String, (u32, i64)> = std::collections::HashMap::new();
                let mut loss_by_asset: std::collections::HashMap<String, (u32, i64)> = std::collections::HashMap::new();
                let mut win_by_signal: std::collections::HashMap<String, (u32, i64)> = std::collections::HashMap::new();
                let mut cheap_wins = 0_u32;
                let mut cheap_total = 0_u32;
                let mut expensive_wins = 0_u32;
                let mut expensive_total = 0_u32;
                let mut avg_gap_from_peak = 0_i64;
                let mut gap_count = 0_u32;

                for line in &lines[1..] {
                    let parts: Vec<&str> = line.split(',').collect();
                    if parts.len() < 10 { continue; }

                    let ticker_name = parts[1];
                    let result = parts[3];
                    let entry: i64 = parts[4].parse().unwrap_or(0);
                    let exit: i64 = parts[5].parse().unwrap_or(0);
                    let profit: i64 = parts[6].parse().unwrap_or(0);
                    let peak: i64 = parts[7].parse().unwrap_or(0);
                    let contracts: i64 = parts[8].parse().unwrap_or(1);
                    let signal = parts[9];

                    // Extract asset name
                    let asset = if ticker_name.contains("BTC") { "BTC" }
                        else if ticker_name.contains("ETH") { "ETH" }
                        else if ticker_name.contains("SOL") { "SOL" }
                        else if ticker_name.contains("XRP") { "XRP" }
                        else { "OTHER" };

                    // Extract signal type
                    let sig_type = if signal.starts_with("bear") { "bear" }
                        else if signal.starts_with("hc") { "hc" }
                        else if signal.starts_with("direct") { "direct" }
                        else if signal.starts_with("scalp") { "scalp" }
                        else { "other" };

                    if result == "WIN" {
                        wins += 1;
                        let trade_profit = profit * contracts;
                        total_profit += trade_profit;
                        let e = win_by_asset.entry(asset.to_string()).or_insert((0, 0));
                        e.0 += 1; e.1 += trade_profit;
                        let s = win_by_signal.entry(sig_type.to_string()).or_insert((0, 0));
                        s.0 += 1; s.1 += trade_profit;
                        if entry <= 40 { cheap_wins += 1; cheap_total += 1; }
                        else { expensive_wins += 1; expensive_total += 1; }
                        if peak > 0 {
                            avg_gap_from_peak += peak - exit;
                            gap_count += 1;
                        }
                    } else {
                        losses += 1;
                        // Realized loss = (entry - exit) * contracts. Using the actual exit
                        // price (not assuming losers go to 0) credits the salvage/stop exits.
                        let trade_loss = (entry - exit).max(0) * contracts;
                        total_loss += trade_loss;
                        let e = loss_by_asset.entry(asset.to_string()).or_insert((0, 0));
                        e.0 += 1; e.1 += trade_loss;
                        if entry <= 40 { cheap_total += 1; }
                        else { expensive_total += 1; }
                    }
                }

                let total = wins + losses;
                if total == 0 { continue; }
                let win_rate = wins as f64 / total as f64 * 100.0;
                let avg_gap = if gap_count > 0 { avg_gap_from_peak / gap_count as i64 } else { 0 };

                // Build the learning report
                let mut report = format!(
                    "\n=== SELF-LEARNING REPORT: {} ===\n\
                    Balance: ${:.2} | Trades: {} | Wins: {} | Losses: {} | Win Rate: {:.1}%\n\
                    Total Profit: +${:.2} | Total Lost: -${:.2} | Net: ${:.2}\n\
                    Avg gap from peak: {}¢ (trailing stop efficiency)\n\n",
                    chrono::Utc::now().format("%Y-%m-%d %H:%M"),
                    balance as f64 / 100.0, total, wins, losses, win_rate,
                    total_profit as f64 / 100.0, total_loss as f64 / 100.0,
                    (total_profit - total_loss) as f64 / 100.0, avg_gap
                );

                // Asset breakdown
                report += "BY ASSET:\n";
                for asset in &["BTC", "ETH", "SOL", "XRP", "DOGE"] {
                    let w = win_by_asset.get(*asset).unwrap_or(&(0, 0));
                    let l = loss_by_asset.get(*asset).unwrap_or(&(0, 0));
                    report += &format!("  {}: {} wins (+${:.2}), {} losses (-${:.2})\n",
                        asset, w.0, w.1 as f64 / 100.0, l.0, l.1 as f64 / 100.0);
                }

                // Strategy breakdown
                report += "\nBY STRATEGY:\n";
                for sig in &["bear", "hc", "direct", "scalp"] {
                    let w = win_by_signal.get(*sig).unwrap_or(&(0, 0));
                    report += &format!("  {}: {} wins, +${:.2}\n", sig, w.0, w.1 as f64 / 100.0);
                }

                // Entry price analysis
                let cheap_rate = if cheap_total > 0 { cheap_wins as f64 / cheap_total as f64 * 100.0 } else { 0.0 };
                let exp_rate = if expensive_total > 0 { expensive_wins as f64 / expensive_total as f64 * 100.0 } else { 0.0 };
                report += &format!("\nENTRY PRICE:\n  Cheap (≤40¢): {} wins/{} total = {:.0}% win rate\n  Expensive (>40¢): {} wins/{} total = {:.0}% win rate\n",
                    cheap_wins, cheap_total, cheap_rate, expensive_wins, expensive_total, exp_rate);

                // ---- EXPECTANCY (the metric that actually maps to profit) ----
                // Win rate alone is a trap: a 63%-win strategy still loses money if the
                // losers are far bigger than the winners. Judge on net $/trade instead.
                let net_cents = total_profit - total_loss;
                let expectancy_cents = net_cents as f64 / total as f64;          // avg $/trade
                let avg_win = if wins > 0 { total_profit as f64 / wins as f64 } else { 0.0 };
                let avg_loss = if losses > 0 { total_loss as f64 / losses as f64 } else { 0.0 };
                let payoff_ratio = if avg_loss > 0.0 { avg_win / avg_loss } else { 0.0 };
                // Break-even payoff ratio given the current win rate: (1-wr)/wr.
                let wr_frac = wins as f64 / total as f64;
                let breakeven_ratio = if wr_frac > 0.0 { (1.0 - wr_frac) / wr_frac } else { 99.0 };

                report += &format!(
                    "\nEXPECTANCY:\n  Net/trade: {:+.1}¢   Avg win: +{:.0}¢   Avg loss: -{:.0}¢   Payoff: {:.2}x (need ≥ {:.2}x to break even)\n",
                    expectancy_cents, avg_win, avg_loss, payoff_ratio, breakeven_ratio
                );

                // Insights and recommendations — keyed on EXPECTANCY, not win rate.
                report += "\nINSIGHTS:\n";
                if expectancy_cents > 2.0 {
                    report += &format!("  ★ Positive expectancy (+{:.1}¢/trade). The edge is real — consider scaling size.\n", expectancy_cents);
                } else if expectancy_cents > 0.0 {
                    report += &format!("  ✓ Marginally positive (+{:.1}¢/trade). Keep tightening before scaling.\n", expectancy_cents);
                } else {
                    report += &format!("  ✗ NEGATIVE expectancy ({:+.1}¢/trade) despite {:.0}% win rate — winners ({:.0}¢) are too small vs losers ({:.0}¢). Cut the loss tail / raise the entry bar.\n",
                        expectancy_cents, win_rate, avg_win, avg_loss);
                }
                if payoff_ratio > 0.0 && payoff_ratio < breakeven_ratio {
                    report += &format!("  ⚠ Payoff {:.2}x is below the {:.2}x break-even line — this is the core leak.\n", payoff_ratio, breakeven_ratio);
                }

                if avg_gap > 15 {
                    report += &format!("  ⚠ Avg gap from peak is {}¢ — trailing stop is missing tops. Consider tighter trail.\n", avg_gap);
                } else if avg_gap <= 5 {
                    report += "  ★ Trailing stop is efficient (≤5¢ from peak).\n";
                }

                if cheap_rate > exp_rate + 10.0 {
                    report += "  ★ Cheap entries outperforming. Prioritize entries ≤40¢.\n";
                }

                // Write the report
                {
                    use std::io::Write;
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true).append(true).open("logs/self_learning.log")
                    {
                        let _ = write!(f, "{}", report);
                    }
                }

                info!("SELF-LEARNING: Win rate {:.0}% | {} wins / {} total | Balance ${:.2} | Report written",
                    win_rate, wins, total, balance as f64 / 100.0);
            }
        });
        info!("Self-learning engine started (analyzes trades every 15 minutes)");
    }

    // ---- Signal loop ----
    // Runs at ~1 Hz; detects signals and generates recommendations.
    let signal_loop_handle = {
        let shared2 = Arc::clone(&shared_state);
        let predictor2 = Arc::clone(&predictor);
        let sse_tx2 = sse_tx.clone();
        let performance2 = Arc::clone(&performance);
        let logger1 = Arc::clone(&logger);
        let kalshi_exec_loop = kalshi_executor.clone();
        let risk_loop = Arc::clone(&risk_manager);
        let kalshi_cents_loop = kalshi_max_cents;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;

                // Sync detector params.
                {
                    let current = params_arc.lock().await.clone();
                    detector.update_params(current);
                }

                // Attempt signal detection.
                let signal = detector.detect();
                let signal = match signal {
                    Some(s) => s,
                    None => continue,
                };

                // Generate recommendation.
                let rec = predictor2.lock().await.generate_recommendation(&signal).await;
                let rec = match rec {
                    Some(r) => r,
                    None => {
                        info!("Signal fired but no suitable market found");
                        continue;
                    }
                };

                info!(
                    "Signal: {:?} conf={:.2} delta={:.3} vol={:.0}",
                    signal.direction, signal.confidence, signal.primary_delta, signal.total_volume_usdc
                );

                // Log signal.
                if let Err(e) = logger1.lock().await.log_signal(&signal, &rec) {
                    warn!("Log signal error: {e}");
                }

                // Update performance counter.
                performance2.lock().await.increment_signals();

                // Resolve pending count before acquiring the RwLock write guard
                // (parking_lot guards are not Send, so no .await while held).
                let pending_count = predictor2.lock().await.pending_count() + 1;

                // Push to shared state.
                let sr = SignalRecord::from_signal_and_rec(&signal, &rec);
                {
                    let mut s = shared2.write();
                    s.push_signal(sr.clone());
                    s.pending_signals = pending_count;
                }

                // Broadcast SSE.
                let _ = sse_tx2.send(SsePayload::Signal(Box::new(sr)));

                // Add to pending queue.
                predictor2.lock().await.add_pending(rec);

                // ---- Kalshi execution (DISABLED) ----
                // Signal-based execution disabled — only the direct trading loop places orders.
                // This prevents uncontrolled trades from the signal path.
                if false {
                    let exec = kalshi_exec_loop.as_ref().unwrap(); // never reached
                    // Sync kill switch from dashboard SharedState.
                    let kill_switch = shared2.read().kalshi_kill_switch;

                    // Check all risk constraints (acquire lock, check, release).
                    let allowed = {
                        let mut rm = risk_loop.lock().await;
                        rm.set_kill_switch(kill_switch);
                        matches!(
                            rm.check(kalshi_cents_loop, signal.confidence),
                            RiskCheckResult::Allowed
                        )
                    };

                    if allowed {
                        // Find the nearest-expiry active BTC contract.
                        match exec.find_active_btc_contract().await {
                            Ok(Some(contract)) => {
                                let side = match signal.direction {
                                    Direction::Long => "yes",
                                    Direction::Short => "no",
                                    Direction::Neutral => "",
                                };

                                if !side.is_empty() {
                                    if let Some(price) =
                                        KalshiExecutor::compute_limit_price(&contract, side)
                                    {
                                        let count = KalshiExecutor::size_from_confidence(
                                            signal.confidence,
                                            price,
                                            kalshi_cents_loop,
                                        );

                                        match exec
                                            .place_order(
                                                &contract.ticker,
                                                "buy",
                                                side,
                                                price,
                                                count,
                                            )
                                            .await
                                        {
                                            Ok(order) => {
                                                let cost = price * count;
                                                let open_pos = OpenPosition {
                                                    order_id: order.order_id.clone(),
                                                    ticker: contract.ticker.clone(),
                                                    side: side.to_owned(),
                                                    count,
                                                    price_cents: price,
                                                    cost_cents: cost,
                                                    opened_at: now_secs(),
                                                    signal_id: signal.signal_id.clone(),
                                                    closed_at: None,
                                                    pnl_cents: None,
                                                };

                                                // Record open position; get updated stats.
                                                let (pos_count, pnl) = {
                                                    let mut rm = risk_loop.lock().await;
                                                    rm.record_open(open_pos);
                                                    (rm.position_count(), rm.daily_pnl_cents())
                                                };

                                                // Update SharedState (sync, no await while held).
                                                {
                                                    let mut s = shared2.write();
                                                    s.push_kalshi_order(order);
                                                    s.kalshi_open_position_count = pos_count;
                                                    s.kalshi_daily_pnl_cents = pnl;
                                                }
                                            }
                                            Err(e) => warn!("Kalshi place_order: {e}"),
                                        }
                                    } else {
                                        debug!(
                                            "Kalshi: no spread available on {}",
                                            contract.ticker
                                        );
                                    }
                                }
                            }
                            Ok(None) => debug!("Kalshi: no active BTC 15-min contract"),
                            Err(e) => warn!("Kalshi find_active_btc_contract: {e}"),
                        }
                    }
                }
            }
        })
    };

    // ---- Resolution loop ----
    // Runs every 10 s; resolves pending recommendations.
    let resolution_loop_handle = {
        let shared2 = Arc::clone(&shared_state);
        let predictor2 = Arc::clone(&predictor);
        let btc_arc2 = Arc::clone(&btc_price_arc);
        let performance2 = Arc::clone(&performance);
        let sse_tx2 = sse_tx.clone();
        let logger2 = Arc::clone(&logger);
        let lessons_path = cfg.adaptive.lessons_log_path.clone();
        let perf_path = cfg.logging.performance_log.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;

                let btc = (*btc_arc2.lock().await).unwrap_or(0.0);
                if btc <= 0.0 {
                    continue;
                }

                let resolved = predictor2.lock().await.resolve_pending(btc);
                if resolved.is_empty() {
                    continue;
                }

                let mut perf = performance2.lock().await;
                for rec in &resolved {
                    perf.record(rec);

                    // Log outcome.
                    if let Err(e) = logger2.lock().await.log_outcome(rec) {
                        warn!("Log outcome error: {e}");
                    }

                    // Record in learner.
                    let adjustments = learner.record_outcome(rec).await;

                    // Resolve pending count before acquiring the RwLock write guard.
                    let pending_count = predictor2.lock().await.pending_count();

                    // Update shared state with outcome.
                    {
                        let mut s = shared2.write();
                        s.update_signal_outcome(
                            &rec.signal_id,
                            rec.outcome.as_deref().unwrap_or("expired"),
                            rec.pnl_usdc.unwrap_or(0.0),
                        );
                        s.performance = perf.snapshot();
                        s.pending_signals = pending_count;
                        s.lessons = learner.lessons.clone();
                        s.adjustments = learner.get_adjustments().to_vec();
                    }

                    // Broadcast outcome SSE.
                    let _ = sse_tx2.send(SsePayload::Outcome {
                        signal_id: rec.signal_id.clone(),
                        outcome: rec.outcome.clone().unwrap_or_default(),
                        pnl_usdc: rec.pnl_usdc.unwrap_or(0.0),
                    });

                    // Broadcast any parameter adjustments.
                    for adj in adjustments {
                        info!(
                            "Param adjusted: {} {:.4} → {:.4} ({})",
                            adj.parameter, adj.old_value, adj.new_value, adj.reason
                        );
                        let _ = sse_tx2.send(SsePayload::ParamChange(Box::new(adj)));
                    }
                }

                // Persist performance and lessons.
                if let Err(e) = perf.save(&perf_path) {
                    warn!("Save performance error: {e}");
                }
                if let Err(e) = learner.save_lessons(&lessons_path) {
                    warn!("Save lessons error: {e}");
                }
            }
        })
    };

    // ---- Wait for ctrl-c or task failure ----
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C, shutting down");
        }
        res = signal_loop_handle => {
            error!("Signal loop exited: {res:?}");
        }
        res = resolution_loop_handle => {
            error!("Resolution loop exited: {res:?}");
        }
    }

    info!("Shutdown complete");
    Ok(())
}
