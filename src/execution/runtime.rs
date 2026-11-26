use super::{
    kalshi::{KalshiExecutor, KalshiOrder, KalshiPosition},
    ledger::CENT,
    managed::{intent, Coordinator, SharedCoordinator},
};
use crate::{
    config::Config,
    dashboard::{
        self,
        routes::{FvSignal, SharedState, SsePayload},
    },
    data::binance::BinancePriceClient,
    signals::fair_value as fv,
};
use anyhow::{ensure, Context, Result};
use parking_lot::RwLock;
use serde_json::json;
use std::io::Write;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, watch, Mutex};
use tracing::{info, warn};

pub async fn run(
    cfg: Config,
    exec: Arc<KalshiExecutor>,
    shared: Arc<RwLock<SharedState>>,
    smoke: bool,
    fill_probe: bool,
) -> Result<()> {
    ensure!(
        exec.is_demo() && !exec.is_paper(),
        "managed runtime requires demo"
    );
    let mut coordinator = Coordinator::open(
        exec.clone(),
        "logs/execution-ledger.json",
        cfg.kalshi.clone().into(),
    )?;
    let discovered = exec.list_all_crypto_15m().await?;
    coordinator
        .shards
        .extend(discovered.iter().filter_map(|m| m.exchange_index));
    // Startup always observes the exchange before any entry. Uncertain owned
    // orders are canceled and reconciled; unknown external positions are left alone.
    if let Err(e) = coordinator.reconcile(chrono::Utc::now().timestamp()).await {
        warn!("Startup reconciliation: {e}");
    }
    if let Err(e) = coordinator.cancel_pending().await {
        warn!("Startup cancellation: {e}");
    }
    let coordinator: SharedCoordinator = Arc::new(Mutex::new(coordinator));
    if smoke || fill_probe {
        return probe(coordinator, exec, fill_probe).await;
    }
    {
        let c = coordinator.lock().await;
        let mut s = shared.write();
        s.mode = "demo".into();
        s.kalshi_mode = "demo".into();
        s.kalshi_kill_switch = c.ledger.kill_switch;
        s.execution_control = Some(coordinator.clone());
    }
    let detail = Arc::new(RwLock::new(json!({"ready":false,"reason":"starting"})));
    let details = detail.clone();
    let (sse_tx, _) = broadcast::channel::<SsePayload>(32);
    let app = dashboard::routes::router(shared.clone(), sse_tx).route(
        "/api/execution",
        axum::routing::get(move || {
            let d = details.read().clone();
            async move { axum::Json(d) }
        }),
    );
    let app = dashboard::auth::with_auth(
        app,
        std::env::var("DASHBOARD_TOKEN")
            .ok()
            .filter(|s| !s.is_empty()),
    );
    let port = std::env::var("DASHBOARD_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(cfg.dashboard.port);
    let addr = format!("{}:{port}", cfg.dashboard.host);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context("prediction API bind failed; no trading started")?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            warn!("Prediction API stopped: {e}");
        }
    });
    info!("Managed demo prediction API listening on http://{addr}");
    let (tx, _rx) = watch::channel(0.0);
    let feed = BinancePriceClient::new(cfg.binance.ws_url.clone(), tx);
    let prices = feed.crypto_prices.clone();
    tokio::spawn(async move { feed.run().await });
    info!("Kalshi-only demo: fair-value signals, durable execution, Coinbase spot feed; legacy heuristic/Polymarket execution disabled");
    let history = Arc::new(RwLock::new(BTreeMap::<String, VecDeque<f64>>::new()));
    let history_writer = history.clone();
    let sampled_prices = prices.clone();
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticks.tick().await;
            for a in ["BTC", "ETH", "SOL", "XRP", "DOGE"] {
                let p = sampled_prices.read().get(a);
                let mut h = history_writer.write();
                if let Some(p) = p {
                    let b = h.entry(a.into()).or_default();
                    b.push_back(p);
                    if b.len() > 600 {
                        b.pop_front();
                    }
                } else {
                    h.remove(a);
                }
            }
        }
    });
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_reconcile = 0;
    let mut last_scan = 0;
    let mut last_reason = String::new();
    let mut last_audit = 0;
    let mut markets = discovered;
    let mut market_scan_started = None;
    loop {
        tokio::select! { _=tokio::signal::ctrl_c()=>{let mut c=coordinator.lock().await;let _=c.cancel_pending().await;let _=c.reconcile(chrono::Utc::now().timestamp()).await;info!("Prediction service stopped; ledger saved, pending cancellations attempted");return Ok(())},_=interval.tick()=>{} }
        let now = chrono::Utc::now().timestamp();
        if now - last_scan >= 5 {
            last_scan = now;
            let started = Instant::now();
            // Bound discovery so account reconciliation continues during API stalls.
            // Age starts before the scan: early series quotes can age while later
            // series are fetched. The executor cache adds at most two seconds.
            match tokio::time::timeout(Duration::from_secs(8), exec.list_all_crypto_15m()).await {
                Ok(Ok(m)) => {
                    markets = m;
                    market_scan_started = Some(started);
                }
                result => {
                    market_scan_started = None;
                    warn!("Market discovery failed or timed out; entries blocked: {result:?}");
                }
            }
        }
        let mut c = coordinator.lock().await;
        if now - last_reconcile >= 3 {
            c.shards
                .extend(markets.iter().filter_map(|m| m.exchange_index));
            let _ = c.reconcile_cycle(chrono::Utc::now().timestamp()).await;
            last_reconcile = chrono::Utc::now().timestamp();
        }
        if c.reason != last_reason {
            info!("Prediction execution: {}", c.reason);
            last_reason = c.reason.clone();
        }
        // Network awaits above must not freeze time-to-close calculations.
        let now = chrono::Utc::now().timestamp();
        let history = history.read().clone();
        let mut best: Option<(FvSignal, i64, f64, i64, serde_json::Value)> = None;
        for market in markets
            .iter()
            .filter(|_| market_quotes_fresh(market_scan_started))
        {
            let a = if market.ticker.starts_with("KXBTC15M-") {
                "BTC"
            } else if market.ticker.starts_with("KXETH15M-") {
                "ETH"
            } else if market.ticker.starts_with("KXSOL15M-") {
                "SOL"
            } else if market.ticker.starts_with("KXXRP15M-") {
                "XRP"
            } else if market.ticker.starts_with("KXDOGE15M-") {
                "DOGE"
            } else {
                continue;
            };
            let spot = match prices.read().get(a) {
                Some(p) => p,
                None => continue,
            };
            let hist = match history.get(a) {
                Some(h) if h.len() >= 120 => h,
                _ => continue,
            };
            let strike = match market.floor_strike {
                Some(v) if v > 0.0 => v,
                _ => continue,
            };
            if !matches!(
                market.strike_type.as_deref(),
                Some("greater") | Some("greater_or_equal")
            ) {
                continue;
            }
            let ttc = market
                .close_time
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|t| t.timestamp() - now)
                .unwrap_or(-1);
            if ttc < 60 || ttc > 900 || ttc as f64 > cfg.kalshi.fv_max_ttc_secs {
                continue;
            }
            let sigma =
                match fv::realized_vol_per_step(&hist.iter().copied().collect::<Vec<_>>(), 300) {
                    Some(v) => v.max(cfg.kalshi.fv_vol_floor_per_sec),
                    None => continue,
                };
            let p = fv::fair_prob_above(spot, strike, sigma, ttc as f64);

            for side in ["yes", "no"] {
                let price = if side == "yes" {
                    market.yes_ask_cents()
                } else {
                    market.yes_bid_cents().map(|v| 100 - v)
                };
                let price = match price {
                    Some(v) if (1..100).contains(&v) => v,
                    _ => continue,
                };
                let depth = if side == "yes" {
                    &market.yes_ask_size
                } else {
                    &market.yes_bid_size
                };
                if depth
                    .as_deref()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0)
                    < 1.0
                {
                    continue;
                }
                let count = bounded_demo_count(
                    price,
                    depth
                        .as_deref()
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    cfg.kalshi.max_position_cents,
                    cfg.kalshi.fv_max_contracts,
                );
                if count == 0 {
                    continue;
                }
                let conf = match fv::side_confidence(p, side) {
                    Some(v) if v >= cfg.kalshi.confidence_threshold => v,
                    _ => continue,
                };
                let sidefair = match fv::conservative_side_value(p, side) {
                    Some(v) => v,
                    None => continue,
                };
                // Limit asymmetric losses during the prospective demo experiment.
                if !fv::conservative_cost_allowed(price, count, cfg.kalshi.fv_slippage_cents) {
                    continue;
                }
                let net = fv::net_edge_cents(sidefair, price, cfg.kalshi.fv_slippage_cents, count);

                let sig = FvSignal {
                    ts: chrono::Utc::now().to_rfc3339(),
                    ticker: market.ticker.clone(),
                    asset: a.into(),
                    side: side.into(),
                    fair_cents: sidefair,
                    market_cents: price,
                    net_edge_cents: net,
                    ttc_secs: ttc as f64,
                    bucket: "hold".into(),
                    decision: "watch".into(),
                };
                if best
                    .as_ref()
                    .map_or(true, |(old, _, _, _, _)| net > old.net_edge_cents)
                {
                    if let Some(shard) = market.exchange_index {
                        best = Some((
                            sig,
                            shard,
                            conf,
                            count,
                            json!({
                                "policy":"conservative-v2", "p_yes_raw":p,
                                "spot":spot, "strike":strike, "sigma_per_sec":sigma,
                                "side_confidence":conf, "count":count,
                                "model_haircut":0.25, "calibrated":false
                            }),
                        ));
                    }
                }
            }
        }
        if let Some((mut signal, shard, conf, count, inputs)) = best {
            // Persist decision-time inputs before sending an order; do not invent
            // retrospective probabilities from outcomes or dashboard summaries.
            let audit_due = now - last_audit >= 5;
            if audit_due {
                let row = json!({"ts":now,"signal":signal,"inputs":inputs,
                    "entries_paused":c.ledger.kill_switch,"ready":c.ready});
                let saved = (|| -> std::io::Result<()> {
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("logs/prediction-decisions-v2.jsonl")?;
                    writeln!(f, "{row}")?;
                    f.sync_data()
                })();
                if saved.is_err() {
                    c.set_pause(true)?;
                    warn!("Prediction decision audit unavailable; entries paused");
                } else {
                    last_audit = now;
                }
            }
            if audit_due
                && market_quotes_fresh(market_scan_started)
                && cfg.kalshi.fv_enabled
                && c.ready
                && !c.ledger.kill_switch
                && signal.net_edge_cents >= cfg.kalshi.fv_min_edge_cents
            {
                let now = chrono::Utc::now().timestamp();
                let i = intent(
                    &signal.ticker,
                    shard,
                    &signal.side,
                    "buy",
                    signal.market_cents,
                    count,
                    &format!("fv-hold-{}", signal.ticker),
                    now,
                );
                let entry_record = json!({"kind":"entry_attempt","ts":now,
                    "client_id":i.client_id,"signal_id":i.signal_id,
                    "signal":signal,"inputs":inputs});
                let entry_saved = (|| -> std::io::Result<()> {
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("logs/prediction-decisions-v2.jsonl")?;
                    writeln!(f, "{entry_record}")?;
                    f.sync_data()
                })();
                if entry_saved.is_err() {
                    c.set_pause(true)?;
                    warn!("Prediction entry audit failed; no order submitted");
                    continue;
                }
                // Disk audit writes may block too; recheck immediately before submission.
                if !market_quotes_fresh(market_scan_started) {
                    signal.decision = "skip:market_quotes_stale".into();
                } else {
                    match c.place(i, conf, chrono::Utc::now().timestamp()).await {
                        Ok(id) => {
                            info!("Prediction demo order accepted: {id}; awaiting fills");
                            signal.decision = "submitted:reconciling".into();
                        }
                        Err(e) => signal.decision = format!("skip:{e}"),
                    }
                }
            } else {
                signal.decision = if !c.ready {
                    format!("skip:{}", c.reason)
                } else if c.ledger.kill_switch {
                    "skip:paused".into()
                } else if !market_quotes_fresh(market_scan_started) {
                    "skip:market_quotes_stale".into()
                } else {
                    "watch:edge".into()
                };
            }
            shared.write().push_fv_signal(signal);
        }
        let mut status = c.status();
        let quotes_fresh = market_quotes_fresh(market_scan_started) && !markets.is_empty();
        status["market_quotes_fresh"] = json!(quotes_fresh);
        if !quotes_fresh {
            status["entries_enabled"] = json!(false);
            if c.ready && !c.ledger.kill_switch {
                status["reason"] = json!("market_quotes_stale_or_unavailable");
            }
        }
        *detail.write() = status;
        let a = c.ledger.accounting()?;
        let snapshot = c.last_snapshot.clone();
        let mut s = shared.write();
        s.status = if c.ready { "running" } else { "reconciling" }.into();
        s.btc_price = prices.read().get("BTC");
        s.kalshi_enabled = true;
        s.kalshi_kill_switch = c.ledger.kill_switch;
        s.active_markets_count = markets.len();
        s.kalshi_btc_markets = markets.clone();
        // Only prediction shards used by active/owned crypto markets contribute
        // cash; unrelated event shard0 and margin balances are excluded.
        let relevant: std::collections::BTreeSet<_> = markets
            .iter()
            .filter_map(|m| m.exchange_index)
            .chain(c.ledger.intents.values().map(|i| i.shard))
            .collect();
        s.kalshi_balance_cents = Some(
            relevant
                .iter()
                .filter_map(|k| snapshot.balances.get(k))
                .sum(),
        );
        s.kalshi_daily_pnl_cents = c
            .ledger
            .daily_pnl(chrono::Utc::now().timestamp())?
            .div_euclid(CENT);
        s.kalshi_open_position_count = a.positions.len();
        s.kalshi_positions = a
            .positions
            .values()
            .map(|p| KalshiPosition {
                ticker: p.ticker.clone(),
                position: (if p.side == "yes" { p.count } else { -p.count }) as f64 / 100.0,
                market_exposure: Some((p.cost + CENT - 1) / CENT),
                realized_pnl: None,
                unrealized_pnl: None,
                fees_paid: None,
            })
            .collect();
        s.kalshi_recent_orders = snapshot
            .orders
            .iter()
            .filter(|o| {
                c.ledger
                    .intents
                    .values()
                    .any(|i| i.order_id.as_deref() == o["order_id"].as_str())
            })
            .filter_map(|o| serde_json::from_value::<KalshiOrder>(o.clone()).ok())
            .rev()
            .take(20)
            .collect();
    }
}

// Eight seconds from scan start plus the executor's two-second cache limit.
// Monotonic time prevents clock corrections from making old quotes fresh again.
fn market_quotes_fresh(started: Option<Instant>) -> bool {
    started.map_or(false, |t| t.elapsed() < Duration::from_secs(8))
}

async fn probe(c: SharedCoordinator, exec: Arc<KalshiExecutor>, fill: bool) -> Result<()> {
    let mut c = c.lock().await;
    for _ in 0..15 {
        if c.ready {
            break;
        }
        let _ = c.cancel_pending().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = c.reconcile(chrono::Utc::now().timestamp()).await;
    }
    ensure!(c.ready, "preflight blocked: {}", c.reason);
    let markets = exec.list_all_crypto_15m().await?;
    let (market, side, price) = markets
        .iter()
        .filter(|m| m.ticker.starts_with("KXBTC15M-"))
        .find_map(|m| probe_quote(m, fill).map(|(side, price)| (m, side, price)))
        .context("no bounded BTC demo quote")?;
    let now = chrono::Utc::now().timestamp();
    let i = intent(
        &market.ticker,
        market.exchange_index.context("missing shard")?,
        side,
        "buy",
        price,
        1,
        "demo-test",
        now,
    );
    let id = i.client_id.clone();
    let submit = c.place(i, 1.0, now).await;
    // Always reconcile even on ambiguous acknowledgement. No repeated POST.
    for _ in 0..45 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = c.reconcile(chrono::Utc::now().timestamp()).await;
        let _ = c.cancel_pending().await;
        if c.ready {
            break;
        }
    }
    let i = c.ledger.intents.get(&id).context("test intent missing")?;
    let filled = c.ledger.filled(i);
    let pending = c.ledger.unresolved(i);
    let report = json!({"environment":"demo","probe":if fill{"bounded-fill-attempt"}else{"create-cancel"},"client_id":id,"order_id":i.order_id,"filled_contracts":filled as f64 / 100.0,"status":i.status,"reconciled":c.ready,"pending":pending,"submission_acknowledged":submit.is_ok(),"price_cents":price,"checked_at":chrono::Utc::now().to_rfc3339(),"execution":c.status()});
    std::fs::create_dir_all("verification")?;
    std::fs::write(
        if fill {
            "verification/managed-fill-probe.json"
        } else {
            "verification/managed-smoke.json"
        },
        serde_json::to_vec_pretty(&report)?,
    )?;
    ensure!(
        c.ready && !pending,
        "demo probe pending reconciliation: {}",
        c.reason
    );
    ensure!(
        submit.is_ok(),
        "demo probe submission was rejected or uncertain; see report"
    );
    info!("Managed demo probe complete: {} filled contracts, terminal status confirmed. Any position remains durably tracked for settlement.",filled as f64 / 100.0);
    Ok(())
}

fn probe_quote(m: &super::kalshi::KalshiMarket, fill: bool) -> Option<(&'static str, i64)> {
    if !fill {
        return Some(("yes", 1));
    }
    [
        ("yes", m.yes_ask_cents()),
        ("no", m.yes_bid_cents().map(|p| 100 - p)),
    ]
    .into_iter()
    .filter_map(|(side, p)| p.filter(|p| (1..=95).contains(p)).map(|p| (side, p)))
    .min_by_key(|(_, p)| *p)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn market_freshness_fails_closed_and_recovers() {
        assert!(!market_quotes_fresh(None));
        let stale = Instant::now() - Duration::from_secs(9);
        assert!(!market_quotes_fresh(Some(stale)));
        assert!(!market_quotes_fresh(Some(
            Instant::now() - Duration::from_secs(8)
        )));
        assert!(market_quotes_fresh(Some(Instant::now())));
        // A scan invalidated by failure cannot keep authorizing cached candidates.
        let mut scan = Some(Instant::now());
        assert!(market_quotes_fresh(scan));
        scan = None;
        assert!(!market_quotes_fresh(scan));
        scan = Some(Instant::now());
        assert!(market_quotes_fresh(scan));
    }
    #[test]
    fn bounded_probe_handles_both_binary_outcomes() {
        let m:super::super::kalshi::KalshiMarket=serde_json::from_value(json!({"ticker":"KXBTC15M-test","status":"open","yes_bid_dollars":"0.7100","yes_ask_dollars":"1.0000"})).unwrap();
        assert_eq!(probe_quote(&m, true), Some(("no", 29)));
        assert_eq!(probe_quote(&m, false), Some(("yes", 1)));
        let empty:super::super::kalshi::KalshiMarket=serde_json::from_value(json!({"ticker":"KXBTC15M-test","status":"open","yes_bid_dollars":"0.0000","yes_ask_dollars":"1.0000"})).unwrap();
        assert_eq!(probe_quote(&empty, true), None);
    }
}

// Keep the demo size experiment bounded even if configuration is increased later.
fn bounded_demo_count(price: i64, depth: f64, budget: i64, configured: i64) -> i64 {
    if !(1..100).contains(&price) || !depth.is_finite() || depth < 1.0 || budget <= 0 {
        return 0;
    }
    (budget.min(125) / (price + 3))
        .min(configured.clamp(0, 2))
        .min(depth.floor() as i64)
}
#[cfg(test)]
mod sizing_tests {
    use super::bounded_demo_count as size;
    #[test]
    fn size_respects_budget_depth_and_hard_cap() {
        assert_eq!(size(59, 5.0, 125, 2), 2);
        assert_eq!(size(60, 5.0, 125, 2), 1);
        assert_eq!(size(40, 1.0, 125, 2), 1);
        assert_eq!(size(40, 0.0, 125, 2), 0);
        assert_eq!(size(40, f64::NAN, 125, 2), 0);
        assert_eq!(size(40, 10.0, 10000, 100), 2);
        assert_eq!(size(40, 5.0, 125, 0), 0);
    }
}
