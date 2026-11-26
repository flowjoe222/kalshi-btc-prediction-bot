use crate::adaptive::learner::{Lesson, ParameterAdjustment};
use crate::arbitrage::ArbitrageOpportunity;
use crate::execution::kalshi::{KalshiMarket, KalshiOrder, KalshiPosition};
use crate::signals::detector::{DetectorParams, ImbalanceSignal};
use crate::signals::predictor::TradeRecommendation;
use crate::tracking::performance::PerformanceSnapshot;
use axum::{
    extract::State,
    response::{IntoResponse, Response, Sse},
    routing::{get, put},
    Json, Router,
};
use axum::response::sse::{Event, KeepAlive};
use futures_util::stream::Stream;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// A condensed signal record suitable for the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRecord {
    pub signal_id: String,
    pub timestamp: f64,
    pub direction: String,
    pub confidence: f64,
    pub delta: f64,
    pub volume: f64,
    pub market_question: String,
    pub side: String,
    pub entry_price: f64,
    pub outcome: Option<String>,
    pub pnl_usdc: Option<f64>,
}

impl SignalRecord {
    pub fn from_signal_and_rec(signal: &ImbalanceSignal, rec: &TradeRecommendation) -> Self {
        Self {
            signal_id: signal.signal_id.clone(),
            timestamp: signal.timestamp,
            direction: format!("{:?}", signal.direction),
            confidence: signal.confidence,
            delta: signal.primary_delta,
            volume: signal.total_volume_usdc,
            market_question: rec.market.question.clone(),
            side: rec.side.clone(),
            entry_price: rec.entry_price,
            outcome: rec.outcome.clone(),
            pnl_usdc: rec.pnl_usdc,
        }
    }
}

/// A single order-flow history tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowHistoryPoint {
    pub timestamp: f64,
    pub delta: f64,
    pub volume: f64,
    pub buy_pct: f64,
}

/// A single fair-value evaluation the FV bot made on a near-expiry contract,
/// surfaced to the dashboard so the user can see the model's view (fair vs market,
/// net edge, and whether it traded or why it skipped).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FvSignal {
    pub ts: String,
    pub ticker: String,
    pub asset: String,
    pub side: String,
    pub fair_cents: i64,
    pub market_cents: i64,
    pub net_edge_cents: i64,
    pub ttc_secs: f64,
    pub bucket: String,   // "conv" (deep ITM/OTM) | "mid"
    pub decision: String, // "TRADE" | "skip:edge" | "skip:risk" | ...
}

/// All mutable runtime state shared between the signal loop and the dashboard.
pub struct SharedState {
    pub execution_control: Option<crate::execution::managed::SharedCoordinator>,
    pub status: String,
    pub mode: String,
    pub started_at: Instant,
    pub btc_price: Option<f64>,
    pub flow_delta: f64,
    pub flow_volume: f64,
    pub flow_buy_pct: f64,
    pub performance: PerformanceSnapshot,
    pub current_params: DetectorParams,
    /// Last 50 signals.
    pub recent_signals: VecDeque<SignalRecord>,
    pub lessons: Vec<Lesson>,
    pub adjustments: Vec<ParameterAdjustment>,
    pub active_markets_count: usize,
    pub pending_signals: usize,
    /// Last 60 flow ticks.
    pub flow_history: VecDeque<FlowHistoryPoint>,
    // ---- Kalshi ----
    pub kalshi_enabled: bool,
    pub kalshi_mode: String,
    pub kalshi_balance_cents: Option<i64>,
    pub kalshi_positions: Vec<KalshiPosition>,
    pub kalshi_recent_orders: Vec<KalshiOrder>,
    pub kalshi_btc_markets: Vec<KalshiMarket>,
    pub kalshi_kill_switch: bool,
    pub kalshi_daily_pnl_cents: i64,
    pub kalshi_open_position_count: usize,
    /// Most recent arbitrage scan results (latest scan's opportunities).
    pub arbitrage_opportunities: Vec<ArbitrageOpportunity>,
    /// Unix timestamp of the last completed arbitrage scan.
    pub last_arb_scan_at: Option<f64>,
    /// Last ~40 fair-value evaluations from the FV bot (for the dashboard FV card).
    pub fv_signals: VecDeque<FvSignal>,
}

impl SharedState {
    pub fn new(mode: String, params: DetectorParams) -> Self {
        Self {
            execution_control: None,
            status: "starting".to_owned(),
            mode,
            started_at: Instant::now(),
            btc_price: None,
            flow_delta: 0.0,
            flow_volume: 0.0,
            flow_buy_pct: 0.5,
            performance: PerformanceSnapshot::default(),
            current_params: params,
            recent_signals: VecDeque::new(),
            lessons: Vec::new(),
            adjustments: Vec::new(),
            active_markets_count: 0,
            pending_signals: 0,
            flow_history: VecDeque::new(),
            kalshi_enabled: false,
            kalshi_mode: "demo".to_owned(),
            kalshi_balance_cents: None,
            kalshi_positions: Vec::new(),
            kalshi_recent_orders: Vec::new(),
            kalshi_btc_markets: Vec::new(),
            kalshi_kill_switch: false,
            kalshi_daily_pnl_cents: 0,
            kalshi_open_position_count: 0,
            arbitrage_opportunities: Vec::new(),
            last_arb_scan_at: None,
            fv_signals: VecDeque::new(),
        }
    }

    /// Push a fair-value evaluation into the ring buffer (max 40, newest at back).
    pub fn push_fv_signal(&mut self, sig: FvSignal) {
        if self.fv_signals.len() >= 40 {
            self.fv_signals.pop_front();
        }
        self.fv_signals.push_back(sig);
    }

    /// Push a new Kalshi order into the recent-orders ring buffer (max 20).
    pub fn push_kalshi_order(&mut self, order: KalshiOrder) {
        if self.kalshi_recent_orders.len() >= 20 {
            self.kalshi_recent_orders.remove(0);
        }
        self.kalshi_recent_orders.push(order);
    }

    pub fn push_signal(&mut self, record: SignalRecord) {
        if self.recent_signals.len() >= 50 {
            self.recent_signals.pop_front();
        }
        self.recent_signals.push_back(record);
    }

    pub fn push_flow_point(&mut self, point: FlowHistoryPoint) {
        if self.flow_history.len() >= 60 {
            self.flow_history.pop_front();
        }
        self.flow_history.push_back(point);
    }

    /// Replace the current arbitrage opportunities list with fresh scan results.
    pub fn update_arbitrage(&mut self, opportunities: Vec<ArbitrageOpportunity>) {
        use std::time::{SystemTime, UNIX_EPOCH};
        self.arbitrage_opportunities = opportunities;
        self.last_arb_scan_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs_f64());
    }

    /// Update a signal's outcome in-place after resolution.
    pub fn update_signal_outcome(&mut self, signal_id: &str, outcome: &str, pnl: f64) {
        for rec in &mut self.recent_signals {
            if rec.signal_id == signal_id {
                rec.outcome = Some(outcome.to_owned());
                rec.pnl_usdc = Some(pnl);
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SSE event types
// ---------------------------------------------------------------------------

/// Events broadcast over the SSE feed.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum SsePayload {
    Signal(Box<SignalRecord>),
    Outcome {
        signal_id: String,
        outcome: String,
        pnl_usdc: f64,
    },
    ParamChange(Box<ParameterAdjustment>),
    Heartbeat {
        timestamp: f64,
        btc_price: Option<f64>,
        flow_delta: f64,
    },
    Snapshot(Box<SnapshotResponse>),
}

// ---------------------------------------------------------------------------
// API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotResponse {
    pub status: String,
    pub mode: String,
    pub uptime_secs: u64,
    pub btc_price: Option<f64>,
    pub flow_delta: f64,
    pub flow_volume: f64,
    pub flow_buy_pct: f64,
    pub performance: PerformanceSnapshot,
    pub current_params: DetectorParams,
    pub recent_signals: Vec<SignalRecord>,
    pub lessons: Vec<Lesson>,
    pub adjustments: Vec<ParameterAdjustment>,
    pub active_markets_count: usize,
    pub pending_signals: usize,
    pub flow_history: Vec<FlowHistoryPoint>,
    pub kalshi_enabled: bool,
    pub kalshi_mode: String,
    pub kalshi_balance_cents: Option<i64>,
    pub kalshi_positions: Vec<KalshiPosition>,
    pub kalshi_recent_orders: Vec<KalshiOrder>,
    pub kalshi_btc_markets: Vec<KalshiMarket>,
    pub kalshi_kill_switch: bool,
    pub kalshi_daily_pnl_cents: i64,
    pub kalshi_open_position_count: usize,
}

fn build_snapshot(state: &SharedState) -> SnapshotResponse {
    SnapshotResponse {
        status: state.status.clone(),
        mode: state.mode.clone(),
        uptime_secs: state.started_at.elapsed().as_secs(),
        btc_price: state.btc_price,
        flow_delta: state.flow_delta,
        flow_volume: state.flow_volume,
        flow_buy_pct: state.flow_buy_pct,
        performance: state.performance.clone(),
        current_params: state.current_params.clone(),
        recent_signals: state.recent_signals.iter().cloned().collect(),
        lessons: state.lessons.clone(),
        adjustments: state.adjustments.clone(),
        active_markets_count: state.active_markets_count,
        pending_signals: state.pending_signals,
        flow_history: state.flow_history.iter().cloned().collect(),
        kalshi_enabled: state.kalshi_enabled,
        kalshi_mode: state.kalshi_mode.clone(),
        kalshi_balance_cents: state.kalshi_balance_cents,
        kalshi_positions: state.kalshi_positions.clone(),
        kalshi_recent_orders: state.kalshi_recent_orders.clone(),
        kalshi_btc_markets: state.kalshi_btc_markets.clone(),
        kalshi_kill_switch: state.kalshi_kill_switch,
        kalshi_daily_pnl_cents: state.kalshi_daily_pnl_cents,
        kalshi_open_position_count: state.kalshi_open_position_count,
    }
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

type AppState = Arc<(Arc<RwLock<SharedState>>, broadcast::Sender<SsePayload>)>;

pub fn router(
    shared: Arc<RwLock<SharedState>>,
    sse_tx: broadcast::Sender<SsePayload>,
) -> Router {
    let state: AppState = Arc::new((shared, sse_tx));
    Router::new()
        .route("/", get(handler_index))
        .route("/pro", get(handler_pro))
        .route("/api/expectancy", get(handler_expectancy))
        .route("/api/fv-signals", get(handler_fv_signals))
        .route("/api/snapshot", get(handler_snapshot))
        .route("/api/performance", get(handler_performance))
        .route("/api/signals", get(handler_signals))
        .route("/api/params", get(handler_params))
        .route("/api/lessons", get(handler_lessons))
        .route("/api/flow-history", get(handler_flow_history))
        .route("/api/status", get(handler_status))
        // Kalshi routes
        .route("/api/kalshi/status", get(handler_kalshi_status))
        .route("/api/kalshi/positions", get(handler_kalshi_positions))
        .route("/api/kalshi/markets", get(handler_kalshi_markets))
        .route("/api/kalshi/orders", get(handler_kalshi_orders))
        // PUT (idempotent set) — body `{ "kill_switch": true | false }`.
        // We removed the legacy POST toggle so a replayed request can't
        // accidentally re-enable trading after an operator disabled it.
        .route("/api/kalshi/kill-switch", put(handler_kalshi_kill_switch_set))
        .route("/api/arbitrage/opportunities", get(handler_arbitrage))
        .route("/feed", get(handler_sse))
        .with_state(state)
}

async fn handler_index() -> impl IntoResponse {
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(DASHBOARD_HTML.to_owned())
        .unwrap()
}

// ---------------------------------------------------------------------------
// Pro dashboard: expectancy-first view of real edge metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct TradePoint {
    t: String,
    ticker: String,
    signal: String,
    result: String,
    pnl_cents: i64,
    cum_cents: i64,
}

/// Edge metrics computed from the trade journal. Realized P&L per trade is
/// `(exit - entry) * contracts`, which is correct across every row type
/// (WIN / STOP-LOSS / TIME-EXIT / EXPIRED / PAPER-SETTLE) regardless of how the
/// `profit_cents` column was written.
#[derive(Debug, Clone, Serialize)]
struct ExpectancyResponse {
    total: u32,
    wins: u32,
    losses: u32,
    win_rate: f64,
    net_cents: i64,
    expectancy_cents: f64,
    avg_win_cents: f64,
    avg_loss_cents: f64,
    payoff_ratio: f64,
    breakeven_ratio: f64,
    today_net_cents: i64,
    today_trades: u32,
    equity_curve: Vec<i64>,
    recent: Vec<TradePoint>,
    by_signal: Vec<(String, u32, i64)>,
}

async fn handler_expectancy() -> impl IntoResponse {
    let journal = std::fs::read_to_string("logs/trade_journal.csv").unwrap_or_default();
    let (mut total, mut wins, mut losses) = (0u32, 0u32, 0u32);
    let (mut gross_win, mut gross_loss, mut net, mut cum) = (0i64, 0i64, 0i64, 0i64);
    let mut equity: Vec<i64> = Vec::new();
    let mut recent: Vec<TradePoint> = Vec::new();
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let (mut today_net, mut today_trades) = (0i64, 0u32);
    let mut by_signal: std::collections::HashMap<String, (u32, i64)> =
        std::collections::HashMap::new();

    for line in journal.lines().skip(1) {
        let p: Vec<&str> = line.split(',').collect();
        if p.len() < 10 {
            continue;
        }
        let entry: i64 = p[4].parse().unwrap_or(0);
        let exit: i64 = p[5].parse().unwrap_or(0);
        let contracts: i64 = p[8].parse().unwrap_or(1);
        let pnl = (exit - entry) * contracts;
        total += 1;
        net += pnl;
        cum += pnl;
        equity.push(cum);
        if pnl >= 0 {
            wins += 1;
            gross_win += pnl;
        } else {
            losses += 1;
            gross_loss += -pnl;
        }
        let signal = p[9];
        let sig_type = if signal.starts_with("bear") {
            "bear"
        } else if signal.starts_with("hc") {
            "hc"
        } else if signal.starts_with("direct") {
            "direct"
        } else if signal.starts_with("scalp") {
            "scalp"
        } else {
            "other"
        };
        let e = by_signal.entry(sig_type.to_string()).or_insert((0, 0));
        e.0 += 1;
        e.1 += pnl;
        if p[0].starts_with(&today) {
            today_net += pnl;
            today_trades += 1;
        }
        recent.push(TradePoint {
            t: p[0].to_string(),
            ticker: p[1].to_string(),
            signal: signal.to_string(),
            result: p[3].to_string(),
            pnl_cents: pnl,
            cum_cents: cum,
        });
    }

    let wr = if total > 0 { wins as f64 / total as f64 } else { 0.0 };
    let avg_win = if wins > 0 { gross_win as f64 / wins as f64 } else { 0.0 };
    let avg_loss = if losses > 0 { gross_loss as f64 / losses as f64 } else { 0.0 };
    let payoff = if avg_loss > 0.0 { avg_win / avg_loss } else { 0.0 };
    let breakeven = if wr > 0.0 { (1.0 - wr) / wr } else { 0.0 };
    let expectancy = if total > 0 { net as f64 / total as f64 } else { 0.0 };

    let equity_curve: Vec<i64> = equity.iter().rev().take(200).rev().cloned().collect();
    recent.reverse();
    recent.truncate(25);
    let mut by_sig: Vec<(String, u32, i64)> =
        by_signal.into_iter().map(|(k, (c, n))| (k, c, n)).collect();
    by_sig.sort_by_key(|x| x.2);

    Json(ExpectancyResponse {
        total,
        wins,
        losses,
        win_rate: wr * 100.0,
        net_cents: net,
        expectancy_cents: expectancy,
        avg_win_cents: avg_win,
        avg_loss_cents: avg_loss,
        payoff_ratio: payoff,
        breakeven_ratio: breakeven,
        today_net_cents: today_net,
        today_trades,
        equity_curve,
        recent,
        by_signal: by_sig,
    })
}

async fn handler_pro() -> impl IntoResponse {
    Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(PRO_DASHBOARD_HTML.to_owned())
        .unwrap()
}

/// Recent fair-value evaluations (newest first) for the dashboard FV card.
async fn handler_fv_signals(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.0.read();
    let mut v: Vec<FvSignal> = s.fv_signals.iter().cloned().collect();
    v.reverse();
    Json(v)
}

const PRO_DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/>
<title>Edge Monitor</title>
<style>
  :root{
    --bg:#000000; --bg2:#0a0a0c; --card:#1c1c1e; --card2:#2c2c2e;
    --sep:#38383a; --label:#ffffff; --label2:rgba(235,235,245,.60); --label3:rgba(235,235,245,.30);
    --green:#34c759; --red:#ff3b30; --blue:#0a84ff; --orange:#ff9f0a; --teal:#64d2ff; --purple:#bf5af2;
    --r:18px;
  }
  *{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
  html,body{margin:0;background:var(--bg);color:var(--label);
    font-family:-apple-system,BlinkMacSystemFont,'SF Pro Display','SF Pro Text','Segoe UI',Roboto,sans-serif;
    -webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
  .num{font-variant-numeric:tabular-nums;font-feature-settings:"tnum"}
  header{position:sticky;top:0;z-index:20;backdrop-filter:saturate(180%) blur(20px);
    -webkit-backdrop-filter:saturate(180%) blur(20px);background:rgba(0,0,0,.72);
    border-bottom:.5px solid var(--sep);padding:14px 20px 12px;
    padding-top:max(14px,env(safe-area-inset-top))}
  .titlerow{display:flex;align-items:flex-end;justify-content:space-between;gap:12px}
  .ltitle{font-size:30px;font-weight:800;letter-spacing:-.5px;display:flex;align-items:center;gap:10px}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--green);
    box-shadow:0 0 0 0 rgba(52,199,89,.6);animation:pulse 2.2s infinite}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(52,199,89,.5)}70%{box-shadow:0 0 0 7px rgba(52,199,89,0)}100%{box-shadow:0 0 0 0 rgba(52,199,89,0)}}
  .chips{display:flex;gap:8px;align-items:center;margin-top:10px;flex-wrap:wrap}
  .chip{font-size:13px;color:var(--label2);background:var(--card);border-radius:11px;padding:6px 11px;
    display:flex;gap:6px;align-items:center}
  .chip b{color:var(--label);font-weight:600}
  .badge{font-size:11px;font-weight:700;letter-spacing:.4px;padding:4px 9px;border-radius:8px;text-transform:uppercase}
  .badge.paper{background:rgba(255,159,10,.18);color:var(--orange)}
  .badge.live{background:rgba(52,199,89,.18);color:var(--green)}
  main{max-width:780px;margin:0 auto;padding:16px 16px 64px}
  .banner{background:rgba(255,159,10,.12);border:.5px solid rgba(255,159,10,.35);color:var(--orange);
    border-radius:14px;padding:11px 14px;font-size:13px;line-height:1.45;margin-bottom:16px}
  .grid{display:grid;gap:12px}
  .kpis{grid-template-columns:1fr 1fr;margin-bottom:12px}
  .card{background:var(--card);border-radius:var(--r);padding:16px;box-shadow:0 1px 0 rgba(255,255,255,.03) inset}
  .card h3{margin:0 0 4px;font-size:13px;font-weight:600;color:var(--label2);letter-spacing:.2px}
  .sectit{font-size:20px;font-weight:700;letter-spacing:-.3px;margin:22px 4px 12px}
  .kpi .k-label{font-size:13px;color:var(--label2);font-weight:500}
  .kpi .k-val{font-size:30px;font-weight:800;letter-spacing:-.5px;margin-top:7px;line-height:1}
  .kpi .k-sub{font-size:12px;color:var(--label3);margin-top:7px}
  .pos{color:var(--green)} .neg{color:var(--red)} .mut{color:var(--label2)} .amber{color:var(--orange)} .blu{color:var(--blue)}
  svg{display:block;width:100%}
  .fvrow{padding:12px 0;border-bottom:.5px solid var(--sep)}
  .fvrow:last-child{border-bottom:none}
  .fvhead{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:9px}
  .fvtk{font-size:14px;font-weight:600}
  .fvtk .asset{color:var(--label3);font-weight:500;font-size:12px;margin-left:6px}
  .pill{font-size:11px;font-weight:700;padding:3px 8px;border-radius:7px;letter-spacing:.3px}
  .pill.trade{background:rgba(52,199,89,.18);color:var(--green)}
  .pill.gate{background:rgba(255,159,10,.16);color:var(--orange)}
  .pill.watch{background:rgba(235,235,245,.10);color:var(--label2)}
  .pill.stop{background:rgba(255,59,48,.16);color:var(--red)}
  .pill.conv{background:rgba(10,132,255,.16);color:var(--blue)}
  .pill.mid{background:rgba(191,90,242,.16);color:var(--purple)}
  .track{position:relative;height:8px;border-radius:5px;background:linear-gradient(90deg,#3a2020,#2c2c2e 50%,#1f3320);margin:7px 0 6px}
  .tick{position:absolute;top:-3px;width:3px;height:14px;border-radius:2px;transform:translateX(-50%)}
  .tick.fair{background:var(--blue)} .tick.mkt{background:#fff}
  .fvmeta{display:flex;justify-content:space-between;font-size:12px;color:var(--label2)}
  .fvmeta .net{font-weight:700}
  table{width:100%;border-collapse:collapse;font-size:13.5px}
  th{text-align:left;color:var(--label3);font-weight:600;font-size:11px;letter-spacing:.4px;text-transform:uppercase;padding:6px 6px 9px}
  td{padding:9px 6px;border-top:.5px solid var(--sep)}
  .tag{font-size:11px;padding:2px 7px;border-radius:7px;background:var(--card2);color:var(--label2);font-weight:600}
  .tag.fv{background:rgba(10,132,255,.18);color:var(--blue)}
  .tag.hold{background:rgba(100,210,255,.16);color:var(--teal)}
  .empty{color:var(--label3);font-size:13px;padding:20px 6px;text-align:center}
  .verdict{font-size:13.5px;line-height:1.5;padding:12px 14px;border-radius:13px;margin-top:12px}
  .verdict.bad{background:rgba(255,59,48,.10);border:.5px solid rgba(255,59,48,.3)}
  .verdict.ok{background:rgba(52,199,89,.10);border:.5px solid rgba(52,199,89,.3)}
  .barwrap{display:flex;flex-direction:column;gap:9px;margin-top:4px}
  .abar{height:30px;border-radius:9px;display:flex;align-items:center;padding:0 11px;font-size:13px;font-weight:600;min-width:46px;white-space:nowrap}
  .abar.win{background:linear-gradient(90deg,#13351f,#1f7a39);color:#d6ffe2}
  .abar.loss{background:linear-gradient(90deg,#3a1614,#9c2a22);color:#ffd9d4;justify-content:flex-end}
  .skel{background:linear-gradient(90deg,#1c1c1e,#252528,#1c1c1e);background-size:200% 100%;animation:sh 1.4s infinite;border-radius:6px;height:14px}
  @keyframes sh{0%{background-position:200% 0}100%{background-position:-200% 0}}
  .ftr{text-align:center;color:var(--label3);font-size:12px;margin-top:26px}
  a{color:var(--blue);text-decoration:none}
</style>
</head>
<body>
<header>
  <div class="titlerow">
    <div class="ltitle"><span class="dot" id="dot"></span>Edge Monitor</div>
    <span class="badge paper" id="modebadge">—</span>
  </div>
  <div class="chips">
    <span class="chip">BTC <b class="num" id="btc">—</b></span>
    <span class="chip">Balance <b class="num" id="bal">—</b></span>
    <span class="chip">Today <b class="num" id="today">—</b></span>
    <span class="chip">Uptime <b class="num" id="up">—</b></span>
  </div>
</header>
<main>
  <div class="banner" id="paperbanner" style="display:none">
    <b>Paper mode.</b> Fills are idealized — instant, at the quoted price, with zero fees or slippage.
    Live P&amp;L will be lower. Treat these numbers as an upper bound and judge the model by calibration, not paper profit.
  </div>

  <div class="grid kpis">
    <div class="card kpi"><div class="k-label">Net P&amp;L · all-time</div><div class="k-val num" id="net">—</div><div class="k-sub" id="trades">—</div></div>
    <div class="card kpi"><div class="k-label">Expectancy / trade</div><div class="k-val num" id="exp">—</div><div class="k-sub">$ per trade — the real metric</div></div>
    <div class="card kpi"><div class="k-label">Payoff ratio</div><div class="k-val num" id="payoff">—</div><div class="k-sub num" id="be">—</div></div>
    <div class="card kpi"><div class="k-label">Win rate</div><div class="k-val num" id="wr">—</div><div class="k-sub num" id="wl">—</div></div>
  </div>

  <div class="sectit">Fair Value · live model</div>
  <div class="card">
    <h3>What the model sees right now — fair value vs the market quote, net of fees</h3>
    <div id="fvlist" style="margin-top:6px"><div class="skel" style="width:80%;margin:14px 0"></div><div class="skel" style="width:60%;margin:14px 0"></div></div>
  </div>

  <div class="sectit">Performance</div>
  <div class="card">
    <h3>Equity curve · cumulative ¢</h3>
    <svg id="eq" viewBox="0 0 600 130" preserveAspectRatio="none" style="height:130px"></svg>
    <div class="verdict" id="verdict">Loading…</div>
  </div>

  <div class="grid" style="grid-template-columns:1fr;margin-top:12px">
    <div class="card">
      <h3>Payoff asymmetry</h3>
      <div class="barwrap">
        <div class="abar win" id="barwin">—</div>
        <div class="abar loss" id="barloss">—</div>
      </div>
    </div>
  </div>

  <div class="sectit">By strategy</div>
  <div class="card">
    <h3>Win/P&amp;L by signal prefix — <span class="blu">fv-</span> is the new fair-value bot</h3>
    <table><thead><tr><th>Signal</th><th style="text-align:right">Trades</th><th style="text-align:right">Net ¢</th></tr></thead>
    <tbody id="bysig"><tr><td colspan="3" class="empty">No trades yet</td></tr></tbody></table>
  </div>

  <div class="sectit">Open positions</div>
  <div class="card">
    <table><thead><tr><th>Market</th><th>Side</th><th style="text-align:right">Qty</th><th style="text-align:right">Exposure</th></tr></thead>
    <tbody id="positions"><tr><td colspan="4" class="empty">None open</td></tr></tbody></table>
  </div>

  <div class="sectit">Recent trades</div>
  <div class="card">
    <table><thead><tr><th>Time</th><th>Market</th><th>Signal</th><th style="text-align:right">P&amp;L</th></tr></thead>
    <tbody id="recent"><tr><td colspan="4" class="empty">No trades yet</td></tr></tbody></table>
  </div>

  <div class="ftr">Refreshes every 2s · <a href="/">classic dashboard</a></div>
</main>

<script>
const $=id=>document.getElementById(id);
const usd=c=>(c<0?"-":"+")+"$"+Math.abs(c/100).toFixed(2);
const cls=v=>v>0?"pos":v<0?"neg":"mut";
const up=s=>{s=s||0;const h=Math.floor(s/3600),m=Math.floor(s%3600/60);return h+"h "+m+"m";};
async function J(u){try{const r=await fetch(u);if(!r.ok)return null;return await r.json();}catch(e){return null;}}

function drawEq(curve){
  const svg=$("eq");
  if(!curve||curve.length<2){svg.innerHTML='<text x="12" y="68" fill="rgba(235,235,245,.3)" font-size="13" font-family="-apple-system">Not enough trades yet</text>';return;}
  const W=600,H=130,pad=8;
  const mn=Math.min(...curve,0),mx=Math.max(...curve,0),rng=(mx-mn)||1;
  const X=i=>pad+i*(W-2*pad)/(curve.length-1);
  const Y=v=>H-pad-(v-mn)/rng*(H-2*pad);
  const zy=Y(0);
  const last=curve[curve.length-1],col=last>=0?"#34c759":"#ff3b30";
  let d="M"+X(0)+","+Y(curve[0]);
  for(let i=1;i<curve.length;i++)d+=" L"+X(i)+","+Y(curve[i]);
  const area=d+" L"+X(curve.length-1)+","+zy+" L"+X(0)+","+zy+" Z";
  svg.innerHTML=
    '<line x1="0" y1="'+zy+'" x2="'+W+'" y2="'+zy+'" stroke="#38383a" stroke-width="1"/>'+
    '<path d="'+area+'" fill="'+col+'" opacity="0.12"/>'+
    '<path d="'+d+'" fill="none" stroke="'+col+'" stroke-width="2.5" stroke-linejoin="round"/>';
}

function fvRow(s){
  const fair=Math.max(0,Math.min(100,s.fair_cents));
  const mkt=Math.max(0,Math.min(100,s.market_cents));
  const net=s.net_edge_cents;
  const dec=s.decision||"watch";
  let pc="watch",pt=dec;
  if(dec==="TRADE"){pc="trade";pt="TRADED";}
  else if(dec.indexOf("daily-stop")>=0){pc="stop";pt="DAILY STOP";}
  else if(dec.indexOf("gate")>=0){pc="gate";pt="EDGE OK · HELD";}
  else {pc="watch";pt="EDGE TOO THIN";}
  const bk=s.bucket==="conv"?"<span class='pill conv'>CONVERGENCE</span>":"<span class='pill mid'>MID</span>";
  const ttc=s.ttc_secs>0?Math.floor(s.ttc_secs/60)+":"+String(Math.floor(s.ttc_secs%60)).padStart(2,"0"):"—";
  return "<div class='fvrow'>"+
    "<div class='fvhead'><div class='fvtk'>"+s.ticker+"<span class='asset'>"+(s.asset||"")+"</span></div>"+
    "<div style='display:flex;gap:6px;align-items:center'>"+bk+"<span class='pill "+pc+"'>"+pt+"</span></div></div>"+
    "<div class='track'><div class='tick fair' style='left:"+fair+"%'></div><div class='tick mkt' style='left:"+mkt+"%'></div></div>"+
    "<div class='fvmeta'><span>fair <b class='blu num'>"+s.fair_cents+"¢</b> · mkt <b class='num'>"+s.market_cents+"¢</b> ("+s.side+")</span>"+
    "<span class='net "+(net>=0?'pos':'neg')+"'>net "+(net>=0?"+":"")+net+"¢</span></div>"+
    "<div class='fvmeta' style='margin-top:3px'><span>expires in "+ttc+"</span></div></div>";
}

async function tick(){
  const [st,ex,fvs,kp,ks]=await Promise.all([
    J("/api/status"),J("/api/expectancy"),J("/api/fv-signals"),J("/api/kalshi/positions"),J("/api/kalshi/status")
  ]);
  if(st){
    $("btc").textContent=st.btc_price?("$"+Math.round(st.btc_price).toLocaleString()):"—";
    $("up").textContent=up(st.uptime_secs);
    $("dot").style.background=st.status==="running"?"#34c759":"#ff3b30";
    const paper=(st.mode||"").toLowerCase()!=="live";
    const mb=$("modebadge");
    mb.textContent=paper?"Paper":"Live";mb.className="badge "+(paper?"paper":"live");
    $("paperbanner").style.display=paper?"block":"none";
  }
  if(ks){
    const b=ks.balance_cents!=null?ks.balance_cents:(ks.balance_dollars!=null?Math.round(ks.balance_dollars*100):null);
    if(b!=null)$("bal").textContent="$"+(b/100).toFixed(2);
  }
  if(ex){
    const n=$("net");n.textContent=usd(ex.net_cents);n.className="k-val num "+cls(ex.net_cents);
    $("trades").textContent=ex.total+" trades · "+ex.wins+"W / "+ex.losses+"L";
    const e=$("exp");e.textContent=(ex.expectancy_cents>=0?"+":"")+ex.expectancy_cents.toFixed(1)+"¢";
    e.className="k-val num "+cls(ex.expectancy_cents);
    const pr=$("payoff");pr.textContent=ex.payoff_ratio.toFixed(2)+"×";
    pr.className="k-val num "+(ex.payoff_ratio>=ex.breakeven_ratio?"pos":"neg");
    $("be").textContent="need ≥ "+ex.breakeven_ratio.toFixed(2)+"×";
    $("wr").textContent=ex.win_rate.toFixed(0)+"%";
    $("wl").textContent=ex.wins+" W / "+ex.losses+" L";
    $("today").textContent=usd(ex.today_net_cents)+" · "+ex.today_trades+"t";
    const mxv=Math.max(ex.avg_win_cents,ex.avg_loss_cents,1);
    $("barwin").style.width=(22+ex.avg_win_cents/mxv*78)+"%";
    $("barwin").textContent="avg win +"+ex.avg_win_cents.toFixed(0)+"¢";
    $("barloss").style.width=(22+ex.avg_loss_cents/mxv*78)+"%";
    $("barloss").textContent="avg loss -"+ex.avg_loss_cents.toFixed(0)+"¢";
    const v=$("verdict");
    if(ex.total===0){v.className="verdict";v.textContent="No trades recorded yet — the model is selective, so expect frequent skips.";}
    else if(ex.expectancy_cents>0){v.className="verdict ok";
      v.innerHTML="<b>Positive expectancy.</b> Winners outrun losers ("+ex.payoff_ratio.toFixed(2)+"× ≥ "+ex.breakeven_ratio.toFixed(2)+"×). Edge looks real — scale carefully.";}
    else{v.className="verdict bad";
      v.innerHTML="<b>Negative expectancy.</b> "+ex.win_rate.toFixed(0)+"% win rate, but avg win +"+ex.avg_win_cents.toFixed(0)+"¢ vs avg loss -"+ex.avg_loss_cents.toFixed(0)+"¢ ("+ex.payoff_ratio.toFixed(2)+"× &lt; "+ex.breakeven_ratio.toFixed(2)+"×). Closing the gap is the whole game.";}
    drawEq(ex.equity_curve);
    const bs=$("bysig");
    if(!ex.by_signal||!ex.by_signal.length){bs.innerHTML='<tr><td colspan="3" class="empty">No trades yet</td></tr>';}
    else bs.innerHTML=ex.by_signal.map(s=>{
      const isfv=s[0]==="fv";
      return "<tr><td><span class='tag"+(isfv?" fv":"")+"'>"+s[0]+"</span></td><td style='text-align:right' class='num'>"+s[1]+"</td><td style='text-align:right' class='num "+cls(s[2])+"'>"+(s[2]>=0?"+":"")+s[2]+"</td></tr>";
    }).join("");
    const rb=$("recent");
    if(!ex.recent||!ex.recent.length){rb.innerHTML='<tr><td colspan="4" class="empty">No trades yet</td></tr>';}
    else rb.innerHTML=ex.recent.map(t=>{
      const tm=t.t.slice(5,16).replace("T"," ");
      const pre=(t.signal||"").split("-")[0];
      const istag=pre==="fv"?"tag fv":"tag";
      return "<tr><td class='mut num'>"+tm+"</td><td>"+t.ticker+"</td><td><span class='"+istag+"'>"+pre+"</span></td><td style='text-align:right' class='num "+(t.pnl_cents>=0?'pos':'neg')+"'>"+(t.pnl_cents>=0?"+":"")+t.pnl_cents+"¢</td></tr>";
    }).join("");
  }
  // fair-value live list
  const fl=$("fvlist");
  if(fvs&&fvs.length){fl.innerHTML=fvs.slice(0,8).map(fvRow).join("");}
  else if(fvs){fl.innerHTML='<div class="empty">Model is warming up (needs ~2 min of price history) or no near-expiry contracts in range.</div>';}
  // positions
  const pb=$("positions");
  const pos=Array.isArray(kp)?kp:(kp&&kp.positions)?kp.positions:[];
  if(!pos.length){pb.innerHTML='<tr><td colspan="4" class="empty">None open</td></tr>';}
  else pb.innerHTML=pos.map(p=>{
    const tk=p.ticker||p.market||"?";
    const posn=(p.position!=null)?p.position:(p.count!=null?p.count:0);
    const sd=posn>0?"yes":(posn<0?"no":(p.side||"—"));
    const q=Math.abs(posn)||"";
    // KalshiPosition exposes market_exposure / unrealized_pnl (cents), not an entry price.
    const ex=p.market_exposure!=null?("$"+(p.market_exposure/100).toFixed(2)):(p.price_cents!=null?p.price_cents+"¢":"—");
    return "<tr><td>"+tk+"</td><td>"+sd+"</td><td style='text-align:right' class='num'>"+q+"</td><td style='text-align:right' class='num'>"+ex+"</td></tr>";
  }).join("");
}
tick();setInterval(tick,2000);
</script>
</body>
</html>"##;

async fn handler_snapshot(State(state): State<AppState>) -> impl IntoResponse {
    let snap = build_snapshot(&state.0.read());
    Json(snap)
}

async fn handler_performance(State(state): State<AppState>) -> impl IntoResponse {
    let perf = state.0.read().performance.clone();
    Json(perf)
}

async fn handler_signals(State(state): State<AppState>) -> impl IntoResponse {
    let signals: Vec<SignalRecord> = state.0.read().recent_signals.iter().cloned().collect();
    Json(signals)
}

async fn handler_params(State(state): State<AppState>) -> impl IntoResponse {
    let params = state.0.read().current_params.clone();
    Json(params)
}

async fn handler_lessons(State(state): State<AppState>) -> impl IntoResponse {
    let lessons = state.0.read().lessons.clone();
    Json(lessons)
}

async fn handler_flow_history(State(state): State<AppState>) -> impl IntoResponse {
    let history: Vec<FlowHistoryPoint> = state.0.read().flow_history.iter().cloned().collect();
    Json(history)
}

#[derive(Serialize)]
struct StatusResponse {
    status: String,
    mode: String,
    uptime_secs: u64,
    btc_price: Option<f64>,
    active_markets: usize,
    pending_signals: usize,
}

async fn handler_status(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.0.read();
    Json(StatusResponse {
        status: s.status.clone(),
        mode: s.mode.clone(),
        uptime_secs: s.started_at.elapsed().as_secs(),
        btc_price: s.btc_price,
        active_markets: s.active_markets_count,
        pending_signals: s.pending_signals,
    })
}

async fn handler_arbitrage(State(state): State<AppState>) -> impl IntoResponse {
    let opps = state.0.read().arbitrage_opportunities.clone();
    Json(opps)
}

async fn handler_sse(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.1.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| {
        match msg {
            Ok(payload) => {
                let event_type = match &payload {
                    SsePayload::Signal(_) => "signal",
                    SsePayload::Outcome { .. } => "outcome",
                    SsePayload::ParamChange(_) => "param_change",
                    SsePayload::Heartbeat { .. } => "heartbeat",
                    SsePayload::Snapshot(_) => "snapshot",
                };
                let data = serde_json::to_string(&payload).unwrap_or_default();
                Some(Ok(Event::default().event(event_type).data(data)))
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

// ---------------------------------------------------------------------------
// Kalshi-specific handlers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct KalshiStatusResponse {
    enabled: bool,
    mode: String,
    balance_cents: Option<i64>,
    balance_dollars: Option<f64>,
    kill_switch: bool,
    daily_pnl_cents: i64,
    daily_pnl_dollars: f64,
    open_positions: usize,
}

async fn handler_kalshi_status(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.0.read();
    Json(KalshiStatusResponse {
        enabled: s.kalshi_enabled,
        mode: s.kalshi_mode.clone(),
        balance_cents: s.kalshi_balance_cents,
        balance_dollars: s.kalshi_balance_cents.map(|c| c as f64 / 100.0),
        kill_switch: s.kalshi_kill_switch,
        daily_pnl_cents: s.kalshi_daily_pnl_cents,
        daily_pnl_dollars: s.kalshi_daily_pnl_cents as f64 / 100.0,
        open_positions: s.kalshi_open_position_count,
    })
}

async fn handler_kalshi_positions(State(state): State<AppState>) -> impl IntoResponse {
    let positions = state.0.read().kalshi_positions.clone();
    Json(positions)
}

async fn handler_kalshi_markets(State(state): State<AppState>) -> impl IntoResponse {
    let markets = state.0.read().kalshi_btc_markets.clone();
    Json(markets)
}

async fn handler_kalshi_orders(State(state): State<AppState>) -> impl IntoResponse {
    let orders = state.0.read().kalshi_recent_orders.clone();
    Json(orders)
}

#[derive(Serialize)]
struct KillSwitchResponse {
    kill_switch: bool,
}

#[derive(Deserialize)]
struct KillSwitchRequest {
    kill_switch: bool,
}

/// PUT /api/kalshi/kill-switch — explicit set (NOT toggle) so a replayed
/// request can't re-enable trading after an operator disabled it.
async fn handler_kalshi_kill_switch_set(
    State(state): State<AppState>,
    Json(req): Json<KillSwitchRequest>,
) -> impl IntoResponse {
    let control = state.0.read().execution_control.clone();
    if let Some(control) = control {
        if let Err(e) = control.lock().await.set_pause(req.kill_switch) {
            return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error":e.to_string()}))).into_response();
        }
    }
    let new_value = {
        let mut s = state.0.write();
        s.kalshi_kill_switch = req.kill_switch;
        s.kalshi_kill_switch
    };
    Json(KillSwitchResponse { kill_switch: new_value }).into_response()
}

// ---------------------------------------------------------------------------
// Embedded dashboard HTML
// ---------------------------------------------------------------------------

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bitcoin Prediction Bot — Dashboard</title>
  <style>
    :root {
      --bg: #0d0d0d;
      --surface: #1a1a1a;
      --surface2: #252525;
      --border: #333;
      --text: #e0e0e0;
      --text-dim: #888;
      --accent: #f7931a;
      --green: #00c853;
      --red: #ff1744;
      --blue: #2196f3;
      --yellow: #ffd600;
      --radius: 8px;
      --gap: 16px;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; font-size: 14px; }
    header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; align-items: center; gap: 16px; }
    header h1 { font-size: 18px; color: var(--accent); letter-spacing: 1px; }
    #status-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--text-dim); display: inline-block; }
    #status-dot.live { background: var(--green); box-shadow: 0 0 6px var(--green); }
    #mode-badge { background: var(--surface2); border: 1px solid var(--border); border-radius: 4px; padding: 2px 8px; font-size: 11px; color: var(--yellow); }
    #uptime { margin-left: auto; color: var(--text-dim); font-size: 12px; }

    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: var(--gap); padding: var(--gap); }
    .card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 16px; }
    .card-title { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
    .card-value { font-size: 28px; font-weight: 700; }
    .card-value.green { color: var(--green); }
    .card-value.red { color: var(--red); }
    .card-value.accent { color: var(--accent); }
    .card-sub { font-size: 12px; color: var(--text-dim); margin-top: 4px; }

    .section { padding: 0 var(--gap) var(--gap); }
    .section-title { font-size: 13px; font-weight: 600; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; padding-top: 8px; border-top: 1px solid var(--border); }

    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th { background: var(--surface2); color: var(--text-dim); text-align: left; padding: 8px 10px; font-weight: 500; border-bottom: 1px solid var(--border); }
    td { padding: 7px 10px; border-bottom: 1px solid #222; }
    tr:hover td { background: var(--surface2); }
    .win { color: var(--green); }
    .loss { color: var(--red); }
    .breakeven { color: var(--yellow); }
    .long-badge { background: rgba(0,200,83,0.15); color: var(--green); padding: 2px 6px; border-radius: 3px; font-size: 11px; }
    .short-badge { background: rgba(255,23,68,0.15); color: var(--red); padding: 2px 6px; border-radius: 3px; font-size: 11px; }
    .neutral-badge { background: rgba(255,255,255,0.08); color: var(--text-dim); padding: 2px 6px; border-radius: 3px; font-size: 11px; }

    .params-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; }
    .param-item { background: var(--surface2); border: 1px solid var(--border); border-radius: 6px; padding: 10px 14px; }
    .param-name { font-size: 11px; color: var(--text-dim); }
    .param-value { font-size: 18px; font-weight: 600; color: var(--accent); margin-top: 4px; }

    .lessons-list { display: flex; flex-direction: column; gap: 8px; }
    .lesson-item { background: var(--surface2); border-left: 3px solid var(--accent); border-radius: 4px; padding: 10px 14px; }
    .lesson-type { font-size: 10px; color: var(--accent); text-transform: uppercase; letter-spacing: 1px; }
    .lesson-desc { margin-top: 4px; color: var(--text); }
    .lesson-adj { margin-top: 4px; font-size: 11px; color: var(--text-dim); }

    #pnl-bar { height: 60px; background: var(--surface2); border-radius: 6px; overflow: hidden; position: relative; display: flex; align-items: flex-end; gap: 2px; padding: 4px; }
    .pnl-segment { flex: 1; min-width: 4px; border-radius: 2px 2px 0 0; }

    .flow-bar-wrap { display: flex; align-items: center; gap: 8px; margin-top: 8px; }
    .flow-bar-bg { flex: 1; height: 10px; background: var(--surface2); border-radius: 5px; overflow: hidden; }
    .flow-bar-fill { height: 100%; border-radius: 5px; transition: width 0.3s; }
    .flow-label { font-size: 11px; width: 40px; }

    #event-log { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); height: 180px; overflow-y: auto; padding: 8px 12px; font-family: monospace; font-size: 11px; color: var(--text-dim); }
    #event-log .ev-signal { color: var(--blue); }
    #event-log .ev-outcome-win { color: var(--green); }
    #event-log .ev-outcome-loss { color: var(--red); }
    #event-log .ev-param { color: var(--yellow); }
    #event-log .ev-hb { color: #444; }
  </style>
</head>
<body>

<header>
  <span id="status-dot"></span>
  <h1>BTC Prediction Bot</h1>
  <span id="mode-badge">LOADING</span>
  <span id="uptime">uptime: --</span>
</header>

<!-- KPI cards -->
<div class="grid" id="kpi-grid">
  <div class="card"><div class="card-title">BTC Price</div><div class="card-value accent" id="kpi-btc">--</div></div>
  <div class="card"><div class="card-title">Win Rate</div><div class="card-value" id="kpi-winrate">--</div><div class="card-sub" id="kpi-winrate-sub">0 resolved</div></div>
  <div class="card"><div class="card-title">Gross PnL</div><div class="card-value" id="kpi-pnl">--</div></div>
  <div class="card"><div class="card-title">Profit Factor</div><div class="card-value" id="kpi-pf">--</div></div>
  <div class="card"><div class="card-title">Streak</div><div class="card-value" id="kpi-streak">--</div></div>
  <div class="card"><div class="card-title">Flow Delta</div><div class="card-value" id="kpi-delta">--</div><div class="card-sub" id="kpi-buypct">buy --</div></div>
  <div class="card"><div class="card-title">Flow Volume</div><div class="card-value accent" id="kpi-vol">--</div></div>
  <div class="card"><div class="card-title">Active Markets</div><div class="card-value" id="kpi-markets">--</div></div>
  <div class="card"><div class="card-title">Arb Opportunities</div><div class="card-value" id="kpi-arb">--</div><div class="card-sub" id="kpi-arb-sub">last scan: --</div></div>
</div>

<!-- Flow bar -->
<div class="section">
  <div class="section-title">Order Flow (10s)</div>
  <div class="flow-bar-wrap">
    <span class="flow-label win" style="text-align:right" id="flow-buy-pct-lbl">50%</span>
    <div class="flow-bar-bg">
      <div class="flow-bar-fill" id="flow-bar-fill" style="width:50%;background:var(--green)"></div>
    </div>
    <span class="flow-label loss" id="flow-sell-pct-lbl">50%</span>
  </div>
</div>

<!-- PnL curve -->
<div class="section">
  <div class="section-title">PnL Curve (last 50)</div>
  <div id="pnl-bar"></div>
</div>

<!-- Recent signals -->
<div class="section">
  <div class="section-title">Recent Signals (last 50)</div>
  <div style="overflow-x:auto">
    <table id="signals-table">
      <thead>
        <tr>
          <th>Time</th><th>Direction</th><th>Confidence</th><th>Delta</th>
          <th>Volume</th><th>Market</th><th>Side</th><th>Entry</th>
          <th>Outcome</th><th>PnL</th>
        </tr>
      </thead>
      <tbody id="signals-tbody"></tbody>
    </table>
  </div>
</div>

<!-- Arbitrage opportunities -->
<div class="section">
  <div class="section-title">Arbitrage Opportunities <span id="arb-refresh-badge" style="font-size:10px;color:var(--text-dim);margin-left:8px">auto-refresh 10s</span></div>
  <div style="overflow-x:auto">
    <table id="arb-table">
      <thead>
        <tr>
          <th>Time</th><th>Kalshi Ticker</th><th>Strike</th><th>Dir</th>
          <th>Kalshi Mid</th><th>Poly Mid</th><th>Gap</th>
          <th>Action</th><th>Severity</th><th>Auto-Exec</th>
        </tr>
      </thead>
      <tbody id="arb-tbody"></tbody>
    </table>
  </div>
  <div id="arb-empty" style="color:var(--text-dim);font-size:12px;padding:8px 0;display:none">No arbitrage gaps above threshold detected.</div>
</div>

<!-- Adaptive parameters -->
<div class="section">
  <div class="section-title">Adaptive Parameters</div>
  <div class="params-grid" id="params-grid"></div>
</div>

<!-- Lessons -->
<div class="section">
  <div class="section-title">Lessons Learned</div>
  <div class="lessons-list" id="lessons-list"></div>
</div>

<!-- Live event log -->
<div class="section">
  <div class="section-title">Live Events</div>
  <div id="event-log"></div>
</div>

<!-- Kalshi Trading Section -->
<div class="section" id="kalshi-section" style="display:none">
  <div class="section-title">Kalshi Execution</div>

  <!-- Kalshi KPI row -->
  <div class="grid" style="padding:0 0 12px 0">
    <div class="card">
      <div class="card-title">Environment</div>
      <div class="card-value" id="k-mode" style="font-size:18px">--</div>
    </div>
    <div class="card">
      <div class="card-title">Balance</div>
      <div class="card-value accent" id="k-balance">--</div>
    </div>
    <div class="card">
      <div class="card-title">Daily P&L</div>
      <div class="card-value" id="k-daily-pnl">--</div>
    </div>
    <div class="card">
      <div class="card-title">Open Positions</div>
      <div class="card-value" id="k-positions">--</div>
    </div>
    <div class="card">
      <div class="card-title">Kill Switch</div>
      <button id="k-kill-btn" onclick="toggleKillSwitch()" style="margin-top:6px;padding:6px 14px;border-radius:6px;border:none;cursor:pointer;font-weight:700;font-size:13px">OFF</button>
    </div>
  </div>

  <!-- Active BTC 15-min markets -->
  <div class="section-title" style="margin-top:8px">Active BTC 15-min Contracts</div>
  <div style="overflow-x:auto">
    <table id="k-markets-table">
      <thead>
        <tr><th>Ticker</th><th>Title</th><th>Status</th><th>YES Bid</th><th>YES Ask</th><th>Closes</th></tr>
      </thead>
      <tbody id="k-markets-tbody"></tbody>
    </table>
  </div>

  <!-- Open positions -->
  <div class="section-title" style="margin-top:12px">Open Positions</div>
  <div style="overflow-x:auto">
    <table id="k-pos-table">
      <thead>
        <tr><th>Ticker</th><th>Position</th><th>Exposure</th><th>Unrealised P&L</th><th>Realised P&L</th></tr>
      </thead>
      <tbody id="k-pos-tbody"></tbody>
    </table>
  </div>

  <!-- Recent orders -->
  <div class="section-title" style="margin-top:12px">Recent Orders</div>
  <div style="overflow-x:auto">
    <table id="k-orders-table">
      <thead>
        <tr><th>Order ID</th><th>Ticker</th><th>Action</th><th>Side</th><th>Count</th><th>Price</th><th>Status</th></tr>
      </thead>
      <tbody id="k-orders-tbody"></tbody>
    </table>
  </div>
</div>

<script>
(function() {
  'use strict';

  // ---- helpers ----
  function fmt(n, d=2) { return (n==null||isNaN(n)) ? '--' : Number(n).toFixed(d); }
  function fmtPrice(n) { return (n==null) ? '--' : '$' + Number(n).toLocaleString('en-US', {maximumFractionDigits:0}); }
  function fmtPct(n) { return fmt(n*100,1)+'%'; }
  function ts2time(ts) {
    const d = new Date(ts*1000);
    return d.toLocaleTimeString('en-US', {hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});
  }
  function el(id) { return document.getElementById(id); }
  function setColor(elem, val, zero='') {
    elem.className = 'card-value ' + (val > 0 ? 'green' : val < 0 ? 'red' : zero);
  }

  let state = null;
  let startedAt = null;

  // ---- initial load ----
  fetch('/api/snapshot')
    .then(r => r.json())
    .then(data => { state = data; startedAt = Date.now() - (data.uptime_secs||0)*1000; renderAll(data); })
    .catch(e => console.warn('snapshot fetch:', e));

  // ---- uptime ticker ----
  setInterval(() => {
    if (!startedAt) return;
    const s = Math.floor((Date.now()-startedAt)/1000);
    const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), ss = s%60;
    el('uptime').textContent = 'uptime: ' + [h,m,ss].map(x=>String(x).padStart(2,'0')).join(':');
  }, 1000);

  // ---- SSE ----
  const evSource = new EventSource('/feed');

  evSource.addEventListener('heartbeat', e => {
    try {
      const d = JSON.parse(e.data);
      const inner = d.data || d;
      if (inner.btc_price != null) updateBtcPrice(inner.btc_price);
      if (inner.flow_delta != null) updateFlowDelta(inner.flow_delta);
      logEvent('hb', '♥ ' + new Date().toLocaleTimeString());
    } catch(err) {}
  });

  evSource.addEventListener('signal', e => {
    try {
      const d = JSON.parse(e.data);
      const sig = d.data || d;
      logEvent('signal', '▲ Signal: ' + (sig.direction||'') + ' conf=' + fmt(sig.confidence));
      fetch('/api/snapshot').then(r=>r.json()).then(renderAll);
    } catch(err) {}
  });

  evSource.addEventListener('outcome', e => {
    try {
      const d = JSON.parse(e.data);
      const o = d.data || d;
      const cls = o.outcome==='win' ? 'outcome-win' : 'outcome-loss';
      logEvent(cls, '● Outcome: ' + (o.outcome||'') + ' pnl=' + fmt(o.pnl_usdc));
      fetch('/api/snapshot').then(r=>r.json()).then(renderAll);
    } catch(err) {}
  });

  evSource.addEventListener('param_change', e => {
    try {
      const d = JSON.parse(e.data);
      const p = d.data || d;
      logEvent('param', '⚙ Param: ' + (p.parameter||'') + ' ' + fmt(p.old_value) + '→' + fmt(p.new_value));
      fetch('/api/params').then(r=>r.json()).then(renderParams);
    } catch(err) {}
  });

  evSource.onerror = () => { el('status-dot').className = ''; };

  // ---- render functions ----
  function renderAll(data) {
    if (!data) return;
    state = data;

    el('status-dot').className = 'live';
    el('mode-badge').textContent = (data.mode||'').toUpperCase();

    // BTC price
    if (data.btc_price != null) {
      el('kpi-btc').textContent = fmtPrice(data.btc_price);
    }

    // Performance
    const p = data.performance || {};
    const wr = p.win_rate || 0;
    const wrEl = el('kpi-winrate');
    wrEl.textContent = fmtPct(wr);
    wrEl.className = 'card-value ' + (wr >= 0.55 ? 'green' : wr >= 0.40 ? 'accent' : 'red');
    el('kpi-winrate-sub').textContent = (p.total_resolved||0) + ' resolved';

    const pnlEl = el('kpi-pnl');
    pnlEl.textContent = '$' + fmt(p.gross_pnl||0);
    setColor(pnlEl, p.gross_pnl||0);

    const pfEl = el('kpi-pf');
    pfEl.textContent = fmt(p.profit_factor||0);
    setColor(pfEl, (p.profit_factor||0)-1);

    const stEl = el('kpi-streak');
    const st = p.current_streak||0;
    stEl.textContent = (st>0 ? '+':'')+st;
    setColor(stEl, st);

    // Flow
    const delta = data.flow_delta || 0;
    const deltaEl = el('kpi-delta');
    deltaEl.textContent = fmt(delta);
    setColor(deltaEl, delta);

    const buyPct = data.flow_buy_pct || 0.5;
    el('kpi-buypct').textContent = 'buy ' + fmtPct(buyPct);

    el('kpi-vol').textContent = '$' + fmt(data.flow_volume||0, 0);
    el('kpi-markets').textContent = data.active_markets_count||0;

    updateFlowBar(buyPct);
    renderPnlCurve(p.pnl_curve||[]);
    renderSignals(data.recent_signals||[]);
    renderParams(data.current_params||{});
    renderLessons(data.lessons||[]);
  }

  function updateBtcPrice(price) {
    el('kpi-btc').textContent = fmtPrice(price);
  }

  function updateFlowDelta(delta) {
    const el2 = el('kpi-delta');
    el2.textContent = fmt(delta);
    setColor(el2, delta);
  }

  function updateFlowBar(buyPct) {
    const pct = Math.round(buyPct * 100);
    const bar = el('flow-bar-fill');
    bar.style.width = pct + '%';
    bar.style.background = pct > 55 ? 'var(--green)' : pct < 45 ? 'var(--red)' : 'var(--yellow)';
    el('flow-buy-pct-lbl').textContent = pct + '%';
    el('flow-sell-pct-lbl').textContent = (100-pct) + '%';
  }

  function renderPnlCurve(curve) {
    const container = el('pnl-bar');
    container.innerHTML = '';
    if (!curve.length) return;
    const min = Math.min(...curve, 0);
    const max = Math.max(...curve, 0.01);
    const range = max - min || 1;
    curve.forEach(v => {
      const div = document.createElement('div');
      div.className = 'pnl-segment';
      const h = Math.max(2, Math.round(((v - min) / range) * 52));
      div.style.height = h + 'px';
      div.style.background = v >= 0 ? 'var(--green)' : 'var(--red)';
      div.style.opacity = '0.8';
      container.appendChild(div);
    });
  }

  function renderSignals(signals) {
    const tbody = el('signals-tbody');
    tbody.innerHTML = '';
    const rows = [...signals].reverse().slice(0, 50);
    rows.forEach(s => {
      const tr = document.createElement('tr');
      const dir = (s.direction||'Neutral').replace('Direction:','');
      const dirBadge = dir==='Long' ? `<span class="long-badge">LONG</span>`
                     : dir==='Short' ? `<span class="short-badge">SHORT</span>`
                     : `<span class="neutral-badge">NEUTRAL</span>`;
      const outcome = s.outcome || '--';
      const outcomeClass = outcome==='win'?'win':outcome==='loss'?'loss':outcome==='breakeven'?'breakeven':'';
      tr.innerHTML = `
        <td>${ts2time(s.timestamp)}</td>
        <td>${dirBadge}</td>
        <td>${fmtPct(s.confidence)}</td>
        <td>${fmt(s.delta)}</td>
        <td>$${fmt(s.volume,0)}</td>
        <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${s.market_question||''}">${(s.market_question||'').substring(0,40)}</td>
        <td>${s.side||'--'}</td>
        <td>${fmt(s.entry_price,3)}</td>
        <td class="${outcomeClass}">${outcome}</td>
        <td class="${s.pnl_usdc!=null&&s.pnl_usdc>0?'win':s.pnl_usdc!=null&&s.pnl_usdc<0?'loss':''}">${s.pnl_usdc!=null?'$'+fmt(s.pnl_usdc):'--'}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  function renderParams(params) {
    if (!params) return;
    const grid = el('params-grid');
    grid.innerHTML = '';
    const entries = [
      ['Imbalance Threshold', fmt(params.imbalance_threshold, 3)],
      ['Window (s)', fmt(params.window_seconds, 1)],
      ['Confirmation Window (s)', fmt(params.confirmation_window_seconds, 1)],
      ['Min Volume USDC', '$' + fmt(params.min_volume_usdc, 0)],
      ['Confidence Min', fmtPct(params.confidence_min)],
      ['Cooldown (s)', fmt(params.cooldown_seconds, 1)],
      ['Large Trade Mult', fmt(params.large_trade_multiplier, 1)],
      ['Large Trade Threshold', '$' + fmt(params.large_trade_threshold_usdc, 0)],
    ];
    entries.forEach(([name, value]) => {
      const div = document.createElement('div');
      div.className = 'param-item';
      div.innerHTML = `<div class="param-name">${name}</div><div class="param-value">${value}</div>`;
      grid.appendChild(div);
    });
  }

  function renderLessons(lessons) {
    const list = el('lessons-list');
    list.innerHTML = '';
    if (!lessons.length) {
      list.innerHTML = '<span style="color:var(--text-dim);font-size:12px">No lessons yet — needs more trade data.</span>';
      return;
    }
    [...lessons].reverse().slice(0,10).forEach(l => {
      const div = document.createElement('div');
      div.className = 'lesson-item';
      div.innerHTML = `
        <div class="lesson-type">${l.pattern_type||''}</div>
        <div class="lesson-desc">${l.description||''}</div>
        <div class="lesson-adj">${l.adjustment_made||''}</div>
      `;
      list.appendChild(div);
    });
  }

  function logEvent(type, msg) {
    const log = el('event-log');
    const line = document.createElement('div');
    line.className = 'ev-' + type;
    const t = new Date().toLocaleTimeString('en-US',{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});
    line.textContent = '['+t+'] ' + msg;
    log.appendChild(line);
    while (log.children.length > 200) log.removeChild(log.firstChild);
    log.scrollTop = log.scrollHeight;
  }

  // ---- Kalshi ----
  function renderKalshi(data) {
    if (!data) return;
    const enabled = data.kalshi_enabled;
    const section = el('kalshi-section');
    if (!section) return;
    section.style.display = enabled ? '' : 'none';
    if (!enabled) return;

    const modeEl = el('k-mode');
    const mode = (data.kalshi_mode || 'demo').toUpperCase();
    modeEl.textContent = mode;
    modeEl.style.color = mode === 'LIVE' ? 'var(--red)' : 'var(--yellow)';

    const bal = data.kalshi_balance_cents;
    el('k-balance').textContent = bal != null ? '$' + (bal / 100).toFixed(2) : '--';

    const pnlEl = el('k-daily-pnl');
    const pnl = data.kalshi_daily_pnl_cents || 0;
    pnlEl.textContent = (pnl >= 0 ? '+' : '') + '$' + (pnl / 100).toFixed(2);
    pnlEl.className = 'card-value ' + (pnl > 0 ? 'green' : pnl < 0 ? 'red' : '');

    el('k-positions').textContent = data.kalshi_open_position_count || 0;

    const ks = data.kalshi_kill_switch;
    const btn = el('k-kill-btn');
    btn.textContent = ks ? 'ON — HALT' : 'OFF';
    btn.style.background = ks ? 'var(--red)' : 'var(--green)';
    btn.style.color = '#000';

    // Markets table
    const mb = el('k-markets-tbody');
    mb.innerHTML = '';
    (data.kalshi_btc_markets || []).slice(0, 10).forEach(m => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td style="font-family:monospace;font-size:11px">${m.ticker||''}</td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${(m.title||'').substring(0,60)}</td>
        <td>${m.status||''}</td>
        <td>${m.yes_bid != null ? m.yes_bid+'¢' : '--'}</td>
        <td>${m.yes_ask != null ? m.yes_ask+'¢' : '--'}</td>
        <td style="font-size:11px">${(m.close_time||'').replace('T',' ').substring(0,16)}</td>`;
      mb.appendChild(tr);
    });

    // Positions table
    const pb = el('k-pos-tbody');
    pb.innerHTML = '';
    (data.kalshi_positions || []).forEach(p => {
      const upnl = p.unrealized_pnl;
      const rpnl = p.realized_pnl;
      const tr = document.createElement('tr');
      tr.innerHTML = `<td style="font-family:monospace;font-size:11px">${p.ticker||''}</td>
        <td>${p.position||0}</td>
        <td>${p.market_exposure != null ? p.market_exposure+'¢' : '--'}</td>
        <td class="${upnl != null && upnl > 0 ? 'win' : upnl != null && upnl < 0 ? 'loss' : ''}">${upnl != null ? (upnl > 0 ? '+' : '') + '$' + (upnl/100).toFixed(2) : '--'}</td>
        <td class="${rpnl != null && rpnl > 0 ? 'win' : rpnl != null && rpnl < 0 ? 'loss' : ''}">${rpnl != null ? (rpnl > 0 ? '+' : '') + '$' + (rpnl/100).toFixed(2) : '--'}</td>`;
      pb.appendChild(tr);
    });

    // Recent orders table
    const ob = el('k-orders-tbody');
    ob.innerHTML = '';
    (data.kalshi_recent_orders || []).slice().reverse().slice(0, 10).forEach(o => {
      const tr = document.createElement('tr');
      const price = o.yes_price != null ? o.yes_price+'¢ YES' : o.no_price != null ? o.no_price+'¢ NO' : '--';
      const statusCls = o.status === 'resting' || o.status === 'filled' ? 'win' : o.status === 'canceled' ? 'loss' : '';
      tr.innerHTML = `<td style="font-family:monospace;font-size:10px">${(o.order_id||'').substring(0,12)}…</td>
        <td style="font-family:monospace;font-size:11px">${o.ticker||''}</td>
        <td>${o.action||''}</td>
        <td>${o.side||''}</td>
        <td>${o.count||0}</td>
        <td>${price}</td>
        <td class="${statusCls}">${o.status||''}</td>`;
      ob.appendChild(tr);
    });
  }

  // Extend renderAll to also render Kalshi.
  const _origRenderAll = renderAll;
  function renderAll(data) {
    _origRenderAll(data);
    renderKalshi(data);
  }

  function toggleKillSwitch() {
    // Read current state from the latest snapshot, then PUT the *opposite*
    // value explicitly. This makes the request idempotent: a replay won't
    // accidentally re-enable trading after an operator disabled it.
    const currentlyActive = !!(state && state.kalshi_kill_switch);
    const desired = !currentlyActive;
    fetch('/api/kalshi/kill-switch', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kill_switch: desired }),
    })
      .then(r => r.json())
      .then(d => {
        logEvent('param', '⚡ Kill switch → ' + (d.kill_switch ? 'ACTIVE' : 'off'));
        fetch('/api/snapshot').then(r => r.json()).then(renderAll);
      })
      .catch(e => console.error('kill switch error:', e));
  }

  // ---- Arbitrage ----
  function fetchArbitrage() {
    fetch('/api/arbitrage/opportunities')
      .then(r => r.json())
      .then(renderArbitrage)
      .catch(e => console.warn('arb fetch:', e));
  }

  function renderArbitrage(opps) {
    const tbody = el('arb-tbody');
    const emptyMsg = el('arb-empty');
    if (!tbody) return;
    tbody.innerHTML = '';

    const kpiArb = el('kpi-arb');
    const kpiArbSub = el('kpi-arb-sub');
    if (kpiArb) {
      kpiArb.textContent = opps.length;
      kpiArb.className = 'card-value ' + (opps.length > 0 ? 'green' : '');
    }
    if (kpiArbSub) {
      kpiArbSub.textContent = 'last scan: ' + new Date().toLocaleTimeString('en-US',{hour12:false});
    }

    if (!opps.length) {
      if (emptyMsg) emptyMsg.style.display = '';
      return;
    }
    if (emptyMsg) emptyMsg.style.display = 'none';

    opps.forEach(opp => {
      const tr = document.createElement('tr');
      const time = new Date(opp.timestamp * 1000).toLocaleTimeString('en-US',{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});
      const sevClass = opp.severity === 'GREEN' ? 'win' : opp.severity === 'YELLOW' ? 'breakeven' : '';
      // Row background hint
      if (opp.severity === 'GREEN') tr.style.background = 'rgba(0,200,83,0.05)';
      else if (opp.severity === 'YELLOW') tr.style.background = 'rgba(255,214,0,0.05)';
      tr.innerHTML = `
        <td>${time}</td>
        <td style="font-family:monospace;font-size:11px">${opp.kalshi_ticker}</td>
        <td>$${Number(opp.strike_price).toLocaleString('en-US')}</td>
        <td>${opp.direction}</td>
        <td>${(opp.kalshi_mid * 100).toFixed(1)}¢</td>
        <td>${(opp.poly_mid * 100).toFixed(1)}¢</td>
        <td class="${sevClass}" style="font-weight:700">${(opp.gap_size * 100).toFixed(1)}¢</td>
        <td style="font-size:10px;color:var(--text-dim)">${opp.recommended_action}</td>
        <td class="${sevClass}">${opp.severity}</td>
        <td>${opp.auto_executed ? '<span style="color:var(--green)">✓</span>' : '<span style="color:var(--text-dim)">—</span>'}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  // Initial fetch + poll every 10 s
  fetchArbitrage();
  setInterval(fetchArbitrage, 10000);

})();
</script>
</body>
</html>
"#;
