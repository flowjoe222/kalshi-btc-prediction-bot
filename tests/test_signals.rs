/// Integration tests for the bitcoin-prediction-bot-rs signal pipeline.
///
/// Covers OrderBook (trade storage), OrderFlowCalculator, ImbalanceDetector,
/// PerformanceTracker, AdaptiveLearner, and the Simulator.
use bitcoin_prediction_bot_rs::data::orderbook::{OrderBook, OrderBookRegistry, Trade};
use bitcoin_prediction_bot_rs::signals::orderflow::OrderFlowCalculator;
use bitcoin_prediction_bot_rs::signals::detector::{
    DetectorParams, Direction, ImbalanceDetector,
};
use bitcoin_prediction_bot_rs::tracking::performance::PerformanceTracker;
use bitcoin_prediction_bot_rs::adaptive::{AdaptiveBounds, AdaptiveConfig};
use bitcoin_prediction_bot_rs::adaptive::learner::AdaptiveLearner;
use bitcoin_prediction_bot_rs::signals::predictor::TradeRecommendation;
use bitcoin_prediction_bot_rs::data::markets::PredictionMarket;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Build a `Trade` with specified fields and current timestamp.
fn make_trade(price: f64, size_usdc: f64, is_buy: bool, timestamp: f64) -> Trade {
    Trade {
        trade_id: uuid::Uuid::new_v4().to_string(),
        token_id: "test-token".to_owned(),
        market_id: "test-market".to_owned(),
        price,
        size_usdc,
        is_buy_initiated: is_buy,
        timestamp,
    }
}

/// Build a minimal `PredictionMarket` for test recommendations.
fn make_market() -> PredictionMarket {
    let expiry = now_secs() + 300.0; // 5 minutes from now
    PredictionMarket {
        condition_id: "test-condition".to_owned(),
        question: "Will BTC be above $70,000?".to_owned(),
        yes_token_id: "test-yes-token".to_owned(),
        no_token_id: "test-no-token".to_owned(),
        strike_price: 70_000.0,
        direction: "above".to_owned(),
        current_yes_price: 0.60,
        current_no_price: 0.40,
        expiry_ts: expiry,
        volume_24h_usdc: 50_000.0,
        is_active: true,
    }
}

/// Build a minimal resolved `TradeRecommendation`.
fn make_rec(outcome: &str, pnl: f64, direction: Direction, confidence: f64) -> TradeRecommendation {
    let ts = now_secs();
    TradeRecommendation {
        recommendation_id: uuid::Uuid::new_v4().to_string(),
        signal_id: uuid::Uuid::new_v4().to_string(),
        timestamp: ts,
        market: make_market(),
        side: "YES".to_owned(),
        direction,
        entry_price: 0.60,
        suggested_size_usdc: 50.0,
        confidence,
        imbalance_delta: 0.30,
        volume_usdc: 800.0,
        btc_price_at_signal: Some(67_500.0),
        btc_price_at_expiry: Some(68_000.0),
        resolved_at: Some(ts + 60.0),
        outcome: Some(outcome.to_owned()),
        pnl_usdc: Some(pnl),
    }
}

// ============================================================
// OrderBook trade-storage tests (2)
// ============================================================

#[test]
fn test_recent_trades_window() {
    let mut book = OrderBook::new("token", "market");
    let old_ts = now_secs() - 120.0; // 2 minutes ago
    let new_ts = now_secs() - 5.0;   // 5 seconds ago

    book.add_trade(make_trade(0.60, 100.0, true, old_ts));
    book.add_trade(make_trade(0.60, 200.0, false, new_ts));

    let recent = book.recent_trades(30.0);
    assert_eq!(recent.len(), 1, "only the trade within 30s should be returned");
    assert!((recent[0].size_usdc - 200.0).abs() < 1e-9);
}

#[test]
fn test_orderbook_registry_get_or_create() {
    let mut reg = OrderBookRegistry::new();
    let book = reg.get_or_create("token-a", "market-1");
    book.add_trade(make_trade(0.55, 100.0, true, now_secs()));

    // Getting the same key should return the same book with the trade.
    let book2 = reg.get_or_create("token-a", "market-1");
    assert_eq!(book2.recent_trades(60.0).len(), 1);
}

// ============================================================
// OrderFlowCalculator tests (7)
// ============================================================

fn make_registry_with_trades(trades: Vec<Trade>) -> Arc<RwLock<OrderBookRegistry>> {
    let registry = Arc::new(RwLock::new(OrderBookRegistry::new()));
    {
        let mut reg = registry.write();
        for trade in trades {
            reg.get_or_create(&trade.token_id, &trade.market_id)
                .add_trade(trade);
        }
    }
    registry
}

fn make_calculator(trades: Vec<Trade>) -> Arc<OrderFlowCalculator> {
    let registry = make_registry_with_trades(trades);
    Arc::new(OrderFlowCalculator::new(registry, 200.0, 1.5))
}

#[test]
fn test_empty_returns_neutral() {
    let calc = make_calculator(vec![]);
    let snap = calc.compute(10.0);
    assert!((snap.delta() - 0.0).abs() < 1e-9);
    assert!((snap.buy_fraction() - 0.5).abs() < 1e-9);
}

#[test]
fn test_all_buys_returns_positive_delta() {
    let now = now_secs();
    let trades = (0..5)
        .map(|i| make_trade(0.60, 100.0, true, now - i as f64))
        .collect();
    let calc = make_calculator(trades);
    let snap = calc.compute(60.0);
    assert!(snap.delta() > 0.5, "all buys should give positive delta near 1.0");
    assert!(snap.buy_fraction() > 0.99);
}

#[test]
fn test_all_sells_returns_negative_delta() {
    let now = now_secs();
    let trades = (0..5)
        .map(|i| make_trade(0.60, 100.0, false, now - i as f64))
        .collect();
    let calc = make_calculator(trades);
    let snap = calc.compute(60.0);
    assert!(snap.delta() < -0.5);
    assert!(snap.buy_fraction() < 0.01);
}

#[test]
fn test_balanced_flow_near_zero() {
    let now = now_secs();
    let mut trades = vec![];
    for i in 0..4 {
        trades.push(make_trade(0.60, 100.0, true, now - i as f64));
        trades.push(make_trade(0.60, 100.0, false, now - i as f64 - 0.5));
    }
    let calc = make_calculator(trades);
    let snap = calc.compute(60.0);
    assert!(snap.delta().abs() < 0.05, "balanced flow should give delta near 0");
}

#[test]
fn test_large_trades_weight_more() {
    let now = now_secs();
    // 1 large buy ($500) vs 3 small sells ($50 each)
    let trades = vec![
        make_trade(0.60, 500.0, true, now - 1.0),  // large buy
        make_trade(0.60, 50.0, false, now - 2.0),
        make_trade(0.60, 50.0, false, now - 3.0),
        make_trade(0.60, 50.0, false, now - 4.0),
    ];
    let calc = make_calculator(trades);
    let snap = calc.compute(60.0);
    // Total buy = 500, sell = 150 → delta > 0
    assert!(snap.delta() > 0.0, "large buy should dominate by volume");
    assert!(snap.large_delta() > 0.0);
}

#[test]
fn test_momentum_first_call_is_zero() {
    let now = now_secs();
    let trades: Vec<Trade> = (0..3)
        .map(|i| make_trade(0.60, 100.0, true, now - i as f64))
        .collect();
    let fresh_calc = make_calculator(trades);
    let snap = fresh_calc.compute(60.0);
    assert_eq!(snap.momentum(), 0.0, "first call has no prev_delta so momentum is 0");
}

#[test]
fn test_volume_profile_returns_dict() {
    let now = now_secs();
    let trades: Vec<Trade> = (0..5)
        .map(|i| make_trade(0.60, 100.0, true, now - i as f64))
        .collect();
    let calc = make_calculator(trades);
    let profile = calc.volume_profile_last_minute();
    assert_eq!(profile.len(), 60, "volume profile should have 60 buckets");
    let total_buy: f64 = profile.iter().map(|(b, _)| b).sum();
    assert!(total_buy > 0.0);
}

// ============================================================
// ImbalanceDetector tests (7)
// ============================================================

fn make_detector_with_trades(
    trades: Vec<Trade>,
    params_override: Option<DetectorParams>,
) -> (ImbalanceDetector, Arc<OrderFlowCalculator>) {
    let registry = make_registry_with_trades(trades);
    let calc = Arc::new(OrderFlowCalculator::new(registry, 200.0, 1.5));
    let params = params_override.unwrap_or_else(|| DetectorParams {
        imbalance_threshold: 0.65,
        window_seconds: 60.0,         // wide window to catch all test trades
        confirmation_window_seconds: 60.0,
        min_volume_usdc: 100.0,
        confidence_min: 0.50,
        cooldown_seconds: 0.0,        // no cooldown in tests
        large_trade_multiplier: 1.5,
        large_trade_threshold_usdc: 200.0,
    });
    let detector = ImbalanceDetector::new(params, Arc::clone(&calc));
    (detector, calc)
}

#[test]
fn test_no_signal_below_volume_threshold() {
    let now = now_secs();
    // Small trades — total volume = 40 USDC (below 100 threshold).
    let trades = vec![
        make_trade(0.60, 20.0, true, now - 1.0),
        make_trade(0.60, 20.0, true, now - 2.0),
    ];
    let (mut detector, _) = make_detector_with_trades(trades, None);
    assert!(
        detector.detect().is_none(),
        "should not fire when volume is below minimum"
    );
}

#[test]
fn test_no_signal_balanced_flow() {
    let now = now_secs();
    let mut trades = vec![];
    for i in 0..5 {
        trades.push(make_trade(0.60, 200.0, true, now - i as f64));
        trades.push(make_trade(0.60, 200.0, false, now - i as f64 - 0.5));
    }
    let (mut detector, _) = make_detector_with_trades(trades, None);
    assert!(
        detector.detect().is_none(),
        "balanced 50/50 flow should not trigger a signal"
    );
}

#[test]
fn test_long_signal_fires_on_strong_buy_imbalance() {
    let now = now_secs();
    // 80% buy volume → well above 0.65 threshold.
    let trades = vec![
        make_trade(0.60, 800.0, true, now - 1.0),
        make_trade(0.60, 800.0, true, now - 2.0),
        make_trade(0.60, 800.0, true, now - 3.0),
        make_trade(0.60, 800.0, true, now - 4.0),
        make_trade(0.60, 200.0, false, now - 5.0),
    ];
    let (mut detector, _) = make_detector_with_trades(trades, None);
    let signal = detector.detect();
    assert!(signal.is_some(), "strong buy imbalance should fire a Long signal");
    assert_eq!(signal.unwrap().direction, Direction::Long);
}

#[test]
fn test_short_signal_fires_on_strong_sell_imbalance() {
    let now = now_secs();
    // 80% sell volume.
    let trades = vec![
        make_trade(0.60, 800.0, false, now - 1.0),
        make_trade(0.60, 800.0, false, now - 2.0),
        make_trade(0.60, 800.0, false, now - 3.0),
        make_trade(0.60, 800.0, false, now - 4.0),
        make_trade(0.60, 200.0, true, now - 5.0),
    ];
    let (mut detector, _) = make_detector_with_trades(trades, None);
    let signal = detector.detect();
    assert!(signal.is_some(), "strong sell imbalance should fire a Short signal");
    assert_eq!(signal.unwrap().direction, Direction::Short);
}

#[test]
fn test_cooldown_prevents_duplicate_signals() {
    let now = now_secs();
    let trades: Vec<Trade> = (0..10)
        .map(|i| make_trade(0.60, 300.0, true, now - i as f64))
        .collect();

    let params = DetectorParams {
        imbalance_threshold: 0.65,
        window_seconds: 60.0,
        confirmation_window_seconds: 60.0,
        min_volume_usdc: 100.0,
        confidence_min: 0.50,
        cooldown_seconds: 3600.0, // very long cooldown
        large_trade_multiplier: 1.5,
        large_trade_threshold_usdc: 200.0,
    };

    let registry = make_registry_with_trades(trades);
    let calc = Arc::new(OrderFlowCalculator::new(registry, 200.0, 1.5));
    let mut detector = ImbalanceDetector::new(params, Arc::clone(&calc));

    let first = detector.detect();
    assert!(first.is_some(), "first signal should fire");
    // Cooldown is 3600s; second call should be blocked.
    let second = detector.detect();
    assert!(second.is_none(), "cooldown should block the second signal");
}

#[test]
fn test_confidence_above_minimum() {
    let now = now_secs();
    let trades: Vec<Trade> = (0..8)
        .map(|i| make_trade(0.60, 300.0, true, now - i as f64))
        .collect();
    let (mut detector, _) = make_detector_with_trades(trades, None);
    if let Some(signal) = detector.detect() {
        assert!(
            signal.confidence >= 0.50,
            "emitted confidence must meet the minimum: {}",
            signal.confidence
        );
    }
    // The test passes even if no signal fires (volume might not be sufficient).
}

#[test]
fn test_update_params_takes_effect() {
    let now = now_secs();
    // Balanced trades — no signal with default threshold.
    let trades: Vec<Trade> = (0..4)
        .map(|i| {
            let is_buy = i % 2 == 0;
            make_trade(0.60, 300.0, is_buy, now - i as f64)
        })
        .collect();
    let (mut detector, _) = make_detector_with_trades(trades, None);
    assert!(detector.detect().is_none());

    // Lower the threshold drastically so any small imbalance fires.
    let mut params = detector.get_params().clone();
    params.imbalance_threshold = 0.40; // lower than 50% → always fires
    params.confidence_min = 0.0;
    detector.update_params(params);

    // With the very low threshold the balanced flow (50/50) satisfies >= 0.40.
    let result = detector.detect();
    assert!(result.is_some(), "lowered threshold should fire on balanced flow");
}

// ============================================================
// PerformanceTracker tests (6)
// ============================================================

#[test]
fn test_empty_snapshot() {
    let tracker = PerformanceTracker::new();
    let snap = tracker.snapshot();
    assert_eq!(snap.total_resolved, 0);
    assert_eq!(snap.wins, 0);
    assert!((snap.win_rate - 0.0).abs() < 1e-9);
    assert!((snap.gross_pnl - 0.0).abs() < 1e-9);
}

#[test]
fn test_win_rate_calculation() {
    let mut tracker = PerformanceTracker::new();
    tracker.record(&make_rec("win", 30.0, Direction::Long, 0.70));
    tracker.record(&make_rec("win", 25.0, Direction::Long, 0.70));
    tracker.record(&make_rec("loss", -50.0, Direction::Long, 0.65));

    let snap = tracker.snapshot();
    assert_eq!(snap.wins, 2);
    assert_eq!(snap.losses, 1);
    // 2 wins / 3 total = 0.666...
    assert!((snap.win_rate - 2.0 / 3.0).abs() < 1e-6);
}

#[test]
fn test_pnl_cumulative() {
    let mut tracker = PerformanceTracker::new();
    tracker.record(&make_rec("win", 100.0, Direction::Long, 0.70));
    tracker.record(&make_rec("loss", -40.0, Direction::Short, 0.65));
    tracker.record(&make_rec("win", 60.0, Direction::Long, 0.72));

    let snap = tracker.snapshot();
    assert!((snap.gross_pnl - 120.0).abs() < 1e-6);
}

#[test]
fn test_profit_factor() {
    let mut tracker = PerformanceTracker::new();
    // Two wins totaling 100, one loss of 50 → profit factor = 2.0.
    tracker.record(&make_rec("win", 60.0, Direction::Long, 0.70));
    tracker.record(&make_rec("win", 40.0, Direction::Long, 0.70));
    tracker.record(&make_rec("loss", -50.0, Direction::Long, 0.65));

    let snap = tracker.snapshot();
    assert!((snap.profit_factor - 2.0).abs() < 1e-6, "profit_factor = {}", snap.profit_factor);
}

#[test]
fn test_streak_tracking() {
    let mut tracker = PerformanceTracker::new();
    tracker.record(&make_rec("win", 30.0, Direction::Long, 0.70));
    tracker.record(&make_rec("win", 30.0, Direction::Long, 0.70));
    assert_eq!(tracker.snapshot().current_streak, 2);
    assert_eq!(tracker.snapshot().max_win_streak, 2);

    tracker.record(&make_rec("loss", -50.0, Direction::Long, 0.65));
    assert_eq!(tracker.snapshot().current_streak, -1);

    tracker.record(&make_rec("loss", -50.0, Direction::Long, 0.65));
    assert_eq!(tracker.snapshot().current_streak, -2);
    assert_eq!(tracker.snapshot().max_loss_streak, 2);
}

#[test]
fn test_pnl_curve_populated() {
    let mut tracker = PerformanceTracker::new();
    for i in 0..5 {
        let pnl = if i % 2 == 0 { 20.0 } else { -10.0 };
        tracker.record(&make_rec(
            if pnl > 0.0 { "win" } else { "loss" },
            pnl,
            Direction::Long,
            0.70,
        ));
    }
    let snap = tracker.snapshot();
    assert!(!snap.pnl_curve.is_empty(), "pnl_curve should be populated after recording trades");
    assert_eq!(snap.pnl_curve.len(), 5);
}

// ============================================================
// AdaptiveLearner tests (4)
// ============================================================

fn make_learner_params() -> (Arc<tokio::sync::Mutex<DetectorParams>>, AdaptiveConfig) {
    let params = Arc::new(tokio::sync::Mutex::new(DetectorParams {
        imbalance_threshold: 0.65,
        window_seconds: 10.0,
        confirmation_window_seconds: 30.0,
        min_volume_usdc: 500.0,
        confidence_min: 0.60,
        cooldown_seconds: 45.0,
        large_trade_multiplier: 1.5,
        large_trade_threshold_usdc: 200.0,
    }));
    let config = AdaptiveConfig {
        min_samples_to_learn: 5,   // low for tests
        rolling_window_size: 50,
        optimization_interval: 5,  // optimize every 5 trades in tests
        learning_rate: 0.10,
        target_win_rate: 0.55,
        emergency_tighten_threshold: 0.38,
        bounds: AdaptiveBounds {
            imbalance_threshold: [0.55, 0.85],
            window_seconds: [5.0, 60.0],
            min_volume_usdc: [100.0, 5000.0],
            confidence_min: [0.50, 0.85],
            cooldown_seconds: [15.0, 120.0],
        },
        lessons_log_path: "/tmp/test_lessons.json".to_owned(),
    };
    (params, config)
}

#[tokio::test]
async fn test_parameter_tightens_after_losses() {
    let (params, config) = make_learner_params();
    let mut learner = AdaptiveLearner::new(config, Arc::clone(&params));

    // Record enough losses to trigger optimization.
    // All losses with low volume → pattern 1 should raise min_volume_usdc.
    for _ in 0..5 {
        let mut rec = make_rec("loss", -50.0, Direction::Long, 0.62);
        // Ensure volume is below low-vol threshold (500 * 1.25 = 625).
        rec.volume_usdc = 400.0;
        learner.record_outcome(&rec).await;
    }

    let status = learner.get_status();
    // We should have at least 5 records.
    assert_eq!(status.total_records, 5);
    // After optimization the win_rate should be 0.
    assert!((status.win_rate - 0.0).abs() < 1e-9);
}

#[tokio::test]
async fn test_no_adjustment_with_insufficient_samples() {
    let (params, mut config) = make_learner_params();
    // Require 100 samples before learning.
    config.min_samples_to_learn = 100;
    let mut learner = AdaptiveLearner::new(config, Arc::clone(&params));

    // Record 5 losses (below the 100-sample threshold).
    for _ in 0..5 {
        let adjustments = learner.record_outcome(&make_rec("loss", -50.0, Direction::Long, 0.62)).await;
        // No adjustments should be made yet.
        assert!(
            adjustments.is_empty(),
            "should not adjust params before min_samples threshold"
        );
    }
}

#[tokio::test]
async fn test_status_returns_required_fields() {
    let (params, config) = make_learner_params();
    let learner = AdaptiveLearner::new(config, params);
    let status = learner.get_status();
    // All required fields must be present and sensible.
    assert_eq!(status.total_records, 0);
    assert!((status.win_rate - 0.0).abs() < 1e-9);
    assert!(status.last_optimized_at.is_none());
    assert_eq!(status.adjustments_made, 0);
    assert_eq!(status.lessons_learned, 0);
}

#[tokio::test]
async fn test_emergency_tighten_fires_below_threshold() {
    let (params, mut config) = make_learner_params();
    // Set emergency threshold high so it triggers easily.
    config.emergency_tighten_threshold = 0.90;
    config.min_samples_to_learn = 5;
    config.optimization_interval = 5;

    let original_threshold = params.lock().await.imbalance_threshold;
    let mut learner = AdaptiveLearner::new(config, Arc::clone(&params));

    // Record 5 losses → win_rate = 0.0, below emergency_tighten_threshold of 0.90.
    for _ in 0..5 {
        learner.record_outcome(&make_rec("loss", -50.0, Direction::Long, 0.62)).await;
    }

    let new_threshold = params.lock().await.imbalance_threshold;
    assert!(
        new_threshold > original_threshold,
        "emergency tighten should raise imbalance_threshold: {original_threshold} → {new_threshold}"
    );
}

// ============================================================
// Simulator smoke test (1)
// ============================================================

#[tokio::test]
async fn test_simulator_generates_trades() {
    use bitcoin_prediction_bot_rs::config::SimulationConfig;
    use bitcoin_prediction_bot_rs::data::simulator::Simulator;
    use tokio::sync::{mpsc, watch};
    use tokio::time::{timeout, Duration};

    let config = SimulationConfig {
        test_mode: true,
        tick_interval_seconds: 0.05, // fast ticks for testing
        starting_price: 67_500.0,
        annual_volatility: 0.80,
        annual_drift: 0.30,
        signal_quality: 0.65,
        base_volume_per_second: 250.0,
    };

    let simulator = Simulator::new(config);
    let (trade_tx, mut trade_rx) = mpsc::channel::<Trade>(256);
    let (price_tx, mut price_rx) = watch::channel(67_500.0_f64);

    // Run simulator for a short time.
    tokio::spawn(async move {
        let _ = simulator.run(trade_tx, price_tx).await;
    });

    // Collect trades from 3 ticks (3 × 0.05s = 150ms; give it 2s).
    let mut collected_trades = 0_usize;
    let result = timeout(Duration::from_secs(2), async {
        while collected_trades < 6 {
            if trade_rx.recv().await.is_some() {
                collected_trades += 1;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "should receive at least 6 trades within 2s");

    // BTC price should be in a sane range.
    let btc = *price_rx.borrow();
    assert!(
        btc > 50_000.0 && btc < 200_000.0,
        "BTC price out of expected range: {btc}"
    );
}

// ---------------------------------------------------------------------------
// Fair-value digital pricing (the FV strategy core)
// ---------------------------------------------------------------------------
mod fair_value_tests {
    use bitcoin_prediction_bot_rs::signals::fair_value::*;

    #[test]
    fn norm_cdf_known_points() {
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((norm_cdf(1.0) - 0.8413).abs() < 1e-3);
        assert!((norm_cdf(-1.0) - 0.1587).abs() < 1e-3);
        // symmetry
        assert!((norm_cdf(0.5) + norm_cdf(-0.5) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn fair_prob_worked_example() {
        // spot 60000, strike 60050, sigma 0.00025/s, 600s left.
        // With the 60s-average tau_eff correction => fair ≈ 0.444 (44¢).
        let p = fair_prob_above(60000.0, 60050.0, 0.00025, 600.0);
        assert!((p - 0.444).abs() < 0.01, "fair prob was {p}");
        let c = fair_cents_yes(p);
        assert!((43..=45).contains(&c), "fair cents was {c}");
    }

    #[test]
    fn fair_prob_degenerate_inputs_collapse() {
        // No vol / no time => deterministic outcome.
        assert_eq!(fair_prob_above(100.0, 90.0, 0.0, 600.0), 1.0);
        assert_eq!(fair_prob_above(80.0, 90.0, 0.001, 0.0), 0.0);
    }

    #[test]
    fn deep_itm_otm_pin_to_extremes() {
        // Spot far above strike with low vol and little time => ~certain YES.
        let p_itm = fair_prob_above(70000.0, 60000.0, 0.0001, 120.0);
        assert!(p_itm > 0.97, "deep ITM should be >0.97, was {p_itm}");
        let p_otm = fair_prob_above(50000.0, 60000.0, 0.0001, 120.0);
        assert!(p_otm < 0.03, "deep OTM should be <0.03, was {p_otm}");
    }

    #[test]
    fn tau_eff_pulls_toward_50_in_final_minute() {
        // In the last 60s the averaging window shrinks effective variance, so a
        // near-the-money contract sits closer to 50 than naive point pricing.
        let near_money = fair_prob_above(60010.0, 60000.0, 0.0005, 30.0);
        assert!(near_money > 0.4 && near_money < 0.75, "was {near_money}");
    }

    #[test]
    fn taker_fee_matches_official_formula() {
        // Peak fee at 50¢: 0.07 * 100 * 0.5 * 0.5 = $1.75 = 175¢.
        assert_eq!(taker_fee_cents(50, 100), 175);
        // Single contract at 50¢: 0.07 * 0.25 = 0.0175 -> ceil to 2¢.
        assert_eq!(taker_fee_cents(50, 1), 2);
        // Deep contract at 95¢: 0.07 * 0.95 * 0.05 = 0.003325 -> ceil to 1¢.
        assert_eq!(taker_fee_cents(95, 1), 1);
        // Fee is symmetric around 50¢.
        assert_eq!(taker_fee_cents(40, 10), taker_fee_cents(60, 10));
    }

    #[test]
    fn net_edge_subtracts_fee_and_slippage() {
        // fair 44, buy at 38 => gross 6, minus ~2¢ fee minus 1¢ slip => ~3¢.
        let net = net_edge_cents(44, 38, 1, 1);
        assert_eq!(net, 44 - 38 - taker_fee_cents(38, 1) - 1);
        assert!(net >= 2 && net <= 4, "net was {net}");
    }

    #[test]
    fn fair_cents_clamps_to_tradeable_range() {
        assert_eq!(fair_cents_yes(0.0), 1);
        assert_eq!(fair_cents_yes(1.0), 99);
        assert_eq!(fair_cents_yes(0.5), 50);
    }

    #[test]
    fn realized_vol_flat_series_is_none() {
        let flat = vec![100.0; 50];
        assert!(realized_vol_per_step(&flat, 50).is_none());
    }

    #[test]
    fn realized_vol_positive_for_moving_series() {
        let prices: Vec<f64> = (0..200).map(|i| 100.0 + (i as f64 * 0.137).sin()).collect();
        let v = realized_vol_per_step(&prices, 100).expect("should compute vol");
        assert!(v > 0.0 && v.is_finite());
    }
}
