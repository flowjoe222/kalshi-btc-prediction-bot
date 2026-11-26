use anyhow::Result;
use async_trait::async_trait;
use bitcoin_prediction_bot_rs::execution::{
    ledger::*,
    managed::*,
    risk::{OpenPosition, RiskManager, RiskParams},
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
const NOW: i64 = 1_800_000_000;
fn path() -> PathBuf {
    std::env::temp_dir()
        .join(format!("kalshi-ledger-test-{}", uuid::Uuid::new_v4()))
        .join("ledger.json")
}
fn limits() -> RiskParams {
    RiskParams {
        max_position_cents: 300,
        max_daily_loss_cents: 1000,
        max_total_exposure_cents: 300,
        max_concurrent_positions: 3,
        cooldown_seconds: 0,
        confidence_threshold: 0.5,
    }
}
fn order(client: &str, id: &str, ticker: &str, status: &str, filled: i64) -> Value {
    json!({"client_order_id":client,"order_id":id,"ticker":ticker,"exchange_index":2,"status":status,"fill_count_fp":format!("{filled}.00")})
}
fn fill(id: &str, oid: &str, ticker: &str, count: i64, price: &str, fee: &str) -> Value {
    json!({"fill_id":id,"order_id":oid,"ticker":ticker,"exchange_index":2,"action":"buy","book_side":"bid","side":"yes","count_fp":format!("{count}.00"),"yes_price_dollars":price,"fee_cost":fee,"created_time":"2027-01-15T08:00:00.000001Z"})
}
fn position(t: &str, n: i64) -> Value {
    json!({"ticker":t,"exchange_index":2,"position_fp":format!("{n}.00")})
}
fn settlement(t: &str, n: i64, revenue: i64, result: &str) -> Value {
    json!({"ticker":t,"exchange_index":2,"yes_count_fp":format!("{n}.00"),"no_count_fp":"0.00","revenue":revenue,"market_result":result,"settled_time":"2027-01-15T08:15:00Z","fee_cost":"0.0200"})
}
struct Mock {
    posts: AtomicUsize,
    cancels: AtomicUsize,
    timeout: bool,
    cancel_error: bool,
    snapshots: Mutex<VecDeque<Snapshot>>,
}
impl Mock {
    fn new(timeout: bool) -> Arc<Self> {
        Arc::new(Self {
            posts: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            timeout,
            cancel_error: false,
            snapshots: Mutex::new(VecDeque::new()),
        })
    }
}
#[async_trait]
impl Venue for Mock {
    async fn submit(&self, _: &Intent) -> Result<SubmitResult> {
        let post = self.posts.fetch_add(1, Ordering::SeqCst) + 1;
        if self.timeout {
            anyhow::bail!("timeout after acceptance")
        }
        Ok(SubmitResult::Accepted(format!("o{post}")))
    }
    async fn cancel(&self, _: &str, _: i64) -> Result<()> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        if self.cancel_error {
            anyhow::bail!("order already terminal / cancel 404")
        }
        Ok(())
    }
    async fn snapshot(&self, _: &BTreeSet<i64>, _: i64) -> Result<Snapshot> {
        self.snapshots
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("network failure"))
    }
}
fn ready(mock: Arc<Mock>, p: PathBuf) -> Coordinator<Mock> {
    let mut c = Coordinator::open(mock, p, limits()).unwrap();
    c.apply_snapshot(&Snapshot::default(), NOW).unwrap();
    c.last_snapshot.balances.insert(2, 1000);
    c
}
fn buy(count: i64) -> Intent {
    intent(
        "KXBTC15M-test",
        2,
        "yes",
        "buy",
        30,
        count,
        "fv-hold-test",
        NOW,
    )
}

#[test]
fn exact_decimal_money_and_fractional_rejection() {
    assert_eq!(fixed("56.186300", 6).unwrap(), 56_186_300);
    assert_eq!(fixed("-0.001200", 6).unwrap(), -1200);
    assert_eq!(contracts("2.0000").unwrap(), 200);
    for s in ["1.001", "NaN", "-1.001"] {
        assert!(contracts(s).is_err())
    }
    assert!(fixed("0.0000001", 6).is_err());
    assert!(fixed("999999999999999999999", 6).is_err());
}
#[tokio::test]
async fn partial_fill_cancel_lag_tracks_quantity_and_fees() {
    let mock = Mock::new(false);
    let mut c = ready(mock.clone(), path());
    let i = buy(3);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let mut s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "resting", 1)],
        fills: vec![fill("f1", "o1", "KXBTC15M-test", 1, "0.3000", "0.0100")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    };
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(
        c.ledger
            .accounting()
            .unwrap()
            .positions
            .values()
            .next()
            .unwrap()
            .count,
        100
    );
    assert_eq!(c.ledger.exposure().unwrap(), 97 * CENT);
    c.cancel_pending().await.unwrap();
    assert_eq!(mock.cancels.load(Ordering::SeqCst), 1);
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(c.ledger.exposure().unwrap(), 97 * CENT);
    s.orders = vec![order(&cid, "o1", "KXBTC15M-test", "canceled", 1)];
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.exposure().unwrap(), 31 * CENT);
    assert!(c.ready);
    // A repeated API page/fill must never double count cost or quantity.
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.fills.len(), 1);
    assert_eq!(c.ledger.exposure().unwrap(), 31 * CENT);
}
#[tokio::test]
async fn timeout_recovers_by_client_id_without_second_post() {
    let mock = Mock::new(true);
    let p = path();
    let mut c = ready(mock.clone(), p.clone());
    let i = buy(1);
    let cid = i.client_id.clone();
    assert!(c.place(i.clone(), 1.0, NOW).await.is_err());
    assert!(c.place(i, 1.0, NOW).await.is_err());
    assert_eq!(mock.posts.load(Ordering::SeqCst), 1);
    drop(c);
    let mut c = Coordinator::open(mock.clone(), p, limits()).unwrap();
    assert!(!c.ready);
    assert_eq!(c.ledger.exposure().unwrap(), 33 * CENT);
    let s = Snapshot {
        orders: vec![order(
            &cid,
            "accepted-before-timeout",
            "canceled",
            "canceled",
            0,
        )],
        ..Default::default()
    };
    let mut s = s;
    s.orders[0]["ticker"] = "KXBTC15M-test".into();
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.exposure().unwrap(), 0);
    assert_eq!(
        c.ledger.intents[&cid].order_id.as_deref(),
        Some("accepted-before-timeout")
    );
    assert_eq!(mock.posts.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn unknown_timeout_remains_reserved_even_after_expiration() {
    let mock = Mock::new(true);
    let mut c = ready(mock.clone(), path());
    let i = buy(1);
    let _ = c.place(i, 1.0, NOW).await;
    assert!(c.apply_snapshot(&Snapshot::default(), NOW + 1000).is_err());
    assert_eq!(c.ledger.exposure().unwrap(), 33 * CENT);
    assert!(!c.ready);
}
#[tokio::test]
async fn settlement_is_authoritative_and_fee_inclusive_exactly_once() {
    let mock = Mock::new(false);
    let mut c = ready(mock, path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let mut s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![fill("f1", "o1", "KXBTC15M-test", 1, "0.3000", "0.0200")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    };
    c.apply_snapshot(&s, NOW).unwrap();
    s.positions.clear();
    assert!(c.apply_snapshot(&s, NOW + 900).is_err());
    assert_eq!(c.ledger.accounting().unwrap().journal.len(), 0);
    assert_eq!(c.ledger.exposure().unwrap(), 32 * CENT);
    s.settlements = vec![settlement("KXBTC15M-test", 1, 100, "yes")];
    c.apply_snapshot(&s, NOW + 900).unwrap();
    c.apply_snapshot(&s, NOW + 901).unwrap();
    let a = c.ledger.accounting().unwrap();
    assert!(a.positions.is_empty());
    assert_eq!(a.journal.len(), 1);
    assert_eq!(a.journal[0].pnl, 68 * CENT);
    assert_eq!(a.journal[0].kind, "settlement:yes");
    assert_eq!(a.journal[0].signal_id, "fv-hold-test");
    // Settlement's fee_cost is not charged a second time.
    assert_eq!(a.journal[0].cost, 32 * CENT);
}
#[tokio::test]
async fn losing_settlement_records_actual_cost_not_zero() {
    let mock = Mock::new(false);
    let mut c = ready(mock, path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![fill("f", "o1", "KXBTC15M-test", 1, "0.2900", "0.015000")],
        settlements: vec![settlement("KXBTC15M-test", 1, 0, "no")],
        ..Default::default()
    };
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.accounting().unwrap().journal[0].pnl, -305_000);
}
#[test]
fn concurrent_reservations_cannot_overbook() {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut handles = vec![];
    for n in 0..8 {
        let l = ledger.clone();
        let b = barrier.clone();
        handles.push(std::thread::spawn(move || {
            b.wait();
            let i = intent(
                &format!("KXBTC15M-{n}"),
                2,
                "yes",
                "buy",
                30,
                3,
                "test",
                NOW,
            );
            l.lock().unwrap().reserve(i, &limits(), 1.0, NOW).is_ok()
        }));
    }
    let ok = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|v| *v)
        .count();
    assert_eq!(ok, 1);
    assert_eq!(ledger.lock().unwrap().exposure().unwrap(), 99 * CENT);
}
#[tokio::test]
async fn stale_or_failed_account_read_blocks_all_new_entries() {
    let mock = Mock::new(false);
    let mut c = ready(mock.clone(), path());
    assert!(c.place(buy(1), 1.0, NOW + 31).await.is_err());
    assert!(c.reconcile(NOW).await.is_err());
    assert!(!c.ready);
    assert!(c.place(buy(1), 1.0, NOW).await.is_err());
    assert_eq!(mock.posts.load(Ordering::SeqCst), 0);
}
#[test]
fn corrupt_ledger_and_second_writer_fail_closed() {
    let p = path();
    let (store, l) = Store::open(&p).unwrap();
    store.save(&l).unwrap();
    assert!(Store::open(&p).is_err());
    drop(store);
    std::fs::write(&p, "{").unwrap();
    assert!(Store::open(&p).is_err());
}
#[test]
fn pause_survives_restart_and_only_logs_transitions() {
    let p = path();
    let mut c = ready(Mock::new(false), p.clone());
    c.set_pause(true).unwrap();
    c.set_pause(true).unwrap();
    drop(c);
    let c = Coordinator::open(Mock::new(false), p, limits()).unwrap();
    assert!(c.ledger.kill_switch);
    let mut risk = RiskManager::new(limits());
    risk.set_kill_switch(false);
    risk.set_kill_switch(true);
    risk.set_kill_switch(true);
    assert!(risk.kill_switch_active());
}
#[test]
fn unknown_crypto_position_blocks_but_unrelated_account_is_untouched() {
    let mut c = ready(Mock::new(false), path());
    let mut s = Snapshot {
        positions: vec![position("KXBTC15M-external", 1)],
        ..Default::default()
    };
    assert!(c.apply_snapshot(&s, NOW).is_err());
    s.positions =
        vec![json!({"ticker":"KXAAAGASW-external","position_fp":"-15744.74","exchange_index":0})];
    assert!(c.apply_snapshot(&s, NOW).is_ok());
}
#[tokio::test]
async fn terminal_order_without_fill_details_remains_reserved() {
    let mut c = ready(Mock::new(false), path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        ..Default::default()
    };
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(c.ledger.exposure().unwrap(), 33 * CENT);
}
#[test]
fn partial_exits_allocate_basis_and_fees_and_prevent_overselling() {
    let mut l = Ledger::default();
    let mut i = buy(3);
    i.order_id = Some("buy".into());
    i.status = "executed".into();
    i.reported_fills = 300;
    l.intents.insert(i.client_id.clone(), i);
    l.observe_fill(Fill {
        id: "f1".into(),
        order_id: "buy".into(),
        ticker: "KXBTC15M-test".into(),
        shard: 2,
        action: "buy".into(),
        side: "yes".into(),
        count: 300,
        price: 30 * CENT,
        fee: 2 * CENT,
        ts: NOW * 1_000_000,
    })
    .unwrap();
    let mut sell = intent("KXBTC15M-test", 2, "yes", "sell", 50, 1, "exit", NOW + 1);
    l.reserve(sell.clone(), &limits(), 1.0, NOW + 1).unwrap();
    let duplicate = intent("KXBTC15M-test", 2, "yes", "sell", 50, 3, "exit", NOW + 2);
    assert!(l.reserve(duplicate, &limits(), 1.0, NOW + 2).is_err());
    sell.order_id = Some("sell".into());
    sell.status = "executed".into();
    sell.reported_fills = 100;
    l.intents.insert(sell.client_id.clone(), sell);
    l.observe_fill(Fill {
        id: "f2".into(),
        order_id: "sell".into(),
        ticker: "KXBTC15M-test".into(),
        shard: 2,
        action: "sell".into(),
        side: "yes".into(),
        count: 100,
        price: 50 * CENT,
        fee: CENT,
        ts: (NOW + 1) * 1_000_000,
    })
    .unwrap();
    let a = l.accounting().unwrap();
    assert_eq!(a.positions.values().next().unwrap().count, 200);
    assert_eq!(
        a.positions.values().next().unwrap().cost + a.journal[0].cost,
        92 * CENT
    );
    assert_eq!(a.journal[0].revenue, 49 * CENT);
}
#[test]
fn duplicate_risk_close_does_not_double_realized_pnl() {
    let mut r = RiskManager::new(limits());
    r.record_open(OpenPosition {
        order_id: "a".into(),
        ticker: "x".into(),
        side: "yes".into(),
        count: 1,
        price_cents: 30,
        cost_cents: 30,
        opened_at: 0.0,
        signal_id: "x".into(),
        closed_at: None,
        pnl_cents: None,
    });
    r.record_close("a", 20, 1.0);
    r.record_close("a", 20, 2.0);
    assert_eq!(r.daily_pnl_cents(), 20);
}
#[test]
fn risk_caps_include_pending_fees_and_all_positions() {
    let mut l = Ledger::default();
    let mut params = limits();
    params.max_position_cents = 30;
    assert!(l.reserve(buy(1), &params, 1.0, NOW).is_err());
    params = limits();
    params.max_total_exposure_cents = 90;
    assert!(l.reserve(buy(3), &params, 1.0, NOW).is_err());
    params = limits();
    params.max_daily_loss_cents = 90;
    assert!(l.reserve(buy(3), &params, 1.0, NOW).is_err());
    assert!(l.reserve(buy(1), &limits(), f64::NAN, NOW).is_err());
}
#[tokio::test]
async fn delayed_final_fill_after_cancel_is_not_forgotten() {
    let mut c = ready(Mock::new(false), path());
    let i = buy(2);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let mut s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "canceled", 2)],
        fills: vec![fill("f1", "o1", "KXBTC15M-test", 1, "0.30", "0.01")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    };
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(c.ledger.exposure().unwrap(), 64 * CENT);
    s.fills
        .push(fill("f2", "o1", "KXBTC15M-test", 1, "0.29", "0.01"));
    s.positions = vec![position("KXBTC15M-test", 2)];
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.exposure().unwrap(), 61 * CENT);
}
#[tokio::test]
async fn restart_preserves_fills_and_journal_without_rebooking() {
    let p = path();
    let mut c = ready(Mock::new(false), p.clone());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![fill("f1", "o1", "KXBTC15M-test", 1, "0.30", "0.01")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    };
    c.apply_snapshot(&s, NOW).unwrap();
    drop(c);
    let mut c = Coordinator::open(Mock::new(false), p, limits()).unwrap();
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.fills.len(), 1);
    assert_eq!(c.ledger.exposure().unwrap(), 31 * CENT);
}
#[tokio::test]
async fn settlement_quantity_mismatch_and_correction_fail_closed() {
    let mut c = ready(Mock::new(false), path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let mut s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![fill("f1", "o1", "KXBTC15M-test", 1, "0.30", "0.01")],
        settlements: vec![settlement("KXBTC15M-test", 2, 200, "yes")],
        ..Default::default()
    };
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(c.ledger.accounting().unwrap().journal.len(), 0);
    s.settlements = vec![settlement("KXBTC15M-test", 1, 100, "yes")];
    c.apply_snapshot(&s, NOW).unwrap();
    s.settlements = vec![settlement("KXBTC15M-test", 1, 0, "no")];
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(c.ledger.accounting().unwrap().journal.len(), 1);
}
#[tokio::test]
async fn persistence_failure_prevents_network_submission() {
    let mock = Mock::new(false);
    let p = path();
    let mut c = ready(mock.clone(), p.clone());
    std::fs::create_dir(p.with_extension("pending")).unwrap();
    assert!(c.place(buy(1), 1.0, NOW).await.is_err());
    assert_eq!(mock.posts.load(Ordering::SeqCst), 0);
    assert!(!c.ready);
}
#[tokio::test]
async fn pause_does_not_prevent_safe_cancellation() {
    let mock = Mock::new(false);
    let mut c = ready(mock.clone(), path());
    c.place(buy(1), 1.0, NOW).await.unwrap();
    c.set_pause(true).unwrap();
    c.cancel_pending().await.unwrap();
    assert_eq!(mock.cancels.load(Ordering::SeqCst), 1);
    assert_eq!(c.ledger.exposure().unwrap(), 33 * CENT);
}
#[tokio::test]
async fn unknown_order_status_and_malformed_fill_never_assume_success() {
    let mut c = ready(Mock::new(false), path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let mut s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "new_exchange_status", 0)],
        ..Default::default()
    };
    assert!(c.apply_snapshot(&s, NOW).is_err());
    s.orders = vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)];
    let mut f = fill("f1", "o1", "KXBTC15M-test", 1, "0.30", "0.01");
    f["fee_cost"] = Value::Null;
    s.fills = vec![f];
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(c.ledger.fills.len(), 0);
}
#[tokio::test]
async fn actual_v2_no_buy_is_reported_as_sell_on_yes_book() {
    let mut c = ready(Mock::new(false), path());
    let i = intent("KXBTC15M-test", 2, "no", "buy", 44, 1, "demo-test", NOW);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    // Shape captured from the real demo fill: action=sell, outcome_side=no.
    let s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![
            json!({"fill_id":"real-shape","order_id":"o1","ticker":"KXBTC15M-test","exchange_index":2,"action":"sell","book_side":"ask","outcome_side":"no","side":"no","count_fp":"1.00","yes_price_dollars":"0.5600","no_price_dollars":"0.4400","fee_cost":"0.017300","created_time":"2026-09-13T16:20:24.184313Z"}),
        ],
        positions: vec![position("KXBTC15M-test", -1)],
        ..Default::default()
    };
    c.apply_snapshot(&s, NOW).unwrap();
    let a = c.ledger.accounting().unwrap();
    let p = a.positions.values().next().unwrap();
    assert_eq!(p.side, "no");
    assert_eq!(p.count, 100);
    assert_eq!(p.cost, 457300);
}
#[tokio::test]
async fn fractional_partial_fill_is_accounted_exactly_through_settlement() {
    let mut c = ready(Mock::new(false), path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let mut o = order(&cid, "o1", "KXBTC15M-test", "resting", 0);
    o["fill_count_fp"] = "0.37".into();
    let mut f = fill("fraction", "o1", "KXBTC15M-test", 0, "0.3000", "0.006500");
    f["count_fp"] = "0.37".into();
    let mut p = position("KXBTC15M-test", 0);
    p["position_fp"] = "0.37".into();
    let mut s = Snapshot {
        orders: vec![o],
        fills: vec![f],
        positions: vec![p],
        ..Default::default()
    };
    assert!(c.apply_snapshot(&s, NOW).is_err());
    assert_eq!(c.ledger.exposure().unwrap(), 325400);
    s.orders[0]["status"] = "canceled".into();
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.exposure().unwrap(), 117500);
    s.positions.clear();
    let mut settle = settlement("KXBTC15M-test", 0, 37, "yes");
    settle["yes_count_fp"] = "0.37".into();
    s.settlements = vec![settle];
    c.apply_snapshot(&s, NOW).unwrap();
    assert_eq!(c.ledger.accounting().unwrap().journal[0].pnl, 252500);
}
#[test]
fn version_one_pending_intent_migrates_without_losing_reservation() {
    let p = path();
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    let mut l = Ledger::default();
    let mut i = buy(1);
    i.count = 1;
    l.intents.insert(i.client_id.clone(), i);
    l.version = 1;
    std::fs::write(&p, serde_json::to_vec(&l).unwrap()).unwrap();
    let (_, l) = Store::open(p).unwrap();
    assert_eq!(l.version, 2);
    assert_eq!(l.exposure().unwrap(), 33 * CENT);
}

#[tokio::test]
async fn known_reduce_only_exit_remains_available_during_entry_pause() {
    let mut c = ready(Mock::new(false), path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![fill("f", "o1", "KXBTC15M-test", 1, "0.30", "0.01")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    };
    c.apply_snapshot(&s, NOW).unwrap();
    c.set_pause(true).unwrap();
    c.ready = false;
    c.last_reconciled = None;
    let exit = intent("KXBTC15M-test", 2, "yes", "sell", 40, 1, "exit", NOW + 1);
    assert!(c.place(exit, 0.0, NOW + 1).await.is_ok());
    assert_eq!(c.ledger.exposure().unwrap(), 31 * CENT);
}

#[tokio::test]
async fn fill_before_cancel_is_reconciled_without_unnecessary_delete() {
    let mock = Mock::new(false);
    let mut c = ready(mock.clone(), path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    mock.snapshots.lock().unwrap().push_back(Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![fill("f", "o1", "KXBTC15M-test", 1, "0.30", "0.01")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    });
    c.reconcile_cycle(NOW).await.unwrap();
    assert_eq!(mock.cancels.load(Ordering::SeqCst), 0);
    assert_eq!(c.ledger.exposure().unwrap(), 31 * CENT);
}
#[tokio::test]
async fn cancel_error_requires_terminal_exchange_evidence_to_clear() {
    let mut mock = Mock::new(false);
    Arc::get_mut(&mut mock).unwrap().cancel_error = true;
    let mut c = ready(mock.clone(), path());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let resting = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "resting", 0)],
        ..Default::default()
    };
    mock.snapshots
        .lock()
        .unwrap()
        .extend([resting.clone(), resting.clone()]);
    assert!(c.reconcile_cycle(NOW).await.is_err());
    assert_eq!(c.ledger.exposure().unwrap(), 33 * CENT);
    let filled = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "executed", 1)],
        fills: vec![fill("f", "o1", "KXBTC15M-test", 1, "0.30", "0.01")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    };
    mock.snapshots.lock().unwrap().extend([resting, filled]);
    c.reconcile_cycle(NOW).await.unwrap();
    assert_eq!(c.ledger.exposure().unwrap(), 31 * CENT);
}

#[tokio::test]
async fn pruned_confirmed_zero_fill_cancel_survives_restart() {
    let mock = Mock::new(false);
    let p = path();
    let mut c = ready(mock.clone(), p.clone());
    let i = buy(1);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    c.apply_snapshot(
        &Snapshot {
            orders: vec![order(&cid, "o1", "KXBTC15M-test", "canceled", 0)],
            ..Default::default()
        },
        NOW,
    )
    .unwrap();
    drop(c);
    let mut c = Coordinator::open(mock, p, limits()).unwrap();
    c.apply_snapshot(&Snapshot::default(), NOW + 1000).unwrap();
    assert!(c.ready);
    assert_eq!(c.ledger.intents.len(), 1);
    assert_eq!(c.ledger.exposure().unwrap(), 0);
    // Missing history cannot hide an actual unmatched position.
    assert!(c
        .apply_snapshot(
            &Snapshot {
                positions: vec![position("KXBTC15M-test", 1)],
                ..Default::default()
            },
            NOW + 1001
        )
        .is_err());
    assert!(!c.ready);
}

#[tokio::test]
async fn pruned_partial_cancel_still_requires_order_history() {
    let mut c = ready(Mock::new(false), path());
    let i = buy(2);
    let cid = i.client_id.clone();
    c.place(i, 1.0, NOW).await.unwrap();
    let mut s = Snapshot {
        orders: vec![order(&cid, "o1", "KXBTC15M-test", "canceled", 1)],
        fills: vec![fill("f1", "o1", "KXBTC15M-test", 1, "0.3000", "0.0100")],
        positions: vec![position("KXBTC15M-test", 1)],
        ..Default::default()
    };
    c.apply_snapshot(&s, NOW).unwrap();
    s.orders.clear();
    assert!(c.apply_snapshot(&s, NOW + 1000).is_err());
    assert!(!c.ready);
}

#[test]
fn windows_portable_ledger_replaces_existing_file_and_releases_lock() {
    let dir = std::env::temp_dir().join(format!("kalshi-win-{}", uuid::Uuid::new_v4()));
    let path = dir.join("ledger.json");
    {
        let (store, ledger) = bitcoin_prediction_bot_rs::execution::ledger::Store::open(&path).unwrap();
        store.save(&ledger).unwrap();
        store.save(&ledger).unwrap();
        assert!(bitcoin_prediction_bot_rs::execution::ledger::Store::open(&path).is_err());
    }
    let (store, ledger) = bitcoin_prediction_bot_rs::execution::ledger::Store::open(&path).unwrap();
    store.save(&ledger).unwrap();
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
