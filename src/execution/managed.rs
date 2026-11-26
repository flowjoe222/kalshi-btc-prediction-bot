//! Single execution owner for actual demo trading. Legacy strategy loops are
//! simulation-only; all exchange changes here follow persist -> send -> reconcile.
use super::{kalshi::KalshiExecutor, ledger::*, risk::RiskParams};
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub enum SubmitResult {
    Accepted(String),
    Rejected(String),
}
#[derive(Default, Clone, Debug)]
pub struct Snapshot {
    pub orders: Vec<Value>,
    pub fills: Vec<Value>,
    pub settlements: Vec<Value>,
    pub positions: Vec<Value>,
    pub balances: BTreeMap<i64, i64>,
}
#[async_trait]
pub trait Venue: Send + Sync {
    async fn submit(&self, i: &Intent) -> Result<SubmitResult>;
    async fn cancel(&self, id: &str, shard: i64) -> Result<()>;
    async fn snapshot(&self, shards: &BTreeSet<i64>, since: i64) -> Result<Snapshot>;
}
#[async_trait]
impl Venue for KalshiExecutor {
    async fn submit(&self, i: &Intent) -> Result<SubmitResult> {
        self.submit_intent(i).await
    }
    async fn cancel(&self, id: &str, shard: i64) -> Result<()> {
        self.cancel_on_shard(id, shard).await
    }
    async fn snapshot(&self, shards: &BTreeSet<i64>, since: i64) -> Result<Snapshot> {
        let mut out = Snapshot::default();
        for &shard in shards {
            let extra = vec![("min_ts".into(), since.to_string())];
            // Read the complete relevant history. A failed/truncated page never
            // becomes an empty account. Resting orders are fetched without age filter.
            out.orders.extend(
                self.demo_pages("/portfolio/orders", "orders", shard, &extra)
                    .await?,
            );
            out.orders.extend(
                self.demo_pages(
                    "/portfolio/orders",
                    "orders",
                    shard,
                    &[("status".into(), "resting".into())],
                )
                .await?,
            );
            out.fills.extend(
                self.demo_pages("/portfolio/fills", "fills", shard, &extra)
                    .await?,
            );
            out.settlements.extend(
                self.demo_pages("/portfolio/settlements", "settlements", shard, &extra)
                    .await?,
            );
            out.positions.extend(
                self.demo_pages("/portfolio/positions", "market_positions", shard, &[])
                    .await?,
            );
            let b = self
                .demo_read(
                    "/portfolio/balance",
                    &[("exchange_index".into(), shard.to_string())],
                )
                .await?;
            out.balances
                .insert(shard, b["balance"].as_i64().context("missing shard cash")?);
        }
        Ok(out)
    }
}
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str().with_context(|| format!("missing {k}"))
}
fn time(v: &Value, k: &str) -> Result<i64> {
    Ok(chrono::DateTime::parse_from_rfc3339(text(v, k)?)?.timestamp())
}
fn shard(v: &Value) -> Result<i64> {
    v["exchange_index"]
        .as_i64()
        .context("missing exchange_index")
}
fn quantity(v: &Value, fp: &str, legacy: &str) -> Result<i64> {
    if let Some(s) = v[fp].as_str() {
        contracts(s)
    } else {
        v[legacy]
            .as_i64()
            .and_then(|n| n.checked_mul(CONTRACT))
            .context("missing/overflowing quantity")
    }
}
fn parse_fill(v: &Value, intent: &Intent) -> Result<Fill> {
    // V2 action/side compatibility fields can describe the YES book: a NO
    // purchase is reported as action=sell. Normalize using persisted intent
    // plus authoritative book_side, never the legacy action label alone.
    let expected_book = if (intent.action == "buy") == (intent.side == "yes") {
        "bid"
    } else {
        "ask"
    };
    ensure!(
        text(v, "book_side")? == expected_book,
        "fill book side disagrees with intent"
    );
    let side = intent.side.as_str();
    let ticker = v["ticker"]
        .as_str()
        .or_else(|| v["market_ticker"].as_str())
        .context("fill ticker")?;
    Ok(Fill {
        id: text(v, "fill_id")?.into(),
        order_id: text(v, "order_id")?.into(),
        ticker: ticker.into(),
        shard: shard(v)?,
        action: intent.action.clone(),
        side: side.into(),
        count: quantity(v, "count_fp", "count")?,
        price: fixed(
            text(
                v,
                if side == "yes" {
                    "yes_price_dollars"
                } else {
                    "no_price_dollars"
                },
            )?,
            6,
        )?,
        fee: fixed(text(v, "fee_cost")?, 6)?,
        ts: chrono::DateTime::parse_from_rfc3339(text(v, "created_time")?)?.timestamp_micros(),
    })
}
fn parse_settlement(v: &Value) -> Result<Settlement> {
    Ok(Settlement {
        ticker: text(v, "ticker")?.into(),
        shard: shard(v)?,
        yes_count: quantity(v, "yes_count_fp", "yes_count")?,
        no_count: quantity(v, "no_count_fp", "no_count")?,
        revenue: v["revenue"]
            .as_i64()
            .context("missing settlement payout")?
            .checked_mul(CENT)
            .context("payout overflow")?,
        ts: time(v, "settled_time")?,
        result: text(v, "market_result")?.into(),
    })
}

pub struct Coordinator<V: Venue> {
    venue: Arc<V>,
    pub ledger: Ledger,
    store: Store,
    pub limits: RiskParams,
    pub ready: bool,
    pub reason: String,
    pub last_reconciled: Option<i64>,
    pub last_snapshot: Snapshot,
    pub shards: BTreeSet<i64>,
    started: i64,
}
impl<V: Venue> Coordinator<V> {
    pub fn open(venue: Arc<V>, path: impl AsRef<Path>, limits: RiskParams) -> Result<Self> {
        let (store, ledger) = Store::open(path)?;
        let shards = ledger
            .intents
            .values()
            .map(|i| i.shard)
            .chain([0, 2])
            .collect();
        Ok(Self {
            venue,
            ledger,
            store,
            limits,
            ready: false,
            reason: "startup reconciliation pending".into(),
            last_reconciled: None,
            last_snapshot: Snapshot::default(),
            shards,
            started: chrono::Utc::now().timestamp(),
        })
    }
    fn commit(&mut self, next: Ledger) -> Result<()> {
        if let Err(e) = self.store.save(&next) {
            self.ready = false;
            self.reason = "ledger persistence failed; execution paused".into();
            return Err(e);
        }
        self.ledger = next;
        Ok(())
    }
    pub fn set_pause(&mut self, active: bool) -> Result<()> {
        if self.ledger.kill_switch == active {
            return Ok(());
        }
        let mut next = self.ledger.clone();
        next.kill_switch = active;
        self.commit(next)?;
        tracing::info!(
            "Prediction entries {} by operator",
            if active { "paused" } else { "enabled" }
        );
        Ok(())
    }
    pub fn status(&self) -> Value {
        let now = chrono::Utc::now().timestamp();
        json!({"environment":"demo","quantity_scale":100,"money_scale":1000000,"strategy":"fair_value_15m","ready":self.ready,"entries_enabled":self.ready&&!self.ledger.kill_switch,"reason":self.reason,"last_reconciled":self.last_reconciled,"kill_switch":self.ledger.kill_switch,"exposure_microdollars":self.ledger.exposure().ok(),"daily_pnl_microdollars":self.ledger.daily_pnl(now).ok(),"pending_orders":self.ledger.intents.values().filter(|i|self.ledger.unresolved(i)).count(),"positions":self.ledger.accounting().ok().map(|a|a.positions),"journal":self.ledger.accounting().ok().map(|a|a.journal),"shard_cash_cents":self.last_snapshot.balances,"orders":self.ledger.intents,"unrelated_positions":self.last_snapshot.positions.iter().filter(|p|!crypto(p["ticker"].as_str().unwrap_or(""))).collect::<Vec<_>>()})
    }
    /// Must be called through one shared Mutex; reservation is durable before
    /// yielding to network I/O, and survives cancellation of this future/crashes.
    pub async fn place(&mut self, intent: Intent, confidence: f64, now: i64) -> Result<String> {
        // Reduce-only exits of reconciled owned quantity remain available while
        // new entries are paused. The ledger rejects any unresolved same-ticker
        // order and the exchange's reduce_only flag prevents creating exposure.
        if intent.action == "buy" {
            ensure!(self.ready, "{}", self.reason);
            ensure!(
                self.last_reconciled
                    .map_or(false, |t| now >= t && now - t <= 30),
                "account snapshot stale"
            );
            let cash = *self
                .last_snapshot
                .balances
                .get(&intent.shard)
                .context("no cash snapshot for shard")?;
            ensure!(
                intent.count * (intent.price_cents + 3) <= cash * CONTRACT,
                "insufficient shard cash including fee reserve"
            );
        }
        let mut next = self.ledger.clone();
        next.reserve(intent.clone(), &self.limits, confidence, now)?;
        self.commit(next)?;
        self.ready = false;
        self.reason = "order awaiting reconciliation".into();
        match self.venue.submit(&intent).await {
            Ok(SubmitResult::Accepted(id)) => {
                let mut next = self.ledger.clone();
                let i = next.intents.get_mut(&intent.client_id).unwrap();
                i.order_id = Some(id.clone());
                i.status = "accepted".into();
                self.commit(next)?;
                Ok(id)
            }
            Ok(SubmitResult::Rejected(reason)) => {
                let mut next = self.ledger.clone();
                next.intents.get_mut(&intent.client_id).unwrap().status = "rejected".into();
                self.commit(next)?;
                anyhow::bail!("order rejected: {reason}")
            }
            Err(e) => {
                self.reason = "submit response uncertain; client ID reserved, no retry".into();
                Err(e)
            }
        }
    }
    pub async fn cancel_pending(&mut self) -> Result<()> {
        let pending: Vec<_> = self
            .ledger
            .intents
            .values()
            .filter(|i| !terminal(&i.status) && i.order_id.is_some())
            .cloned()
            .collect();
        for i in pending {
            // Persist cancellation intent before DELETE; acknowledgement does not
            // free risk. Repeat DELETE is safe; repeat POST is never attempted.
            let mut next = self.ledger.clone();
            next.intents.get_mut(&i.client_id).unwrap().cancel_requested = true;
            self.commit(next)?;
            self.venue
                .cancel(i.order_id.as_deref().unwrap(), i.shard)
                .await?;
        }
        Ok(())
    }
    /// Refresh before cancellation so filled orders are not canceled blindly.
    /// If a fill wins a cancellation race, a confirming snapshot resolves it;
    /// a failed cancellation without terminal evidence remains an error.
    pub async fn reconcile_cycle(&mut self, now: i64) -> Result<()> {
        let first = self.reconcile(now).await;
        if !self
            .ledger
            .intents
            .values()
            .any(|i| i.order_id.is_some() && !terminal(&i.status))
        {
            return first;
        }
        let cancellation = self.cancel_pending().await;
        let refreshed = self.reconcile(now).await;
        if let Err(e) = cancellation {
            if self
                .ledger
                .intents
                .values()
                .any(|i| i.order_id.is_some() && !terminal(&i.status))
            {
                return Err(e);
            }
        }
        refreshed
    }

    pub async fn reconcile(&mut self, now: i64) -> Result<()> {
        self.ready = false;
        let since = self
            .ledger
            .intents
            .values()
            .map(|i| i.created_ts)
            .min()
            .unwrap_or(self.started)
            - 60;
        let snapshot = match self.venue.snapshot(&self.shards, since).await {
            Ok(s) => s,
            Err(e) => {
                self.reason = format!("account read failed: {e}");
                return Err(e);
            }
        };
        self.last_snapshot = snapshot.clone();
        let result = self.apply_snapshot(&snapshot, now);
        if let Err(e) = &result {
            self.reason = e.to_string();
        }
        result
    }
    pub fn apply_snapshot(&mut self, s: &Snapshot, now: i64) -> Result<()> {
        self.ready = false;
        let mut next = self.ledger.clone();
        let mut seen = BTreeSet::new();
        for order in &s.orders {
            let ticker = order["ticker"].as_str().unwrap_or("");
            if !crypto(ticker) {
                continue;
            }
            let oid = text(order, "order_id")?;
            // Lists overlap (resting + all). Keep monotonic observations.
            let client = order["client_order_id"].as_str().unwrap_or("");
            let owned = next
                .intents
                .values_mut()
                .find(|i| i.client_id == client || i.order_id.as_deref() == Some(oid));
            if let Some(i) = owned {
                ensure!(
                    i.ticker == ticker && i.shard == shard(order)?,
                    "order identity mismatch"
                );
                ensure!(
                    i.order_id.as_deref().map_or(true, |id| id == oid),
                    "duplicate acceptance for one intent"
                );
                i.order_id = Some(oid.into());
                let filled = quantity(order, "fill_count_fp", "fill_count")?;
                ensure!(
                    filled >= 0 && filled <= i.count,
                    "invalid cumulative filled quantity"
                );
                i.reported_fills = i.reported_fills.max(filled);
                let status = text(order, "status")?;
                if !terminal(&i.status) || terminal(status) {
                    i.status = status.into();
                }
                seen.insert(i.client_id.clone());
            } else {
                ensure!(
                    terminal(text(order, "status")?),
                    "unowned resting crypto order; entries paused"
                );
            }
        }
        let mut fills: Vec<_> = s
            .fills
            .iter()
            .filter(|f| {
                next.intents
                    .values()
                    .any(|i| i.order_id.as_deref() == f["order_id"].as_str())
            })
            .map(|f| {
                parse_fill(
                    f,
                    next.intents
                        .values()
                        .find(|i| i.order_id.as_deref() == f["order_id"].as_str())
                        .unwrap(),
                )
            })
            .collect::<Result<_>>()?;
        fills.sort_by_key(|f| (f.ts, f.id.clone()));
        for f in fills {
            next.observe_fill(f)?;
        }
        // Persist known fills even if the position endpoint is temporarily stale.
        self.commit(next.clone())?;
        for i in next.intents.values() {
            ensure!(
                !next.unresolved(i),
                "order {} awaiting terminal status / fill reconciliation",
                i.client_id
            );
            if i.status != "rejected" {
                ensure!(
                    seen.contains(&i.client_id)
                        // The venue may prune canceled, unfilled orders. A durable
                        // terminal observation with exactly zero reconciled fills
                        // has no remaining exposure; retain its audit record.
                        || (i.order_id.is_some()
                            && matches!(i.status.as_str(), "canceled" | "cancelled" | "expired")
                            && i.reported_fills == 0
                            && next.filled(i) == 0),
                    "owned order missing from complete history; entries paused"
                );
            }
        }
        for raw in &s.settlements {
            let key = format!(
                "{}:{}",
                raw["exchange_index"],
                raw["ticker"].as_str().unwrap_or("")
            );
            if next.accounting()?.positions.contains_key(&key)
                || next.settlements.contains_key(&key)
            {
                next.observe_settlement(parse_settlement(raw)?)?;
            }
        }
        self.commit(next.clone())?;
        let mut actual = BTreeMap::new();
        for p in &s.positions {
            let ticker = text(p, "ticker")?;
            if !crypto(ticker) {
                continue;
            }
            let count = quantity(p, "position_fp", "position")?;
            if count != 0 {
                let key = format!("{}:{ticker}", shard(p)?);
                if let Some(old) = actual.insert(key, count) {
                    ensure!(old == count, "inconsistent duplicate position");
                }
            }
        }
        next.match_positions(&actual)?;
        ensure!(
            next.exposure()? <= self.limits.max_total_exposure_cents * CENT,
            "actual cost exceeded exposure budget"
        );
        self.last_reconciled = Some(now);
        self.ready = true;
        self.reason = "orders, fills, settlements and positions reconciled".into();
        Ok(())
    }
}

pub fn intent(
    ticker: &str,
    shard: i64,
    side: &str,
    action: &str,
    price: i64,
    count: i64,
    signal: &str,
    now: i64,
) -> Intent {
    Intent {
        client_id: uuid::Uuid::new_v4().to_string(),
        order_id: None,
        ticker: ticker.into(),
        shard,
        side: side.into(),
        action: action.into(),
        price_cents: price,
        count: count * CONTRACT,
        created_ts: now,
        signal_id: signal.into(),
        status: "pending".into(),
        reported_fills: 0,
        cancel_requested: false,
    }
}

pub type SharedCoordinator = Arc<Mutex<Coordinator<KalshiExecutor>>>;
