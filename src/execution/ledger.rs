//! Durable prediction accounting: microdollars and hundredths of a contract.
//! Never infer a fill or settlement from order acceptance or balance changes.
use super::risk::RiskParams;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
pub const CONTRACT: i64 = 100;
pub const CENT: i64 = 10_000;
pub const DOLLAR: i64 = 1_000_000;

pub fn fixed(s: &str, scale: u32) -> Result<i64> {
    let (negative, s) = if let Some(v) = s.strip_prefix('-') {
        (true, v)
    } else {
        (false, s)
    };
    let mut pieces = s.split('.');
    let whole = pieces.next().unwrap_or("");
    let fraction = pieces.next().unwrap_or("");
    ensure!(
        !whole.is_empty()
            && whole.bytes().all(|b| b.is_ascii_digit())
            && fraction.bytes().all(|b| b.is_ascii_digit())
            && pieces.next().is_none(),
        "invalid fixed decimal"
    );
    let extra = fraction.get(scale as usize..).unwrap_or("");
    ensure!(
        extra.bytes().all(|b| b == b'0'),
        "unsupported decimal precision"
    );
    let fraction = &fraction[..fraction.len().min(scale as usize)];
    let unit = 10_i64.pow(scale);
    let n = whole
        .parse::<i64>()?
        .checked_mul(unit)
        .and_then(|n| {
            n.checked_add(if fraction.is_empty() {
                0
            } else {
                fraction
                    .parse::<i64>()
                    .unwrap_or(i64::MAX)
                    .saturating_mul(10_i64.pow(scale - fraction.len() as u32))
            })
        })
        .context("decimal overflow")?;
    Ok(if negative { -n } else { n })
}
pub fn contracts(s: &str) -> Result<i64> {
    fixed(s, 2)
}
pub fn crypto(t: &str) -> bool {
    [
        "KXBTC15M-",
        "KXETH15M-",
        "KXSOL15M-",
        "KXXRP15M-",
        "KXDOGE15M-",
    ]
    .iter()
    .any(|p| t.starts_with(p))
}
pub fn asset(t: &str) -> &str {
    t.split('-').next().unwrap_or(t)
}
pub fn terminal(s: &str) -> bool {
    matches!(
        s,
        "canceled" | "cancelled" | "executed" | "filled" | "rejected" | "expired"
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Intent {
    pub client_id: String,
    pub order_id: Option<String>,
    pub ticker: String,
    pub shard: i64,
    pub action: String,
    pub side: String,
    pub count: i64,
    pub price_cents: i64,
    pub created_ts: i64,
    pub signal_id: String,
    pub status: String,
    pub reported_fills: i64,
    pub cancel_requested: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fill {
    pub id: String,
    pub order_id: String,
    pub ticker: String,
    pub shard: i64,
    pub action: String,
    pub side: String,
    pub count: i64,
    pub price: i64,
    pub fee: i64,
    pub ts: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Settlement {
    pub ticker: String,
    pub shard: i64,
    pub yes_count: i64,
    pub no_count: i64,
    pub revenue: i64,
    pub ts: i64,
    pub result: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Position {
    pub ticker: String,
    pub shard: i64,
    pub side: String,
    pub count: i64,
    pub cost: i64,
    pub signal_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JournalEntry {
    pub id: String,
    pub ticker: String,
    pub kind: String,
    pub ts: i64,
    pub count: i64,
    pub revenue: i64,
    pub cost: i64,
    pub pnl: i64,
    pub signal_id: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Accounts {
    pub positions: BTreeMap<String, Position>,
    pub journal: Vec<JournalEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ledger {
    pub version: u32,
    pub environment: String,
    pub kill_switch: bool,
    pub intents: BTreeMap<String, Intent>,
    pub fills: BTreeMap<String, Fill>,
    pub settlements: BTreeMap<String, Settlement>,
}
impl Default for Ledger {
    fn default() -> Self {
        Self {
            version: 2,
            environment: "demo".into(),
            kill_switch: false,
            intents: BTreeMap::new(),
            fills: BTreeMap::new(),
            settlements: BTreeMap::new(),
        }
    }
}
impl Ledger {
    pub fn accounting(&self) -> Result<Accounts> {
        let mut out = Accounts::default();
        let mut fills: Vec<_> = self.fills.values().collect();
        fills.sort_by_key(|f| (f.ts, &f.id));
        for f in fills {
            let intent = self
                .intents
                .values()
                .find(|i| i.order_id.as_deref() == Some(&f.order_id))
                .context("fill has no owned intent")?;
            ensure!(
                intent.ticker == f.ticker
                    && intent.shard == f.shard
                    && intent.side == f.side
                    && intent.action == f.action,
                "fill/intent mismatch"
            );
            ensure!(
                f.count > 0 && (0..=DOLLAR).contains(&f.price) && f.fee >= 0,
                "invalid fill economics"
            );
            let key = format!("{}:{}", f.shard, f.ticker);
            if f.action == "buy" {
                let p = out.positions.entry(key).or_insert(Position {
                    ticker: f.ticker.clone(),
                    shard: f.shard,
                    side: f.side.clone(),
                    count: 0,
                    cost: 0,
                    signal_id: intent.signal_id.clone(),
                });
                ensure!(p.side == f.side, "opposite outcomes cannot be mixed");
                p.count = p.count.checked_add(f.count).context("quantity overflow")?;
                p.cost = p
                    .cost
                    .checked_add(
                        f.price
                            .checked_mul(f.count)
                            .map(|n| n / CONTRACT)
                            .and_then(|n| n.checked_add(f.fee))
                            .context("fill overflow")?,
                    )
                    .context("cost overflow")?;
            } else {
                ensure!(f.action == "sell", "invalid fill action");
                let p = out
                    .positions
                    .get_mut(&key)
                    .context("sale without owned position")?;
                ensure!(
                    p.side == f.side && p.count >= f.count,
                    "sale exceeds owned contracts"
                );
                let cost = if p.count == f.count {
                    p.cost
                } else {
                    (p.cost as i128 * f.count as i128 / p.count as i128) as i64
                };
                let revenue = f
                    .price
                    .checked_mul(f.count)
                    .map(|n| n / CONTRACT)
                    .and_then(|n| n.checked_sub(f.fee))
                    .context("sale overflow")?;
                out.journal.push(JournalEntry {
                    id: f.id.clone(),
                    ticker: f.ticker.clone(),
                    kind: "sale".into(),
                    ts: f.ts / 1_000_000,
                    count: f.count,
                    revenue,
                    cost,
                    pnl: revenue - cost,
                    signal_id: p.signal_id.clone(),
                });
                p.count -= f.count;
                p.cost -= cost;
            }
        }
        for (key, s) in &self.settlements {
            let p = out
                .positions
                .get_mut(key)
                .context("settlement without owned fills")?;
            let qty = if p.side == "yes" {
                s.yes_count
            } else {
                s.no_count
            };
            let opposite = if p.side == "yes" {
                s.no_count
            } else {
                s.yes_count
            };
            ensure!(
                qty == p.count && opposite == 0,
                "settlement quantity does not reconcile with fills"
            );
            ensure!(
                (0..=p.count * DOLLAR / CONTRACT).contains(&s.revenue),
                "invalid settlement revenue"
            );
            // Revenue is exchange payout. Entry costs already include fill fees;
            // settlement fee_cost is not deducted again (it summarizes fees).
            out.journal.push(JournalEntry {
                id: format!("settlement:{key}"),
                ticker: s.ticker.clone(),
                kind: format!("settlement:{}", s.result),
                ts: s.ts,
                count: p.count,
                revenue: s.revenue,
                cost: p.cost,
                pnl: s.revenue - p.cost,
                signal_id: p.signal_id.clone(),
            });
            p.count = 0;
            p.cost = 0;
        }
        out.positions.retain(|_, p| p.count > 0);
        Ok(out)
    }
    pub fn filled(&self, i: &Intent) -> i64 {
        self.fills
            .values()
            .filter(|f| Some(f.order_id.as_str()) == i.order_id.as_deref())
            .map(|f| f.count)
            .sum()
    }
    pub fn unresolved(&self, i: &Intent) -> bool {
        !terminal(&i.status) || self.filled(i) != i.reported_fills
    }
    pub fn exposure(&self) -> Result<i64> {
        let a = self.accounting()?;
        let held: i64 = a.positions.values().map(|p| p.cost).sum();
        let reserved: i64 = self
            .intents
            .values()
            .filter(|i| i.action == "buy" && self.unresolved(i))
            .map(|i| (i.count - self.filled(i)).max(0) * (i.price_cents + 3) * CENT / CONTRACT)
            .sum();
        Ok(held + reserved)
    }
    pub fn daily_pnl(&self, now: i64) -> Result<i64> {
        Ok(self
            .accounting()?
            .journal
            .iter()
            .filter(|e| e.ts.div_euclid(86400) == now.div_euclid(86400))
            .map(|e| e.pnl)
            .sum())
    }
    pub fn reserve(
        &mut self,
        i: Intent,
        limits: &RiskParams,
        confidence: f64,
        now: i64,
    ) -> Result<()> {
        ensure!(
            !self.intents.contains_key(&i.client_id),
            "duplicate intent; never resubmit"
        );
        ensure!(
            crypto(&i.ticker)
                && i.shard >= 0
                && matches!(i.side.as_str(), "yes" | "no")
                && matches!(i.action.as_str(), "buy" | "sell")
                && i.count > 0
                && i.count <= 1000 * CONTRACT
                && (1..100).contains(&i.price_cents),
            "invalid order"
        );
        ensure!(
            i.order_id.is_none() && i.status == "pending" && i.reported_fills == 0,
            "invalid new intent state"
        );
        let a = self.accounting()?;
        if i.action == "buy" {
            ensure!(!self.kill_switch, "entries paused by operator");
            ensure!(
                confidence.is_finite() && confidence >= limits.confidence_threshold,
                "confidence gate"
            );
            ensure!(
                !self.intents.values().any(|x| self.unresolved(x)),
                "unreconciled order blocks new entries"
            );
            ensure!(
                !a.positions
                    .values()
                    .any(|p| asset(&p.ticker) == asset(&i.ticker)),
                "asset already held"
            );
            ensure!(
                !self
                    .settlements
                    .contains_key(&format!("{}:{}", i.shard, i.ticker)),
                "market already settled"
            );
            ensure!(
                !self
                    .intents
                    .values()
                    .any(|x| x.action == "buy"
                        && x.created_ts.div_euclid(900) == now.div_euclid(900)),
                "one entry attempt per window"
            );
            ensure!(
                !self
                    .intents
                    .values()
                    .any(|x| x.created_ts > now - limits.cooldown_seconds as i64),
                "cooldown"
            );
            let reserve = i.count * (i.price_cents + 3) * CENT / CONTRACT;
            ensure!(
                reserve <= limits.max_position_cents * CENT,
                "per-order budget"
            );
            ensure!(
                self.exposure()? + reserve <= limits.max_total_exposure_cents * CENT,
                "aggregate budget"
            );
            ensure!(
                (-self.daily_pnl(now)?).max(0) + self.exposure()? + reserve
                    <= limits.max_daily_loss_cents * CENT,
                "daily loss plus at-risk budget"
            );
            ensure!(
                a.positions.len() < limits.max_concurrent_positions,
                "position count budget"
            );
        } else {
            let p = a
                .positions
                .get(&format!("{}:{}", i.shard, i.ticker))
                .context("exit has no owned position")?;
            ensure!(
                p.side == i.side && i.count <= p.count,
                "exit exceeds owned position"
            );
            ensure!(
                !self
                    .intents
                    .values()
                    .any(|x| x.ticker == i.ticker && self.unresolved(x)),
                "unresolved order on exit ticker"
            );
        }
        self.intents.insert(i.client_id.clone(), i);
        Ok(())
    }
    pub fn observe_fill(&mut self, f: Fill) -> Result<()> {
        if let Some(old) = self.fills.get(&f.id) {
            ensure!(old == &f, "fill changed after accounting");
            return Ok(());
        }
        let intent = self
            .intents
            .values()
            .find(|i| i.order_id.as_deref() == Some(&f.order_id))
            .context("unknown fill")?;
        ensure!(
            self.filled(intent) + f.count <= intent.count,
            "fills exceed requested count"
        );
        let mut next = self.clone();
        next.fills.insert(f.id.clone(), f);
        next.accounting()?;
        *self = next;
        Ok(())
    }
    pub fn observe_settlement(&mut self, s: Settlement) -> Result<()> {
        let key = format!("{}:{}", s.shard, s.ticker);
        if let Some(old) = self.settlements.get(&key) {
            ensure!(old == &s, "settlement correction requires reconciliation");
            return Ok(());
        }
        ensure!(
            !self
                .intents
                .values()
                .any(|i| i.ticker == s.ticker && self.unresolved(i)),
            "settlement while order unresolved"
        );
        let mut next = self.clone();
        next.settlements.insert(key, s);
        next.accounting()?;
        *self = next;
        Ok(())
    }
    pub fn match_positions(&self, actual: &BTreeMap<String, i64>) -> Result<()> {
        let expected: BTreeMap<_, _> = self
            .accounting()?
            .positions
            .into_iter()
            .map(|(k, p)| (k, if p.side == "yes" { p.count } else { -p.count }))
            .collect();
        ensure!(
            &expected == actual,
            "exchange positions differ from durable ledger; entries paused"
        );
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 2 && self.environment == "demo",
            "unsupported ledger version/environment"
        );
        let mut ids = BTreeSet::new();
        for (id, i) in &self.intents {
            ensure!(id == &i.client_id, "intent key mismatch");
            if let Some(oid) = &i.order_id {
                ensure!(ids.insert(oid), "order owned by multiple intents");
            }
        }
        self.accounting()?;
        Ok(())
    }
}

pub struct Store {
    pub path: PathBuf,
    _lock: File,
}
impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Ledger)> {
        let path = path.as_ref().to_owned();
        let parent = path.parent().context("ledger parent")?;
        std::fs::create_dir_all(parent)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.with_extension("lock"))?;
        lock.try_lock().map_err(|e| anyhow::anyhow!(
            "Cannot lock prediction ledger (another process may own it): {e}"
        ))?;
        let mut ledger = match std::fs::read(&path) {
            Ok(b) => serde_json::from_slice::<Ledger>(&b)
                .context("corrupt ledger; refusing empty fallback")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ledger::default(),
            Err(e) => return Err(e.into()),
        };
        // Explicit migration from whole-contract prototype; never discard state.
        if ledger.version == 1 {
            for i in ledger.intents.values_mut() {
                i.count = i
                    .count
                    .checked_mul(CONTRACT)
                    .context("quantity migration overflow")?;
                i.reported_fills = i
                    .reported_fills
                    .checked_mul(CONTRACT)
                    .context("fill migration overflow")?;
            }
            for f in ledger.fills.values_mut() {
                f.count = f
                    .count
                    .checked_mul(CONTRACT)
                    .context("fill migration overflow")?;
            }
            for s in ledger.settlements.values_mut() {
                s.yes_count = s
                    .yes_count
                    .checked_mul(CONTRACT)
                    .context("settlement migration overflow")?;
                s.no_count = s
                    .no_count
                    .checked_mul(CONTRACT)
                    .context("settlement migration overflow")?;
            }
            ledger.version = 2;
        }
        ledger.validate()?;
        Ok((Self { path, _lock: lock }, ledger))
    }
    pub fn save(&self, l: &Ledger) -> Result<()> {
        l.validate()?;
        let tmp = self.path.with_extension("pending");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&serde_json::to_vec_pretty(l)?)?;
        f.sync_all()?;
        drop(f); // Close before replacement on Windows.
        std::fs::rename(tmp, &self.path)?;
        #[cfg(unix)]
        File::open(self.path.parent().unwrap())?.sync_all()?;
        #[cfg(windows)]
        OpenOptions::new().write(true).open(&self.path)?.sync_all()?;
        Ok(())
    }
}
