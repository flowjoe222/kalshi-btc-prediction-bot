#!/usr/bin/env python3
"""Kalshi KXBTC15M 15-minute UP/DOWN fair-value prediction.

Public-data snapshot that mirrors the Rust bot's conservative-v2
Black-Scholes digital fair-value logic in src/signals/fair_value.rs
and the entry gate in src/execution/runtime.rs.

Fetches the active KXBTC15M market from Kalshi's public API and BTC-USD
spot (plus 1-minute candles for realized vol) from Coinbase. Prints a
prediction summary. Always exits 0 on a completed run, including the
Thursday 03:00-05:00 America/New_York maintenance skip.

Optional --ledger PATH writes a paper-trading ledger (latest snapshot,
open 1-contract entries, and settled P&L) for the GitHub Pages dashboard.
Stdlib only. This does not place orders.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KALSHI_MARKETS_URL = (
    "https://api.elections.kalshi.com/trade-api/v2/markets"
    "?series_ticker=KXBTC15M&status=open&limit=20"
)
KALSHI_MARKET_URL = "https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}"
COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/BTC-USD/candles"

# config.yaml kalshi.fv_* / confidence_threshold (conservative-v2)
FV_MIN_EDGE_CENTS = 8
FV_SLIPPAGE_CENTS = 2
FV_VOL_FLOOR_PER_SEC = 0.00005
FV_MAX_TTC_SECS = 2400.0
CONFIDENCE_THRESHOLD = 0.55
CONTRACT_COUNT = 1
USER_AGENT = "kalshi-btc-15m-predictor/1.0 (stdlib; public data only)"
TIMEOUT_SECS = 20
LEDGER_SCHEMA_VERSION = 1
CHICAGO = ZoneInfo("America/Chicago")
NEW_YORK = ZoneInfo("America/New_York")


def http_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECS) as resp:
        return json.loads(resp.read().decode("utf-8"))


def erf(x: float) -> float:
    """Abramowitz & Stegun 7.1.26 (same coefficients as the Rust bot)."""
    sign = -1.0 if x < 0.0 else 1.0
    x = abs(x)
    t = 1.0 / (1.0 + 0.3275911 * x)
    y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * math.exp(-x * x)
    return sign * y


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / math.sqrt(2.0)))


def fair_prob_above(spot: float, strike: float, sigma_per_sec: float, secs_to_expiry: float) -> float:
    if spot <= 0.0 or strike <= 0.0 or sigma_per_sec <= 0.0 or secs_to_expiry <= 0.0:
        return 1.0 if spot >= strike else 0.0
    t = secs_to_expiry
    tau_eff = (t - 60.0) + 60.0 / 3.0 if t > 60.0 else t / 3.0
    sigma_h = sigma_per_sec * math.sqrt(tau_eff)
    if sigma_h <= 0.0:
        return 1.0 if spot >= strike else 0.0
    d2 = math.log(spot / strike) / sigma_h
    return norm_cdf(d2)


def fair_cents_yes(p_above: float) -> int:
    return max(1, min(99, int(round(p_above * 100.0))))


def taker_fee_cents(price_cents: int, count: int) -> int:
    p = price_cents / 100.0
    cents = 0.07 * count * p * (1.0 - p) * 100.0
    cents = round(cents * 1_000_000.0) / 1_000_000.0
    return int(math.ceil(cents))


def net_edge_cents(fair_cents: int, price_cents: int, slippage_cents: int, count: int) -> int:
    gross = fair_cents - price_cents
    if count > 0:
        fee_per = (taker_fee_cents(price_cents, count) + count - 1) // count
    else:
        fee_per = taker_fee_cents(price_cents, 1)
    return gross - fee_per - slippage_cents


def realized_vol_from_minute_closes(closes: list[float]) -> float | None:
    """Stddev of 1-minute log-returns, scaled to per-second, matching realized_vol_per_step."""
    if len(closes) < 3:
        return None
    rets = []
    for a, b in zip(closes, closes[1:]):
        if a > 0.0 and b > 0.0:
            rets.append(math.log(b / a))
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    s_per_min = math.sqrt(var)
    if not math.isfinite(s_per_min) or s_per_min <= 0.0:
        return None
    return s_per_min / math.sqrt(60.0)


def side_confidence(p_yes: float, side: str) -> float | None:
    if not math.isfinite(p_yes) or not (0.0 <= p_yes <= 1.0):
        return None
    if side == "yes":
        p = p_yes
    elif side == "no":
        p = 1.0 - p_yes
    else:
        return None
    return max(0.0, 2.0 * p - 1.0)


def conservative_side_value(p_yes: float, side: str) -> int | None:
    if side_confidence(p_yes, side) is None:
        return None
    p = p_yes if side == "yes" else 1.0 - p_yes
    return int(math.floor((0.5 + 0.75 * (p - 0.5)) * 100.0))


def conservative_cost_allowed(price: int, count: int, slippage: int) -> bool:
    if not (1 <= price < 100) or count <= 0 or slippage < 0:
        return False
    fee_per = (taker_fee_cents(price, count) + count - 1) // count
    return price + fee_per + slippage <= 75


def dollars_to_cents(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v > 0.0:
        return int(round(v * 100.0))
    return None


def parse_rfc3339(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def in_maintenance_window(now_et: datetime) -> bool:
    # Thursday 03:00-05:00 America/New_York (2:00-4:00 AM America/Chicago).
    return now_et.weekday() == 3 and 3 <= now_et.hour < 5


def pick_market(markets: list[dict], now: datetime) -> dict | None:
    btc = [m for m in markets if str(m.get("ticker") or "").startswith("KXBTC15M-")]
    live = []
    upcoming = []
    for m in btc:
        try:
            open_t = parse_rfc3339(m["open_time"])
            close_t = parse_rfc3339(m["close_time"])
        except (KeyError, TypeError, ValueError):
            continue
        if open_t <= now < close_t:
            live.append(m)
        elif close_t > now:
            upcoming.append(m)
    if live:
        return min(live, key=lambda m: parse_rfc3339(m["close_time"]))
    if upcoming:
        return min(upcoming, key=lambda m: parse_rfc3339(m["open_time"]))
    return None


def fetch_spot() -> float:
    data = http_json(COINBASE_SPOT_URL)
    amount = float(data["data"]["amount"])
    if amount <= 0:
        raise RuntimeError("Coinbase spot was non-positive")
    return amount


def fetch_minute_closes(minutes: int = 60) -> list[float]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes + 2)
    url = (
        f"{COINBASE_CANDLES_URL}?granularity=60"
        f"&start={start.isoformat().replace('+00:00', 'Z')}"
        f"&end={end.isoformat().replace('+00:00', 'Z')}"
    )
    rows = http_json(url)
    # Each row: [time, low, high, open, close, volume], newest first.
    rows = sorted(rows, key=lambda r: r[0])
    return [float(r[4]) for r in rows if float(r[4]) > 0]


def evaluate_side(p_yes: float, side: str, price: int | None) -> dict[str, Any]:
    conf = side_confidence(p_yes, side)
    fair = conservative_side_value(p_yes, side)
    out: dict[str, Any] = {
        "side": side,
        "market_cents": price,
        "conservative_fair_cents": fair,
        "confidence": conf,
        "net_edge_cents": None,
        "cost_allowed": False,
        "clears_edge": False,
        "reason": None,
    }
    if price is None or not (1 <= price < 100):
        out["reason"] = "no tradeable ask"
        return out
    if conf is None or fair is None:
        out["reason"] = "invalid probability"
        return out
    allowed = conservative_cost_allowed(price, CONTRACT_COUNT, FV_SLIPPAGE_CENTS)
    net = net_edge_cents(fair, price, FV_SLIPPAGE_CENTS, CONTRACT_COUNT)
    out["cost_allowed"] = allowed
    out["net_edge_cents"] = net
    if conf < CONFIDENCE_THRESHOLD:
        out["reason"] = f"confidence {conf:.3f} < {CONFIDENCE_THRESHOLD:.2f}"
    elif not allowed:
        out["reason"] = "fee+slippage-inclusive cost above 75c ceiling"
    elif net < FV_MIN_EDGE_CENTS:
        out["reason"] = f"net edge {net}c < fv_min_edge_cents {FV_MIN_EDGE_CENTS}c"
    else:
        out["clears_edge"] = True
        out["reason"] = f"clears conservative-v2 gate (net edge {net}c >= {FV_MIN_EDGE_CENTS}c)"
    return out


def fmt_cents(c: int | None) -> str:
    if c is None:
        return "n/a"
    return f"{c}c (${c / 100:.2f})"


def fmt_prob(p: float | None) -> str:
    if p is None:
        return "n/a"
    return f"{p * 100:.2f}%"


def empty_ledger() -> dict[str, Any]:
    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "updated_at": None,
        "latest_run": None,
        "open_trades": [],
        "settled_trades": [],
    }


def load_ledger(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return empty_ledger()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty_ledger()
    if not isinstance(data, dict):
        return empty_ledger()
    data.setdefault("schema_version", LEDGER_SCHEMA_VERSION)
    data.setdefault("open_trades", [])
    data.setdefault("settled_trades", [])
    data.setdefault("latest_run", None)
    if not isinstance(data["open_trades"], list):
        data["open_trades"] = []
    if not isinstance(data["settled_trades"], list):
        data["settled_trades"] = []
    return data


def save_ledger(path: str, ledger: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    ledger["schema_version"] = LEDGER_SCHEMA_VERSION
    ledger["updated_at"] = iso_utc(datetime.now(timezone.utc))
    text = json.dumps(ledger, indent=2) + "\n"
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(p)


def fetch_market_by_ticker(ticker: str) -> dict[str, Any]:
    url = KALSHI_MARKET_URL.format(ticker=urllib.parse.quote(ticker, safe=""))
    data = http_json(url)
    market = data.get("market") if isinstance(data, dict) else None
    if not isinstance(market, dict):
        raise RuntimeError(f"unexpected market payload for {ticker}")
    return market


def market_is_settled(market: dict[str, Any]) -> bool:
    status = str(market.get("status") or "").strip().lower()
    result = str(market.get("result") or "").strip().lower()
    if status in {"finalized", "settled"}:
        return True
    if result in {"yes", "no"}:
        return True
    return False


def paper_pnl(side: str, entry_cents: int, fee_cents: int, result: str) -> tuple[bool, float, float]:
    """1-contract paper P&L: $1.00 payout if won, minus entry cost and taker fee."""
    won = side.strip().lower() == result.strip().lower()
    cost = (entry_cents + fee_cents) / 100.0
    payout = 1.00 if won else 0.00
    pnl = round(payout - cost, 2)
    return won, pnl, payout


def settle_open_trades(ledger: dict[str, Any]) -> int:
    still_open: list[dict[str, Any]] = []
    settled_now = 0
    for trade in ledger.get("open_trades") or []:
        ticker = str(trade.get("ticker") or "")
        if not ticker:
            still_open.append(trade)
            continue
        try:
            market = fetch_market_by_ticker(ticker)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError, KeyError, ValueError) as exc:
            print(f"Ledger: could not check {ticker}: {exc}")
            still_open.append(trade)
            continue
        if not market_is_settled(market):
            still_open.append(trade)
            continue
        result = str(market.get("result") or "").strip().lower()
        if result not in {"yes", "no"}:
            still_open.append(trade)
            continue
        side = str(trade.get("side") or "").strip().lower()
        try:
            entry_cents = int(trade["entry_cents"])
            fee_cents = int(trade.get("fee_cents") or 0)
        except (KeyError, TypeError, ValueError):
            still_open.append(trade)
            continue
        won, pnl, payout = paper_pnl(side, entry_cents, fee_cents, result)
        settled = dict(trade)
        settled_at = market.get("settlement_ts") or iso_utc(datetime.now(timezone.utc))
        settled["settled_at"] = settled_at
        settled["result"] = result
        settled["won"] = won
        settled["pnl_dollars"] = pnl
        settled["payout_dollars"] = payout
        settled["market_status"] = market.get("status")
        ledger["settled_trades"].append(settled)
        settled_now += 1
        outcome = "WIN" if won else "LOSS"
        print(f"Ledger: settled {ticker} {outcome} {pnl:+.2f}")
    ledger["open_trades"] = still_open
    return settled_now


def already_have_trade(ledger: dict[str, Any], ticker: str) -> bool:
    for trade in (ledger.get("open_trades") or []) + (ledger.get("settled_trades") or []):
        if trade.get("ticker") == ticker:
            return True
    return False


def maybe_open_paper_trade(ledger: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    prediction = snapshot.get("prediction")
    if prediction not in {"UP", "DOWN"}:
        return False
    ticker = snapshot.get("ticker")
    if not ticker or already_have_trade(ledger, ticker):
        return False
    side = "yes" if prediction == "UP" else "no"
    entry = snapshot.get("yes_ask_cents") if side == "yes" else snapshot.get("no_ask_cents")
    if not isinstance(entry, int) or not (1 <= entry < 100):
        return False
    # Re-check the conservative-v2 gate so a HOLD path can never open a fill.
    if not snapshot.get("entry_cleared"):
        return False
    fee = taker_fee_cents(entry, CONTRACT_COUNT)
    edge_key = "yes_net_edge_cents" if side == "yes" else "no_net_edge_cents"
    trade = {
        "id": f"{ticker}:{side}:{snapshot.get('run_at')}",
        "ticker": ticker,
        "side": side,
        "prediction": prediction,
        "entry_cents": entry,
        "fee_cents": fee,
        "contracts": CONTRACT_COUNT,
        "opened_at": snapshot.get("run_at"),
        "open_time": snapshot.get("open_time"),
        "close_time": snapshot.get("close_time"),
        "strike": snapshot.get("strike"),
        "spot_at_entry": snapshot.get("spot"),
        "p_up_at_entry": snapshot.get("p_up"),
        "net_edge_cents": snapshot.get(edge_key),
    }
    ledger["open_trades"].append(trade)
    print(f"Ledger: opened 1-contract paper {prediction} on {ticker} at {entry}c + {fee}c fee")
    return True


def update_ledger(path: str, snapshot: dict[str, Any]) -> None:
    ledger = load_ledger(path)
    n_settled = settle_open_trades(ledger)
    ledger["latest_run"] = snapshot
    opened = maybe_open_paper_trade(ledger, snapshot)
    save_ledger(path, ledger)
    print()
    print(
        f"Ledger: wrote {path}  settled this run: {n_settled}  "
        f"opened: {1 if opened else 0}  "
        f"open: {len(ledger['open_trades'])}  "
        f"settled total: {len(ledger['settled_trades'])}"
    )


def finish(code: int, ledger_path: str | None, snapshot: dict[str, Any]) -> int:
    if ledger_path:
        try:
            update_ledger(ledger_path, snapshot)
        except (OSError, urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError, ValueError) as exc:
            print(f"ERROR: failed to update ledger: {exc}")
    return code


def base_snapshot(now_utc: datetime, status: str) -> dict[str, Any]:
    return {
        "run_at": iso_utc(now_utc),
        "status": status,
        "ticker": None,
        "title": None,
        "market_status": None,
        "open_time": None,
        "close_time": None,
        "window_label": None,
        "strike": None,
        "spot": None,
        "yes_bid_cents": None,
        "yes_ask_cents": None,
        "no_ask_cents": None,
        "p_up": None,
        "p_down": None,
        "yes_net_edge_cents": None,
        "no_net_edge_cents": None,
        "yes_reason": None,
        "no_reason": None,
        "prediction": "HOLD",
        "prediction_detail": None,
        "entry_cleared": False,
        "message": None,
    }


def main(ledger_path: str | None = None) -> int:
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(NEW_YORK)
    now_ct = now_utc.astimezone(CHICAGO)
    snapshot = base_snapshot(now_utc, "ok")
    print("Kalshi KXBTC15M 15-minute fair-value prediction")
    print(f"Time: {now_ct.strftime('%Y-%m-%d %H:%M:%S %Z')} / {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()

    if in_maintenance_window(now_et):
        msg = (
            "SKIP: Kalshi scheduled maintenance is Thursday 03:00-05:00 America/New_York"
        )
        print(msg)
        print("(2:00-4:00 AM America/Chicago). No prediction this window.")
        snapshot["status"] = "maintenance"
        snapshot["prediction"] = "HOLD"
        snapshot["prediction_detail"] = "HOLD during Thursday maintenance window"
        snapshot["message"] = "Kalshi Thursday maintenance window (2:00–4:00 AM CST)"
        return finish(0, ledger_path, snapshot)

    try:
        payload = http_json(KALSHI_MARKETS_URL)
        markets = payload.get("markets") or []
        market = pick_market(markets, now_utc)
        if market is None:
            print("No open or upcoming KXBTC15M market found on the public API.")
            snapshot["status"] = "no_market"
            snapshot["prediction_detail"] = "No open or upcoming KXBTC15M market found"
            snapshot["message"] = snapshot["prediction_detail"]
            return finish(0, ledger_path, snapshot)
        spot = fetch_spot()
        closes = fetch_minute_closes(60)
        sigma_raw = realized_vol_from_minute_closes(closes)
        sigma = FV_VOL_FLOOR_PER_SEC if sigma_raw is None else max(sigma_raw, FV_VOL_FLOOR_PER_SEC)
        used_floor = sigma_raw is None or sigma_raw < FV_VOL_FLOOR_PER_SEC
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, KeyError, ValueError, RuntimeError) as exc:
        print(f"ERROR: failed to fetch public market data: {exc}")
        snapshot["status"] = "error"
        snapshot["prediction_detail"] = f"Failed to fetch public market data: {exc}"
        snapshot["message"] = snapshot["prediction_detail"]
        return finish(0, ledger_path, snapshot)

    ticker = market.get("ticker", "?")
    title = market.get("title", "")
    status = market.get("status", "")
    strike = market.get("floor_strike")
    try:
        strike_f = float(strike) if strike not in (None, "") else None
    except (TypeError, ValueError):
        strike_f = None
    close_t = parse_rfc3339(market["close_time"])
    open_t = parse_rfc3339(market["open_time"])
    ttc = (close_t - now_utc).total_seconds()
    yes_ask = dollars_to_cents(market.get("yes_ask_dollars") or market.get("yes_ask"))
    yes_bid = dollars_to_cents(market.get("yes_bid_dollars") or market.get("yes_bid"))
    no_ask_quoted = dollars_to_cents(market.get("no_ask_dollars") or market.get("no_ask"))
    no_ask = (100 - yes_bid) if yes_bid is not None else no_ask_quoted

    window_label = (
        f"{open_t.astimezone(CHICAGO).strftime('%H:%M %Z')} -> "
        f"{close_t.astimezone(CHICAGO).strftime('%H:%M %Z')}"
    )
    snapshot.update({
        "ticker": ticker,
        "title": title or None,
        "market_status": status or None,
        "open_time": iso_utc(open_t),
        "close_time": iso_utc(close_t),
        "window_label": window_label,
        "strike": strike_f,
        "spot": spot,
        "yes_bid_cents": yes_bid,
        "yes_ask_cents": yes_ask,
        "no_ask_cents": no_ask,
    })

    print(f"Market:     {ticker}")
    if title:
        print(f"Title:      {title}")
    print(f"Status:     {status}")
    print(f"Window:     {window_label}")
    print(f"Time left:  {max(0, int(ttc))} seconds")
    if strike_f:
        print(f"Strike K:   ${strike_f:,.2f}  (60s BRTI average at open; Yes if close TWAP >= K)")
    else:
        print("Strike K:   TBD (not published yet)")
    print(f"Coinbase:   ${spot:,.2f}  (index proxy; CF Benchmarks BRTI is the official settlement)")
    print(f"Kalshi Yes: bid {fmt_cents(yes_bid)} / ask {fmt_cents(yes_ask)}")
    print(f"Kalshi No:  ask {fmt_cents(no_ask)}  (100 - Yes bid, matching the Rust bot)")
    print()
    print(f"Realized vol: {sigma:.8f} per second"
          f"{'  [vol floor applied]' if used_floor else ''}"
          f"  from {len(closes)} Coinbase 1-minute closes")
    print("              (one-shot job uses 1-minute candles scaled by 1/sqrt(60);")
    print("               the live Rust bot uses ~1s Coinbase ticks, window=300.)")

    if not strike_f or strike_f <= 0:
        print()
        print("PREDICTION: withheld; floor_strike is not available yet.")
        snapshot["status"] = "no_strike"
        snapshot["prediction"] = "HOLD"
        snapshot["prediction_detail"] = "withheld; floor_strike is not available yet"
        snapshot["message"] = snapshot["prediction_detail"]
        return finish(0, ledger_path, snapshot)
    if ttc <= 0:
        print()
        print("PREDICTION: window already closed.")
        snapshot["status"] = "window_closed"
        snapshot["prediction"] = "HOLD"
        snapshot["prediction_detail"] = "window already closed"
        snapshot["message"] = snapshot["prediction_detail"]
        return finish(0, ledger_path, snapshot)

    p_yes = fair_prob_above(spot, strike_f, sigma, ttc)
    p_no = 1.0 - p_yes
    raw_yes = fair_cents_yes(p_yes)
    yes_eval = evaluate_side(p_yes, "yes", yes_ask)
    no_eval = evaluate_side(p_yes, "no", no_ask)

    snapshot["p_up"] = round(p_yes, 6)
    snapshot["p_down"] = round(p_no, 6)
    snapshot["yes_net_edge_cents"] = yes_eval["net_edge_cents"]
    snapshot["no_net_edge_cents"] = no_eval["net_edge_cents"]
    snapshot["yes_reason"] = yes_eval["reason"]
    snapshot["no_reason"] = no_eval["reason"]

    print()
    print(f"Model P(UP / Yes):   {fmt_prob(p_yes)}   raw fair {raw_yes}c")
    print(f"Model P(DOWN / No):  {fmt_prob(p_no)}")
    print(f"Conservative Yes:    {fmt_cents(yes_eval['conservative_fair_cents'])}  "
          f"(25% model-risk haircut; not a calibrated probability)")
    print(f"Conservative No:     {fmt_cents(no_eval['conservative_fair_cents'])}")
    print()
    print(f"Yes net edge: {yes_eval['net_edge_cents']}c   {yes_eval['reason']}")
    print(f"No  net edge: {no_eval['net_edge_cents']}c   {no_eval['reason']}")
    print()

    candidates = [e for e in (yes_eval, no_eval) if e["clears_edge"]]
    if ttc > FV_MAX_TTC_SECS:
        detail = (
            f"HOLD  (time to close {ttc:.0f}s exceeds fv_max_ttc_secs {FV_MAX_TTC_SECS:.0f}s)"
        )
        print(f"PREDICTION: {detail}")
        snapshot["prediction"] = "HOLD"
        snapshot["prediction_detail"] = detail
        snapshot["entry_cleared"] = False
    elif ttc < 60:
        detail = "HOLD  (inside the final 60-second settlement averaging window)"
        print(f"PREDICTION: {detail}")
        snapshot["prediction"] = "HOLD"
        snapshot["prediction_detail"] = detail
        snapshot["entry_cleared"] = False
    elif candidates:
        best = max(candidates, key=lambda e: e["net_edge_cents"])
        label = "UP (buy Yes)" if best["side"] == "yes" else "DOWN (buy No)"
        print(f"PREDICTION: {label}")
        print(f"            conservative fair {fmt_cents(best['conservative_fair_cents'])} vs market {fmt_cents(best['market_cents'])}")
        print(f"            net edge after fee+slippage: {best['net_edge_cents']}c  (gate is {FV_MIN_EDGE_CENTS}c)")
        snapshot["prediction"] = "UP" if best["side"] == "yes" else "DOWN"
        snapshot["prediction_detail"] = (
            f"{label}; conservative fair {fmt_cents(best['conservative_fair_cents'])} "
            f"vs market {fmt_cents(best['market_cents'])}; "
            f"net edge {best['net_edge_cents']}c"
        )
        snapshot["entry_cleared"] = True
    else:
        direction = "UP" if p_yes >= 0.5 else "DOWN"
        detail = (
            f"HOLD  (model leans {direction} at {fmt_prob(max(p_yes, p_no))}, "
            f"but no side clears the {FV_MIN_EDGE_CENTS}c net-edge gate)"
        )
        print(f"PREDICTION: {detail}")
        snapshot["prediction"] = "HOLD"
        snapshot["prediction_detail"] = detail
        snapshot["entry_cleared"] = False

    print()
    print("No orders were placed. Demo/live trading requires Kalshi API credentials and the Rust bot.")
    return finish(0, ledger_path, snapshot)


if __name__ == "__main__":
    # Sanity checks against the Rust unit tests in fair_value.rs
    assert conservative_side_value(1.0, "yes") == 87
    assert conservative_side_value(0.9, "yes") == 80
    assert conservative_side_value(0.1, "no") == 80
    assert conservative_cost_allowed(71, 1, 2)
    assert not conservative_cost_allowed(73, 1, 2)
    assert not conservative_cost_allowed(86, 1, 2)
    parser = argparse.ArgumentParser(
        description="Kalshi KXBTC15M 15-minute UP/DOWN fair-value prediction"
    )
    parser.add_argument(
        "--ledger",
        default=None,
        help="Path to a paper-trading ledger JSON file (e.g. docs/data/ledger.json)",
    )
    args = parser.parse_args()
    sys.exit(main(args.ledger))
