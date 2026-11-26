#!/usr/bin/env python3
"""Kalshi KXBTC15M 15-minute UP/DOWN fair-value prediction.

Public-data snapshot that mirrors the Rust bot's conservative-v2
Black-Scholes digital fair-value logic in src/signals/fair_value.rs
and the entry gate in src/execution/runtime.rs.

Fetches the active KXBTC15M market from Kalshi's public API and BTC-USD
spot (plus 1-minute candles for realized vol) from Coinbase. Prints a
prediction summary. Always exits 0 on a completed run, including the
Thursday 03:00-05:00 America/New_York maintenance skip.

Stdlib only. This does not place orders.
"""

from __future__ import annotations

import json
import math
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Any
from zoneinfo import ZoneInfo

KALSHI_MARKETS_URL = (
    "https://api.elections.kalshi.com/trade-api/v2/markets"
    "?series_ticker=KXBTC15M&status=open&limit=20"
)
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


def main() -> int:
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    now_ct = now_utc.astimezone(ZoneInfo("America/Chicago"))
    print("Kalshi KXBTC15M 15-minute fair-value prediction")
    print(f"Time: {now_ct.strftime('%Y-%m-%d %H:%M:%S %Z')} / {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()

    if in_maintenance_window(now_et):
        print("SKIP: Kalshi scheduled maintenance is Thursday 03:00-05:00 America/New_York")
        print("(2:00-4:00 AM America/Chicago). No prediction this window.")
        return 0

    try:
        payload = http_json(KALSHI_MARKETS_URL)
        markets = payload.get("markets") or []
        market = pick_market(markets, now_utc)
        if market is None:
            print("No open or upcoming KXBTC15M market found on the public API.")
            return 0
        spot = fetch_spot()
        closes = fetch_minute_closes(60)
        sigma_raw = realized_vol_from_minute_closes(closes)
        sigma = FV_VOL_FLOOR_PER_SEC if sigma_raw is None else max(sigma_raw, FV_VOL_FLOOR_PER_SEC)
        used_floor = sigma_raw is None or sigma_raw < FV_VOL_FLOOR_PER_SEC
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, KeyError, ValueError, RuntimeError) as exc:
        print(f"ERROR: failed to fetch public market data: {exc}")
        return 0

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

    print(f"Market:     {ticker}")
    if title:
        print(f"Title:      {title}")
    print(f"Status:     {status}")
    print(f"Window:     {open_t.astimezone(ZoneInfo('America/Chicago')).strftime('%H:%M %Z')} -> "
          f"{close_t.astimezone(ZoneInfo('America/Chicago')).strftime('%H:%M %Z')}")
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
        return 0
    if ttc <= 0:
        print()
        print("PREDICTION: window already closed.")
        return 0

    p_yes = fair_prob_above(spot, strike_f, sigma, ttc)
    p_no = 1.0 - p_yes
    raw_yes = fair_cents_yes(p_yes)
    yes_eval = evaluate_side(p_yes, "yes", yes_ask)
    no_eval = evaluate_side(p_yes, "no", no_ask)

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
        print(f"PREDICTION: HOLD  (time to close {ttc:.0f}s exceeds fv_max_ttc_secs {FV_MAX_TTC_SECS:.0f}s)")
    elif ttc < 60:
        print("PREDICTION: HOLD  (inside the final 60-second settlement averaging window)")
    elif candidates:
        best = max(candidates, key=lambda e: e["net_edge_cents"])
        label = "UP (buy Yes)" if best["side"] == "yes" else "DOWN (buy No)"
        print(f"PREDICTION: {label}")
        print(f"            conservative fair {fmt_cents(best['conservative_fair_cents'])} vs market {fmt_cents(best['market_cents'])}")
        print(f"            net edge after fee+slippage: {best['net_edge_cents']}c  (gate is {FV_MIN_EDGE_CENTS}c)")
    else:
        direction = "UP" if p_yes >= 0.5 else "DOWN"
        print(f"PREDICTION: HOLD  (model leans {direction} at {fmt_prob(max(p_yes, p_no))}, "
              f"but no side clears the {FV_MIN_EDGE_CENTS}c net-edge gate)")

    print()
    print("No orders were placed. Demo/live trading requires Kalshi API credentials and the Rust bot.")
    return 0


if __name__ == "__main__":
    # Sanity checks against the Rust unit tests in fair_value.rs
    assert conservative_side_value(1.0, "yes") == 87
    assert conservative_side_value(0.9, "yes") == 80
    assert conservative_side_value(0.1, "no") == 80
    assert conservative_cost_allowed(71, 1, 2)
    assert not conservative_cost_allowed(73, 1, 2)
    assert not conservative_cost_allowed(86, 1, 2)
    sys.exit(main())
