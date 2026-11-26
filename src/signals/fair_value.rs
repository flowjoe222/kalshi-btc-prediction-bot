//! Fair-value pricing for short-dated Kalshi crypto binaries.
//!
//! Kalshi BTC/ETH/etc. markets settle to the **mean of the final 60 one-second
//! CF Benchmarks index prints** before expiry (a 60-second TWAP), NOT the last
//! trade. We price an "above strike K" binary with a driftless Black–Scholes
//! *digital* — `P(S_T >= K) = Φ(ln(S/K) / (σ·√τ_eff))` — using:
//!   * `S`   : live spot (Coinbase feed, used as an index proxy — see caveat)
//!   * `K`   : the contract's `floor_strike`
//!   * `σ`   : realized per-second vol of recent spot log-returns
//!   * `τ_eff`: seconds-to-expiry, reduced for the terminal 60s averaging window
//!
//! Over a minutes horizon the discount factor (≈0.999999) and drift term
//! (≈1e-6 vs σ√τ ≈ 1e-2) are negligible, so we drop them.
//!
//! Everything here is a pure function with no external crates so it is trivially
//! unit-testable. CAVEAT: Coinbase spot has unhedgeable basis vs the official
//! CF Benchmarks index, so near-the-money strikes carry real model error.

/// Error function via Abramowitz & Stegun 7.1.26 (max error ≈ 1.5e-7).
fn erf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
            + 0.254829592)
            * t
            * (-x * x).exp();
    sign * y
}

/// Standard normal CDF.
pub fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Risk-neutral probability that spot finishes **at or above** `strike` at expiry.
///
/// `sigma_per_sec` is the per-second realized vol of log-returns; `secs_to_expiry`
/// is the time left. `tau_eff` shrinks the effective horizon to account for the
/// final-60s averaging window: variance over the averaged tail is ≈ (1/3)σ²·60.
pub fn fair_prob_above(spot: f64, strike: f64, sigma_per_sec: f64, secs_to_expiry: f64) -> f64 {
    if spot <= 0.0 || strike <= 0.0 || sigma_per_sec <= 0.0 || secs_to_expiry <= 0.0 {
        // Degenerate: no uncertainty we can model — collapse to the deterministic outcome.
        return if spot >= strike { 1.0 } else { 0.0 };
    }
    // 60s-average settlement correction (Kalshi settles on the mean of the final
    // 60 one-second prints). The averaged tail contributes 1/3 of a point's variance.
    let t = secs_to_expiry;
    let tau_eff = if t > 60.0 {
        (t - 60.0) + 60.0 / 3.0
    } else {
        t / 3.0
    };
    let sigma_h = sigma_per_sec * tau_eff.sqrt();
    if sigma_h <= 0.0 {
        return if spot >= strike { 1.0 } else { 0.0 };
    }
    let d2 = (spot / strike).ln() / sigma_h; // driftless
    norm_cdf(d2)
}

/// Convert a probability (0..1) to a YES price in cents, clamped to a tradeable [1, 99].
pub fn fair_cents_yes(p_above: f64) -> i64 {
    ((p_above * 100.0).round() as i64).clamp(1, 99)
}

/// Kalshi **taker** fee in cents for `count` contracts at `price_cents`.
///
/// Official formula (Fee Schedule, eff. 2026-02-05): `round_up(0.07 · C · P · (1−P))`
/// in dollars, where `P` is price in dollars. Crypto pays the full 0.07 multiplier
/// (not the 0.035 index-product discount). Rounded UP to the next whole cent, so
/// tiny lots effectively pay a 1-cent floor.
pub fn taker_fee_cents(price_cents: i64, count: i64) -> i64 {
    let p = price_cents as f64 / 100.0;
    let fee_dollars = 0.07 * count as f64 * p * (1.0 - p);
    let cents = fee_dollars * 100.0;
    // Round away f64 noise (e.g. 175.0000000000003) to 6 dp before the ceil so an
    // exact-cent fee doesn't get bumped a whole cent higher.
    let cents = (cents * 1_000_000.0).round() / 1_000_000.0;
    cents.ceil() as i64
}

/// Net fee-adjusted edge in cents for **buying** a contract whose fair value is
/// `fair_cents` at price `price_cents`, paying one taker leg plus a slippage buffer.
/// Positive = expected profit per contract after costs.
pub fn net_edge_cents(fair_cents: i64, price_cents: i64, slippage_cents: i64, count: i64) -> i64 {
    let gross = fair_cents - price_cents;
    let fee_per_contract = if count > 0 {
        // Fee is roughly linear in count; normalize to per-contract for the gate.
        (taker_fee_cents(price_cents, count) + count - 1) / count
    } else {
        taker_fee_cents(price_cents, 1)
    };
    gross - fee_per_contract - slippage_cents
}

/// Realized per-step (≈ per-second) volatility = stddev of log-returns over the
/// most recent `window` prices. Returns `None` if there isn't enough data or the
/// series is degenerate (flat). Caller should floor σ to a sane minimum.
pub fn realized_vol_per_step(prices: &[f64], window: usize) -> Option<f64> {
    let n = prices.len();
    if n < 3 {
        return None;
    }
    let w = window.min(n);
    let slice = &prices[n - w..];
    let rets: Vec<f64> = slice
        .windows(2)
        .filter_map(|p| {
            if p[0] > 0.0 && p[1] > 0.0 {
                Some((p[1] / p[0]).ln())
            } else {
                None
            }
        })
        .collect();
    if rets.len() < 2 {
        return None;
    }
    let mean = rets.iter().sum::<f64>() / rets.len() as f64;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (rets.len() - 1) as f64;
    let s = var.sqrt();
    if s.is_finite() && s > 0.0 {
        Some(s)
    } else {
        None
    }
}

/// Side-specific conviction. Strong NO evidence must never authorize a YES buy.
pub fn side_confidence(p_yes: f64, side: &str) -> Option<f64> {
    if !p_yes.is_finite() || !(0.0..=1.0).contains(&p_yes) {
        return None;
    }
    let p = match side {
        "yes" => p_yes,
        "no" => 1.0 - p_yes,
        _ => return None,
    };
    Some((2.0 * p - 1.0).max(0.0))
}

/// Experimental model-risk haircut, not an empirically calibrated probability.
/// Retain 75% of the modeled deviation from 50%, rounding value down.
pub fn conservative_side_value(p_yes: f64, side: &str) -> Option<i64> {
    side_confidence(p_yes, side)?;
    let p = if side == "yes" { p_yes } else { 1.0 - p_yes };
    Some(((0.5 + 0.75 * (p - 0.5)) * 100.0).floor() as i64)
}

#[cfg(test)]
mod conviction_tests {
    use super::*;
    #[test]
    fn confidence_tracks_the_purchased_outcome() {
        assert_eq!(side_confidence(0.9, "no"), Some(0.0));
        assert_eq!(side_confidence(0.1, "yes"), Some(0.0));
        assert!((side_confidence(0.9, "yes").unwrap() - 0.8).abs() < 1e-9);
        assert!((side_confidence(0.1, "no").unwrap() - 0.8).abs() < 1e-9);
    }
    #[test]
    fn invalid_probabilities_fail_closed() {
        for p in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
            assert!(side_confidence(p, "yes").is_none());
            assert!(conservative_side_value(p, "yes").is_none());
        }
        assert!(side_confidence(0.9, "invalid").is_none());
    }
    #[test]
    fn haircut_reduces_favored_outcome_value() {
        assert_eq!(conservative_side_value(1.0, "yes"), Some(87));
        assert_eq!(conservative_side_value(0.9, "yes"), Some(80));
        assert_eq!(conservative_side_value(0.1, "no"), Some(80));
        assert_eq!(conservative_side_value(0.5, "yes"), Some(50));
    }
}

/// Fee/slippage-inclusive cost ceiling for the conservative demo experiment.
pub fn conservative_cost_allowed(price: i64, count: i64, slippage: i64) -> bool {
    if !(1..100).contains(&price) || count <= 0 || slippage < 0 {
        return false;
    }
    price + (taker_fee_cents(price, count) + count - 1) / count + slippage <= 75
}
#[cfg(test)]
mod cost_tests {
    use super::*;
    #[test]
    fn expensive_and_invalid_entries_are_rejected() {
        assert!(!conservative_cost_allowed(86, 1, 2));
        assert!(conservative_cost_allowed(71, 1, 2));
        assert!(!conservative_cost_allowed(73, 1, 2));
        assert!(!conservative_cost_allowed(50, 0, 2));
        assert!(!conservative_cost_allowed(50, 1, -1));
    }
}
