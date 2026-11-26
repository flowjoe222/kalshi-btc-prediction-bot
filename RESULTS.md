# Demo results — September 22, 2026

Snapshot captured September 23, 2026 at 03:36:09 UTC (September 22, 10:36 p.m. CDT). Source: the running Rust demo bot's reconciled execution journal. This is a dated snapshot, not a live feed.

| Sample | Settled trades | Wins | Losses | Win rate | Net USD after recorded fees |
|---|---:|---:|---:|---:|---:|
| All automated strategy trades | 184 | 149 | 35 | 80.98% | +66.5144 |
| Conservative-v2 cohort | 92 | 82 | 10 | 89.13% | +43.7128 |
| Latest 30 by settlement time | 30 | 26 | 4 | 86.67% | +14.4938 |

[Settlement data and summary JSON](results/2026-09-22-demo.json)

## Method

Include only settlement rows with an `fv-hold-` strategy identifier. Exclude the manual `demo-test` trade and unsettled positions. Sort by settlement timestamp before selecting the latest 30. A win has positive net P&L; a loss has negative net P&L. This snapshot has no flat outcomes. Net is settlement revenue minus recorded fee-inclusive cost. No deposits, unrelated positions, or account-wide unrealized gains are counted as bot profit.

The revised cohort includes strategy identifiers whose entry intents were created at or after September 16, 2026, 21:38:43 UTC. Its 92 outcomes are a subset of the 184 total, not an additional sample. JSON `cost`, `revenue`, and `pnl` are integer millionths of a dollar; `count` uses a scale of 100 per contract. Timestamps are Unix seconds. Account identifiers, client order identifiers, credentials, and private paths are omitted.

## What changed

The published Rust code includes side-specific confidence checks, conservative price estimates, bounded order sizing, durable execution and settlement accounting, protection against stale account data, and a bounded market scan with quote-age checks before entry. Confirmed zero-fill canceled orders can survive exchange history pruning without deleting the ledger. These changes improve reliability; they do not make predictions certain.

Two of the earliest revised-strategy losses were BTC trades costing $0.4168 and $0.3660. Neither established an execution error. The larger current sample contains ten revised-strategy losses. The early 39/41 win record should not be advertised as the current result.

## Limitations and perpetuals

All results come from a demo exchange. They are not a backtest, a live-money track record, audited returns, or a guarantee. The fair-value confidence measure is not a calibrated probability of profitability. Fees are included as recorded; demo execution cannot establish production slippage or capacity.

The separate perpetuals bot had zero owned fills, zero closed round trips, and $0 recorded bot P&L at the review. Two entry attempts had no fills. Its results are not combined with this prediction bot, and it has no demonstrated profitability.

## Release validation

The published source passed 95 targeted Rust tests: 29 library, 29 managed execution, and 37 signal tests. Validation command: `cargo test --offline --lib --test managed_execution --test test_signals`. No live trading test was performed.
