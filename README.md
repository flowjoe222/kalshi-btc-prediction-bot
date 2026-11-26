# CT Web Solutions — Kalshi prediction bot for Windows

Separate **Windows 11 x64** edition of our Rust prediction bot. Demo exchange only. No ChatGPT subscription or AI API usage.

- [Windows setup guide](WINDOWS.md)
- [Download Windows release](https://github.com/bitbuyer420/ct-kalshi-windows/releases/latest)
- [Build and test status](https://github.com/bitbuyer420/ct-kalshi-windows/actions)
- [Historical demo results and limitations](RESULTS.md)

Start with WINDOWS.md. Releases include an executable, launcher, configuration template, and setup guide. Credentials and private trading ledgers are never included. Launching the configured bot enables bounded demo exchange orders. Production execution is rejected.

The published 184-trade snapshot (149 wins / 35 losses, +$66.51 net after recorded fees) is historical demo data from the original deployment, not Windows performance or live-money returns.

Independent educational project by CT Web Solutions, not affiliated with Kalshi. Source remains subject to its authors' rights; no third-party license grant is implied.

## Paper dashboard

`docs/` is a static dark-mode Prediction dashboard for the 15-minute `KXBTC15M` paper ledger. The scheduled GitHub Action runs `scripts/predict_btc_15m.py --ledger docs/data/ledger.json`, settles any completed paper trades, and records a live snapshot. The ledger starts empty and only opens a 1-contract paper trade when the conservative fair-value model clears the 8¢ net-edge gate (cost ≤ 75¢, confidence ≥ 0.55).

After GitHub Pages is enabled from the `main` branch `/docs` folder, the dashboard is at <https://flowjoe222.github.io/kalshi-btc-prediction-bot/>. Use the repo [Actions](https://github.com/flowjoe222/kalshi-btc-prediction-bot/actions) tab to inspect each 15-minute run.
