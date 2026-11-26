# Windows edition — Kalshi prediction bot

For Windows 11 x64. Windows ARM and older Windows versions are not tested.
This is a separate Windows package of the Rust demo strategy, not the older Python bot.
It runs locally without ChatGPT, OpenAI API calls, or a paid AI subscription.

## Download and run

1. Download `kalshi-prediction-windows-x64.zip` from this repository's Releases page.
2. Extract the whole ZIP to a local writable folder, such as `C:\KalshiBot` (not OneDrive).
3. Copy `.env.example` to `.env` using PowerShell: `Copy-Item .env.example .env`.
4. Edit `.env` in Notepad. Supply **your own demo API key ID** and the path to your private PEM key. Use forward slashes, e.g. `C:/KalshiBot/demo-private-key.pem`. Never share these files.
5. Double-click `Start-Windows.cmd`. This starts bounded **demo exchange orders**, not real-money trading. Keep the console open; Ctrl+C stops it.
6. Open http://127.0.0.1:5150 in your browser. Execution status: http://127.0.0.1:5150/api/execution.

The executable is unsigned. Only run downloads you trust; do not disable antivirus. No administrator rights should be needed. The dashboard listens only on your own computer.

## Build from source (optional)

Install Rust using https://rustup.rs/ and Visual Studio Build Tools with **Desktop development with C++**, MSVC, and a Windows SDK. Open a new PowerShell window in the extracted source folder:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\Build-Windows.ps1
```

This execution-policy setting applies only to this script process. The prebuilt release does not need Rust or Visual Studio. Developers need Rust 1.89 or newer.

## Restarts and upgrades

Stop the old process first. Preserve `.env`, your PEM key, and the entire `logs` directory. Never run Mac and Windows copies against the same account at once: the lock only protects processes sharing the same local ledger. Moving machines requires copying the stopped bot's ledger too. If Windows sleeps, the bot stops processing until the computer resumes. This package does not install an automatic startup task or notification monitor.

If port 5150 is busy, stop the other bot; do not run a second copy. If the ledger is corrupt or locked, investigate instead of deleting it. Logs and credentials are excluded from Git.

## Validation and limits

The GitHub Windows workflow builds the actual x64 executable, runs accounting and signal regression tests, exercises repeated ledger saves and locking, and checks `--help` without credentials. CI does not place orders. Historical results in RESULTS.md came from the original Mac demo deployment, not a Windows trading run. Live Windows exchange trading still requires testing with your own demo account. No profit is guaranteed.

File locking uses Rust's cross-platform standard library: https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock
