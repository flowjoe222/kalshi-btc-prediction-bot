$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw 'Install Rust and the Visual Studio C++ build tools first. See WINDOWS.md.'
}
cargo build --release --locked
if ($LASTEXITCODE -ne 0) { throw 'Rust build failed.' }
Copy-Item 'target/release/bitcoin-prediction-bot.exe' './bitcoin-prediction-bot.exe' -Force
Write-Host 'Build complete. Configure .env, then double-click Start-Windows.cmd.'
