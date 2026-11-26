use crate::adaptive::{AdaptiveBounds, AdaptiveConfig};
use crate::arbitrage::ArbitrageConfig;
use crate::execution::risk::RiskParams;
use crate::signals::detector::DetectorParams;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Top-level configuration loaded from `config.yaml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub signal: SignalConfig,
    pub polymarket: PolymarketConfig,
    pub binance: BinanceConfig,
    pub markets: MarketsConfig,
    pub adaptive: AdaptiveConfigRaw,
    pub resolution: ResolutionConfig,
    pub logging: LoggingConfig,
    pub dashboard: DashboardConfig,
    pub simulation: SimulationConfig,
    #[serde(default)]
    pub kalshi: KalshiConfig,
    #[serde(default)]
    pub arbitrage: ArbitrageConfigRaw,
}

/// Kalshi execution configuration (non-secret parameters only).
/// Secrets are read from environment variables at runtime.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KalshiConfig {
    /// Maximum cost per single trade in cents.  Default: 200 (= $2.00).
    pub max_position_cents: i64,
    /// Maximum cumulative daily loss in cents before halting.  Default: 1000 (= $10.00).
    pub max_daily_loss_cents: i64,
    /// Maximum number of positions open simultaneously.  Default: 3.
    pub max_concurrent_positions: usize,
    /// Minimum seconds between consecutive trades.  Default: 30.
    pub cooldown_seconds: u64,
    /// Minimum signal confidence required to submit an order.  Default: 0.70.
    pub confidence_threshold: f64,
    /// Maximum aggregate exposure across all open positions in cents.
    /// Default: 1500 (= $15.00). Enforced inside `RiskManager::check()`.
    #[serde(default = "default_kalshi_max_total_exposure_cents")]
    pub max_total_exposure_cents: i64,

    // ---- Fair-value (FV) strategy params (strategy knobs, NOT risk gates) ----
    /// Enable the fair-value digital-pricing bot.
    #[serde(default = "default_true")]
    pub fv_enabled: bool,
    /// Minimum net (fee+slippage-adjusted) edge in cents required to enter. Default 4.
    #[serde(default = "default_fv_min_edge_cents")]
    pub fv_min_edge_cents: i64,
    /// Assumed taker slippage in cents on thin books, added to the cost hurdle. Default 2.
    #[serde(default = "default_fv_slippage_cents")]
    pub fv_slippage_cents: i64,
    /// Fraction of bankroll to risk per FV trade. Default 0.08 (smaller than heuristic bots).
    #[serde(default = "default_fv_size_pct")]
    pub fv_size_pct: f64,
    /// Max contracts per FV trade. Default 2.
    #[serde(default = "default_fv_max_contracts")]
    pub fv_max_contracts: i64,
    /// Hold deep ITM/OTM (convergence-bucket) positions to fee-free settlement. Default true.
    #[serde(default = "default_true")]
    pub fv_hold_to_settlement: bool,
    /// Daily realized-loss stop for FV bot, in cents. Default 300 (= -$3). 0 disables.
    #[serde(default = "default_fv_daily_stop_cents")]
    pub fv_daily_stop_cents: i64,
    /// Floor on per-second realized vol so a flat tape can't produce false certainty. Default 5e-5.
    #[serde(default = "default_fv_vol_floor_per_sec")]
    pub fv_vol_floor_per_sec: f64,
    /// Skip contracts with more than this many seconds to expiry (avoid far/stale). Default 2400.
    #[serde(default = "default_fv_max_ttc_secs")]
    pub fv_max_ttc_secs: f64,
}

fn default_kalshi_max_total_exposure_cents() -> i64 {
    1_500
}
fn default_true() -> bool { true }
fn default_fv_min_edge_cents() -> i64 { 4 }
fn default_fv_slippage_cents() -> i64 { 2 }
fn default_fv_size_pct() -> f64 { 0.08 }
fn default_fv_max_contracts() -> i64 { 2 }
fn default_fv_daily_stop_cents() -> i64 { 300 }
fn default_fv_vol_floor_per_sec() -> f64 { 0.00005 }
fn default_fv_max_ttc_secs() -> f64 { 2400.0 }

impl Default for KalshiConfig {
    fn default() -> Self {
        Self {
            max_position_cents: 200,
            max_daily_loss_cents: 1_000,
            max_concurrent_positions: 3,
            cooldown_seconds: 30,
            confidence_threshold: 0.70,
            max_total_exposure_cents: default_kalshi_max_total_exposure_cents(),
            fv_enabled: default_true(),
            fv_min_edge_cents: default_fv_min_edge_cents(),
            fv_slippage_cents: default_fv_slippage_cents(),
            fv_size_pct: default_fv_size_pct(),
            fv_max_contracts: default_fv_max_contracts(),
            fv_hold_to_settlement: default_true(),
            fv_daily_stop_cents: default_fv_daily_stop_cents(),
            fv_vol_floor_per_sec: default_fv_vol_floor_per_sec(),
            fv_max_ttc_secs: default_fv_max_ttc_secs(),
        }
    }
}

impl From<KalshiConfig> for RiskParams {
    fn from(c: KalshiConfig) -> Self {
        Self {
            max_position_cents: c.max_position_cents,
            max_daily_loss_cents: c.max_daily_loss_cents,
            max_concurrent_positions: c.max_concurrent_positions,
            cooldown_seconds: c.cooldown_seconds,
            confidence_threshold: c.confidence_threshold,
            max_total_exposure_cents: c.max_total_exposure_cents,
        }
    }
}

/// Signal detection parameters (mirrors `DetectorParams`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignalConfig {
    pub imbalance_threshold: f64,
    pub window_seconds: f64,
    pub confirmation_window_seconds: f64,
    pub min_volume_usdc: f64,
    pub confidence_min: f64,
    pub cooldown_seconds: f64,
    pub large_trade_multiplier: f64,
    pub large_trade_threshold_usdc: f64,
}

impl From<SignalConfig> for DetectorParams {
    fn from(c: SignalConfig) -> Self {
        Self {
            imbalance_threshold: c.imbalance_threshold,
            window_seconds: c.window_seconds,
            confirmation_window_seconds: c.confirmation_window_seconds,
            min_volume_usdc: c.min_volume_usdc,
            confidence_min: c.confidence_min,
            cooldown_seconds: c.cooldown_seconds,
            large_trade_multiplier: c.large_trade_multiplier,
            large_trade_threshold_usdc: c.large_trade_threshold_usdc,
        }
    }
}

/// Polymarket WebSocket and REST API configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolymarketConfig {
    pub clob_ws_url: String,
    pub clob_rest_url: String,
    pub gamma_api_url: String,
    pub market_refresh_interval: u64,
    pub ws_ping_interval: u64,
    pub ws_ping_timeout: u64,
    pub ws_max_reconnect_delay: u64,
}

/// Binance WebSocket configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceConfig {
    pub ws_url: String,
}

/// Market discovery configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketsConfig {
    pub search_keywords: Vec<String>,
    pub preferred_expiry_minutes: u64,
    pub max_strike_distance_pct: f64,
}

/// Adaptive learner configuration as deserialized from YAML.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AdaptiveConfigRaw {
    pub min_samples_to_learn: usize,
    pub rolling_window_size: usize,
    pub optimization_interval: usize,
    pub learning_rate: f64,
    pub target_win_rate: f64,
    pub emergency_tighten_threshold: f64,
    pub bounds: AdaptiveBoundsRaw,
    pub lessons_log_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AdaptiveBoundsRaw {
    pub imbalance_threshold: [f64; 2],
    pub window_seconds: [f64; 2],
    pub min_volume_usdc: [f64; 2],
    pub confidence_min: [f64; 2],
    pub cooldown_seconds: [f64; 2],
}

impl From<AdaptiveConfigRaw> for AdaptiveConfig {
    fn from(raw: AdaptiveConfigRaw) -> Self {
        Self {
            min_samples_to_learn: raw.min_samples_to_learn,
            rolling_window_size: raw.rolling_window_size,
            optimization_interval: raw.optimization_interval,
            learning_rate: raw.learning_rate,
            target_win_rate: raw.target_win_rate,
            emergency_tighten_threshold: raw.emergency_tighten_threshold,
            bounds: AdaptiveBounds {
                imbalance_threshold: raw.bounds.imbalance_threshold,
                window_seconds: raw.bounds.window_seconds,
                min_volume_usdc: raw.bounds.min_volume_usdc,
                confidence_min: raw.bounds.confidence_min,
                cooldown_seconds: raw.bounds.cooldown_seconds,
            },
            lessons_log_path: raw.lessons_log_path,
        }
    }
}

/// Trade outcome resolution configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResolutionConfig {
    pub check_after_seconds: f64,
    pub min_move_bps: f64,
}

/// Logging and persistence configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: String,
    pub trade_log_csv: String,
    pub signal_log_jsonl: String,
    pub performance_log: String,
    pub max_entries_per_file: usize,
}

/// Dashboard server configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DashboardConfig {
    pub host: String,
    pub port: u16,
    pub poll_interval_ms: u64,
    pub max_history_rows: usize,
}

/// Cross-platform arbitrage scanner configuration (YAML-deserialized form).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArbitrageConfigRaw {
    /// Master on/off switch.
    pub enabled: bool,
    /// Minimum price gap (0–1) to flag as an opportunity. Default 0.03.
    pub min_gap: f64,
    /// Seconds between scans. Default 30.
    pub scan_interval_secs: u64,
    /// When `true`, automatically place the Kalshi side of detected gaps.
    pub auto_execute: bool,
    /// Maximum USD value per auto-executed Kalshi order.
    pub max_position_size_usd: f64,
    /// CSV file path for logging all detected opportunities.
    pub opportunities_log_csv: String,
}

impl Default for ArbitrageConfigRaw {
    fn default() -> Self {
        Self {
            enabled: true,
            min_gap: 0.03,
            scan_interval_secs: 30,
            auto_execute: false,
            max_position_size_usd: 50.0,
            opportunities_log_csv: "logs/arbitrage_opportunities.csv".to_owned(),
        }
    }
}

impl From<ArbitrageConfigRaw> for ArbitrageConfig {
    fn from(raw: ArbitrageConfigRaw) -> Self {
        Self {
            enabled: raw.enabled,
            min_gap: raw.min_gap,
            scan_interval_secs: raw.scan_interval_secs,
            auto_execute: raw.auto_execute,
            max_position_size_usd: raw.max_position_size_usd,
            opportunities_log_csv: raw.opportunities_log_csv,
            poly_max_markets: 200,
        }
    }
}

/// Simulation mode configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SimulationConfig {
    pub test_mode: bool,
    pub tick_interval_seconds: f64,
    pub starting_price: f64,
    pub annual_volatility: f64,
    pub annual_drift: f64,
    pub signal_quality: f64,
    pub base_volume_per_second: f64,
}

impl Config {
    /// Load configuration from a YAML file at `path`.
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file '{path}'"))?;
        let config: Self = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing config file '{path}'"))?;
        Ok(config)
    }

    /// Load configuration from `config.yaml` in the current directory, falling
    /// back to built-in defaults if the file does not exist.
    pub fn load_or_default(path: &str) -> Self {
        Self::load(path).unwrap_or_else(|_| Self::default())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            signal: SignalConfig {
                imbalance_threshold: 0.65,
                window_seconds: 10.0,
                confirmation_window_seconds: 30.0,
                min_volume_usdc: 500.0,
                confidence_min: 0.60,
                cooldown_seconds: 45.0,
                large_trade_multiplier: 1.5,
                large_trade_threshold_usdc: 200.0,
            },
            polymarket: PolymarketConfig {
                clob_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
                clob_rest_url: "https://clob.polymarket.com".to_owned(),
                gamma_api_url: "https://gamma-api.polymarket.com".to_owned(),
                market_refresh_interval: 300,
                ws_ping_interval: 30,
                ws_ping_timeout: 10,
                ws_max_reconnect_delay: 60,
            },
            binance: BinanceConfig {
                ws_url: "wss://stream.binance.com:9443/ws/btcusdt@aggTrade".to_owned(),
            },
            markets: MarketsConfig {
                search_keywords: vec![
                    "bitcoin".to_owned(),
                    "BTC".to_owned(),
                    "btc".to_owned(),
                ],
                preferred_expiry_minutes: 5,
                max_strike_distance_pct: 0.03,
            },
            adaptive: AdaptiveConfigRaw {
                min_samples_to_learn: 20,
                rolling_window_size: 50,
                optimization_interval: 10,
                learning_rate: 0.10,
                target_win_rate: 0.55,
                emergency_tighten_threshold: 0.38,
                bounds: AdaptiveBoundsRaw {
                    imbalance_threshold: [0.55, 0.85],
                    window_seconds: [5.0, 60.0],
                    min_volume_usdc: [100.0, 5000.0],
                    confidence_min: [0.50, 0.85],
                    cooldown_seconds: [15.0, 120.0],
                },
                lessons_log_path: "logs/lessons.json".to_owned(),
            },
            resolution: ResolutionConfig {
                check_after_seconds: 60.0,
                min_move_bps: 5.0,
            },
            logging: LoggingConfig {
                level: "info".to_owned(),
                trade_log_csv: "logs/trades.csv".to_owned(),
                signal_log_jsonl: "logs/signals.jsonl".to_owned(),
                performance_log: "logs/performance.json".to_owned(),
                max_entries_per_file: 10_000,
            },
            dashboard: DashboardConfig {
                host: "127.0.0.1".to_owned(),
                port: 5000,
                poll_interval_ms: 1000,
                max_history_rows: 100,
            },
            simulation: SimulationConfig {
                test_mode: true,
                tick_interval_seconds: 1.0,
                starting_price: 67_500.0,
                annual_volatility: 0.80,
                annual_drift: 0.30,
                signal_quality: 0.65,
                base_volume_per_second: 250.0,
            },
            kalshi: KalshiConfig::default(),
            arbitrage: ArbitrageConfigRaw::default(),
        }
    }
}
