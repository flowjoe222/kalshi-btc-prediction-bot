//! Kalshi prediction-market executor.
//!
//! Supports two authentication modes that are selected automatically via
//! environment variables (RSA-PSS is preferred; JWT email/password is the
//! fallback):
//!
//! **RSA-PSS (preferred)**
//! - `KALSHI_API_KEY`          — the key ID shown in the Kalshi web UI
//! - `KALSHI_PRIVATE_KEY_PATH` — path to a PEM file containing the RSA private key
//!
//! **JWT email/password (deprecated fallback)**
//! - `KALSHI_EMAIL`
//! - `KALSHI_PASSWORD`
//!
//! Optional:
//! - `KALSHI_MODE=live|demo` (default: `demo`)

use anyhow::{anyhow, Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use reqwest::{Client, RequestBuilder, header};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::{BlindedSigningKey, Signature};
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Debug)]
struct DispatchFailure(reqwest::Error);
impl std::fmt::Display for DispatchFailure {
    fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {write!(f,"request dispatch failed: {}",self.0)}
}
impl std::error::Error for DispatchFailure {}

const LIVE_BASE_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";
const DEMO_BASE_URL: &str = "https://external-api.demo.kalshi.co/trade-api/v2";

// ---- Mode ----

/// Whether the executor targets Kalshi's live or demo environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KalshiMode {
    Live,
    Demo,
}

// ---- Authentication strategy ----

/// Holds the credentials for one of the two supported auth methods.
///
/// `RsaPss` is stateless — each request recomputes a fresh signature.
/// `JwtPassword` caches a token in `AuthState` and refreshes it before expiry.
enum AuthMethod {
    /// RSA-PSS per-request signing (modern Kalshi API).
    RsaPss {
        /// Key ID displayed in the Kalshi web UI (sent as `KALSHI-ACCESS-KEY`).
        api_key_id: String,
        /// Loaded RSA private key used to sign each request.
        signing_key: BlindedSigningKey<Sha256>,
    },
    /// Deprecated email + password JWT flow (kept as fallback).
    JwtPassword {
        email: String,
        password: String,
        /// Cached JWT token and its scheduled refresh time.
        auth: Arc<Mutex<Option<AuthState>>>,
    },
}

// The `BlindedSigningKey` inside `AuthMethod::RsaPss` is not `Clone`, but
// `KalshiExecutor` is only used behind `Arc` so `Clone` is not needed.

/// Cached JWT authentication state (used only for the `JwtPassword` path).
struct AuthState {
    token: String,
    /// Unix seconds after which the cached token should be refreshed.
    refresh_after: i64,
}

// ---- Request/response types for the JWT login endpoint ----

#[derive(Serialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct LoginResponse {
    token: String,
}

// ---- Market types ----

/// A single Kalshi prediction market (BTC contract).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiMarket {
    #[serde(default)]
    pub exchange_index: Option<i64>,
    pub ticker: String,
    #[serde(default)]
    pub title: String,
    pub status: String,
    /// YES bid as dollar string (e.g. "0.4500") — converted to cents internally.
    #[serde(default, rename = "yes_bid_dollars")]
    pub yes_bid_dollars: Option<String>,
    /// YES ask as dollar string (e.g. "0.5500") — converted to cents internally.
    #[serde(default, rename = "yes_ask_dollars")]
    pub yes_ask_dollars: Option<String>,
    /// NO bid as dollar string.
    #[serde(default, rename = "no_bid_dollars")]
    pub no_bid_dollars: Option<String>,
    /// NO ask as dollar string.
    #[serde(default, rename = "no_ask_dollars")]
    pub no_ask_dollars: Option<String>,
    /// Legacy cent fields (older API versions).
    #[serde(default)]
    pub yes_bid: Option<i64>,
    #[serde(default)]
    pub yes_ask: Option<i64>,
    pub close_time: Option<String>,
    pub expected_expiration_time: Option<String>,
    #[serde(default)]
    pub floor_strike: Option<f64>,
    #[serde(default)]
    pub strike_type: Option<String>,
    #[serde(default)]
    pub event_ticker: Option<String>,
    #[serde(default)]
    pub open_time: Option<String>,
    #[serde(default, rename = "yes_ask_size_fp")]
    pub yes_ask_size: Option<String>,
    #[serde(default, rename = "yes_bid_size_fp")]
    pub yes_bid_size: Option<String>,
}

impl KalshiMarket {
    /// Get YES bid in cents, preferring dollar fields over legacy cent fields.
    pub fn yes_bid_cents(&self) -> Option<i64> {
        if let Some(ref s) = self.yes_bid_dollars {
            if let Ok(v) = s.parse::<f64>() {
                if v > 0.0 {
                    return Some((v * 100.0).round() as i64);
                }
            }
        }
        self.yes_bid
    }

    /// Get YES ask in cents, preferring dollar fields over legacy cent fields.
    pub fn yes_ask_cents(&self) -> Option<i64> {
        if let Some(ref s) = self.yes_ask_dollars {
            if let Ok(v) = s.parse::<f64>() {
                if v > 0.0 {
                    return Some((v * 100.0).round() as i64);
                }
            }
        }
        self.yes_ask
    }

    /// Check if this market has any liquidity (at least one side quoted).
    pub fn has_liquidity(&self) -> bool {
        self.yes_bid_cents().is_some() || self.yes_ask_cents().is_some()
    }
}

// ---- Order types ----

/// A placed order returned by the Kalshi API.
///
/// Supports both live API (integer cents) and demo API (dollar strings) formats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiOrder {
    pub order_id: String,
    pub ticker: String,
    /// `"buy"` | `"sell"`
    pub action: String,
    /// `"yes"` | `"no"`
    pub side: String,
    /// Contract count (live API). Demo API uses `initial_count_fp` instead.
    #[serde(default)]
    pub count: i64,
    /// Price in cents (live API).
    #[serde(default)]
    pub yes_price: Option<i64>,
    #[serde(default)]
    pub no_price: Option<i64>,
    pub status: String,
    pub created_time: Option<String>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    // ---- Demo API fields (dollar-denominated strings) ----
    #[serde(default)]
    pub yes_price_dollars: Option<String>,
    #[serde(default)]
    pub no_price_dollars: Option<String>,
    #[serde(default)]
    pub initial_count_fp: Option<String>,
    #[serde(default)]
    pub fill_count_fp: Option<String>,
    #[serde(default)]
    pub taker_fill_cost_dollars: Option<String>,
    #[serde(default)]
    pub taker_fees_dollars: Option<String>,
}

impl KalshiOrder {
    /// Get the contract count, preferring live field, falling back to demo field.
    pub fn effective_count(&self) -> i64 {
        if self.count > 0 {
            return self.count;
        }
        self.initial_count_fp.as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|f| f.round() as i64)
            .unwrap_or(1)
    }
}

// ---- Position types ----

/// An open position on a Kalshi market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiPosition {
    pub ticker: String,
    /// Number of YES contracts held (negative = NO position).
    pub position: f64,
    pub market_exposure: Option<i64>,
    pub realized_pnl: Option<i64>,
    pub unrealized_pnl: Option<i64>,
    pub fees_paid: Option<i64>,
}

// ---- API response envelopes ----

#[derive(Deserialize)]
struct BalanceResponse {
    balance: i64,
}

#[derive(Deserialize)]
struct OrderResponse {
    order: KalshiOrder,
}

#[derive(Deserialize)]
struct MarketsResponse {
    markets: Vec<KalshiMarket>,
    #[allow(dead_code)]
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct PositionsResponse {
    market_positions: Vec<KalshiPosition>,
}

// ---- Executor ----

/// Executes trades on Kalshi's prediction market platform.
///
/// Construct via [`KalshiExecutor::from_env`].  All public methods preserve
/// the same signatures regardless of which auth method is active.
pub struct KalshiExecutor {
    http: Client,
    order_tickers: std::sync::Mutex<std::collections::HashMap<String, (String, i64)>>,
    read_budget: std::sync::Mutex<super::rate_limit::Bucket>,
    write_budget: std::sync::Mutex<super::rate_limit::Bucket>,
    endpoint_costs: std::sync::RwLock<serde_json::Value>,
    pub base_url: String,
    /// Market-DATA source URL. Usually equals `base_url`, but can be pointed at a
    /// different environment (e.g. Kalshi production) so a demo+paper bot can read
    /// REAL prices/liquidity while still executing (paper) through `base_url`.
    /// The Kalshi demo exchange has no crypto liquidity (every book is 0¢/100¢), so
    /// without this a money-safe paper bot would never see a tradeable price.
    pub data_base_url: String,
    pub mode: KalshiMode,
    auth_method: AuthMethod,
    /// Paper-trading mode: real market data, but fills are simulated locally and
    /// no real orders are placed. Sidesteps the Kalshi-demo "can't fill" problem
    /// so the strategy can actually open/close positions and be watched.
    paper: bool,
    /// Simulated account balance (cents), mutated on each simulated fill.
    paper_balance: std::sync::Arc<std::sync::atomic::AtomicI64>,
    /// Shared market data cache — all clones (via Arc) hit the same cache.
    /// Prevents 5 bot loops from each making 4 API calls every 2-4 seconds.
    market_cache: std::sync::Arc<tokio::sync::Mutex<(Vec<KalshiMarket>, std::time::Instant)>>,
}

impl KalshiExecutor {
    /// Construct from environment variables.
    ///
    /// Auth selection (first match wins):
    /// 1. `KALSHI_API_KEY` + `KALSHI_PRIVATE_KEY_PATH` → RSA-PSS signing
    /// 2. `KALSHI_EMAIL` + `KALSHI_PASSWORD`           → JWT login
    ///
    /// Optional:
    /// - `KALSHI_MODE=live|demo` (default: `demo`)
    pub fn from_env() -> Result<Self> {
        let mode_str = std::env::var("KALSHI_MODE").unwrap_or_else(|_| "demo".to_owned());
        let mode = match mode_str.to_lowercase().as_str() {
            "demo" => KalshiMode::Demo,
            _ => return Err(anyhow!("This deployment requires KALSHI_MODE=demo")),
        };

        let base_url = match mode {
            KalshiMode::Live => LIVE_BASE_URL.to_owned(),
            KalshiMode::Demo => DEMO_BASE_URL.to_owned(),
        };

        // Decouple market-DATA source from the execution venue. The Kalshi DEMO
        // exchange has zero liquidity on crypto markets (every book is 0¢/100¢),
        // so a demo+paper bot can never see a tradeable price there. Set
        // `KALSHI_DATA_MODE=live` (or `KALSHI_DATA_BASE_URL=<url>`) to READ real,
        // public, unauthenticated prices from Kalshi production while STILL
        // executing through `base_url` (demo) in paper mode — no real order is
        // ever placed (see place_order's paper short-circuit). Defaults to
        // `base_url` (unchanged behaviour) when neither var is set.
        let data_base_url = std::env::var("KALSHI_DATA_BASE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                match std::env::var("KALSHI_DATA_MODE")
                    .ok()
                    .map(|s| s.trim().to_lowercase())
                    .as_deref()
                {
                    Some("live") | Some("prod") | Some("production") => Some(LIVE_BASE_URL.to_owned()),
                    Some("demo") => Some(DEMO_BASE_URL.to_owned()),
                    _ => None,
                }
            })
            .unwrap_or_else(|| base_url.clone());

        // --- Try RSA-PSS first ---
        let rsa_key_id = std::env::var("KALSHI_API_KEY").ok();
        let rsa_key_path = std::env::var("KALSHI_PRIVATE_KEY_PATH").ok();

        let auth_method = match (rsa_key_id, rsa_key_path) {
            (Some(api_key_id), Some(key_path)) => {
                let signing_key = load_rsa_signing_key(&key_path)
                    .with_context(|| format!("Failed to load RSA private key from {key_path}"))?;
                info!(
                    "KalshiExecutor: RSA-PSS auth configured, mode={mode:?}"
                );
                AuthMethod::RsaPss { api_key_id, signing_key }
            }

            // --- Fall back to email/password JWT ---
            _ => {
                let email = std::env::var("KALSHI_EMAIL").ok();
                let password = std::env::var("KALSHI_PASSWORD")
                    .or_else(|_| std::env::var("KALSHI_API_SECRET"))
                    .ok();

                match (email, password) {
                    (Some(email), Some(password)) => {
                        warn!(
                            "KalshiExecutor: using deprecated email/password auth — \
                             migrate to KALSHI_API_KEY + KALSHI_PRIVATE_KEY_PATH"
                        );
                        info!("KalshiExecutor: JWT auth, mode={mode:?}");
                        AuthMethod::JwtPassword {
                            email,
                            password,
                            auth: Arc::new(Mutex::new(None)),
                        }
                    }
                    _ => {
                        return Err(anyhow!(
                            "Kalshi credentials missing. \
                             Set KALSHI_API_KEY + KALSHI_PRIVATE_KEY_PATH (RSA-PSS), \
                             or KALSHI_EMAIL + KALSHI_PASSWORD (deprecated JWT)."
                        ));
                    }
                }
            }
        };

        let http = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        // Paper-trading mode: simulate fills against real market data (no real orders).
        let paper = std::env::var("KALSHI_PAPER")
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let paper_start = std::env::var("KALSHI_PAPER_BALANCE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(10_000); // $100.00 default paper bankroll
        if paper {
            info!(
                "KalshiExecutor: PAPER mode — fills simulated, starting balance ${:.2}",
                paper_start as f64 / 100.0
            );
        }

        info!("KalshiExecutor: base_url={base_url}");
        if data_base_url != base_url {
            info!(
                "KalshiExecutor: market DATA sourced from {data_base_url} (read-only, \
                 unauthenticated) — execution stays on {base_url}"
            );
        }

        Ok(Self {
            http, base_url, data_base_url, mode, auth_method,
            order_tickers: std::sync::Mutex::new(std::collections::HashMap::new()),
            read_budget: std::sync::Mutex::new(super::rate_limit::Bucket::new(200.0, 400.0, 0.0, std::time::Instant::now())),
            write_budget: std::sync::Mutex::new(super::rate_limit::Bucket::new(100.0, 100.0, 10.0, std::time::Instant::now())),
            endpoint_costs: std::sync::RwLock::new(serde_json::json!({"default_cost": 10, "endpoint_costs": []})),
            paper,
            paper_balance: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(paper_start)),
            market_cache: std::sync::Arc::new(tokio::sync::Mutex::new(
                (Vec::new(), std::time::Instant::now() - std::time::Duration::from_secs(60))
            )),
        })
    }

    /// Returns `true` when this executor targets the demo environment.
    pub fn is_demo(&self) -> bool {
        self.mode == KalshiMode::Demo
    }

    /// Returns `true` when running in paper-trading mode (simulated fills).
    pub fn is_paper(&self) -> bool {
        self.paper
    }

    /// Central request gate. All trading loops sharing this executor share its budgets.
    /// Writes are never retried here: an ambiguous response requires reconciliation.
    async fn send_limited(&self, builder: RequestBuilder, method: &str, path: &str,
        authenticated: bool, urgent: bool) -> Result<reqwest::Response> {
        let budget = if method == "GET" { &self.read_budget } else { &self.write_budget };
        let cost = {
            let costs = self.endpoint_costs.read().unwrap();
            let default = costs["default_cost"].as_f64().unwrap_or(10.0);
            costs["endpoint_costs"].as_array().and_then(|rows| rows.iter().find(|row| {
                if row["method"].as_str() != Some(method) { return false; }
                let template = row["path"].as_str().unwrap_or("");
                let normalize = |v: &str| v.strip_prefix("/trade-api/v2").unwrap_or(v).to_owned();
                let template = normalize(template);
                let actual = normalize(path);
                let a: Vec<_> = template.split('/').collect();
                let b: Vec<_> = actual.split('/').collect();
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| a == b || (a.starts_with('{') && a.ends_with('}')))
            })).and_then(|row| row["cost"].as_f64()).unwrap_or(default)
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let wait = budget.lock().unwrap().acquire(cost, urgent, std::time::Instant::now())
                .map_err(|e| anyhow!(e))?;
            match wait {
                None => break,
                Some(delay) => {
                    if std::time::Instant::now() + delay > deadline {
                        return Err(anyhow!("Rate-limit queue deadline exceeded; refresh signal before resubmitting"));
                    }
                    tokio::time::sleep(delay.max(std::time::Duration::from_millis(1))).await;
                }
            }
        }
        let builder = if authenticated { self.apply_auth(builder, method, path).await? } else { builder };
        let response = builder.send().await.map_err(|e|anyhow!(DispatchFailure(e)))?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            budget.lock().unwrap().throttled(std::time::Instant::now());
            warn!("Kalshi {} rate limited; adaptive budget reduced", method);
        }
        Ok(response)
    }

    async fn refresh_rate_limits(&self) -> Result<()> {
        for suffix in ["/account/limits", "/account/endpoint_costs"] {
            let url = format!("{}{}", self.base_url, suffix);
            let path = Self::path_from_url(&url).to_owned();
            let data: serde_json::Value = self.send_limited(self.http.get(&url), "GET", &path, true, false)
                .await?.error_for_status()?.json().await?;
            if suffix.ends_with("endpoint_costs") {
                if data["default_cost"].as_f64().filter(|n| n.is_finite() && *n > 0.0).is_none()
                    || !data["endpoint_costs"].is_array() { return Err(anyhow!("Invalid endpoint costs")); }
                *self.endpoint_costs.write().unwrap() = data;
            } else {
                for (name, bucket) in [("read", &self.read_budget), ("write", &self.write_budget)] {
                    let rate = data[name]["refill_rate"].as_f64().ok_or_else(|| anyhow!("Missing refill_rate"))?;
                    let capacity = data[name]["bucket_capacity"].as_f64().ok_or_else(|| anyhow!("Missing bucket_capacity"))?;
                    bucket.lock().unwrap().configure(rate, capacity).map_err(|e| anyhow!(e))?;
                }
            }
        }
        info!("Kalshi rate budgets loaded; target utilization 97%, with exit reserve");
        Ok(())
    }

    // ---- Internal auth helpers ----

    /// Apply authentication headers to a `RequestBuilder`.
    ///
    /// - RSA-PSS path: computes a fresh signature for `(method, path)` and
    ///   attaches the three Kalshi-specific headers.
    /// - JWT path: attaches `Authorization: Bearer <token>`, refreshing the
    ///   cached token if it is about to expire.
    ///
    /// `method` must be uppercase (e.g. `"GET"`, `"POST"`, `"DELETE"`).
    /// `path` must be the full path component including the `/trade-api/v2`
    /// prefix (e.g. `"/trade-api/v2/portfolio/balance"`).
    async fn apply_auth(
        &self,
        builder: RequestBuilder,
        method: &str,
        path: &str,
    ) -> Result<RequestBuilder> {
        match &self.auth_method {
            AuthMethod::RsaPss { api_key_id, signing_key } => {
                // Timestamp in milliseconds.
                let ts_ms = Utc::now().timestamp_millis().to_string();

                // Message = timestamp_ms + METHOD + path
                // e.g. "1234567890GET/trade-api/v2/portfolio/balance"
                let message = format!("{ts_ms}{method}{path}");

                // Sign with RSA-PSS / SHA-256 / MGF1-SHA-256 / salt=32 bytes.
                let mut rng = rand::thread_rng();
                let sig: Signature = signing_key.sign_with_rng(&mut rng, message.as_bytes());
                let sig_b64 = BASE64.encode(sig.to_bytes().as_ref());

                let builder = builder
                    .header("KALSHI-ACCESS-KEY", api_key_id.as_str())
                    .header("KALSHI-ACCESS-TIMESTAMP", ts_ms.as_str())
                    .header("KALSHI-ACCESS-SIGNATURE", sig_b64);

                Ok(builder)
            }

            AuthMethod::JwtPassword { email, password, auth } => {
                // Re-use the cached token when it is still fresh.
                let now = Utc::now().timestamp();
                {
                    let guard = auth.lock().await;
                    if let Some(ref state) = *guard {
                        if state.refresh_after > now {
                            return Ok(builder
                                .header(header::AUTHORIZATION, format!("Bearer {}", state.token)));
                        }
                    }
                }

                // Token is absent or stale — log in again.
                let bearer = self.jwt_login(email, password, auth).await?;
                Ok(builder.header(header::AUTHORIZATION, bearer))
            }
        }
    }

    /// Perform email/password JWT login and cache the resulting token.
    ///
    /// Returns the `Authorization: Bearer <token>` header value.
    async fn jwt_login(
        &self,
        email: &str,
        password: &str,
        auth_cache: &Arc<Mutex<Option<AuthState>>>,
    ) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/log_in", self.base_url))
            .json(&LoginRequest {
                email: email.to_owned(),
                password: password.to_owned(),
            })
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Kalshi login failed: HTTP {status} — {body}"));
        }

        let data: LoginResponse = resp.json().await?;
        let token = data.token.clone();

        // Tokens last 30 min; schedule a refresh 5 min early.
        let refresh_after = Utc::now().timestamp() + 1500;
        *auth_cache.lock().await = Some(AuthState { token: token.clone(), refresh_after });

        info!("Kalshi: logged in via email/password (token refreshes in ~25 min)");
        Ok(format!("Bearer {token}"))
    }

    /// Extract the URL path component (including query string) from a full URL.
    ///
    /// Used to derive the path string for the RSA-PSS signature message.
    /// For example `"https://api.../trade-api/v2/portfolio/balance"` →
    /// `"/trade-api/v2/portfolio/balance"`.
    fn path_from_url(url: &str) -> &str {
        // Find the third `/` which begins the path after `https://host`.
        let after_scheme = url.strip_prefix("https://").unwrap_or(url);
        after_scheme
            .find('/')
            .map(|i| &after_scheme[i..])
            .unwrap_or("/")
    }

    // ---- Public API ----

    fn v2_order_body(ticker: &str, action: &str, side: &str, price: i64, count: i64, client_id: &str) -> Result<serde_json::Value> {
        anyhow::ensure!(matches!(action, "buy" | "sell") && matches!(side, "yes" | "no"), "Invalid order direction");
        anyhow::ensure!((1..=99).contains(&price) && count > 0, "Invalid order price or count");
        let bid = (action == "buy") == (side == "yes");
        let yes_price = if side == "yes" { price } else { 100 - price };
        Ok(serde_json::json!({
            "ticker":ticker, "client_order_id":client_id,
            "side":if bid { "bid" } else { "ask" },
            "price":format!("0.{:02}00", yes_price), "count":format!("{count}.00"),
            "time_in_force":"good_till_canceled", "expiration_time":Utc::now().timestamp() + 30,
            "self_trade_prevention_type":"taker_at_cross", "cancel_order_on_pause":true,
            "reduce_only":action == "sell"
        }))
    }

    /// Read a signed demo endpoint; redirects never receive auth outside this origin.
    pub async fn demo_read(&self, endpoint: &str, query: &[(String,String)]) -> Result<serde_json::Value> {
        anyhow::ensure!(self.is_demo() && !self.paper, "requires actual demo exchange");
        anyhow::ensure!(endpoint.starts_with('/') && !endpoint.contains('?'), "invalid API path");
        let url=format!("{}{}",self.base_url,endpoint);
        let path=Self::path_from_url(&url).to_owned();
        let r=self.send_limited(self.http.get(&url).query(query),"GET",&path,true,false).await?;
        anyhow::ensure!(r.status().is_success(),"demo read {endpoint}: HTTP {}",r.status());
        Ok(r.json().await?)
    }

    pub async fn demo_pages(&self, endpoint:&str, field:&str, shard:i64, extra:&[(String,String)]) -> Result<Vec<serde_json::Value>> {
        let mut result=Vec::new(); let mut cursor=String::new();let mut seen=std::collections::HashSet::new();
        for _ in 0..1000 {
            let mut q=vec![("exchange_index".into(),shard.to_string()),("subaccount".into(),"0".into()),("limit".into(),"100".into())];
            q.extend_from_slice(extra);if !cursor.is_empty(){q.push(("cursor".into(),cursor.clone()));}
            let page=self.demo_read(endpoint,&q).await?;
            result.extend(page[field].as_array().ok_or_else(||anyhow!("missing paginated array {field}"))?.clone());
            cursor=page["cursor"].as_str().unwrap_or("").to_owned();
            if cursor.is_empty(){return Ok(result)}
            anyhow::ensure!(seen.insert(cursor.clone()),"repeated pagination cursor");
        }
        Err(anyhow!("pagination limit exceeded; incomplete reconciliation"))
    }

    /// Send one persisted intent. The caller owns journaling and reconciliation.
    /// No automatic POST retries, including on timeouts and ambiguous responses.
    pub async fn submit_intent(&self,i:&super::ledger::Intent)->Result<super::managed::SubmitResult>{
        anyhow::ensure!(self.is_demo()&&!self.paper,"demo execution required");
        let mut body=Self::v2_order_body(&i.ticker,&i.action,&i.side,i.price_cents,(i.count+99)/100,&i.client_id)?;
        body["count"]=format!("{}.{:02}",i.count/100,i.count%100).into();
        body["exchange_index"]=i.shard.into();
        body["expiration_time"]=(i.created_ts+30).into();
        let url=format!("{}/portfolio/events/orders",self.base_url);let path=Self::path_from_url(&url).to_owned();
        let r=match self.send_limited(self.http.post(&url).json(&body),"POST",&path,true,i.action=="sell").await {
            Ok(r)=>r,
            Err(e) if e.downcast_ref::<DispatchFailure>().is_none()=>return Ok(super::managed::SubmitResult::Rejected(format!("not dispatched: {e}"))),
            Err(e)=>return Err(e),
        };
        let status=r.status();
        if matches!(status.as_u16(),400|401|403|404|422|429){return Ok(super::managed::SubmitResult::Rejected(format!("HTTP {status}")))}
        anyhow::ensure!(status.is_success(),"ambiguous submit HTTP {status}; reconcile client ID before further entry");
        let ack:serde_json::Value=r.json().await?;
        let id=ack["order_id"].as_str().context("accepted response missing order_id")?.to_owned();
        Ok(super::managed::SubmitResult::Accepted(id))
    }

    pub async fn cancel_on_shard(&self,id:&str,shard:i64)->Result<()> {
        self.order_tickers.lock().unwrap().insert(id.into(),(String::new(),shard));
        self.cancel_order(id).await
    }

    /// Place a limit order on Kalshi.
    ///
    /// - `ticker`  — e.g. `"KXBTC-25DEC3100-T97500"`
    /// - `action`  — `"buy"` or `"sell"`
    /// - `side`    — `"yes"` or `"no"`
    /// - `price`   — limit price in cents (1–99), from the perspective of `side`
    /// - `count`   — number of contracts
    pub async fn place_order(
        &self,
        ticker: &str,
        action: &str,
        side: &str,
        price: i64,
        count: i64,
    ) -> Result<KalshiOrder> {
        let client_order_id = Uuid::new_v4().to_string();

        // ---- PAPER MODE: simulate an instant fill at the requested price ----
        // No HTTP call; just adjust the local balance (buy debits cost, sell
        // credits proceeds) and return a synthetic FILLED order so the rest of
        // the bot (risk manager, exit manager, journal) behaves normally.
        if self.paper {
            let notional = price * count;
            if action == "sell" {
                self.paper_balance
                    .fetch_add(notional, std::sync::atomic::Ordering::SeqCst);
            } else {
                self.paper_balance
                    .fetch_sub(notional, std::sync::atomic::Ordering::SeqCst);
            }
            let bal = self.paper_balance.load(std::sync::atomic::Ordering::SeqCst);
            let order = KalshiOrder {
                order_id: format!("paper-{client_order_id}"),
                ticker: ticker.to_owned(),
                action: action.to_owned(),
                side: side.to_owned(),
                count,
                yes_price: if side == "yes" { Some(price) } else { None },
                no_price: if side == "no" { Some(price) } else { None },
                status: "filled".to_owned(),
                created_time: None,
                client_order_id: Some(client_order_id),
                yes_price_dollars: None,
                no_price_dollars: None,
                initial_count_fp: None,
                fill_count_fp: None,
                taker_fill_cost_dollars: None,
                taker_fees_dollars: None,
            };
            info!(
                "PAPER FILL: {} {} {} ×{} @ {}¢ (balance ${:.2}) → {}",
                action, side, ticker, count, price, bal as f64 / 100.0, order.order_id
            );
            return Ok(order);
        }

        let shard = self.market_cache.lock().await.0.iter()
            .find(|m| m.ticker == ticker).and_then(|m| m.exchange_index)
            .ok_or_else(|| anyhow!("Missing exchange shard for {ticker}; refresh market discovery"))?;
        let mut body = Self::v2_order_body(ticker, action, side, price, count, &client_order_id)?;
        body["exchange_index"] = shard.into();

        // Don't dump the full JSON body in debug logs — it may include
        // identifiers that aren't useful to leak. Log only the trade-shape
        // fields that matter for diagnostics.
        debug!(
            "Kalshi place_order: ticker={} side={} price={}c count={} action={}",
            ticker, side, price, count, action
        );

        let url = format!("{}/portfolio/events/orders", self.base_url);
        let path = Self::path_from_url(&url).to_owned();
        let builder = self.http.post(&url).json(&body);
        let resp = self.send_limited(builder, "POST", &path, true, action == "sell").await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!("place_order failed: HTTP {status} — {text}"));
        }

        // V2 acknowledges acceptance separately from the full order record.
        let ack: serde_json::Value = serde_json::from_str(&text)?;
        let order_id = ack["order_id"].as_str().ok_or_else(|| anyhow!("V2 acknowledgment missing order_id"))?;
        self.order_tickers.lock().unwrap().insert(order_id.to_owned(), (ticker.to_owned(), shard));
        let order = self.get_order(order_id).await.map_err(|e| anyhow!(
            "Order {order_id} accepted but reconciliation failed: {e}; do not retry placement"))?;

        info!(
            "Kalshi order placed: {} {} {} ×{} @ {}¢ → order_id={}",
            action, side, ticker, count, price, order.order_id
        );
        Ok(order)
    }

    /// Cancel an open order.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        if self.paper {
            return Ok(()); // nothing was ever placed on the exchange
        }
        let url = format!("{}/portfolio/events/orders/{order_id}", self.base_url);
        let path = Self::path_from_url(&url).to_owned();
        let shard = self.order_tickers.lock().unwrap().get(order_id).map(|(_, shard)| *shard)
            .ok_or_else(|| anyhow!("Missing shard for order cancellation"))?;
        let builder = self.http.delete(&url).query(&[("exchange_index", shard)]);
        let resp = self.send_limited(builder, "DELETE", &path, true, true).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("cancel_order failed: HTTP {status} — {text}"));
        }

        info!("Kalshi order cancelled: {order_id}");
        Ok(())
    }

    /// Fetch a single order by ID to check its current fill status.
    ///
    /// Returns the order with its current `status` field which can be:
    /// `"resting"` (unfilled limit on book), `"canceled"`, `"executed"` (filled).
    pub async fn get_order(&self, order_id: &str) -> Result<KalshiOrder> {
        // Order acknowledgments can precede the read API's replicated record.
        // Retry only reads; never replay an accepted placement.
        for attempt in 0..10 {
            let url = format!("{}/portfolio/orders/{order_id}", self.base_url);
            let path = Self::path_from_url(&url).to_owned();
            let mut builder = self.http.get(&url);
            if let Some((_, shard)) = self.order_tickers.lock().unwrap().get(order_id).cloned() {
                builder = builder.query(&[("exchange_index", shard)]);
            }
            let resp = self.send_limited(builder, "GET", &path, true, false).await?;
            if resp.status() == reqwest::StatusCode::NOT_FOUND && attempt < 9 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            if !resp.status().is_success() {
                let status = resp.status();
                return Err(anyhow!("get_order failed: HTTP {status}"));
            }
            let data: OrderResponse = resp.json().await?;
            return Ok(data.order);
        }
        unreachable!()
    }

    /// Strictly check whether an order status indicates the contracts have
    /// actually been filled (i.e. risk has actually moved to a position).
    ///
    /// Allowlist (rather than denylist) so unknown/new statuses default to
    /// `false` — the auto-reconcile loop will catch up when the next
    /// `get_order` poll returns a definitive status.
    ///
    /// "resting" means the limit order is sitting on the book unfilled —
    /// this is NOT a fill. The caller must reconcile it later.
    pub fn is_order_filled(status: &str) -> bool {
        matches!(status, "executed" | "filled")
    }

    /// Fetch all open positions.
    pub async fn get_positions(&self) -> Result<Vec<KalshiPosition>> {
        let url = format!("{}/portfolio/positions", self.base_url);
        let path = Self::path_from_url(&url).to_owned();
        let builder = self.http.get(&url);
        let resp = self.send_limited(builder, "GET", &path, true, false).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("get_positions failed: HTTP {status} — {text}"));
        }

        let data: PositionsResponse = resp.json().await?;
        Ok(data.market_positions)
    }

    /// Fetch available account balance in cents.
    pub async fn get_balance(&self) -> Result<i64> {
        if self.paper {
            return Ok(self.paper_balance.load(std::sync::atomic::Ordering::SeqCst));
        }
        let url = format!("{}/portfolio/balance", self.base_url);
        let path = Self::path_from_url(&url).to_owned();
        let builder = self.http.get(&url);
        let resp = self.send_limited(builder, "GET", &path, true, false).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("get_balance failed: HTTP {status} — {text}"));
        }

        let data: BalanceResponse = resp.json().await?;
        Ok(data.balance)
    }

    /// List open markets for a given ticker prefix.
    pub async fn list_markets_by_prefix(&self, prefix: &str) -> Result<Vec<KalshiMarket>> {
        let url = format!("{}/markets", self.data_base_url);
        let path = Self::path_from_url(&url).to_owned();
        let builder = self
            .http
            .get(&url)
            .query(&[("status", "open"), ("ticker_prefix", prefix), ("limit", "200")]);
        // Public, read-only endpoint — skip auth on cross-environment data reads.
        let resp = self.send_limited(builder, "GET", &path, self.data_base_url == self.base_url, false).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("list_markets({prefix}) failed: HTTP {status} — {text}"));
        }

        let data: MarketsResponse = resp.json().await?;
        debug!("Kalshi: {} markets found for prefix '{prefix}'", data.markets.len());
        Ok(data.markets)
    }

    /// List open markets by series_ticker (e.g. "KXBTC15M").
    pub async fn list_markets_by_series(&self, series: &str) -> Result<Vec<KalshiMarket>> {
        let url = format!("{}/markets", self.data_base_url);
        let path = Self::path_from_url(&url).to_owned();
        let builder = self
            .http
            .get(&url)
            .query(&[("status", "open"), ("series_ticker", series), ("limit", "200")]);
        // Cross-environment data reads (e.g. prod prices for a demo bot) hit a
        // PUBLIC endpoint and must NOT carry this env's auth — the foreign
        // signature would be rejected. Same-environment reads keep auth.
        let resp = self.send_limited(builder, "GET", &path, self.data_base_url == self.base_url, false).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("list_markets_series({series}) failed: HTTP {status} — {text}"));
        }

        let data: MarketsResponse = resp.json().await?;
        debug!("Kalshi: {} markets found for series '{series}'", data.markets.len());
        Ok(data.markets)
    }

    /// All strategy loops share the same 15-minute market cache.
    pub async fn list_btc_markets(&self) -> Result<Vec<KalshiMarket>> {
        self.list_all_crypto_15m().await
    }

    /// List ALL crypto 15-min contracts for active trading.
    /// ONLY 15-min series — no 5-min, daily, or weekly products.
    pub async fn list_all_crypto_15m(&self) -> Result<Vec<KalshiMarket>> {
        // Cache: all 5 bot loops share this via Arc. Only one actually hits the API
        // per 2-second window. Cuts API calls from ~20/cycle to ~4/cycle.
        let mut cache = self.market_cache.lock().await;
        if cache.1.elapsed() < std::time::Duration::from_secs(2) {
            return Ok(cache.0.clone());
        }

        // Quote age includes time spent fetching subsequent series.
        let scan_started = std::time::Instant::now();
        let series = vec![
            "KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M", "KXDOGE15M",
        ];
        let mut all = Vec::new();
        let series_count = series.len();
        for s in &series {
            match self.list_markets_by_series(s).await {
                Ok(markets) => {
                    if !markets.is_empty() {
                        // debug! to suppress duplicate log noise when
                        // multiple strategy loops scan concurrently. Aggregate
                        // info! is emitted once below.
                        debug!("Kalshi: {} open {} markets", markets.len(), s);
                    }
                    all.extend(markets);
                }
                Err(e) => debug!("Kalshi: {s} fetch failed: {e}"),
            }
        }
        if !all.is_empty() {
            info!("Kalshi crypto-15m scan: {} markets across {} series", all.len(), series_count);
        }

        // Demo can report historical contracts as open; never trade expired ones.
        all.retain(|m| m.close_time.as_ref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map_or(false, |t| t.timestamp() > Utc::now().timestamp()));
        cache.0 = all.clone();
        cache.1 = scan_started;
        Ok(all)
    }

    pub async fn list_all_tradeable_markets(&self) -> Result<Vec<KalshiMarket>> {
        self.list_all_crypto_15m().await
    }

    pub async fn find_active_btc_contract(&self) -> Result<Option<KalshiMarket>> {
        let markets = self.list_btc_markets().await?;

        let mut tradeable: Vec<KalshiMarket> = markets
            .into_iter()
            // Kalshi API returns status as "open" or "active" depending on version.
            .filter(|m| m.status == "open" || m.status == "active")
            // Must have at least one side quoted (has liquidity).
            .filter(|m| m.has_liquidity())
            .collect();

        // Sort by close_time ascending → nearest-expiry first.
        tradeable.sort_by(|a, b| {
            a.close_time
                .as_deref()
                .unwrap_or("")
                .cmp(b.close_time.as_deref().unwrap_or(""))
        });

        if !tradeable.is_empty() {
            info!(
                "Found {} tradeable BTC contracts, nearest: {}",
                tradeable.len(),
                tradeable[0].ticker
            );
        }

        Ok(tradeable.into_iter().next())
    }

    /// Warm up: verify credentials and return the current balance.
    ///
    /// Call this once at startup to surface credential errors early.
    pub async fn warm_up(&self) -> Result<i64> {
        if !self.paper { self.refresh_rate_limits().await?; }
        let balance = self.get_balance().await?;
        let env = if self.paper { "PAPER" } else if self.is_demo() { "DEMO" } else { "LIVE" };
        info!(
            "Kalshi [{}] connected — balance: ${:.2}",
            env,
            balance as f64 / 100.0
        );
        Ok(balance)
    }

    /// Compute the limit price to use for a YES or NO buy.
    ///
    /// Given the market's bid/ask, returns a price just inside the spread
    /// (aggressive limit) clamped to 1–99.
    pub fn compute_limit_price(market: &KalshiMarket, side: &str) -> Option<i64> {
        let price = if side == "yes" {
            // Buy YES just above the best bid.
            let bid = market.yes_bid_cents()?;
            let ask = market.yes_ask_cents().unwrap_or(bid + 5);
            (bid + (ask - bid).max(1) / 2).clamp(1, 99)
        } else {
            // Buy NO: NO price = 100 − YES price; target midpoint of NO spread.
            let yes_ask = market.yes_ask_cents()?;
            let yes_bid = market.yes_bid_cents().unwrap_or(yes_ask - 5);
            let no_bid = 100 - yes_ask;
            let no_ask = 100 - yes_bid;
            (no_bid + (no_ask - no_bid).max(1) / 2).clamp(1, 99)
        };
        Some(price)
    }

    /// Convert a confidence score (0–1) to a suggested position size in contracts.
    ///
    /// Caps at `max_cost_cents / price` to respect the dollar limit.
    pub fn size_from_confidence(confidence: f64, price_cents: i64, max_cost_cents: i64) -> i64 {
        if price_cents <= 0 {
            return 1;
        }
        let base_count = ((confidence - 0.5) * 4.0).clamp(0.5, 2.0);
        let max_from_budget = (max_cost_cents / price_cents).max(1);
        (base_count.round() as i64).clamp(1, max_from_budget)
    }

    /// Log a human-readable warning when the account balance is low.
    pub fn warn_low_balance(balance_cents: i64, daily_loss_cents: i64) {
        let balance_dollars = balance_cents as f64 / 100.0;
        let loss_dollars = (-daily_loss_cents).max(0) as f64 / 100.0;
        if balance_dollars < 5.0 {
            warn!(
                "Kalshi: LOW BALANCE ${:.2} (daily loss so far: ${:.2})",
                balance_dollars, loss_dollars
            );
        }
    }
}

// ---- RSA key loading ----

/// Load an RSA private key from a PEM file and wrap it in a
/// `BlindedSigningKey<Sha256>` configured for PSS signing with:
/// - hash:       SHA-256
/// - MGF:        MGF1-SHA-256
/// - salt length: 32 bytes (equal to the hash output length)
fn load_rsa_signing_key(path: &str) -> Result<BlindedSigningKey<Sha256>> {
    let pem_str = std::fs::read_to_string(path)
        .with_context(|| format!("Cannot read private key file: {path}"))?;

    // `RsaPrivateKey::from_pkcs8_pem` handles PKCS#8 PEM (BEGIN PRIVATE KEY).
    // `RsaPrivateKey::from_pkcs1_pem` handles PKCS#1 PEM (BEGIN RSA PRIVATE KEY).
    // Try PKCS#8 first, then fall back to PKCS#1.
    let private_key = RsaPrivateKey::from_pkcs8_pem(&pem_str)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem_str))
        .context("Failed to parse RSA private key (expected PKCS#8 or PKCS#1 PEM)")?;

    // `BlindedSigningKey` uses constant-time blinding to protect against
    // side-channel attacks; it selects PSS with the SHA-256 type parameter.
    Ok(BlindedSigningKey::<Sha256>::new(private_key))
}

/// Redact lines or JSON fields that look like they contain credentials before
/// emitting them in logs. We do a coarse line-by-line filter — anything that
/// case-insensitively mentions `authorization`, `token`, `signature`, `secret`,
/// or `key` is replaced with `[redacted]`. Trades a little log readability for
/// guaranteed credential-safety in the `RAW RESPONSE` warn path.
fn redact_sensitive_response(text: &str) -> String {
    // We look for these substrings case-insensitively. Using lowercase compare
    // avoids pulling in the `regex` crate just for this redaction.
    const NEEDLES: &[&str] = &[
        "authorization", "token", "signature", "secret", "key",
    ];
    let mut out = String::with_capacity(text.len());
    for (i, line) in text.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        let lower = line.to_ascii_lowercase();
        let sensitive = NEEDLES.iter().any(|n| lower.contains(n));
        if sensitive {
            out.push_str("[redacted]");
        } else {
            out.push_str(line);
        }
    }
    out
}

#[cfg(test)]
mod startup_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn v2_directions_preserve_yes_no_economics() {
        for (action, side, book, price) in [("buy","yes","bid","0.3000"),("buy","no","ask","0.7000"),("sell","yes","ask","0.3000"),("sell","no","bid","0.7000")] {
            let body = KalshiExecutor::v2_order_body("KXBTC15M-test",action,side,30,1,"test").unwrap();
            assert_eq!(body["side"],book); assert_eq!(body["price"],price);
            assert_eq!(body["reduce_only"],action == "sell");
        }
    }

    #[tokio::test]
    async fn warm_up_rejects_auth_failure_before_fetching_balance() {
        // Local fixture only: no credentials and no Kalshi connection.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let size = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.starts_with("GET /trade-api/v2/account/limits "));
            socket.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
        });
        let now = std::time::Instant::now();
        let executor = KalshiExecutor {
            http: Client::new(),
            order_tickers: std::sync::Mutex::new(std::collections::HashMap::new()),
            base_url: format!("http://{address}/trade-api/v2"),
            data_base_url: format!("http://{address}/trade-api/v2"),
            mode: KalshiMode::Demo,
            auth_method: AuthMethod::JwtPassword {
                email: String::new(), password: String::new(),
                auth: Arc::new(Mutex::new(Some(AuthState {
                    token: "local-test-token".into(),
                    refresh_after: Utc::now().timestamp() + 60,
                }))),
            },
            paper: false,
            paper_balance: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            market_cache: Arc::new(Mutex::new((Vec::new(), now))),
            read_budget: std::sync::Mutex::new(super::super::rate_limit::Bucket::new(200.0, 400.0, 0.0, now)),
            write_budget: std::sync::Mutex::new(super::super::rate_limit::Bucket::new(100.0, 100.0, 10.0, now)),
            endpoint_costs: std::sync::RwLock::new(serde_json::json!({"default_cost":10,"endpoint_costs":[]})),
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), executor.warm_up()).await.unwrap();
        assert!(result.unwrap_err().to_string().contains("401"));
        server.await.unwrap();
    }
}
