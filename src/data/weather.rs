//! Weather forecast data from Open-Meteo API.
//!
//! Fetches hourly temperature forecasts for Kalshi's weather market cities
//! (NYC, Chicago, LA, Miami). The bot compares forecast high temps against
//! Kalshi weather market strikes to find mispriced contracts.

use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// City coordinates for Open-Meteo API.
pub struct City {
    pub name: &'static str,
    pub lat: f64,
    pub lon: f64,
    /// Kalshi ticker prefix for this city's high-temp markets.
    pub kalshi_prefix: &'static str,
}

/// All cities Kalshi has weather markets for.
pub const WEATHER_CITIES: &[City] = &[
    City { name: "New York", lat: 40.7128, lon: -74.0060, kalshi_prefix: "HIGHNY" },
    City { name: "Chicago", lat: 41.8781, lon: -87.6298, kalshi_prefix: "HIGHCHI" },
    City { name: "Los Angeles", lat: 34.0522, lon: -118.2437, kalshi_prefix: "HIGHLA" },
    City { name: "Miami", lat: 25.7617, lon: -80.1918, kalshi_prefix: "HIGHMIA" },
];

/// Open-Meteo API response.
#[derive(Debug, Deserialize)]
struct OpenMeteoResponse {
    hourly: Option<HourlyData>,
    daily: Option<DailyData>,
}

#[derive(Debug, Deserialize)]
struct HourlyData {
    time: Vec<String>,
    temperature_2m: Vec<f64>,
}

#[derive(Debug, Deserialize)]
struct DailyData {
    time: Vec<String>,
    temperature_2m_max: Vec<f64>,
    temperature_2m_min: Vec<f64>,
}

/// Forecast for a single city on a specific date.
#[derive(Debug, Clone)]
pub struct CityForecast {
    pub city: String,
    pub kalshi_prefix: String,
    pub date: String,
    /// Forecast high temperature in Fahrenheit.
    pub high_f: f64,
    /// Forecast low temperature in Fahrenheit.
    pub low_f: f64,
    /// Confidence range: how much the high could vary (±degrees F).
    /// Estimated from model spread — typically ±2-4°F for next-day.
    pub uncertainty_f: f64,
}

/// Fetches weather forecasts from Open-Meteo.
pub struct WeatherFetcher {
    http: reqwest::Client,
}

impl WeatherFetcher {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Fetch forecasts for all cities, returning daily high/low for the next 3 days.
    pub async fn fetch_all(&self) -> Vec<CityForecast> {
        let mut forecasts = Vec::new();

        for city in WEATHER_CITIES {
            match self.fetch_city(city).await {
                Ok(city_forecasts) => {
                    info!(
                        "Weather: {} — next day high={:.0}°F low={:.0}°F",
                        city.name,
                        city_forecasts.first().map(|f| f.high_f).unwrap_or(0.0),
                        city_forecasts.first().map(|f| f.low_f).unwrap_or(0.0),
                    );
                    forecasts.extend(city_forecasts);
                }
                Err(e) => {
                    warn!("Weather fetch failed for {}: {e}", city.name);
                }
            }
        }

        forecasts
    }

    async fn fetch_city(&self, city: &City) -> Result<Vec<CityForecast>> {
        let url = format!(
            "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&daily=temperature_2m_max,temperature_2m_min&temperature_unit=fahrenheit&timezone=America/New_York&forecast_days=3",
            city.lat, city.lon
        );

        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(anyhow::anyhow!("Open-Meteo HTTP {status}"));
        }

        let data: OpenMeteoResponse = resp.json().await?;
        let daily = data.daily.ok_or_else(|| anyhow::anyhow!("No daily data"))?;

        let mut forecasts = Vec::new();
        for i in 0..daily.time.len() {
            let high = daily.temperature_2m_max.get(i).copied().unwrap_or(0.0);
            let low = daily.temperature_2m_min.get(i).copied().unwrap_or(0.0);

            // Uncertainty increases with forecast distance.
            // Day 1: ±2°F, Day 2: ±3°F, Day 3: ±4°F.
            let uncertainty = 2.0 + i as f64;

            forecasts.push(CityForecast {
                city: city.name.to_owned(),
                kalshi_prefix: city.kalshi_prefix.to_owned(),
                date: daily.time[i].clone(),
                high_f: high,
                low_f: low,
                uncertainty_f: uncertainty,
            });
        }

        Ok(forecasts)
    }
}

/// Determine if a weather market is mispriced based on our forecast.
///
/// Kalshi weather markets ask: "Will the high temperature in [city] be
/// above X°F on [date]?"
///
/// If our forecast says the high will be 75°F ± 2°F, and the market strike
/// is 70°F, the probability of YES is very high (~97%) — so if YES is
/// priced below 90¢, it's underpriced.
///
/// Returns (should_trade, side, estimated_probability).
pub fn evaluate_weather_market(
    forecast_high: f64,
    uncertainty: f64,
    strike_f: f64,
) -> (bool, &'static str, f64) {
    // How many standard deviations the strike is from our forecast.
    // Using uncertainty as ~1 standard deviation.
    if uncertainty < 0.1 {
        return (false, "", 0.5);
    }

    let z_score = (forecast_high - strike_f) / uncertainty;

    // Convert z-score to approximate probability using a simple sigmoid.
    // z=0 → 50%, z=1 → ~84%, z=2 → ~97%, z=-1 → ~16%, z=-2 → ~3%.
    let prob_above = 1.0 / (1.0 + (-z_score * 1.5).exp());

    // We need at least 70% confidence to trade.
    if prob_above > 0.70 {
        // Forecast high is well above strike → buy YES.
        (true, "yes", prob_above)
    } else if prob_above < 0.30 {
        // Forecast high is well below strike → buy NO.
        (true, "no", 1.0 - prob_above)
    } else {
        // Too close to call — skip.
        (false, "", prob_above)
    }
}
