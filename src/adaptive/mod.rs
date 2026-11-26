pub mod learner;

use serde::{Deserialize, Serialize};

/// Bounds for each adaptive parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveBounds {
    pub imbalance_threshold: [f64; 2],
    pub window_seconds: [f64; 2],
    pub min_volume_usdc: [f64; 2],
    pub confidence_min: [f64; 2],
    pub cooldown_seconds: [f64; 2],
}

impl Default for AdaptiveBounds {
    fn default() -> Self {
        Self {
            imbalance_threshold: [0.55, 0.85],
            window_seconds: [5.0, 60.0],
            min_volume_usdc: [100.0, 5000.0],
            confidence_min: [0.50, 0.85],
            cooldown_seconds: [15.0, 120.0],
        }
    }
}

/// Configuration for the adaptive parameter learner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    pub min_samples_to_learn: usize,
    pub rolling_window_size: usize,
    pub optimization_interval: usize,
    pub learning_rate: f64,
    pub target_win_rate: f64,
    pub emergency_tighten_threshold: f64,
    pub bounds: AdaptiveBounds,
    pub lessons_log_path: String,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            min_samples_to_learn: 20,
            rolling_window_size: 50,
            optimization_interval: 10,
            learning_rate: 0.10,
            target_win_rate: 0.55,
            emergency_tighten_threshold: 0.38,
            bounds: AdaptiveBounds::default(),
            lessons_log_path: "logs/lessons.json".to_owned(),
        }
    }
}
