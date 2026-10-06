use super::super::types::BenchFailure;
use serde::Serialize;

#[derive(Clone, Serialize)]
pub(super) struct Config {
    pub rates: Vec<u64>,
    pub stage_seconds: u64,
    pub producer_connections: usize,
    pub consumer_delay_ms: u64,
    pub max_backlog: u64,
    pub max_attempts_per_stage: usize,
    pub drain_seconds: u64,
}

fn setting(name: &str, default: u64, maximum: u64) -> Result<u64, BenchFailure> {
    let value = match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|error| BenchFailure::validation(error.to_string()))?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(BenchFailure::validation(error.to_string())),
    };
    if value == 0 || value > maximum {
        return Err(BenchFailure::validation(format!(
            "{name} must be in 1..={maximum}"
        )));
    }
    Ok(value)
}

impl Config {
    pub fn from_env() -> Result<Self, BenchFailure> {
        let raw = match std::env::var("FITZ_QUEUE_PRESSURE_RATES") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "100,200,400,800,1600,3200,6400,12800".into(),
            Err(error) => return Err(BenchFailure::validation(error.to_string())),
        };
        let rates = raw
            .split(',')
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| BenchFailure::validation(error.to_string()))?;
        if rates.is_empty()
            || rates.len() > 16
            || rates.iter().any(|rate| *rate == 0 || *rate > 100_000)
            || rates.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(BenchFailure::validation(
                "rates must be 1..=100000, strictly increasing, at most 16 stages",
            ));
        }
        Ok(Self {
            rates,
            stage_seconds: setting("FITZ_QUEUE_PRESSURE_STAGE_SECS", 30, 120)?,
            producer_connections: usize::try_from(setting(
                "FITZ_QUEUE_PRESSURE_PRODUCERS",
                32,
                256,
            )?)
            .map_err(|error| BenchFailure::validation(error.to_string()))?,
            consumer_delay_ms: setting("FITZ_QUEUE_PRESSURE_CONSUMER_DELAY_MS", 5, 1000)?,
            max_backlog: setting("FITZ_QUEUE_PRESSURE_MAX_BACKLOG", 50_000, 100_000)?,
            max_attempts_per_stage: usize::try_from(setting(
                "FITZ_QUEUE_PRESSURE_MAX_ATTEMPTS",
                200_000,
                500_000,
            )?)
            .map_err(|error| BenchFailure::validation(error.to_string()))?,
            drain_seconds: setting("FITZ_QUEUE_PRESSURE_DRAIN_SECS", 600, 600)?,
        })
    }
}
