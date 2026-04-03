pub mod criterion;
pub mod stress;

use anyhow::Result;

use super::config::BenchSummaryConfig;
use super::model::BenchmarkRecord;

pub trait BenchmarkAdapter {
    fn name(&self) -> &'static str;
    fn collect(&self, config: &BenchSummaryConfig) -> Result<Vec<BenchmarkRecord>>;
}

pub fn enabled_adapters(config: &BenchSummaryConfig) -> Vec<Box<dyn BenchmarkAdapter>> {
    let mut adapters: Vec<Box<dyn BenchmarkAdapter>> = Vec::new();
    if config.adapters.criterion.is_some() {
        adapters.push(Box::new(criterion::CriterionAdapter));
    }
    if config.adapters.stress.is_some() {
        adapters.push(Box::new(stress::StressAdapter));
    }
    adapters
}
