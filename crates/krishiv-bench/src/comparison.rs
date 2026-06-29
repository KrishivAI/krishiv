//! Benchmark comparison framework for TPC-H results.
//!
//! Provides structured result types and comparison logic for publishing
//! benchmark results across Krishiv versions or against external engines.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

/// Result of a single query benchmark.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Query name (e.g., "q1", "q3").
    pub name: String,
    /// Query execution time.
    pub duration: Duration,
    /// Number of rows returned.
    pub row_count: usize,
    /// Optional memory usage in bytes.
    pub memory_bytes: Option<usize>,
}

impl QueryResult {
    pub fn new(name: &str, duration: Duration, row_count: usize) -> Self {
        Self {
            name: name.to_string(),
            duration,
            row_count,
            memory_bytes: None,
        }
    }

    pub fn with_memory(mut self, bytes: usize) -> Self {
        self.memory_bytes = Some(bytes);
        self
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:>4}: {:>10.3?}  ({:>8} rows",
            self.name, self.duration, self.row_count
        )?;
        if let Some(mem) = self.memory_bytes {
            write!(f, ", {mem:>10} bytes")?;
        }
        write!(f, ")")
    }
}

/// Complete benchmark run for a scale factor.
#[derive(Debug, Clone)]
pub struct BenchmarkRun {
    /// Engine name (e.g., "krishiv", "spark", "datafusion").
    pub engine: String,
    /// Scale factor (e.g., "sf1", "sf10", "sf100").
    pub scale_factor: String,
    /// Individual query results.
    pub results: Vec<QueryResult>,
    /// Wall-clock time for the entire run (all queries sequentially).
    pub total_duration: Duration,
}

impl BenchmarkRun {
    pub fn new(engine: &str, scale_factor: &str) -> Self {
        Self {
            engine: engine.to_string(),
            scale_factor: scale_factor.to_string(),
            results: Vec::new(),
            total_duration: Duration::ZERO,
        }
    }

    pub fn add_result(&mut self, result: QueryResult) {
        self.results.push(result);
    }

    pub fn set_total_duration(&mut self, duration: Duration) {
        self.total_duration = duration;
    }

    /// Get the result for a specific query.
    pub fn query(&self, name: &str) -> Option<&QueryResult> {
        self.results.iter().find(|r| r.name == name)
    }

    /// Average query time across all queries.
    pub fn avg_query_duration(&self) -> Duration {
        if self.results.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.results.iter().map(|r| r.duration).sum();
        total / self.results.len() as u32
    }
}

/// Comparison between two benchmark runs.
#[derive(Debug, Clone)]
pub struct BenchmarkComparison {
    pub baseline: BenchmarkRun,
    pub contender: BenchmarkRun,
}

impl BenchmarkComparison {
    pub fn new(baseline: BenchmarkRun, contender: BenchmarkRun) -> Self {
        Self {
            baseline,
            contender,
        }
    }

    /// Speedup ratio for a specific query (>1.0 means contender is faster).
    pub fn speedup(&self, query_name: &str) -> Option<f64> {
        let base = self.baseline.query(query_name)?;
        let cont = self.contender.query(query_name)?;
        if cont.duration.is_zero() {
            return None;
        }
        Some(base.duration.as_secs_f64() / cont.duration.as_secs_f64())
    }

    /// Overall speedup ratio (total durations).
    pub fn overall_speedup(&self) -> f64 {
        if self.contender.total_duration.is_zero() {
            return 0.0;
        }
        self.baseline.total_duration.as_secs_f64() / self.contender.total_duration.as_secs_f64()
    }

    /// Format as a human-readable comparison table.
    pub fn format_table(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "TPC-H Benchmark Comparison: {} vs {}\n",
            self.baseline.engine, self.contender.engine
        ));
        out.push_str(&format!("Scale Factor: {}\n", self.baseline.scale_factor));
        out.push_str(&"─".repeat(60));
        out.push('\n');
        out.push_str(&format!(
            "{:>6} {:>12} {:>12} {:>8}\n",
            "Query", self.baseline.engine, self.contender.engine, "Speedup"
        ));
        out.push_str(&"─".repeat(60));
        out.push('\n');

        for base_result in &self.baseline.results {
            if let Some(cont_result) = self.contender.query(&base_result.name) {
                let speedup = if cont_result.duration.is_zero() {
                    0.0
                } else {
                    base_result.duration.as_secs_f64() / cont_result.duration.as_secs_f64()
                };
                out.push_str(&format!(
                    "{:>6} {:>10.3?} {:>10.3?} {:>7.2}x\n",
                    base_result.name, base_result.duration, cont_result.duration, speedup
                ));
            }
        }

        out.push_str(&"─".repeat(60));
        out.push('\n');
        out.push_str(&format!(
            "{:>6} {:>10.3?} {:>10.3?} {:>7.2}x\n",
            "TOTAL",
            self.baseline.total_duration,
            self.contender.total_duration,
            self.overall_speedup()
        ));

        out
    }
}

impl fmt::Display for BenchmarkComparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_table())
    }
}

/// Aggregate results across multiple runs.
#[derive(Debug, Clone, Default)]
pub struct BenchmarkAggregate {
    pub runs: Vec<BenchmarkRun>,
}

impl BenchmarkAggregate {
    pub fn add_run(&mut self, run: BenchmarkRun) {
        self.runs.push(run);
    }

    /// Compute median duration per query across all runs.
    pub fn median_per_query(&self) -> HashMap<String, Duration> {
        let mut by_query: HashMap<String, Vec<Duration>> = HashMap::new();
        for run in &self.runs {
            for result in &run.results {
                by_query
                    .entry(result.name.clone())
                    .or_default()
                    .push(result.duration);
            }
        }
        by_query
            .into_iter()
            .map(|(name, mut durations)| {
                durations.sort();
                let median = durations
                    .get(durations.len() / 2)
                    .copied()
                    .unwrap_or(Duration::ZERO);
                (name, median)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_result_display() {
        let r = QueryResult::new("q1", Duration::from_millis(123), 1000);
        let s = format!("{r}");
        assert!(s.contains("q1"));
        assert!(s.contains("123"));
    }

    #[test]
    fn benchmark_run_avg() {
        let mut run = BenchmarkRun::new("krishiv", "sf1");
        run.add_result(QueryResult::new("q1", Duration::from_millis(100), 10));
        run.add_result(QueryResult::new("q3", Duration::from_millis(200), 20));
        assert_eq!(run.avg_query_duration(), Duration::from_millis(150));
    }

    #[test]
    fn comparison_speedup() {
        let mut base = BenchmarkRun::new("spark", "sf1");
        base.set_total_duration(Duration::from_secs(10));
        base.add_result(QueryResult::new("q1", Duration::from_millis(500), 100));

        let mut cont = BenchmarkRun::new("krishiv", "sf1");
        cont.set_total_duration(Duration::from_secs(5));
        cont.add_result(QueryResult::new("q1", Duration::from_millis(250), 100));

        let cmp = BenchmarkComparison::new(base, cont);
        assert!((cmp.speedup("q1").unwrap() - 2.0).abs() < 0.01);
        assert!((cmp.overall_speedup() - 2.0).abs() < 0.01);
    }

    #[test]
    fn comparison_table_format() {
        let mut base = BenchmarkRun::new("spark", "sf1");
        base.set_total_duration(Duration::from_secs(10));
        base.add_result(QueryResult::new("q1", Duration::from_millis(500), 100));

        let mut cont = BenchmarkRun::new("krishiv", "sf1");
        cont.set_total_duration(Duration::from_secs(5));
        cont.add_result(QueryResult::new("q1", Duration::from_millis(250), 100));

        let cmp = BenchmarkComparison::new(base, cont);
        let table = cmp.format_table();
        assert!(table.contains("spark"));
        assert!(table.contains("krishiv"));
        assert!(table.contains("2.00x"));
    }

    #[test]
    fn aggregate_median() {
        let mut agg = BenchmarkAggregate::default();
        for i in 0..5 {
            let mut run = BenchmarkRun::new("krishiv", "sf1");
            run.add_result(QueryResult::new(
                "q1",
                Duration::from_millis(100 + i * 10),
                10,
            ));
            agg.add_run(run);
        }
        let medians = agg.median_per_query();
        assert_eq!(medians["q1"], Duration::from_millis(120));
    }
}
