use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use sysinfo::System;

/// Process-level system metrics snapshot collected via `sysinfo`.
///
/// Values are stored as atomics so they can be read without locks from the
/// Prometheus render path. A background task should call [`refresh`] periodically
/// (every 5-10 s is sufficient) to keep the gauges current.
#[derive(Debug)]
pub struct SystemMetrics {
    /// RSS (resident set size) of the current process in bytes.
    process_memory_bytes: AtomicU64,
    /// CPU usage of the current process (0.0 – num_cpus, stored × 100 for precision).
    process_cpu_usage_x100: AtomicU64,
    /// Total virtual memory of the current process in bytes.
    process_virtual_memory_bytes: AtomicU64,
    /// Number of tasks (threads) owned by the current process.
    process_thread_count: AtomicU64,
    /// Total system memory in bytes.
    system_total_memory_bytes: AtomicU64,
    /// Available system memory in bytes.
    system_available_memory_bytes: AtomicU64,
    /// System-wide CPU usage as a percentage × 100 (e.g. 250.0 = 2.5 cores → 25000).
    system_cpu_usage_x100: AtomicU64,
}

static SYSTEM_METRICS: OnceLock<SystemMetrics> = OnceLock::new();

/// Process-wide system metrics singleton (lazy-initialized).
pub fn system_metrics() -> &'static SystemMetrics {
    SYSTEM_METRICS.get_or_init(|| SystemMetrics {
        process_memory_bytes: AtomicU64::new(0),
        process_cpu_usage_x100: AtomicU64::new(0),
        process_virtual_memory_bytes: AtomicU64::new(0),
        process_thread_count: AtomicU64::new(0),
        system_total_memory_bytes: AtomicU64::new(0),
        system_available_memory_bytes: AtomicU64::new(0),
        system_cpu_usage_x100: AtomicU64::new(0),
    })
}

impl SystemMetrics {
    /// Snapshot the current process and system metrics into the atomics.
    pub fn refresh(&self) {
        let mut sys = System::new_all();
        sys.refresh_memory();
        sys.refresh_cpu_all();

        // Process-level metrics.
        let pid = sysinfo::get_current_pid().unwrap_or_else(|_| sysinfo::Pid::from_u32(0));
        if let Some(proc) = sys.process(pid) {
            self.process_memory_bytes
                .store(proc.memory(), Ordering::Relaxed);
            self.process_cpu_usage_x100
                .store((proc.cpu_usage() * 100.0) as u64, Ordering::Relaxed);
            self.process_virtual_memory_bytes
                .store(proc.virtual_memory(), Ordering::Relaxed);
        }

        // System-level metrics.
        self.system_total_memory_bytes
            .store(sys.total_memory(), Ordering::Relaxed);
        self.system_available_memory_bytes
            .store(sys.available_memory(), Ordering::Relaxed);
        let cpu_usage: f32 = sys.cpus().iter().map(|c| c.cpu_usage()).sum();
        self.system_cpu_usage_x100
            .store((cpu_usage * 100.0) as u64, Ordering::Relaxed);

        // Thread count via /proc/self/status (Linux-specific, best-effort).
        #[cfg(target_os = "linux")]
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("Threads:") {
                    if let Ok(n) = rest.trim().parse::<u64>() {
                        self.process_thread_count.store(n, Ordering::Relaxed);
                    }
                    break;
                }
            }
        }
    }

    // ── Accessors ──────────────────────────────────────────────────────────

    /// RSS of the current process in bytes.
    pub fn process_memory_bytes(&self) -> u64 {
        self.process_memory_bytes.load(Ordering::Relaxed)
    }

    /// CPU usage of the current process × 100 (e.g. 150.0 = 1.5 cores → 15000).
    pub fn process_cpu_usage_x100(&self) -> u64 {
        self.process_cpu_usage_x100.load(Ordering::Relaxed)
    }

    /// Virtual memory of the current process in bytes.
    pub fn process_virtual_memory_bytes(&self) -> u64 {
        self.process_virtual_memory_bytes.load(Ordering::Relaxed)
    }

    /// Number of threads in the current process.
    pub fn process_thread_count(&self) -> u64 {
        self.process_thread_count.load(Ordering::Relaxed)
    }

    /// Total system memory in bytes.
    pub fn system_total_memory_bytes(&self) -> u64 {
        self.system_total_memory_bytes.load(Ordering::Relaxed)
    }

    /// Available system memory in bytes.
    pub fn system_available_memory_bytes(&self) -> u64 {
        self.system_available_memory_bytes.load(Ordering::Relaxed)
    }

    /// System-wide CPU usage × 100 (e.g. 250.0 = 2.5 cores → 25000).
    pub fn system_cpu_usage_x100(&self) -> u64 {
        self.system_cpu_usage_x100.load(Ordering::Relaxed)
    }

    /// Render system metrics in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        self.render_prometheus_inner().unwrap_or_default()
    }

    fn render_prometheus_inner(&self) -> Result<String, std::fmt::Error> {
        let mut out = String::with_capacity(1024);

        let rss = self.process_memory_bytes();
        writeln!(
            out,
            "# HELP krishiv_process_memory_bytes Resident set size (RSS) of the current process"
        )?;
        writeln!(out, "# TYPE krishiv_process_memory_bytes gauge")?;
        writeln!(out, "krishiv_process_memory_bytes {rss}")?;

        let cpu = self.process_cpu_usage_x100();
        writeln!(
            out,
            "# HELP krishiv_process_cpu_usage CPU usage of the current process (percentage x 100)"
        )?;
        writeln!(out, "# TYPE krishiv_process_cpu_usage gauge")?;
        writeln!(out, "krishiv_process_cpu_usage {cpu}")?;

        let vmem = self.process_virtual_memory_bytes();
        writeln!(
            out,
            "# HELP krishiv_process_virtual_memory_bytes Virtual memory of the current process"
        )?;
        writeln!(out, "# TYPE krishiv_process_virtual_memory_bytes gauge")?;
        writeln!(out, "krishiv_process_virtual_memory_bytes {vmem}")?;

        let threads = self.process_thread_count();
        writeln!(
            out,
            "# HELP krishiv_process_threads Number of threads in the current process"
        )?;
        writeln!(out, "# TYPE krishiv_process_threads gauge")?;
        writeln!(out, "krishiv_process_threads {threads}")?;

        let sys_total = self.system_total_memory_bytes();
        writeln!(
            out,
            "# HELP krishiv_system_memory_bytes_total Total system memory"
        )?;
        writeln!(out, "# TYPE krishiv_system_memory_bytes_total gauge")?;
        writeln!(out, "krishiv_system_memory_bytes_total {sys_total}")?;

        let sys_avail = self.system_available_memory_bytes();
        writeln!(
            out,
            "# HELP krishiv_system_memory_bytes_available Available system memory"
        )?;
        writeln!(out, "# TYPE krishiv_system_memory_bytes_available gauge")?;
        writeln!(out, "krishiv_system_memory_bytes_available {sys_avail}")?;

        let sys_cpu = self.system_cpu_usage_x100();
        writeln!(
            out,
            "# HELP krishiv_system_cpu_usage System-wide CPU usage (percentage x 100)"
        )?;
        writeln!(out, "# TYPE krishiv_system_cpu_usage gauge")?;
        writeln!(out, "krishiv_system_cpu_usage {sys_cpu}")?;

        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn system_metrics_refresh_does_not_panic() {
        let m = system_metrics();
        m.refresh();
        // After refresh, total system memory should be non-zero.
        assert!(m.system_total_memory_bytes() > 0);
    }

    #[test]
    fn system_metrics_render_prometheus() {
        let m = system_metrics();
        m.refresh();
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_process_memory_bytes"));
        assert!(body.contains("krishiv_process_cpu_usage"));
        assert!(body.contains("krishiv_process_virtual_memory_bytes"));
        assert!(body.contains("krishiv_process_threads"));
        assert!(body.contains("krishiv_system_memory_bytes_total"));
        assert!(body.contains("krishiv_system_memory_bytes_available"));
        assert!(body.contains("krishiv_system_cpu_usage"));
    }

    #[test]
    fn system_metrics_thread_count_positive() {
        let m = system_metrics();
        m.refresh();
        assert!(m.process_thread_count() > 0);
    }
}
