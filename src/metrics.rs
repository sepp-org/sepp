use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tonic::Status;
use tracing::Span;

use crate::config::MetricsConfig;

pub struct MetricsGuard {
    provider: Option<SdkMeterProvider>,
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("opentelemetry meter shutdown failed: {e}");
        }
    }
}

pub fn init(
    cfg: &MetricsConfig,
    service_name: &str,
) -> Result<MetricsGuard, Box<dyn std::error::Error>> {
    if !cfg.enabled {
        return Ok(MetricsGuard { provider: None });
    }
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.otlp_endpoint.clone())
        .build()?;
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_millis(cfg.export_interval_ms))
        .build();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.to_string())
                .build(),
        )
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());
    Ok(MetricsGuard {
        provider: Some(provider),
    })
}

#[derive(Clone)]
pub struct Metrics {
    enabled: bool,
    requests: Counter<u64>,
    request_duration_ms: Histogram<f64>,
    jobs_enqueued: Counter<u64>,
    jobs_reserved: Counter<u64>,
    jobs_acked: Counter<u64>,
    jobs_nacked: Counter<u64>,
    jobs_dead_lettered: Counter<u64>,
    commit_duration_ms: Histogram<f64>,
    ready_depth: Arc<AtomicU64>,
    scheduled_depth: Arc<AtomicU64>,
    inflight_depth: Arc<AtomicU64>,
}

impl Metrics {
    pub fn new(enabled: bool) -> Self {
        let meter = opentelemetry::global::meter("sepp");
        Self {
            enabled,
            requests: meter.u64_counter("sepp.requests").build(),
            request_duration_ms: meter
                .f64_histogram("sepp.request.duration")
                .with_unit("ms")
                .build(),
            jobs_enqueued: meter.u64_counter("sepp.jobs.enqueued").build(),
            jobs_reserved: meter.u64_counter("sepp.jobs.reserved").build(),
            jobs_acked: meter.u64_counter("sepp.jobs.acked").build(),
            jobs_nacked: meter.u64_counter("sepp.jobs.nacked").build(),
            jobs_dead_lettered: meter.u64_counter("sepp.jobs.dead_lettered").build(),
            commit_duration_ms: meter
                .f64_histogram("sepp.commit.duration")
                .with_unit("ms")
                .build(),
            ready_depth: Arc::new(AtomicU64::new(0)),
            scheduled_depth: Arc::new(AtomicU64::new(0)),
            inflight_depth: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn observe<T>(
        &self,
        method: &'static str,
        started: Instant,
        span: &Span,
        result: &Result<T, Status>,
    ) {
        if let Err(status) = result {
            span.record("error", tracing::field::display(status));
            let _enter = span.enter();
            if status.code() == tonic::Code::Internal {
                tracing::error!(method, error = %status, "request failed");
            } else {
                tracing::debug!(method, error = %status, "request rejected");
            }
        }
        if !self.enabled {
            return;
        }
        let outcome = if result.is_ok() { "ok" } else { "error" };
        self.requests.add(
            1,
            &[
                KeyValue::new("method", method),
                KeyValue::new("outcome", outcome),
            ],
        );
        self.request_duration_ms.record(
            started.elapsed().as_secs_f64() * 1000.0,
            &[KeyValue::new("method", method)],
        );
    }

    pub fn record_enqueued(&self, jobs: u64) {
        if self.enabled {
            self.jobs_enqueued.add(jobs, &[]);
        }
    }

    pub fn record_reserved(&self, jobs: u64) {
        if self.enabled {
            self.jobs_reserved.add(jobs, &[]);
        }
    }

    pub fn record_acked(&self) {
        if self.enabled {
            self.jobs_acked.add(1, &[]);
        }
    }

    pub fn record_nacked(&self, dead_lettered: bool) {
        if self.enabled {
            self.jobs_nacked.add(1, &[]);
            if dead_lettered {
                self.jobs_dead_lettered.add(1, &[]);
            }
        }
    }

    pub fn record_commit(&self, elapsed: Duration) {
        if self.enabled {
            self.commit_duration_ms
                .record(elapsed.as_secs_f64() * 1000.0, &[]);
        }
    }

    pub fn set_queue_depths(&self, ready: u64, scheduled: u64, inflight: u64) {
        if self.enabled {
            self.ready_depth.store(ready, Ordering::Relaxed);
            self.scheduled_depth.store(scheduled, Ordering::Relaxed);
            self.inflight_depth.store(inflight, Ordering::Relaxed);
        }
    }

    pub fn register_queue_depth_gauges(&self) -> Vec<ObservableGauge<u64>> {
        let meter = opentelemetry::global::meter("sepp");
        let gauge = |name: &'static str, cell: Arc<AtomicU64>| {
            meter
                .u64_observable_gauge(name)
                .with_callback(move |observer| observer.observe(cell.load(Ordering::Relaxed), &[]))
                .build()
        };
        vec![
            gauge("sepp.queue.ready", self.ready_depth.clone()),
            gauge("sepp.queue.scheduled", self.scheduled_depth.clone()),
            gauge("sepp.queue.inflight", self.inflight_depth.clone()),
        ]
    }
}

pub fn register_command_queue_gauge<F>(depth: F) -> ObservableGauge<u64>
where
    F: Fn() -> u64 + Send + Sync + 'static,
{
    opentelemetry::global::meter("sepp")
        .u64_observable_gauge("sepp.command_queue.depth")
        .with_callback(move |observer| observer.observe(depth(), &[]))
        .build()
}
