use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
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
            tracing::debug!("opentelemetry meter shutdown failed: {e}");
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

#[derive(Default)]
pub struct QueueDepthSnapshot {
    pub ready: HashMap<String, u64>,
    pub scheduled: HashMap<String, u64>,
    pub inflight: HashMap<String, u64>,
}

#[derive(Default)]
pub struct CycleMetrics {
    pub enqueued_by_queue: HashMap<String, u64>,
    pub reserved_by_queue: HashMap<String, u64>,
    pub acked_by_queue: HashMap<String, u64>,
    pub nacked_by_queue: HashMap<String, u64>,
    pub dead_lettered_by_queue_cause: HashMap<(String, &'static str), u64>,
    pub deduplicated_by_queue: HashMap<String, u64>,
    pub sweep_promotions_by_queue: HashMap<String, u64>,
    pub sweep_lease_redeliveries_by_queue: HashMap<String, u64>,
    pub sweep_dedup_expirations_by_queue: HashMap<String, u64>,
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
    jobs_deduplicated: Counter<u64>,
    jobs_rejected: Counter<u64>,
    reserve_empty: Counter<u64>,
    sweep_promotions: Counter<u64>,
    sweep_lease_redeliveries: Counter<u64>,
    sweep_dedup_expirations: Counter<u64>,
    commit_duration_ms: Histogram<f64>,
    queue_depths: Arc<ArcSwap<QueueDepthSnapshot>>,
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
            jobs_deduplicated: meter.u64_counter("sepp.jobs.deduplicated").build(),
            jobs_rejected: meter.u64_counter("sepp.jobs.rejected").build(),
            reserve_empty: meter.u64_counter("sepp.reserve.empty").build(),
            sweep_promotions: meter.u64_counter("sepp.sweep.promotions").build(),
            sweep_lease_redeliveries: meter.u64_counter("sepp.sweep.lease_redeliveries").build(),
            sweep_dedup_expirations: meter.u64_counter("sepp.sweep.dedup_expirations").build(),
            commit_duration_ms: meter
                .f64_histogram("sepp.commit.duration")
                .with_unit("ms")
                .build(),
            queue_depths: Arc::new(ArcSwap::from_pointee(QueueDepthSnapshot::default())),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
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
            let server_fault = matches!(
                status.code(),
                tonic::Code::Internal | tonic::Code::Unknown | tonic::Code::DataLoss
            );
            let _enter = span.enter();
            if server_fault {
                span.record("otel.status_code", "error");
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

    pub fn flush_cycle(&self, m: &CycleMetrics) {
        if !self.enabled {
            return;
        }
        add_by_queue(&self.jobs_enqueued, &m.enqueued_by_queue);
        add_by_queue(&self.jobs_reserved, &m.reserved_by_queue);
        add_by_queue(&self.jobs_acked, &m.acked_by_queue);
        add_by_queue(&self.jobs_nacked, &m.nacked_by_queue);
        add_by_queue(&self.jobs_deduplicated, &m.deduplicated_by_queue);
        add_by_queue(&self.sweep_promotions, &m.sweep_promotions_by_queue);
        add_by_queue(
            &self.sweep_lease_redeliveries,
            &m.sweep_lease_redeliveries_by_queue,
        );
        add_by_queue(
            &self.sweep_dedup_expirations,
            &m.sweep_dedup_expirations_by_queue,
        );
        for ((queue, cause), n) in &m.dead_lettered_by_queue_cause {
            self.jobs_dead_lettered.add(
                *n,
                &[
                    KeyValue::new("queue", queue.clone()),
                    KeyValue::new("cause", *cause),
                ],
            );
        }
    }

    pub fn record_rejected(&self, queue: &str, reason: &'static str) {
        if self.enabled {
            self.jobs_rejected.add(
                1,
                &[
                    KeyValue::new("queue", queue.to_string()),
                    KeyValue::new("reason", reason),
                ],
            );
        }
    }

    pub fn record_reserve_empty(&self, queues: &[String]) {
        if self.enabled {
            for queue in queues {
                self.reserve_empty
                    .add(1, &[KeyValue::new("queue", queue.clone())]);
            }
        }
    }

    pub fn record_commit(&self, elapsed: Duration) {
        if self.enabled {
            self.commit_duration_ms
                .record(elapsed.as_secs_f64() * 1000.0, &[]);
        }
    }

    pub fn set_queue_depths(&self, snapshot: QueueDepthSnapshot) {
        if self.enabled {
            self.queue_depths.store(Arc::new(snapshot));
        }
    }

    pub fn register_queue_depth_gauges(&self) -> Vec<ObservableGauge<u64>> {
        let meter = opentelemetry::global::meter("sepp");
        let ready = {
            let depths = self.queue_depths.clone();
            meter
                .u64_observable_gauge("sepp.queue.ready")
                .with_callback(move |observer| {
                    for (queue, depth) in &depths.load().ready {
                        observer.observe(*depth, &[KeyValue::new("queue", queue.clone())]);
                    }
                })
                .build()
        };
        let scheduled = {
            let depths = self.queue_depths.clone();
            meter
                .u64_observable_gauge("sepp.queue.scheduled")
                .with_callback(move |observer| {
                    for (queue, depth) in &depths.load().scheduled {
                        observer.observe(*depth, &[KeyValue::new("queue", queue.clone())]);
                    }
                })
                .build()
        };
        let inflight = {
            let depths = self.queue_depths.clone();
            meter
                .u64_observable_gauge("sepp.queue.inflight")
                .with_callback(move |observer| {
                    for (queue, depth) in &depths.load().inflight {
                        observer.observe(*depth, &[KeyValue::new("queue", queue.clone())]);
                    }
                })
                .build()
        };
        vec![ready, scheduled, inflight]
    }
}

fn add_by_queue(counter: &Counter<u64>, counts: &HashMap<String, u64>) {
    for (queue, n) in counts {
        counter.add(*n, &[KeyValue::new("queue", queue.clone())]);
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
