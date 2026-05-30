use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tonic::metadata::MetadataMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::{LogFormat, LoggingConfig, TracingConfig};
use crate::pb::sepp::v1::TraceContext;

static TRACING_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn enabled() -> bool {
    TRACING_ENABLED.load(Ordering::Relaxed)
}

pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::debug!("opentelemetry tracer shutdown failed: {e}");
        }
    }
}

pub fn init(
    logging: &LoggingConfig,
    tracing_cfg: &TracingConfig,
) -> Result<TelemetryGuard, Box<dyn std::error::Error>> {
    let filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(&logging.level))?;

    if !tracing_cfg.enabled {
        match logging.format {
            LogFormat::Text => fmt().with_env_filter(filter).init(),
            LogFormat::Json => fmt().with_env_filter(filter).json().init(),
        }

        return Ok(TelemetryGuard { provider: None });
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(tracing_cfg.otlp_endpoint.clone())
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(tracing_cfg.service_name.clone())
                .build(),
        )
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            tracing_cfg.sample_ratio,
        ))))
        .build();

    // The W3C propagator is what `extract_*`/`current_trace_context` rely on.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let otel_filter = Targets::new()
        .with_target("sepp", LevelFilter::TRACE)
        .with_default(LevelFilter::OFF);
    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("sepp"))
        .with_filter(otel_filter);

    let registry = tracing_subscriber::registry().with(otel_layer);
    match logging.format {
        LogFormat::Text => registry.with(fmt::layer().with_filter(filter)).init(),
        LogFormat::Json => registry
            .with(fmt::layer().json().with_filter(filter))
            .init(),
    }

    TRACING_ENABLED.store(true, Ordering::Relaxed);
    tracing::info!(
        endpoint = %tracing_cfg.otlp_endpoint,
        service_name = %tracing_cfg.service_name,
        sample_ratio = tracing_cfg.sample_ratio,
        "OTLP tracing enabled",
    );

    Ok(TelemetryGuard {
        provider: Some(provider),
    })
}

pub fn set_parent_from_metadata(span: &Span, metadata: &MetadataMap) {
    if !enabled() {
        return;
    }

    let parent = TraceContextPropagator::new().extract(&MetadataExtractor(metadata));
    if parent.span().span_context().is_valid() {
        let _ = span.set_parent(parent);
    }
}

pub fn link_from_proto(span: &Span, trace_context: Option<&TraceContext>) {
    if !enabled() {
        return;
    }
    let Some(tc) = trace_context else {
        return;
    };

    let mut carrier = HashMap::new();
    carrier.insert("traceparent".to_string(), tc.traceparent.clone());
    if let Some(ts) = &tc.tracestate {
        carrier.insert("tracestate".to_string(), ts.clone());
    }

    let cx = TraceContextPropagator::new().extract(&MapExtractor(&carrier));
    let span_context = cx.span().span_context().clone();
    if span_context.is_valid() {
        span.add_link(span_context);
    }
}

pub fn current_trace_context() -> Option<TraceContext> {
    if !enabled() {
        return None;
    }

    let cx = Span::current().context();
    if !cx.span().span_context().is_valid() {
        return None;
    }

    let mut carrier = HashMap::new();
    TraceContextPropagator::new().inject_context(&cx, &mut MapInjector(&mut carrier));

    Some(TraceContext {
        traceparent: carrier.remove("traceparent")?,
        tracestate: carrier.remove("tracestate"),
    })
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                tonic::metadata::KeyRef::Ascii(k) => Some(k.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

struct MapInjector<'a>(&'a mut HashMap<String, String>);

impl Injector for MapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

struct MapExtractor<'a>(&'a HashMap<String, String>);

impl Extractor for MapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_is_disabled_by_default() {
        assert!(!enabled());
        assert!(current_trace_context().is_none());
    }
}
