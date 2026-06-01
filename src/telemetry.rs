//! OpenTelemetry distributed-tracing wiring for the Tapo REST gateway.
//!
//! This module installs a `tracing` subscriber so that:
//!   * existing `log::{info,warn,error,...}` records keep flowing to stderr
//!     (via the `tracing-log` bridge built into `tracing_subscriber`), and
//!   * per-request spans emitted by `tower_http::trace::TraceLayer` are
//!     captured and — when an OTLP collector endpoint is configured — exported
//!     to that collector so the gateway's spans can join the end-to-end trace.
//!
//! # Fail-open contract
//!
//! The gateway MUST start and serve exactly as before when telemetry cannot be
//! set up. Therefore:
//!   * If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset/empty, only a plain
//!     `tracing_subscriber` fmt layer is installed and we return early — no
//!     OTLP pipeline is built.
//!   * If building the OTLP pipeline fails for any reason, the error is logged
//!     and we degrade to the plain fmt subscriber.
//!   * No `panic!`/`unwrap`/`expect` is used on the OpenTelemetry path.

use std::env;

use log::{info, warn};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_http::HeaderExtractor;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";
const DEFAULT_SERVICE_NAME: &str = "ouroboros-tapo";

/// Guard returned from [`init_telemetry`]. Holding it keeps the OpenTelemetry
/// tracer provider alive; dropping it (or calling [`TelemetryGuard::shutdown`])
/// flushes and tears down the export pipeline. When telemetry is disabled the
/// guard is inert.
#[must_use = "dropping the guard tears down the OTel export pipeline; bind it for the process lifetime"]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    fn disabled() -> Self {
        Self { provider: None }
    }

    /// Best-effort flush + shutdown of the export pipeline. Safe to call when
    /// telemetry is disabled (no-op). Never panics.
    pub fn shutdown(&self) {
        if let Some(provider) = self.provider.as_ref()
            && let Err(err) = provider.shutdown()
        {
            warn!("OpenTelemetry shutdown failed (ignored): {err}");
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Build the env filter from `RUST_LOG`, falling back to the supplied default
/// directive (which carries the CLI `--verbosity` level for crate logs).
fn env_filter(default_directive: &str) -> EnvFilter {
    EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_directive))
        .unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Install a plain `tracing_subscriber` fmt subscriber (ANSI colored, writing
/// to stderr) bridging `log` records. This is the no-OTel fallback and the
/// default-off experience. Returns an inert [`TelemetryGuard`].
///
/// Idempotent at the process level: a second `try_init` is ignored.
fn init_plain(default_directive: &str) -> TelemetryGuard {
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    // `try_init` returns Err if a global subscriber is already set; ignore it
    // so a double-init can never abort startup.
    let _ = tracing_subscriber::registry()
        .with(env_filter(default_directive))
        .with(fmt_layer)
        .try_init();
    TelemetryGuard::disabled()
}

/// Initialize logging and (optionally) OpenTelemetry tracing exactly once at
/// startup.
///
/// `verbosity` is the CLI `--verbosity` level; it seeds the default log filter
/// when `RUST_LOG` is not set.
///
/// Behavior:
///   * `OTEL_EXPORTER_OTLP_ENDPOINT` unset/empty  → plain fmt subscriber only.
///   * endpoint set                                → fmt + OTel layer exporting
///     over OTLP/gRPC (tonic), batching on the tokio runtime.
///   * any OTel setup error                        → logged, degrade to plain.
///
/// Never panics; never blocks on collector availability (the OTLP batch
/// exporter connects lazily/asynchronously).
pub fn init_telemetry(verbosity: log::LevelFilter) -> TelemetryGuard {
    // Seed the filter with the crate name at the requested verbosity so the
    // existing CLI flag keeps controlling our own log volume, while keeping
    // noisy dependencies (h2/tonic/hyper) quiet by default.
    let level = verbosity.to_string().to_ascii_lowercase();
    let default_directive =
        format!("{level},tapo_rest={level},h2=warn,hyper=warn,tonic=warn,tower=warn");

    let endpoint = match env::var(ENDPOINT_ENV) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            // Endpoint not configured: plain logging only, no OTLP pipeline.
            return init_plain(&default_directive);
        }
    };

    match build_otel_provider(&endpoint) {
        Ok(provider) => {
            let tracer = provider.tracer(env_service_name());
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

            let init_result = tracing_subscriber::registry()
                .with(env_filter(&default_directive))
                .with(fmt_layer)
                .with(otel_layer)
                .try_init();

            match init_result {
                Ok(()) => {
                    info!(
                        "OpenTelemetry tracing enabled (service.name=\"{}\", endpoint=\"{endpoint}\")",
                        env_service_name()
                    );
                    // Set as the global provider so traceparent propagation
                    // helpers can reach it; ignore errors.
                    opentelemetry::global::set_tracer_provider(provider.clone());
                    // Install the W3C trace-context propagator so incoming
                    // `traceparent` headers can be extracted into span parents.
                    opentelemetry::global::set_text_map_propagator(
                        TraceContextPropagator::new(),
                    );
                    TelemetryGuard {
                        provider: Some(provider),
                    }
                }
                Err(err) => {
                    // A subscriber was already installed (or registry init
                    // failed): tear down the provider and fall back.
                    if let Err(shutdown_err) = provider.shutdown() {
                        warn!("OpenTelemetry shutdown during fallback failed: {shutdown_err}");
                    }
                    warn!(
                        "tracing subscriber already initialized ({err}); OpenTelemetry export disabled"
                    );
                    init_plain(&default_directive)
                }
            }
        }
        Err(err) => {
            warn!(
                "Failed to initialize OpenTelemetry OTLP exporter ({err}); falling back to plain logging"
            );
            init_plain(&default_directive)
        }
    }
}

/// Resolve the OTel service name from `OTEL_SERVICE_NAME`, defaulting to
/// [`DEFAULT_SERVICE_NAME`].
fn env_service_name() -> String {
    env::var(SERVICE_NAME_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned())
}

/// Build an OTLP/gRPC (tonic) span exporter and a batching SDK tracer provider
/// tagged with the `service.name` resource. Returns an error instead of
/// panicking on any failure so the caller can fail open.
fn build_otel_provider(endpoint: &str) -> anyhow::Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_owned())
        .build()?;

    let resource = Resource::new(vec![KeyValue::new("service.name", env_service_name())]);

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(resource)
        .build();

    Ok(provider)
}

/// Best-effort W3C `traceparent` extraction for an incoming request.
///
/// Reads the W3C trace-context from `headers` using the globally installed
/// text-map propagator and, if a remote parent context is present, attaches it
/// as the parent of the current `tracing` span (typically the per-request span
/// created by `TraceLayer`). This is a no-op when no propagator is installed
/// (telemetry disabled) or when the request carries no `traceparent`.
///
/// Never panics; failures simply leave the span without a remote parent.
pub fn extract_remote_parent(headers: &http::HeaderMap, span: &tracing::Span) {
    let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(headers))
    });
    span.set_parent(parent_cx);
}
