#![forbid(unsafe_code)]
#![warn(clippy::pedantic)]

use std::{fmt::Display, future::Future, process::exit, sync::Arc, time::Duration};

use argh::FromArgs;
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    command::{argon, bootstrap, maintenance, prune, reconcile, scanner, scrub, server, worker},
    configuration::{Configuration, ObservabilityConfig, watcher::ConfigWatcher},
    metrics_provider::initialize_metrics,
};

mod auth;
mod cache;
mod cache_fill;
mod command;
mod configuration;
mod event_webhook;
pub mod http_client;
mod http_response;
mod identity;
mod jobs;
mod layer;
mod metrics_provider;
mod policy;
mod registry;
pub mod registry_client;
mod replication;
mod scan;
mod secret;

#[cfg(test)]
pub mod test_fixtures;

fn set_tracing(
    config: Option<ObservabilityConfig>,
) -> Result<Option<SdkTracerProvider>, configuration::Error> {
    let tracing_config = config.and_then(|config| config.tracing);
    let provider = if let Some(tracing_config) = tracing_config {
        let resource = Resource::builder()
            .with_service_name(env!("CARGO_PKG_NAME"))
            .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
            .build();

        let Ok(otlp_exporter) = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&tracing_config.endpoint)
            .with_timeout(Duration::from_secs(10))
            .build()
        else {
            let msg = "Failed to create OTLP exporter".to_string();
            return Err(configuration::Error::Initialization(msg));
        };

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(otlp_exporter)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource)
            .with_sampler(Sampler::TraceIdRatioBased(
                tracing_config.sampling_rate.into(),
            ))
            .build();

        // Clone before registering globally so the caller retains a handle to shut
        // down the batch exporter and flush in-flight spans before the process exits.
        global::set_tracer_provider(tracer_provider.clone());
        Some(tracer_provider)
    } else {
        None
    };

    // An absent layer is a no-op in the stack.
    let telemetry = provider
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("angos")));
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json())
        .with(telemetry)
        .try_init();

    Ok(provider)
}

const DEFAULT_CONFIG_PATH: &str = "config.toml";

#[derive(FromArgs, PartialEq, Debug)]
/// An OCI-compliant and docker-compatible registry service
struct GlobalArguments {
    #[argh(option, short = 'c')]
    /// path to a configuration file, repeatable to merge several with later
    /// files winning, defaults to `config.toml`
    config: Vec<String>,

    #[argh(subcommand)]
    subcommand: SubCommand,
}

/// The configuration files to load, in merge order.
fn config_paths(arguments: &GlobalArguments) -> Vec<String> {
    if arguments.config.is_empty() {
        return vec![DEFAULT_CONFIG_PATH.to_string()];
    }
    arguments.config.clone()
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum SubCommand {
    Argon(argon::Options),
    Prune(prune::Options),
    Reconcile(reconcile::Options),
    Scrub(scrub::Options),
    Serve(server::Options),
    Scanner(scanner::Options),
    Worker(worker::Options),
}

fn main() {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("Failed to install rustls crypto provider");
        exit(1);
    }

    let cli_args: GlobalArguments = argh::from_env();
    let config_paths = config_paths(&cli_args);
    initialize_metrics();
    let exit_code = match cli_args.subcommand {
        SubCommand::Scanner(options) => scanner_main(&options, &config_paths),
        subcommand => registry_main(subcommand, &config_paths),
    };
    if exit_code != 0 {
        exit(exit_code);
    }
}

/// Every subcommand but the scanner service runs against a registry's whole
/// configuration, on a runtime sized by it.
fn registry_main(subcommand: SubCommand, config_paths: &[String]) -> i32 {
    let config = match Configuration::load_all(config_paths) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!(
                "Failed to load configuration from {}: {e}",
                config_paths.join(", ")
            );
            return 1;
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.global.max_concurrent_requests.get())
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("Failed to create Tokio runtime: {e}");
            return 1;
        }
    };
    let observability = config.observability.clone();
    runtime.block_on(traced(
        observability,
        run_command(subcommand, config, config_paths),
    ))
}

/// The scanner service runs on a scanner host, so it reads its own section
/// and the observability settings alone, not a registry's configuration.
fn scanner_main(options: &scanner::Options, config_paths: &[String]) -> i32 {
    let document: scanner::Document = match Configuration::load_section(config_paths) {
        Ok(document) => document,
        Err(e) => {
            eprintln!(
                "Failed to load configuration from {}: {e}",
                config_paths.join(", ")
            );
            return 1;
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("Failed to create Tokio runtime: {e}");
            return 1;
        }
    };
    runtime.block_on(traced(document.observability, async {
        report("Scanner", scanner::run(options, document.scanner).await)
    }))
}

/// Runs `command` under the configured tracing and flushes its spans on the
/// way out.
async fn traced(
    observability: Option<ObservabilityConfig>,
    command: impl Future<Output = i32>,
) -> i32 {
    let tracer_provider = match set_tracing(observability) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("Failed to set up tracing: {err}");
            return 1;
        }
    };
    let exit_code = command.await;
    if let Some(provider) = tracer_provider
        && let Err(err) = provider.shutdown()
    {
        eprintln!("Failed to flush tracer provider: {err}");
    }
    exit_code
}

async fn run_command(
    subcommand: SubCommand,
    config: Configuration,
    config_paths: &[String],
) -> i32 {
    match subcommand {
        SubCommand::Argon(_) => report("Argon", argon::run()),
        SubCommand::Prune(prune_options) => {
            report("Prune", prune::run(&prune_options, &config).await)
        }
        SubCommand::Reconcile(reconcile_options) => report(
            "Reconcile",
            reconcile::run(&reconcile_options, &config).await,
        ),
        SubCommand::Scrub(scrub_options) => report("Scrub", run_scrub(scrub_options, config).await),
        SubCommand::Serve(_) => report("Server", run_server(config_paths, config).await),
        SubCommand::Scanner(_) => 0,
        SubCommand::Worker(worker_options) => report(
            "Worker",
            run_worker(config_paths, worker_options, config).await,
        ),
    }
}

/// Log a failed subcommand under `label` and map the result to an exit code.
fn report(label: &str, result: Result<(), impl Display>) -> i32 {
    match result {
        Ok(()) => 0,
        Err(err) => {
            error!("{label} error: {err}");
            1
        }
    }
}

async fn run_scrub(
    options: scrub::Options,
    config: Configuration,
) -> Result<(), maintenance::Error> {
    let mut scrub = scrub::Command::new(&options, &config).await?;
    scrub.run().await
}

async fn run_worker(
    config_paths: &[String],
    worker_options: worker::Options,
    config: Configuration,
) -> Result<(), bootstrap::Error> {
    let worker = Arc::new(worker::Command::new(&worker_options, &config).await?);

    let Ok(_watcher) = ConfigWatcher::new(config_paths, worker.clone()) else {
        error!("Failed to start configuration watcher");
        exit(1);
    };

    tokio::select! {
        () = worker.run() => Ok(()),
        () = shutdown_signal() => {
            info!("Shutdown signal received, draining in-flight jobs");
            worker
                .shutdown_with_timeout(Duration::from_secs(config.global.shutdown_drain_secs))
                .await;
            info!("Graceful shutdown complete");
            Ok(())
        }
    }
}

async fn run_server(config_paths: &[String], config: Configuration) -> Result<(), server::Error> {
    let server = Arc::new(server::Command::new(&config).await?);

    let Ok(_watcher) = ConfigWatcher::new(config_paths, server.clone()) else {
        error!("Failed to start configuration watcher");
        exit(1);
    };

    tokio::select! {
        result = server.run() => result,
        () = shutdown_signal() => {
            info!("Shutdown signal received, draining in-flight requests");
            server
                .shutdown_with_timeout(Duration::from_secs(config.global.shutdown_drain_secs))
                .await;
            info!("Graceful shutdown complete");
            Ok(())
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                _ = ctrl_c => {}
                _ = sigterm.recv() => {}
            }
        }
        Err(e) => {
            warn!("Failed to register SIGTERM handler, falling back to ctrl-c only: {e}");
            let _ = ctrl_c.await;
        }
    }

    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{Span as _, Tracer as _, TracerProvider as _};
    use opentelemetry_sdk::trace::{
        InMemorySpanExporterBuilder, SdkTracerProvider, SimpleSpanProcessor,
    };

    /// Verifies that spans emitted before `provider.shutdown()` are flushed to
    /// the exporter.  With `SimpleSpanProcessor` each span is exported
    /// synchronously when it ends, so `force_flush` followed by `shutdown`
    /// must leave the span visible in the exporter's buffer.
    #[test]
    fn tracer_provider_shutdown_flushes_spans() {
        let exporter = InMemorySpanExporterBuilder::new().build();
        let provider = SdkTracerProvider::builder()
            .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
            .build();

        let tracer = provider.tracer("angos-test");
        tracer.start("pre-shutdown-span").end();

        provider.force_flush().expect("force_flush must succeed");

        let spans = exporter
            .get_finished_spans()
            .expect("must be able to read finished spans");
        assert_eq!(spans.len(), 1, "one span must be captured before shutdown");
        assert_eq!(spans[0].name, "pre-shutdown-span");

        provider.shutdown().expect("shutdown must succeed");
    }
}
