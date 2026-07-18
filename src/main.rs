#![forbid(unsafe_code)]
#![warn(clippy::pedantic)]

use std::{fmt::Display, process::exit, sync::Arc, time::Duration};

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
    command::{argon, bootstrap, maintenance, migrate, prune, replicate, scrub, server, worker},
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
mod identity;
mod jobs;
mod metrics_provider;
mod oci;
mod policy;
mod registry;
pub mod registry_client;
mod replication;
mod secret;
mod timing;

#[cfg(test)]
pub mod test_fixtures;

fn set_tracing(
    config: Option<ObservabilityConfig>,
) -> Result<Option<SdkTracerProvider>, configuration::Error> {
    if let Some(ObservabilityConfig {
        tracing: Some(tracing_config),
    }) = config
    {
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

        let tracer = tracer_provider.tracer("angos");
        // Clone before registering globally so the caller retains a handle to shut
        // down the batch exporter and flush in-flight spans before the process exits.
        global::set_tracer_provider(tracer_provider.clone());
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        let _ = tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().json())
            .with(telemetry)
            .try_init();

        Ok(Some(tracer_provider))
    } else {
        let _ = tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().json())
            .try_init();

        Ok(None)
    }
}

#[derive(FromArgs, PartialEq, Debug)]
/// An OCI-compliant and docker-compatible registry service
struct GlobalArguments {
    #[argh(option, short = 'c', default = "String::from(\"config.toml\")")]
    /// the path to the configuration file, defaults to `config.toml`
    config: String,

    #[argh(subcommand)]
    subcommand: SubCommand,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum SubCommand {
    Argon(argon::Options),
    Migrate(migrate::Options),
    Prune(prune::Options),
    Replicate(replicate::Options),
    Scrub(scrub::Options),
    Serve(server::Options),
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

    let config = match Configuration::load(&cli_args.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to load configuration from {}: {e}", cli_args.config);
            exit(1);
        }
    };

    initialize_metrics();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.global.max_concurrent_requests)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("Failed to create Tokio runtime: {e}");
            exit(1);
        }
    };
    runtime.block_on(run_command(cli_args, config));
}

async fn run_command(cli_args: GlobalArguments, config: Configuration) {
    let tracer_provider = match set_tracing(config.observability.clone()) {
        Ok(p) => p,
        Err(err) => {
            eprintln!("Failed to set up tracing: {err}");
            exit(1);
        }
    };

    let exit_code = match cli_args.subcommand {
        SubCommand::Argon(_) => report("Argon", argon::run()),
        SubCommand::Migrate(migrate_options) => {
            report("Migrate", migrate::run(&migrate_options, &config).await)
        }
        SubCommand::Prune(prune_options) => {
            report("Prune", prune::run(&prune_options, &config).await)
        }
        SubCommand::Replicate(replicate_options) => report(
            "Replicate",
            replicate::run(&replicate_options, &config).await,
        ),
        SubCommand::Scrub(scrub_options) => report("Scrub", run_scrub(scrub_options, config).await),
        SubCommand::Serve(_) => report("Server", run_server(cli_args, config).await),
        SubCommand::Worker(worker_options) => report(
            "Worker",
            run_worker(&cli_args.config, worker_options, config).await,
        ),
    };

    if let Some(provider) = tracer_provider
        && let Err(err) = provider.shutdown()
    {
        eprintln!("Failed to flush tracer provider: {err}");
    }

    if exit_code != 0 {
        exit(exit_code);
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
    config_path: &str,
    worker_options: worker::Options,
    config: Configuration,
) -> Result<(), bootstrap::Error> {
    let worker = Arc::new(worker::Command::new(&worker_options, &config).await?);

    let Ok(_watcher) = ConfigWatcher::new(config_path, worker.clone()) else {
        error!("Failed to start configuration watcher");
        exit(1);
    };

    tokio::select! {
        () = worker.run() => Ok(()),
        () = shutdown_signal() => {
            info!("Shutdown signal received, draining in-flight jobs");
            worker.shutdown_with_timeout(Duration::from_secs(30)).await;
            info!("Graceful shutdown complete");
            Ok(())
        }
    }
}

async fn run_server(options: GlobalArguments, config: Configuration) -> Result<(), server::Error> {
    let server = Arc::new(server::Command::new(&config).await?);

    let Ok(_watcher) = ConfigWatcher::new(&options.config, server.clone()) else {
        error!("Failed to start configuration watcher");
        exit(1);
    };

    tokio::select! {
        result = server.run() => result,
        () = shutdown_signal() => {
            info!("Shutdown signal received, draining in-flight webhook deliveries");
            server.shutdown_with_timeout(Duration::from_secs(30)).await;
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
