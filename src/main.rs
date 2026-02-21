#![forbid(unsafe_code)]
#![warn(clippy::pedantic)]

use std::{sync::Arc, time::Duration};

use argh::FromArgs;
use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    command::{argon, scrub, server},
    configuration::{Configuration, ObservabilityConfig},
    watcher::ConfigWatcher,
};

mod cache;
mod command;
mod configuration;
mod event_webhook;
mod identity;
mod metrics_provider;
mod oci;
mod policy;
mod registry;
mod secret;
mod watcher;

// TODO: to be moved
fn set_tracing(config: Option<ObservabilityConfig>) -> Result<(), configuration::Error> {
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
            .with_timeout(std::time::Duration::from_secs(10))
            .build()
        else {
            let msg = "Failed to create OTLP exporter".to_string();
            return Err(configuration::Error::Initialization(msg));
        };

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(otlp_exporter)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource)
            .with_sampler(Sampler::TraceIdRatioBased(tracing_config.sampling_rate))
            .build();

        let tracer = tracer_provider.tracer("angos");
        global::set_tracer_provider(tracer_provider);
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        let _ = tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().json())
            .with(telemetry)
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer().json())
            .try_init();
    }
    Ok(())
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
    Scrub(scrub::Options),
    Serve(server::Options),
}

fn main() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let cli_args: GlobalArguments = argh::from_env();

    let Ok(config) = Configuration::load(&cli_args.config) else {
        eprintln!("Failed to load configuration from {}", &cli_args.config);
        std::process::exit(1);
    };

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.global.max_concurrent_requests)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(run_command(cli_args, config));
}

async fn run_command(cli_args: GlobalArguments, config: Configuration) {
    if let Err(err) = set_tracing(config.observability.clone()) {
        eprintln!("Failed to set up tracing: {err}");
        std::process::exit(1);
    }

    match cli_args.subcommand {
        SubCommand::Argon(_) => {
            if let Err(err) = argon::Command::run() {
                error!("Argon error: {}", err);
                std::process::exit(1);
            }
        }
        SubCommand::Scrub(scrub_options) => {
            if let Err(err) = run_scrub(scrub_options, config).await {
                error!("Scrub error: {err}");
                std::process::exit(1);
            }
        }
        SubCommand::Serve(_) => {
            if let Err(err) = run_server(cli_args, config).await {
                error!("Server error: {err}");
                std::process::exit(1);
            }
        }
    }
}

async fn run_scrub(options: scrub::Options, config: Configuration) -> Result<(), scrub::Error> {
    let scrub = scrub::Command::new(&options, &config)?;
    scrub.run().await
}

async fn run_server(options: GlobalArguments, config: Configuration) -> Result<(), server::Error> {
    let server = Arc::new(server::Command::new(&config)?);

    let Ok(_watcher) = ConfigWatcher::new(&options.config, server.clone()) else {
        error!("Failed to start configuration watcher");
        std::process::exit(1);
    };

    let shutdown_server = Arc::clone(&server);
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("Shutdown signal received, draining in-flight webhook deliveries");
        shutdown_server
            .shutdown_with_timeout(Duration::from_secs(30))
            .await;
        info!("Graceful shutdown complete");
        std::process::exit(0);
    });

    server.run().await
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("failed to listen for Ctrl+C");
    }
}
