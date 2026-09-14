//! The `angos scanner` subcommand: a stateless scanner service. A scan job's
//! handler posts an image's coordinates to `/scan`; the service pulls the
//! image with its own registry identity, runs the named scanner, and answers
//! with the SARIF report. It reads the `[scanner]` section alone.

mod config;
mod tool;

pub use config::ScannerConfig;
pub use tool::Scanner;

use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use argh::FromArgs;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    signal,
    sync::{RwLock, Semaphore},
    time::sleep,
};
use tracing::{debug, error, info, warn};

use angos_secret::Secret;

use crate::{configuration::ObservabilityConfig, scan::ScanImagePayload};

/// A scan request names one image; cap the read well above that so a hostile
/// body cannot exhaust memory.
const MAX_REQUEST_BYTES: usize = 64 * 1024;

/// How often the service pulls the database of a scanner that owns one.
const DATABASE_REFRESH: Duration = Duration::from_hours(24);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no [scanner] configuration section is present")]
    MissingConfig,
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("scanner '{scanner}' failed: {message}")]
    Scanner {
        scanner: &'static str,
        message: String,
    },
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "scanner",
    description = "Run the scanner service that answers scan jobs with SARIF reports"
)]
pub struct Options {
    #[argh(positional)]
    /// scanner to run on each requested image: `grype` or `trivy`
    pub scanner: Scanner,
}

/// What the service reads out of a configuration: its own section and the
/// observability settings it shares with the registry.
#[derive(Deserialize)]
pub struct Document {
    pub scanner: Option<ScannerConfig>,
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,
}

struct Service {
    scanner: Scanner,
    token: Option<Secret<String>>,
    registry_authority: String,
    plain_http: bool,
    credentials: Option<(String, String)>,
    scans: Arc<Semaphore>,
    /// Held shared by a scan and exclusively while the database is replaced.
    database: RwLock<()>,
}

pub async fn run(options: &Options, config: Option<ScannerConfig>) -> Result<(), Error> {
    let config = config.ok_or(Error::MissingConfig)?;
    let url = config.registry.url.trim_end_matches('/');
    let authority = url
        .split_once("://")
        .map_or(url, |(_, rest)| rest)
        .to_string();
    let credentials = match (&config.registry.username, &config.registry.password) {
        (Some(username), Some(password)) => Some((username.clone(), password.expose().clone())),
        _ => None,
    };
    let permits = if options.scanner.serial() {
        if config.max_concurrent_scans.get() > 1 {
            warn!(
                "{} runs one scan at a time; max_concurrent_scans = {} is ignored",
                options.scanner.as_str(),
                config.max_concurrent_scans
            );
        }
        1
    } else {
        config.max_concurrent_scans.get()
    };
    let service = Arc::new(Service {
        scanner: options.scanner,
        token: config.token.clone(),
        registry_authority: authority,
        plain_http: config.registry.url.starts_with("http://"),
        credentials,
        scans: Arc::new(Semaphore::new(permits)),
        database: RwLock::new(()),
    });
    if service.scanner.owns_database() {
        update_database(&service).await?;
        tokio::spawn(refresh_database(service.clone()));
    }
    let address = format!("{}:{}", config.bind_address, config.port);
    let address: SocketAddr = address
        .parse()
        .map_err(|e| Error::Invalid(format!("invalid bind address {address}: {e}")))?;
    serve(address, service).await
}

async fn serve(address: SocketAddr, service: Arc<Service>) -> Result<(), Error> {
    let listener = TcpListener::bind(address).await?;
    info!(
        "scanner service for {} listening on {address}",
        service.scanner.as_str()
    );
    loop {
        let accepted = tokio::select! {
            () = shutdown_signal() => {
                info!("Shutting down scanner service");
                return Ok(());
            }
            accepted = listener.accept() => accepted,
        };
        let (stream, _peer) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                warn!("Failed to accept a scanner connection: {e}");
                continue;
            }
        };
        let service = service.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let handler = service_fn(move |request| handle(service.clone(), request));
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, handler)
                .await
            {
                debug!("scanner connection error: {e}");
            }
        });
    }
}

/// Pulls the scanner's database, keeping scans out while it is replaced.
async fn update_database(service: &Service) -> Result<(), Error> {
    let _exclusive = service.database.write().await;
    service.scanner.update_database().await?;
    info!("Updated the {} database", service.scanner.as_str());
    Ok(())
}

/// Refreshes the database once a day; a failed refresh keeps the previous
/// database and is retried the next day.
async fn refresh_database(service: Arc<Service>) {
    loop {
        sleep(DATABASE_REFRESH).await;
        if let Err(e) = update_database(&service).await {
            error!(
                "Failed to refresh the {} database: {e}",
                service.scanner.as_str()
            );
        }
    }
}

async fn shutdown_signal() {
    if let Err(e) = signal::ctrl_c().await {
        error!("Failed to listen for shutdown signal: {e}");
    }
}

async fn handle(
    service: Arc<Service>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.method() != Method::POST || request.uri().path() != "/scan" {
        return Ok(text_response(StatusCode::NOT_FOUND, ""));
    }
    if let Some(token) = &service.token
        && !bearer_valid(token.expose(), request.headers().get("authorization"))
    {
        return Ok(text_response(StatusCode::UNAUTHORIZED, ""));
    }
    let body = match Limited::new(request.into_body(), MAX_REQUEST_BYTES)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Ok(text_response(StatusCode::BAD_REQUEST, "")),
    };
    let Ok(payload) = serde_json::from_slice::<ScanImagePayload>(&body) else {
        return Ok(text_response(
            StatusCode::BAD_REQUEST,
            "expected {namespace, digest}",
        ));
    };

    let image = format!(
        "{}/{}@{}",
        service.registry_authority, payload.namespace, payload.digest
    );
    let _database = service.database.read().await;
    let Ok(_permit) = service.scans.acquire().await else {
        return Ok(text_response(StatusCode::SERVICE_UNAVAILABLE, ""));
    };
    match service
        .scanner
        .scan(&image, service.plain_http, service.credentials.as_ref())
        .await
    {
        Ok(report) => {
            info!("Scanned {image}");
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", crate::scan::SARIF_MEDIA_TYPE)
                .body(Full::new(Bytes::from(report)))
                .unwrap_or_else(|_| text_response(StatusCode::INTERNAL_SERVER_ERROR, "")))
        }
        Err(e) => {
            error!("Scan of {image} failed: {e}");
            Ok(text_response(StatusCode::BAD_GATEWAY, &e.to_string()))
        }
    }
}

/// `Authorization: Bearer <token>`, compared in constant time.
fn bearer_valid(token: &str, header: Option<&hyper::header::HeaderValue>) -> bool {
    let Some(presented) = header
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    let (a, b) = (presented.as_bytes(), token.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn text_response(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(text.to_owned())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

#[cfg(test)]
mod tests {
    use hyper::header::HeaderValue;

    use super::bearer_valid;

    #[test]
    fn bearer_check_accepts_the_token_alone() {
        let good = HeaderValue::from_static("Bearer s3cret");
        assert!(bearer_valid("s3cret", Some(&good)));
        assert!(!bearer_valid("other", Some(&good)));
        assert!(!bearer_valid("s3cret", None));
        assert!(!bearer_valid(
            "s3cret",
            Some(&HeaderValue::from_static("Basic s3cret"))
        ));
    }
}
