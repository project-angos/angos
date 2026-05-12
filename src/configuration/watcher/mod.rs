mod classify;

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use classify::{ChangeKind, classify_event, merge_change_kind};
use notify::{Event, RecursiveMode, Watcher};
use tokio::{sync::mpsc, time::timeout};
use tracing::{debug, error, info, warn};

use super::{Configuration, Error, ServerConfig};
use crate::configuration::listeners::tls::ServerTlsConfig;

/// Window during which filesystem events are coalesced into a single reload.
/// Editors typically emit several `Modify`/`Create`/`Remove` events per save
/// (atomic-write rename, swap-file dance, etc.); waiting this long after the
/// last event before reloading collapses the burst into one config-load pass.
const DEBOUNCE: Duration = Duration::from_millis(100);

/// After the first relevant event, drains the channel until no event arrives
/// for `DEBOUNCE`. Returns the coalesced `ChangeKind` across the burst.
/// Returns `None` if the channel closes during the debounce window.
async fn coalesce_events(
    rx: &mut mpsc::Receiver<Event>,
    initial: ChangeKind,
    canonical_config_path: &Path,
    canonical_config_dir: &Path,
    canonical_tls_dirs: &HashSet<PathBuf>,
) -> Option<ChangeKind> {
    let mut accumulated = initial;
    loop {
        match timeout(DEBOUNCE, rx.recv()).await {
            Ok(Some(event)) => {
                let kind = classify_event(
                    &event,
                    canonical_config_path,
                    canonical_config_dir,
                    canonical_tls_dirs,
                );
                accumulated = merge_change_kind(accumulated, kind);
            }
            Ok(None) => return None,
            Err(_elapsed) => return Some(accumulated),
        }
    }
}

#[async_trait]
pub trait ConfigNotifier: Send + Sync {
    async fn notify_config_change(&self, config: &Configuration);
    fn notify_tls_config_change(&self, tls: &ServerTlsConfig);
}

pub struct ConfigWatcher {
    _handle: tokio::task::JoinHandle<()>,
}

impl ConfigWatcher {
    pub fn new(config_path: &str, notifier: Arc<dyn ConfigNotifier>) -> Result<Self, Error> {
        info!("Setting up config watcher for: {config_path}");

        let config_file_path = PathBuf::from(config_path);
        if !config_file_path.exists() {
            let msg = format!("Config file does not exist: {config_path}");
            return Err(Error::NotReadable(msg));
        }

        let handle = tokio::spawn(async move {
            if let Err(e) = watch_config_loop(config_file_path, notifier).await {
                error!("Config watcher failed: {e}");
            }
        });

        Ok(Self { _handle: handle })
    }
}

fn handle_notify_result(res: Result<Event, notify::Error>, tx: &mpsc::Sender<Event>) {
    match res {
        Ok(event) => {
            let _ = tx.blocking_send(event);
        }
        Err(e) => warn!("File system watcher error: {e}"),
    }
}

fn resolve_tls_dir(config_dir: &Path, path: &Path) -> Option<PathBuf> {
    let full = if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    };
    full.parent().map(Path::to_path_buf)
}

fn compute_tls_dirs(config: &Configuration, config_dir: &Path) -> HashSet<PathBuf> {
    let ServerConfig::Tls(tls_config) = &config.server else {
        return HashSet::new();
    };

    [
        Some(&tls_config.tls.server_certificate_bundle),
        Some(&tls_config.tls.server_private_key),
        tls_config.tls.client_ca_bundle.as_ref(),
    ]
    .into_iter()
    .flatten()
    .filter_map(|p| resolve_tls_dir(config_dir, p))
    .collect()
}

/// Returns the cached `Configuration`, loading it from disk and storing it in
/// `cached` when the cache is empty. Returns `None` and logs a warning if the
/// disk load fails.
fn ensure_config_cached<'a>(
    cached: &'a mut Option<Configuration>,
    config_path: &Path,
) -> Option<&'a Configuration> {
    if cached.is_none() {
        match Configuration::load(config_path) {
            Ok(cfg) => {
                *cached = Some(cfg);
            }
            Err(e) => {
                warn!(
                    "TLS file change detected but configuration could not be \
                     loaded from disk; TLS reload skipped: {e}"
                );
                return None;
            }
        }
    }
    cached.as_ref()
}

/// Handles a `ChangeKind::Tls` event: ensures a usable `Configuration` is
/// available (loading from disk when the cache is empty), then notifies the
/// subscriber if the server is configured for TLS.
fn reload_tls(
    cached_config: &mut Option<Configuration>,
    config_path: &Path,
    notifier: &dyn ConfigNotifier,
) {
    info!("TLS certificate change detected, reloading");
    let Some(cfg) = ensure_config_cached(cached_config, config_path) else {
        return;
    };
    match cfg {
        Configuration {
            server: ServerConfig::Tls(tls_config),
            ..
        } => {
            notifier.notify_tls_config_change(&tls_config.tls);
            info!("TLS configuration reloaded");
        }
        _ => {
            debug!("TLS file change detected but server is not configured for TLS; ignoring");
        }
    }
}

fn load_initial_config(config_path: &Path) -> Option<Configuration> {
    match Configuration::load(config_path) {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            warn!("Failed to load configuration, watching for changes: {e}");
            None
        }
    }
}

fn build_watcher(
    config_dir: &Path,
    tls_dirs: &HashSet<PathBuf>,
    tx: mpsc::Sender<Event>,
) -> Result<notify::RecommendedWatcher, Error> {
    let mut watcher = notify::recommended_watcher(move |res| handle_notify_result(res, &tx))?;
    watcher.watch(config_dir, RecursiveMode::NonRecursive)?;
    for dir in tls_dirs {
        if dir != config_dir
            && let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive)
        {
            warn!("Failed to watch TLS directory {:?}: {e}", dir);
        }
    }
    Ok(watcher)
}

struct WatchPaths<'a> {
    config_path: &'a Path,
    config_dir: &'a Path,
    canonical_config_path: &'a Path,
    canonical_config_dir: &'a Path,
    canonical_tls_dirs: &'a HashSet<PathBuf>,
    tls_dirs: &'a HashSet<PathBuf>,
}

async fn run_event_loop(
    rx: &mut mpsc::Receiver<Event>,
    paths: &WatchPaths<'_>,
    cached_config: &mut Option<Configuration>,
    notifier: &dyn ConfigNotifier,
) -> bool {
    loop {
        let Some(event) = rx.recv().await else {
            error!("Config watcher channel closed");
            return false;
        };

        let initial_kind = classify_event(
            &event,
            paths.canonical_config_path,
            paths.canonical_config_dir,
            paths.canonical_tls_dirs,
        );
        if matches!(initial_kind, ChangeKind::Irrelevant) {
            continue;
        }

        let Some(kind) = coalesce_events(
            rx,
            initial_kind,
            paths.canonical_config_path,
            paths.canonical_config_dir,
            paths.canonical_tls_dirs,
        )
        .await
        else {
            error!("Config watcher channel closed");
            return false;
        };

        match kind {
            ChangeKind::Irrelevant => {}
            ChangeKind::Config => {
                if reload_config(
                    paths.config_path,
                    paths.config_dir,
                    cached_config,
                    paths.tls_dirs,
                    notifier,
                )
                .await
                {
                    return true;
                }
            }
            ChangeKind::Tls => {
                reload_tls(cached_config, paths.config_path, notifier);
            }
        }
    }
}

/// Handles a `ChangeKind::Config` event: loads the new configuration, notifies
/// the subscriber, and returns `true` when the set of watched TLS directories
/// has changed — signalling that the outer loop must rebuild the watcher.
async fn reload_config(
    config_path: &Path,
    config_dir: &Path,
    cached_config: &mut Option<Configuration>,
    tls_dirs: &HashSet<PathBuf>,
    notifier: &dyn ConfigNotifier,
) -> bool {
    info!("Configuration change detected, reloading");
    match Configuration::load(config_path) {
        Ok(cfg) => {
            notifier.notify_config_change(&cfg).await;
            info!("Configuration reloaded");
            let new_tls_dirs = compute_tls_dirs(&cfg, config_dir);
            *cached_config = Some(cfg);
            new_tls_dirs != *tls_dirs
        }
        Err(e) => {
            warn!("Failed to reload configuration: {e}");
            false
        }
    }
}

async fn watch_config_loop(
    config_path: PathBuf,
    notifier: Arc<dyn ConfigNotifier>,
) -> Result<(), Error> {
    let (tx, mut rx) = mpsc::channel::<Event>(100);
    let config_dir = match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let canonical_config_path =
        fs::canonicalize(&config_path).unwrap_or_else(|_| config_path.clone());
    let canonical_config_dir = fs::canonicalize(&config_dir).unwrap_or_else(|_| config_dir.clone());
    let mut cached_config = load_initial_config(&config_path);
    loop {
        let tls_dirs = cached_config
            .as_ref()
            .map(|cfg| compute_tls_dirs(cfg, &config_dir))
            .unwrap_or_default();
        let canonical_tls_dirs: HashSet<PathBuf> = tls_dirs
            .iter()
            .map(|d| fs::canonicalize(d).unwrap_or_else(|_| d.clone()))
            .collect();
        let _watcher = build_watcher(&config_dir, &tls_dirs, tx.clone())?;
        let paths = WatchPaths {
            config_path: &config_path,
            config_dir: &config_dir,
            canonical_config_path: &canonical_config_path,
            canonical_config_dir: &canonical_config_dir,
            canonical_tls_dirs: &canonical_tls_dirs,
            tls_dirs: &tls_dirs,
        };
        if !run_event_loop(&mut rx, &paths, &mut cached_config, notifier.as_ref()).await {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests;
