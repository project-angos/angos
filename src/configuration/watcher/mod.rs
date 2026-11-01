mod classify;

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf, absolute};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use classify::{ChangeKind, classify_event, merge_change_kind};
use notify::{
    Error as NotifyError, Event, RecommendedWatcher, RecursiveMode, Watcher, recommended_watcher,
};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::configuration::listeners::ServerTlsConfig;
use crate::configuration::{Configuration, Error, ServerConfig};

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
    config_paths: &HashSet<PathBuf>,
    config_dirs: &HashSet<PathBuf>,
    tls_dirs: &HashSet<PathBuf>,
) -> Option<ChangeKind> {
    let mut accumulated = initial;
    loop {
        match timeout(DEBOUNCE, rx.recv()).await {
            Ok(Some(event)) => {
                let kind = classify_event(&event, config_paths, config_dirs, tls_dirs);
                accumulated = merge_change_kind(accumulated, kind);
            }
            Ok(None) => return None,
            Err(_elapsed) => return Some(accumulated),
        }
    }
}

#[async_trait]
pub trait ConfigNotifier: Send + Sync {
    /// Apply `config`, reporting whether it took effect. A refused
    /// configuration must not be cached: what the watcher holds is what a
    /// later certificate rotation rebuilds TLS from.
    async fn notify_config_change(&self, config: &Configuration) -> bool;
    fn notify_tls_config_change(&self, tls: &ServerTlsConfig);
}

pub struct ConfigWatcher {
    _handle: JoinHandle<()>,
}

impl ConfigWatcher {
    pub fn new<P: AsRef<Path>>(
        config_paths: &[P],
        notifier: Arc<dyn ConfigNotifier>,
    ) -> Result<Self, Error> {
        let paths: Vec<PathBuf> = config_paths
            .iter()
            .map(|path| path.as_ref().to_path_buf())
            .collect();
        info!("Setting up config watcher for: {paths:?}");

        if paths.is_empty() {
            let msg = "No configuration file to watch".to_string();
            return Err(Error::NotReadable(msg));
        }
        for path in &paths {
            if !path.exists() {
                let msg = format!("Config file does not exist: {}", path.display());
                return Err(Error::NotReadable(msg));
            }
        }

        let handle = spawn(async move {
            if let Err(e) = watch_config_loop(paths, notifier).await {
                error!("Config watcher failed: {e}");
            }
        });

        Ok(Self { _handle: handle })
    }
}

fn handle_notify_result(res: Result<Event, NotifyError>, tx: &mpsc::Sender<Event>) {
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
    config_paths: &[PathBuf],
) -> Option<&'a Configuration> {
    if cached.is_none() {
        match Configuration::load_all(config_paths) {
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
    config: &WatchedConfig,
    notifier: &dyn ConfigNotifier,
) {
    info!("TLS certificate change detected, reloading");
    let Some(cfg) = ensure_config_cached(cached_config, &config.paths) else {
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

fn load_initial_config(config_paths: &[PathBuf]) -> Option<Configuration> {
    match Configuration::load_all(config_paths) {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            warn!("Failed to load configuration, watching for changes: {e}");
            None
        }
    }
}

fn build_watcher(
    config_dirs: &HashSet<PathBuf>,
    tls_dirs: &HashSet<PathBuf>,
    tx: mpsc::Sender<Event>,
) -> Result<RecommendedWatcher, Error> {
    let mut watcher = recommended_watcher(move |res| handle_notify_result(res, &tx))?;
    for dir in config_dirs {
        watcher.watch(dir, RecursiveMode::NonRecursive)?;
    }
    for dir in tls_dirs {
        if !config_dirs.contains(dir)
            && let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive)
        {
            warn!("Failed to watch TLS directory {:?}: {e}", dir);
        }
    }
    Ok(watcher)
}

/// The watched TLS directories: `raw` is the operator-supplied path handed to
/// `notify`, `matchable` holds every spelling an event for it can arrive as.
/// Rebuilt each loop iteration from the freshly loaded configuration.
struct WatchedDirs {
    raw: HashSet<PathBuf>,
    matchable: HashSet<PathBuf>,
}

impl WatchedDirs {
    fn new(raw: HashSet<PathBuf>) -> Self {
        let matchable: HashSet<PathBuf> = raw.iter().flat_map(|dir| match_forms(dir)).collect();
        Self { raw, matchable }
    }
}

/// The configuration files to merge and the directories holding them. Fixed at
/// startup from the command line, so unlike the TLS directories this set never
/// has to be recomputed.
struct WatchedConfig {
    paths: Vec<PathBuf>,
    matchable_paths: HashSet<PathBuf>,
    dirs: WatchedDirs,
}

impl WatchedConfig {
    fn new(paths: Vec<PathBuf>) -> Self {
        let matchable_paths: HashSet<PathBuf> =
            paths.iter().flat_map(|path| match_forms(path)).collect();
        let dirs: HashSet<PathBuf> = paths.iter().map(|path| parent_dir(path)).collect();
        Self {
            paths,
            matchable_paths,
            dirs: WatchedDirs::new(dirs),
        }
    }

    /// Relative TLS paths resolve against the first file's directory, the one
    /// an operator names as the configuration proper.
    fn primary_dir(&self) -> PathBuf {
        self.paths
            .first()
            .map_or_else(|| PathBuf::from("."), |path| parent_dir(path))
    }
}

fn parent_dir(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Every spelling of `path` an event may arrive as: inotify absolutizes without
/// resolving symlinks while fsevents resolves them, so a path reached through a
/// symlink matches only one of these depending on the platform.
fn match_forms(path: &Path) -> impl Iterator<Item = PathBuf> {
    [
        Some(path.to_path_buf()),
        absolute(path).ok(),
        fs::canonicalize(path).ok(),
    ]
    .into_iter()
    .flatten()
}

struct WatchState<'a> {
    rx: &'a mut mpsc::Receiver<Event>,
    config: &'a WatchedConfig,
    tls_dirs: WatchedDirs,
    cached_config: &'a mut Option<Configuration>,
    notifier: &'a dyn ConfigNotifier,
    _watcher: RecommendedWatcher,
}

/// Why [`run_event_loop`] handed control back to [`watch_config_loop`].
enum LoopOutcome {
    /// The watched TLS directory set changed; rebuild the watcher over the new
    /// set and resume watching.
    RebuildWatcher,
    /// The event channel closed; stop watching.
    ChannelClosed,
}

async fn run_event_loop(state: &mut WatchState<'_>) -> LoopOutcome {
    loop {
        let Some(event) = state.rx.recv().await else {
            error!("Config watcher channel closed");
            return LoopOutcome::ChannelClosed;
        };

        let initial_kind = classify_event(
            &event,
            &state.config.matchable_paths,
            &state.config.dirs.matchable,
            &state.tls_dirs.matchable,
        );
        if matches!(initial_kind, ChangeKind::Irrelevant) {
            continue;
        }

        let Some(kind) = coalesce_events(
            state.rx,
            initial_kind,
            &state.config.matchable_paths,
            &state.config.dirs.matchable,
            &state.tls_dirs.matchable,
        )
        .await
        else {
            error!("Config watcher channel closed");
            return LoopOutcome::ChannelClosed;
        };

        match kind {
            ChangeKind::Irrelevant => {}
            ChangeKind::Config => {
                let tls_dirs_changed = reload_config(
                    state.config,
                    state.cached_config,
                    &state.tls_dirs.raw,
                    state.notifier,
                )
                .await;
                if tls_dirs_changed {
                    return LoopOutcome::RebuildWatcher;
                }
            }
            ChangeKind::Tls => {
                reload_tls(state.cached_config, state.config, state.notifier);
            }
        }
    }
}

/// Handles a `ChangeKind::Config` event: loads the new configuration and
/// notifies the subscriber, reporting whether the watched TLS directory set
/// changed.
async fn reload_config(
    config: &WatchedConfig,
    cached_config: &mut Option<Configuration>,
    tls_dirs: &HashSet<PathBuf>,
    notifier: &dyn ConfigNotifier,
) -> bool {
    info!("Configuration change detected, reloading");
    match Configuration::load_all(&config.paths) {
        Ok(cfg) => {
            if !notifier.notify_config_change(&cfg).await {
                // Refused: the previous configuration is still the one in
                // effect, so the cache and the watched TLS directories must
                // keep describing that one.
                warn!("Configuration was not applied; keeping the previous one");
                return false;
            }
            info!("Configuration reloaded");
            let new_tls_dirs = compute_tls_dirs(&cfg, &config.primary_dir());
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
    config_paths: Vec<PathBuf>,
    notifier: Arc<dyn ConfigNotifier>,
) -> Result<(), Error> {
    let (tx, mut rx) = mpsc::channel::<Event>(100);
    let config = WatchedConfig::new(config_paths);
    let primary_dir = config.primary_dir();
    let mut cached_config = load_initial_config(&config.paths);
    loop {
        let tls_dirs = cached_config
            .as_ref()
            .map(|cfg| compute_tls_dirs(cfg, &primary_dir))
            .unwrap_or_default();
        let tls_dirs = WatchedDirs::new(tls_dirs);
        let watcher = build_watcher(&config.dirs.raw, &tls_dirs.raw, tx.clone())?;
        let mut state = WatchState {
            rx: &mut rx,
            config: &config,
            tls_dirs,
            cached_config: &mut cached_config,
            notifier: notifier.as_ref(),
            _watcher: watcher,
        };
        match run_event_loop(&mut state).await {
            LoopOutcome::RebuildWatcher => {}
            LoopOutcome::ChannelClosed => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
