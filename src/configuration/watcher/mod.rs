use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf, absolute};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher, recommended_watcher};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::configuration::listeners::ServerTlsConfig;
use crate::configuration::{Configuration, Error, ServerConfig};

/// Quiet period that coalesces an event burst into one reload: editors emit
/// several events per save (atomic-write rename, swap files).
const DEBOUNCE: Duration = Duration::from_millis(100);

/// Ordered by strength, so a burst coalesces to its maximum: a config reload
/// covers TLS changes too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ChangeKind {
    Irrelevant,
    Tls,
    Config,
}

/// Each set holds every spelling of a watched path, so one reached through a
/// symlink matches whichever form the platform's `notify` backend reports.
fn classify_event(
    event: &Event,
    config_paths: &HashSet<PathBuf>,
    config_dirs: &HashSet<PathBuf>,
    tls_dirs: &HashSet<PathBuf>,
) -> ChangeKind {
    if !matches!(
        event.kind,
        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
    ) {
        return ChangeKind::Irrelevant;
    }

    // A Kubernetes ConfigMap/Secret mount updates by swapping its `..data`
    // symlink to a new versioned directory, never by writing the files.
    let is_k8s_data_symlink = |p: &PathBuf| {
        p.file_name().is_some_and(|n| n == "..data")
            && p.parent().is_some_and(|d| config_dirs.contains(d))
    };
    if event
        .paths
        .iter()
        .any(|p| config_paths.contains(p) || is_k8s_data_symlink(p))
    {
        ChangeKind::Config
    } else if event
        .paths
        .iter()
        .any(|p| p.parent().is_some_and(|d| tls_dirs.contains(d)))
    {
        ChangeKind::Tls
    } else {
        ChangeKind::Irrelevant
    }
}

/// After the first relevant event, drains the channel until no event arrives
/// for `DEBOUNCE`. Returns `None` if the channel closes during the window.
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
                accumulated = accumulated.max(kind);
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

/// Relative paths resolve against `config_dir`; `join` keeps absolute ones as-is.
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
    .filter_map(|p| config_dir.join(p).parent().map(Path::to_path_buf))
    .collect()
}

/// Loads the configuration from disk when the cache is empty, then notifies
/// the subscriber if the server is configured for TLS.
fn reload_tls(
    cached_config: &mut Option<Configuration>,
    config: &WatchedConfig,
    notifier: &dyn ConfigNotifier,
) {
    info!("TLS certificate change detected, reloading");
    if cached_config.is_none() {
        match Configuration::load_all(&config.paths) {
            Ok(cfg) => *cached_config = Some(cfg),
            Err(e) => {
                warn!(
                    "TLS file change detected but configuration could not be \
                     loaded from disk; TLS reload skipped: {e}"
                );
                return;
            }
        }
    }
    if let Some(Configuration {
        server: ServerConfig::Tls(tls_config),
        ..
    }) = cached_config
    {
        notifier.notify_tls_config_change(&tls_config.tls);
        info!("TLS configuration reloaded");
    } else {
        debug!("TLS file change detected but server is not configured for TLS; ignoring");
    }
}

fn build_watcher(
    config_dirs: &HashSet<PathBuf>,
    tls_dirs: &HashSet<PathBuf>,
    tx: mpsc::Sender<Event>,
) -> Result<RecommendedWatcher, Error> {
    let mut watcher = recommended_watcher(move |res| match res {
        Ok(event) => {
            let _ = tx.blocking_send(event);
        }
        Err(e) => warn!("File system watcher error: {e}"),
    })?;
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

/// `raw` is the operator-supplied path handed to `notify`, `matchable` holds
/// every spelling an event for it can arrive as.
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

/// The configuration files to merge and their directories, fixed at startup.
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

/// Loads the new configuration and notifies the subscriber, reporting whether
/// the watched TLS directory set changed.
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
                // The previous configuration is still in effect, so the cache
                // and the watched TLS directories keep describing it.
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
    let mut cached_config = Configuration::load_all(&config.paths)
        .inspect_err(|e| warn!("Failed to load configuration, watching for changes: {e}"))
        .ok();
    // Each pass watches the TLS directories of the cached configuration; a
    // reload that changes them starts a new pass.
    loop {
        let tls_dirs = WatchedDirs::new(
            cached_config
                .as_ref()
                .map(|cfg| compute_tls_dirs(cfg, &primary_dir))
                .unwrap_or_default(),
        );
        let _watcher = build_watcher(&config.dirs.raw, &tls_dirs.raw, tx.clone())?;
        loop {
            let Some(event) = rx.recv().await else {
                error!("Config watcher channel closed");
                return Ok(());
            };
            let initial = classify_event(
                &event,
                &config.matchable_paths,
                &config.dirs.matchable,
                &tls_dirs.matchable,
            );
            if initial == ChangeKind::Irrelevant {
                continue;
            }
            let Some(kind) = coalesce_events(
                &mut rx,
                initial,
                &config.matchable_paths,
                &config.dirs.matchable,
                &tls_dirs.matchable,
            )
            .await
            else {
                error!("Config watcher channel closed");
                return Ok(());
            };

            match kind {
                ChangeKind::Irrelevant => {}
                ChangeKind::Config => {
                    let tls_dirs_changed = reload_config(
                        &config,
                        &mut cached_config,
                        &tls_dirs.raw,
                        notifier.as_ref(),
                    )
                    .await;
                    if tls_dirs_changed {
                        break;
                    }
                }
                ChangeKind::Tls => reload_tls(&mut cached_config, &config, notifier.as_ref()),
            }
        }
    }
}

#[cfg(test)]
mod tests;
