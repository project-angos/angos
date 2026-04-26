use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use tokio::{sync::mpsc, time::timeout};
use tracing::{debug, error, info, warn};

use super::{Configuration, Error, ServerConfig};
use crate::configuration::listeners::tls::ServerTlsConfig;

const DEBOUNCE: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq)]
enum ChangeKind {
    Config,
    Tls,
    Irrelevant,
}

fn classify_event(
    event: &Event,
    canonical_config_path: &Path,
    canonical_config_dir: &Path,
    canonical_tls_dirs: &HashSet<PathBuf>,
) -> ChangeKind {
    if !matches!(
        event.kind,
        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
    ) {
        return ChangeKind::Irrelevant;
    }

    let is_data_symlink = |p: &PathBuf| {
        p.file_name().is_some_and(|n| n == "..data") && p.parent() == Some(canonical_config_dir)
    };
    let affects_config = event
        .paths
        .iter()
        .any(|p| p == canonical_config_path || is_data_symlink(p));
    if affects_config {
        return ChangeKind::Config;
    }

    let affects_tls = event
        .paths
        .iter()
        .any(|p| p.parent().is_some_and(|d| canonical_tls_dirs.contains(d)));
    if affects_tls {
        return ChangeKind::Tls;
    }

    ChangeKind::Irrelevant
}

/// Combines two `ChangeKind` values, preferring the stronger action.
/// `Config` dominates `Tls`; a config edit covers TLS-path changes too.
fn merge_change_kind(a: ChangeKind, b: ChangeKind) -> ChangeKind {
    match (a, b) {
        (ChangeKind::Config, _) | (_, ChangeKind::Config) => ChangeKind::Config,
        (ChangeKind::Tls, _) | (_, ChangeKind::Tls) => ChangeKind::Tls,
        _ => ChangeKind::Irrelevant,
    }
}

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
    .filter_map(|p| {
        let full = if p.is_absolute() {
            p.clone()
        } else {
            config_dir.join(p)
        };
        full.parent().map(Path::to_path_buf)
    })
    .collect()
}

/// Resolves the active `Configuration`, loading from disk when the cache is
/// empty.  Returns `None` and logs a warning if the disk load fails.
fn resolve_config<'a>(
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
fn handle_tls_event(
    cached_config: &mut Option<Configuration>,
    config_path: &Path,
    notifier: &dyn ConfigNotifier,
) {
    info!("TLS certificate change detected, reloading");
    let Some(cfg) = resolve_config(cached_config, config_path) else {
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

    let mut cached_config: Option<Configuration> = match Configuration::load(&config_path) {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            warn!("Failed to load configuration, watching for changes: {e}");
            None
        }
    };

    loop {
        let tls_dirs = cached_config
            .as_ref()
            .map(|cfg| compute_tls_dirs(cfg, &config_dir))
            .unwrap_or_default();
        let canonical_tls_dirs: HashSet<PathBuf> = tls_dirs
            .iter()
            .map(|d| fs::canonicalize(d).unwrap_or_else(|_| d.clone()))
            .collect();

        let tx_clone = tx.clone();
        let mut watcher =
            notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
                Ok(event) => {
                    let _ = tx_clone.blocking_send(event);
                }
                Err(e) => warn!("File system watcher error: {e}"),
            })?;

        watcher.watch(&config_dir, RecursiveMode::NonRecursive)?;
        for dir in &tls_dirs {
            if *dir != config_dir
                && let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive)
            {
                warn!("Failed to watch TLS directory {:?}: {e}", dir);
            }
        }

        loop {
            let Some(event) = rx.recv().await else {
                error!("Config watcher channel closed");
                return Ok(());
            };

            let initial_kind = classify_event(
                &event,
                &canonical_config_path,
                &canonical_config_dir,
                &canonical_tls_dirs,
            );
            if matches!(initial_kind, ChangeKind::Irrelevant) {
                continue;
            }

            let Some(kind) = coalesce_events(
                &mut rx,
                initial_kind,
                &canonical_config_path,
                &canonical_config_dir,
                &canonical_tls_dirs,
            )
            .await
            else {
                error!("Config watcher channel closed");
                return Ok(());
            };

            match kind {
                ChangeKind::Irrelevant => {}
                ChangeKind::Config => {
                    info!("Configuration change detected, reloading");
                    match Configuration::load(&config_path) {
                        Ok(cfg) => {
                            notifier.notify_config_change(&cfg).await;
                            info!("Configuration reloaded");
                            let new_tls_dirs = compute_tls_dirs(&cfg, &config_dir);
                            cached_config = Some(cfg);
                            if new_tls_dirs != tls_dirs {
                                break;
                            }
                        }
                        Err(e) => warn!("Failed to reload configuration: {e}"),
                    }
                }
                ChangeKind::Tls => {
                    handle_tls_event(&mut cached_config, &config_path, notifier.as_ref());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, sync::Mutex, time::Duration};

    use notify::event::{AccessKind, ModifyKind};
    use tempfile::TempDir;

    use super::*;

    fn make_event(kind: EventKind, paths: Vec<PathBuf>) -> Event {
        Event {
            kind,
            paths,
            attrs: notify::event::EventAttributes::default(),
        }
    }

    #[test]
    fn classify_access_event_is_irrelevant() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dirs = HashSet::new();

        let event = make_event(
            EventKind::Access(AccessKind::Read),
            vec![config_path.clone()],
        );

        assert_eq!(
            classify_event(&event, &config_path, &config_dir, &tls_dirs),
            ChangeKind::Irrelevant,
        );
    }

    #[test]
    fn classify_modify_on_config_path_is_config() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dirs = HashSet::new();

        let event = make_event(
            EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Content)),
            vec![config_path.clone()],
        );

        assert_eq!(
            classify_event(&event, &config_path, &config_dir, &tls_dirs),
            ChangeKind::Config,
        );
    }

    #[test]
    fn classify_modify_on_data_symlink_in_config_dir_is_config() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dirs = HashSet::new();

        let data_symlink = config_dir.join("..data");
        let event = make_event(
            EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Content)),
            vec![data_symlink],
        );

        assert_eq!(
            classify_event(&event, &config_path, &config_dir, &tls_dirs),
            ChangeKind::Config,
        );
    }

    #[test]
    fn classify_modify_in_tls_dir_is_tls() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dir = PathBuf::from("/etc/app/tls");
        let tls_dirs: HashSet<PathBuf> = [tls_dir.clone()].into_iter().collect();

        let cert = tls_dir.join("server.pem");
        let event = make_event(
            EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Content)),
            vec![cert],
        );

        assert_eq!(
            classify_event(&event, &config_path, &config_dir, &tls_dirs),
            ChangeKind::Tls,
        );
    }

    #[test]
    fn classify_modify_on_unrelated_path_is_irrelevant() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dirs: HashSet<PathBuf> = [PathBuf::from("/etc/app/tls")].into_iter().collect();

        let unrelated = PathBuf::from("/var/log/app.log");
        let event = make_event(
            EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Content)),
            vec![unrelated],
        );

        assert_eq!(
            classify_event(&event, &config_path, &config_dir, &tls_dirs),
            ChangeKind::Irrelevant,
        );
    }

    const MINIMAL_CONFIG: &str = r#"
[server]
bind_address = "0.0.0.0"
"#;

    const MINIMAL_TLS_CONFIG_TEMPLATE: &str = r#"
[server]
bind_address = "0.0.0.0"

[server.tls]
server_certificate_bundle = "{cert_path}"
server_private_key = "{key_path}"
"#;

    fn minimal_tls_config(cert_path: &str, key_path: &str) -> String {
        MINIMAL_TLS_CONFIG_TEMPLATE
            .replace("{cert_path}", cert_path)
            .replace("{key_path}", key_path)
    }

    struct TestNotifier {
        config_changes: Mutex<Vec<Configuration>>,
        tls_changes: Mutex<Vec<ServerTlsConfig>>,
    }

    impl TestNotifier {
        fn new() -> Self {
            Self {
                config_changes: Mutex::new(Vec::new()),
                tls_changes: Mutex::new(Vec::new()),
            }
        }

        fn config_change_count(&self) -> usize {
            self.config_changes.lock().unwrap().len()
        }

        fn tls_change_count(&self) -> usize {
            self.tls_changes.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl ConfigNotifier for TestNotifier {
        async fn notify_config_change(&self, config: &Configuration) {
            self.config_changes.lock().unwrap().push(config.clone());
        }

        fn notify_tls_config_change(&self, tls: &ServerTlsConfig) {
            self.tls_changes.lock().unwrap().push(tls.clone());
        }
    }

    #[cfg(unix)]
    fn create_k8s_mount(dir: &Path, files: &[(&str, &str)]) {
        use std::os::unix::fs::symlink;

        let timestamp_dir = dir.join("..2024_01_01_00_00_00.000000000");
        fs::create_dir_all(&timestamp_dir).unwrap();

        for (name, content) in files {
            let file_path = timestamp_dir.join(name);
            fs::write(&file_path, content).unwrap();
        }

        let data_link = dir.join("..data");
        symlink("..2024_01_01_00_00_00.000000000", &data_link).unwrap();

        for (name, _) in files {
            let file_link = dir.join(name);
            let target = format!("..data/{name}");
            symlink(&target, &file_link).unwrap();
        }
    }

    #[cfg(unix)]
    fn rotate_k8s_mount(dir: &Path, files: &[(&str, &str)], new_timestamp: &str) {
        use std::os::unix::fs::symlink;

        let new_dir = dir.join(format!("..{new_timestamp}"));
        fs::create_dir_all(&new_dir).unwrap();

        for (name, content) in files {
            let file_path = new_dir.join(name);
            fs::write(&file_path, content).unwrap();
        }

        let data_link = dir.join("..data");
        let tmp_link = dir.join("..data_tmp");
        symlink(format!("..{new_timestamp}"), &tmp_link).unwrap();
        fs::rename(&tmp_link, &data_link).unwrap();
    }

    async fn wait_for_condition<F>(mut condition: F, timeout: Duration) -> bool
    where
        F: FnMut() -> bool,
    {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    #[tokio::test]
    async fn test_regular_config_change() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, MINIMAL_CONFIG).unwrap();

        let notifier = Arc::new(TestNotifier::new());
        let _watcher = ConfigWatcher::new(
            config_path.to_str().unwrap(),
            Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
        )
        .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let new_config = r#"
[server]
bind_address = "127.0.0.1"
"#;
        let mut file = fs::File::create(&config_path).unwrap();
        file.write_all(new_config.as_bytes()).unwrap();
        file.sync_all().unwrap();

        let detected = wait_for_condition(
            || notifier.config_change_count() >= 1,
            Duration::from_secs(5),
        )
        .await;
        assert!(detected, "Config change was not detected");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_kubernetes_config_mount() {
        let temp_dir = TempDir::new().unwrap();
        create_k8s_mount(temp_dir.path(), &[("config.toml", MINIMAL_CONFIG)]);

        let config_path = temp_dir.path().join("config.toml");
        let notifier = Arc::new(TestNotifier::new());
        let _watcher = ConfigWatcher::new(
            config_path.to_str().unwrap(),
            Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let new_config = r#"
[server]
bind_address = "192.168.1.1"
"#;
        rotate_k8s_mount(
            temp_dir.path(),
            &[("config.toml", new_config)],
            "2024_01_02_00_00_00.000000000",
        );

        let detected = wait_for_condition(
            || notifier.config_change_count() >= 1,
            Duration::from_secs(5),
        )
        .await;
        assert!(detected, "K8s config mount change was not detected");
    }

    #[tokio::test]
    async fn test_tls_certificate_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let tls_dir = temp_dir.path().join("tls");
        fs::create_dir_all(&tls_dir).unwrap();

        let cert_path = tls_dir.join("server.pem");
        let key_path = tls_dir.join("server.key");
        fs::write(&cert_path, "initial-cert").unwrap();
        fs::write(&key_path, "initial-key").unwrap();

        let config_path = temp_dir.path().join("config.toml");
        let config_content =
            minimal_tls_config(cert_path.to_str().unwrap(), key_path.to_str().unwrap());
        fs::write(&config_path, &config_content).unwrap();

        let notifier = Arc::new(TestNotifier::new());
        let _watcher = ConfigWatcher::new(
            config_path.to_str().unwrap(),
            Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut file = fs::File::create(&cert_path).unwrap();
        file.write_all(b"rotated-cert").unwrap();
        file.sync_all().unwrap();

        let detected =
            wait_for_condition(|| notifier.tls_change_count() >= 1, Duration::from_secs(5)).await;
        assert!(detected, "TLS certificate change was not detected");
        assert_eq!(
            notifier.config_change_count(),
            0,
            "Config change should not be triggered for TLS-only change"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_kubernetes_tls_mount() {
        let temp_dir = TempDir::new().unwrap();
        let tls_dir = temp_dir.path().join("tls");
        fs::create_dir_all(&tls_dir).unwrap();
        create_k8s_mount(
            &tls_dir,
            &[
                ("server.pem", "initial-cert"),
                ("server.key", "initial-key"),
            ],
        );

        let cert_path = tls_dir.join("server.pem");
        let key_path = tls_dir.join("server.key");

        let config_path = temp_dir.path().join("config.toml");
        let config_content =
            minimal_tls_config(cert_path.to_str().unwrap(), key_path.to_str().unwrap());
        fs::write(&config_path, &config_content).unwrap();

        let notifier = Arc::new(TestNotifier::new());
        let _watcher = ConfigWatcher::new(
            config_path.to_str().unwrap(),
            Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        rotate_k8s_mount(
            &tls_dir,
            &[
                ("server.pem", "rotated-cert"),
                ("server.key", "rotated-key"),
            ],
            "2024_01_02_00_00_00.000000000",
        );

        let detected =
            wait_for_condition(|| notifier.tls_change_count() >= 1, Duration::from_secs(5)).await;
        assert!(detected, "K8s TLS mount change was not detected");
    }

    #[tokio::test]
    async fn test_invalid_config_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, MINIMAL_CONFIG).unwrap();

        let notifier = Arc::new(TestNotifier::new());
        let _watcher = ConfigWatcher::new(
            config_path.to_str().unwrap(),
            Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut file = fs::File::create(&config_path).unwrap();
        file.write_all(b"invalid toml [[[").unwrap();
        file.sync_all().unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let valid_config = r#"
[server]
bind_address = "10.0.0.1"
"#;
        let mut file = fs::File::create(&config_path).unwrap();
        file.write_all(valid_config.as_bytes()).unwrap();
        file.sync_all().unwrap();

        let detected = wait_for_condition(
            || notifier.config_change_count() >= 1,
            Duration::from_secs(5),
        )
        .await;
        assert!(
            detected,
            "Watcher should recover and detect valid config after invalid config"
        );
    }

    #[test]
    fn merge_change_kind_config_dominates_tls() {
        assert_eq!(
            merge_change_kind(ChangeKind::Config, ChangeKind::Tls),
            ChangeKind::Config,
        );
        assert_eq!(
            merge_change_kind(ChangeKind::Tls, ChangeKind::Config),
            ChangeKind::Config,
        );
    }

    #[test]
    fn merge_change_kind_tls_dominates_irrelevant() {
        assert_eq!(
            merge_change_kind(ChangeKind::Tls, ChangeKind::Irrelevant),
            ChangeKind::Tls,
        );
        assert_eq!(
            merge_change_kind(ChangeKind::Irrelevant, ChangeKind::Tls),
            ChangeKind::Tls,
        );
    }

    #[test]
    fn merge_change_kind_both_irrelevant_stays_irrelevant() {
        assert_eq!(
            merge_change_kind(ChangeKind::Irrelevant, ChangeKind::Irrelevant),
            ChangeKind::Irrelevant,
        );
    }

    /// Verifies that a burst of N events coalesces into a single `ChangeKind`
    /// result rather than producing N separate results. The coalescer must
    /// drain all events and then return after the quiet-period timeout, not
    /// once per event received.
    #[tokio::test]
    async fn coalesce_events_collapses_burst_into_single_result() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dirs: HashSet<PathBuf> = HashSet::new();

        let (tx, mut rx) = mpsc::channel(100);

        // Send a burst of 10 events that classify as Config — all sent before
        // the coalescer has a chance to time out, simulating e.g. an editor
        // write → truncate → write sequence.
        for _ in 0..10 {
            let event = make_event(
                EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Content,
                )),
                vec![config_path.clone()],
            );
            tx.send(event).await.unwrap();
        }
        // Keep `tx` alive so the coalescer terminates via the quiet-period
        // timeout (Err(_elapsed)) path, not via channel close (Ok(None)).

        let result = coalesce_events(
            &mut rx,
            ChangeKind::Config,
            &config_path,
            &config_dir,
            &tls_dirs,
        )
        .await;

        // All 10 events must have been folded into exactly one Config result.
        assert_eq!(result, Some(ChangeKind::Config));
    }

    /// Verifies that a mixed burst (Tls events followed by a Config event)
    /// coalesces to Config, since Config dominates Tls.
    #[tokio::test]
    async fn coalesce_events_config_dominates_tls_in_mixed_burst() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dir = PathBuf::from("/etc/app/tls");
        let tls_dirs: HashSet<PathBuf> = [tls_dir.clone()].into_iter().collect();

        let (tx, mut rx) = mpsc::channel(100);

        // Send TLS events first, then a Config event.
        for _ in 0..5 {
            let tls_event = make_event(
                EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Content,
                )),
                vec![tls_dir.join("server.pem")],
            );
            tx.send(tls_event).await.unwrap();
        }
        let config_event = make_event(
            EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
            vec![config_path.clone()],
        );
        tx.send(config_event).await.unwrap();
        // Keep `tx` alive so the coalescer exits via quiet-period timeout.

        let result = coalesce_events(
            &mut rx,
            ChangeKind::Tls,
            &config_path,
            &config_dir,
            &tls_dirs,
        )
        .await;

        assert_eq!(result, Some(ChangeKind::Config));
    }

    /// Verifies that channel closure during debounce returns None.
    #[tokio::test]
    async fn coalesce_events_returns_none_on_channel_close() {
        let config_path = PathBuf::from("/etc/app/config.toml");
        let config_dir = PathBuf::from("/etc/app");
        let tls_dirs: HashSet<PathBuf> = HashSet::new();

        let (tx, mut rx) = mpsc::channel(1);
        // Close immediately without sending anything.
        drop(tx);

        let result = coalesce_events(
            &mut rx,
            ChangeKind::Config,
            &config_path,
            &config_dir,
            &tls_dirs,
        )
        .await;

        assert_eq!(result, None);
    }

    // --- Direct unit tests for the cache-empty path ---
    //
    // These exercise the bug fix at the unit level: the integration path
    // through `watch_config_loop` does not normally exhibit the empty-cache
    // race in tests (a config-repair write fires `ChangeKind::Config` first,
    // populating the cache before any TLS event), so it cannot reliably cover
    // the `cached.is_none()` branch in `resolve_config` / `handle_tls_event`.
    // Calling those helpers directly does.

    #[test]
    fn resolve_config_loads_from_disk_when_cache_is_empty() {
        let temp_dir = TempDir::new().unwrap();
        let tls_dir = temp_dir.path().join("tls");
        fs::create_dir_all(&tls_dir).unwrap();
        let cert_path = tls_dir.join("server.pem");
        let key_path = tls_dir.join("server.key");
        fs::write(&cert_path, "cert").unwrap();
        fs::write(&key_path, "key").unwrap();

        let config_path = temp_dir.path().join("config.toml");
        fs::write(
            &config_path,
            minimal_tls_config(cert_path.to_str().unwrap(), key_path.to_str().unwrap()),
        )
        .unwrap();

        let mut cached: Option<Configuration> = None;
        let resolved = resolve_config(&mut cached, &config_path);

        assert!(resolved.is_some(), "must load from disk when cache empty");
        assert!(
            cached.is_some(),
            "must populate cache after successful load"
        );
    }

    #[test]
    fn resolve_config_returns_none_when_cache_empty_and_disk_invalid() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, b"invalid toml [[[").unwrap();

        let mut cached: Option<Configuration> = None;
        let resolved = resolve_config(&mut cached, &config_path);

        assert!(resolved.is_none());
        assert!(cached.is_none(), "must not populate cache on load failure");
    }

    #[test]
    fn resolve_config_returns_cached_without_reading_disk() {
        let temp_dir = TempDir::new().unwrap();
        // Cache is preloaded with a non-TLS config.
        let cached_cfg: Configuration =
            Configuration::load_from_str("[server]\nbind_address = \"127.0.0.1\"").unwrap();
        let mut cached: Option<Configuration> = Some(cached_cfg);

        // Point at a non-existent path; must not be touched.
        let bogus_path = temp_dir.path().join("does-not-exist.toml");
        let resolved = resolve_config(&mut cached, &bogus_path);

        assert!(resolved.is_some(), "must reuse cached value");
    }

    #[tokio::test]
    async fn handle_tls_event_loads_from_disk_when_cache_empty_and_notifies() {
        let temp_dir = TempDir::new().unwrap();
        let tls_dir = temp_dir.path().join("tls");
        fs::create_dir_all(&tls_dir).unwrap();
        let cert_path = tls_dir.join("server.pem");
        let key_path = tls_dir.join("server.key");
        fs::write(&cert_path, "cert").unwrap();
        fs::write(&key_path, "key").unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(
            &config_path,
            minimal_tls_config(cert_path.to_str().unwrap(), key_path.to_str().unwrap()),
        )
        .unwrap();

        let notifier = TestNotifier::new();
        let mut cached: Option<Configuration> = None;
        handle_tls_event(&mut cached, &config_path, &notifier);

        assert_eq!(
            notifier.tls_change_count(),
            1,
            "must notify once when cache is empty but disk has a TLS config"
        );
        assert!(
            cached.is_some(),
            "cache must be populated after fallback load"
        );
    }

    #[tokio::test]
    async fn handle_tls_event_does_not_notify_when_cache_empty_and_disk_invalid() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, b"invalid toml [[[").unwrap();

        let notifier = TestNotifier::new();
        let mut cached: Option<Configuration> = None;
        handle_tls_event(&mut cached, &config_path, &notifier);

        assert_eq!(notifier.tls_change_count(), 0);
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn handle_tls_event_does_not_notify_when_server_is_not_tls() {
        let temp_dir = TempDir::new().unwrap();
        // Insecure server config — no TLS section.
        let cached_cfg: Configuration =
            Configuration::load_from_str("[server]\nbind_address = \"127.0.0.1\"").unwrap();

        let notifier = TestNotifier::new();
        let mut cached: Option<Configuration> = Some(cached_cfg);
        let bogus_path = temp_dir.path().join("does-not-exist.toml");
        handle_tls_event(&mut cached, &bogus_path, &notifier);

        assert_eq!(notifier.tls_change_count(), 0);
    }
}
