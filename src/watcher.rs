use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use notify::{Event, EventKind, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::{
    command::server::listeners::tls::ServerTlsConfig,
    configuration::{Configuration, Error, ServerConfig},
};

pub trait ConfigNotifier: Send + Sync {
    fn notify_config_change(&self, config: &Configuration);
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

fn get_tls_dirs(config: &Configuration, config_dir: &Path) -> HashSet<PathBuf> {
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
        std::fs::canonicalize(&config_path).unwrap_or_else(|_| config_path.clone());
    let canonical_config_dir =
        std::fs::canonicalize(&config_dir).unwrap_or_else(|_| config_dir.clone());

    loop {
        let tls_dirs = match Configuration::load(&config_path) {
            Ok(config) => get_tls_dirs(&config, &config_dir),
            Err(e) => {
                warn!("Failed to load configuration, watching for changes: {e}");
                HashSet::new()
            }
        };
        let canonical_tls_dirs: HashSet<PathBuf> = tls_dirs
            .iter()
            .map(|d| std::fs::canonicalize(d).unwrap_or_else(|_| d.clone()))
            .collect();

        let tx_clone = tx.clone();
        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx_clone.blocking_send(event);
            }
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

            if !matches!(
                event.kind,
                EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
            ) {
                continue;
            }

            let is_data_symlink = |p: &PathBuf| {
                p.file_name().is_some_and(|n| n == "..data")
                    && p.parent() == Some(canonical_config_dir.as_path())
            };
            let affects_config = event
                .paths
                .iter()
                .any(|p| p == &canonical_config_path || is_data_symlink(p));
            let affects_tls = !affects_config
                && event
                    .paths
                    .iter()
                    .any(|p| p.parent().is_some_and(|d| canonical_tls_dirs.contains(d)));

            if !affects_config && !affects_tls {
                continue;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            if affects_config {
                info!("Configuration change detected, reloading");
                match Configuration::load(&config_path) {
                    Ok(ref cfg) => {
                        notifier.notify_config_change(cfg);
                        info!("Configuration reloaded");
                    }
                    Err(e) => warn!("Failed to reload configuration: {e}"),
                }
            } else {
                info!("TLS certificate change detected, reloading");
                match Configuration::load(&config_path) {
                    Ok(Configuration {
                        server: ServerConfig::Tls(tls_config),
                        ..
                    }) => {
                        notifier.notify_tls_config_change(&tls_config.tls);
                        info!("TLS configuration reloaded");
                    }
                    Ok(_) => {}
                    Err(e) => warn!("Failed to load configuration for TLS reload: {e}"),
                }
            }

            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, sync::Mutex, time::Duration};

    use tempfile::TempDir;

    use super::*;

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

    impl ConfigNotifier for TestNotifier {
        fn notify_config_change(&self, config: &Configuration) {
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
}
