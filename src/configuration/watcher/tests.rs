use std::fs;
use std::fs::File;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::absolute;
use std::slice::from_ref;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use notify::EventKind;
use notify::event::{AccessKind, DataChange, EventAttributes, ModifyKind};
use tempfile::TempDir;
use tokio::time::sleep;

use super::*;
use crate::configuration::listeners::ServerTlsConfig;
use crate::test_fixtures::configuration::{MINIMAL_CONFIG_TOML, config_toml};

fn make_event(kind: EventKind, paths: Vec<PathBuf>) -> Event {
    Event {
        kind,
        paths,
        attrs: EventAttributes::default(),
    }
}

fn path_set(path: &Path) -> HashSet<PathBuf> {
    [path.to_path_buf()].into_iter().collect()
}

#[test]
fn classify_event_by_kind_and_path() {
    let config_path = PathBuf::from("/etc/app/config.toml");
    let config_dirs = path_set(Path::new("/etc/app"));
    let tls_dirs = path_set(Path::new("/etc/app/tls"));
    let modify = EventKind::Modify(ModifyKind::Data(DataChange::Content));

    for (kind, path, expected) in [
        (
            EventKind::Access(AccessKind::Read),
            "/etc/app/config.toml",
            ChangeKind::Irrelevant,
        ),
        (modify, "/etc/app/config.toml", ChangeKind::Config),
        (modify, "/etc/app/..data", ChangeKind::Config),
        (modify, "/etc/app/tls/server.pem", ChangeKind::Tls),
        (modify, "/etc/app/other.toml", ChangeKind::Irrelevant),
        (modify, "/var/lib/other/..data", ChangeKind::Irrelevant),
        (modify, "/var/log/app.log", ChangeKind::Irrelevant),
    ] {
        let event = make_event(kind, vec![PathBuf::from(path)]);
        assert_eq!(
            classify_event(&event, &path_set(&config_path), &config_dirs, &tls_dirs),
            expected,
            "{path}"
        );
    }
}

fn minimal_tls_config(cert_path: &str, key_path: &str) -> String {
    config_toml(&format!(
        r#"
        [server.tls]
        server_certificate_bundle = "{cert_path}"
        server_private_key = "{key_path}"
    "#
    ))
}

struct TestNotifier {
    config_changes: Mutex<Vec<Configuration>>,
    tls_changes: Mutex<Vec<ServerTlsConfig>>,
    /// When set, every configuration is reported as refused, as a server that
    /// cannot apply one does.
    refuse: AtomicBool,
}

impl TestNotifier {
    fn new() -> Self {
        Self {
            config_changes: Mutex::new(Vec::new()),
            tls_changes: Mutex::new(Vec::new()),
            refuse: AtomicBool::new(false),
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
    async fn notify_config_change(&self, config: &Configuration) -> bool {
        self.config_changes.lock().unwrap().push(config.clone());
        !self.refuse.load(Ordering::Acquire)
    }

    fn notify_tls_config_change(&self, tls: &ServerTlsConfig) {
        self.tls_changes.lock().unwrap().push(tls.clone());
    }
}

#[cfg(unix)]
fn create_k8s_mount(dir: &Path, files: &[(&str, &str)]) {
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
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
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
        from_ref(&config_path),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

    let mut file = File::create(&cert_path).unwrap();
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
        from_ref(&config_path),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

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
    fs::write(&config_path, MINIMAL_CONFIG_TOML).unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        from_ref(&config_path),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

    let mut file = File::create(&config_path).unwrap();
    file.write_all(b"invalid toml [[[").unwrap();
    file.sync_all().unwrap();

    sleep(Duration::from_millis(500)).await;

    let valid_config = r#"
[blob_store.fs]
root_dir = "/tmp/test"

[server]
bind_address = "10.0.0.1"
"#;
    let mut file = File::create(&config_path).unwrap();
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

#[tokio::test]
async fn coalesce_events_config_dominates_tls_in_mixed_burst() {
    let config_path = PathBuf::from("/etc/app/config.toml");
    let config_dirs = path_set(Path::new("/etc/app"));
    let tls_dir = PathBuf::from("/etc/app/tls");
    let tls_dirs: HashSet<PathBuf> = [tls_dir.clone()].into_iter().collect();

    let (tx, mut rx) = mpsc::channel(100);

    for _ in 0..5 {
        let tls_event = make_event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            vec![tls_dir.join("server.pem")],
        );
        tx.send(tls_event).await.unwrap();
    }
    let config_event = make_event(
        EventKind::Modify(ModifyKind::Data(DataChange::Content)),
        vec![config_path.clone()],
    );
    tx.send(config_event).await.unwrap();
    // `tx` stays alive so the coalescer exits on the quiet-period timeout.

    let result = coalesce_events(
        &mut rx,
        ChangeKind::Tls,
        &path_set(&config_path),
        &config_dirs,
        &tls_dirs,
    )
    .await;

    assert_eq!(result, Some(ChangeKind::Config));
}

#[tokio::test]
async fn coalesce_events_returns_none_on_channel_close() {
    let config_path = PathBuf::from("/etc/app/config.toml");
    let config_dirs = path_set(Path::new("/etc/app"));
    let tls_dirs: HashSet<PathBuf> = HashSet::new();

    let (tx, mut rx) = mpsc::channel(1);
    drop(tx);

    let result = coalesce_events(
        &mut rx,
        ChangeKind::Config,
        &path_set(&config_path),
        &config_dirs,
        &tls_dirs,
    )
    .await;

    assert_eq!(result, None);
}

#[test]
fn tls_dirs_resolve_relative_paths_against_the_config_dir() {
    let config = Configuration::load_from_str(&minimal_tls_config(
        "tls/server.pem",
        "/var/secrets/tls/server.key",
    ))
    .unwrap();

    let dirs = compute_tls_dirs(&config, Path::new("/etc/registry"));

    let expected: HashSet<PathBuf> = [
        PathBuf::from("/etc/registry/tls"),
        PathBuf::from("/var/secrets/tls"),
    ]
    .into_iter()
    .collect();
    assert_eq!(dirs, expected);
}

/// A cached configuration is reused without touching the disk.
#[tokio::test]
async fn reload_tls_uses_the_cached_configuration() {
    let temp_dir = TempDir::new().unwrap();
    let cached_cfg =
        Configuration::load_from_str(&minimal_tls_config("server.pem", "server.key")).unwrap();

    let notifier = TestNotifier::new();
    let mut cached = Some(cached_cfg);
    reload_tls(
        &mut cached,
        &WatchedConfig::new(vec![temp_dir.path().join("does-not-exist.toml")]),
        &notifier,
    );

    assert_eq!(notifier.tls_change_count(), 1);
}

/// The cache is what a later certificate rotation rebuilds TLS from, so a
/// configuration the server refused must not become the cached one: it is not
/// what is in effect.
#[tokio::test]
async fn a_refused_configuration_is_not_cached() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    fs::write(&config_path, config_toml("")).unwrap();

    let notifier = TestNotifier::new();
    notifier.refuse.store(true, Ordering::Release);
    let mut cached: Option<Configuration> = None;

    let dirs = reload_config(
        &WatchedConfig::new(vec![config_path]),
        &mut cached,
        &HashSet::new(),
        &notifier,
    )
    .await;

    assert_eq!(
        notifier.config_change_count(),
        1,
        "the configuration must still be offered to the server"
    );
    assert!(
        cached.is_none(),
        "a refused configuration must not be cached"
    );
    assert!(
        !dirs,
        "the watched TLS directories must keep describing the configuration in effect"
    );
}

#[tokio::test]
async fn reload_tls_loads_from_disk_when_cache_empty_and_notifies() {
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
    reload_tls(
        &mut cached,
        &WatchedConfig::new(vec![config_path.clone()]),
        &notifier,
    );

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
async fn reload_tls_does_not_notify_when_cache_empty_and_disk_invalid() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    fs::write(&config_path, b"invalid toml [[[").unwrap();

    let notifier = TestNotifier::new();
    let mut cached: Option<Configuration> = None;
    reload_tls(
        &mut cached,
        &WatchedConfig::new(vec![config_path.clone()]),
        &notifier,
    );

    assert_eq!(notifier.tls_change_count(), 0);
    assert!(cached.is_none());
}

#[tokio::test]
async fn reload_tls_does_not_notify_when_server_is_not_tls() {
    let temp_dir = TempDir::new().unwrap();
    let cached_cfg: Configuration = Configuration::load_from_str(
        "[blob_store.fs]\nroot_dir = \"/tmp/test\"\n\n[server]\nbind_address = \"127.0.0.1\"",
    )
    .unwrap();

    let notifier = TestNotifier::new();
    let mut cached: Option<Configuration> = Some(cached_cfg);
    let bogus_path = temp_dir.path().join("does-not-exist.toml");
    reload_tls(
        &mut cached,
        &WatchedConfig::new(vec![bogus_path.clone()]),
        &notifier,
    );

    assert_eq!(notifier.tls_change_count(), 0);
}

/// The server command aborts startup on this rather than watching a path that never fires.
#[tokio::test]
async fn startup_with_nonexistent_config_returns_err() {
    let temp_dir = TempDir::new().unwrap();
    let missing = temp_dir.path().join("does-not-exist.toml");
    let notifier = Arc::new(TestNotifier::new());

    let result = ConfigWatcher::new(
        from_ref(&missing),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    );

    assert!(
        matches!(result, Err(Error::NotReadable(_))),
        "expected Err(NotReadable), got {:?}",
        result.err()
    );
}

/// An existing but invalid file starts the watcher, which waits for a fix
/// rather than killing the process.
#[tokio::test]
async fn startup_with_invalid_toml_recovers_on_valid_write() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    fs::write(&config_path, b"this is not valid toml [[[").unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        from_ref(&config_path),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .expect("ConfigWatcher::new must succeed even with invalid TOML at startup");

    sleep(Duration::from_millis(500)).await;

    fs::write(&config_path, MINIMAL_CONFIG_TOML).unwrap();

    let detected = wait_for_condition(
        || notifier.config_change_count() >= 1,
        Duration::from_secs(5),
    )
    .await;

    assert!(
        detected,
        "watcher did not recover and notify after invalid-TOML startup"
    );
}

/// Ten writes collapse into one reload in practice; up to three tolerates a
/// slow CI runner delivering the burst across two quiet gaps.
#[tokio::test]
async fn burst_config_writes_produce_bounded_reloads() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    fs::write(&config_path, MINIMAL_CONFIG_TOML).unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        from_ref(&config_path),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

    for i in 0_u8..10 {
        let content = format!(
            "[blob_store.fs]\nroot_dir = \"/tmp/test\"\n\n[server]\nbind_address = \"10.0.0.{i}\"\n"
        );
        let mut file = File::create(&config_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    sleep(Duration::from_millis(800)).await;

    let count = notifier.config_change_count();
    assert!(
        count >= 1,
        "expected at least one reload after burst, got {count}"
    );
    assert!(
        count <= 3,
        "debounce should collapse burst to <= 3 reloads, got {count}"
    );
}

#[tokio::test]
async fn missing_tls_dir_does_not_prevent_config_reload() {
    let temp_dir = TempDir::new().unwrap();

    let phantom_cert = temp_dir.path().join("nonexistent/tls/server.pem");
    let phantom_key = temp_dir.path().join("nonexistent/tls/server.key");
    let config_path = temp_dir.path().join("config.toml");
    fs::write(
        &config_path,
        minimal_tls_config(
            phantom_cert.to_str().unwrap(),
            phantom_key.to_str().unwrap(),
        ),
    )
    .unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        from_ref(&config_path),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .expect("watcher must start even when TLS directory is absent");

    sleep(Duration::from_millis(500)).await;

    let updated =
        "[blob_store.fs]\nroot_dir = \"/tmp/test\"\n\n[server]\nbind_address = \"192.0.2.1\"\n";
    let mut file = File::create(&config_path).unwrap();
    file.write_all(updated.as_bytes()).unwrap();
    file.sync_all().unwrap();

    let detected = wait_for_condition(
        || notifier.config_change_count() >= 1,
        Duration::from_secs(5),
    )
    .await;

    assert!(
        detected,
        "watcher must detect config changes even when TLS dir is missing"
    );
}

#[tokio::test]
async fn dynamic_tls_path_change_rewatches_new_directory() {
    let temp_dir = TempDir::new().unwrap();

    let tls_v1 = temp_dir.path().join("tls_v1");
    fs::create_dir_all(&tls_v1).unwrap();
    let cert_v1 = tls_v1.join("server.pem");
    let key_v1 = tls_v1.join("server.key");
    fs::write(&cert_v1, "cert-v1").unwrap();
    fs::write(&key_v1, "key-v1").unwrap();

    let tls_v2 = temp_dir.path().join("tls_v2");
    fs::create_dir_all(&tls_v2).unwrap();
    let cert_v2 = tls_v2.join("server.pem");
    let key_v2 = tls_v2.join("server.key");
    fs::write(&cert_v2, "cert-v2").unwrap();
    fs::write(&key_v2, "key-v2").unwrap();

    let config_path = temp_dir.path().join("config.toml");
    fs::write(
        &config_path,
        minimal_tls_config(cert_v1.to_str().unwrap(), key_v1.to_str().unwrap()),
    )
    .unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        from_ref(&config_path),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

    fs::write(
        &config_path,
        minimal_tls_config(cert_v2.to_str().unwrap(), key_v2.to_str().unwrap()),
    )
    .unwrap();

    let config_reloaded = wait_for_condition(
        || notifier.config_change_count() >= 1,
        Duration::from_secs(5),
    )
    .await;
    assert!(
        config_reloaded,
        "config change to new TLS dir was not detected"
    );

    sleep(Duration::from_millis(600)).await;

    let tls_before = notifier.tls_change_count();

    let mut file = File::create(&cert_v2).unwrap();
    file.write_all(b"cert-v2-rotated").unwrap();
    file.sync_all().unwrap();

    let tls_detected = wait_for_condition(
        || notifier.tls_change_count() > tls_before,
        Duration::from_secs(5),
    )
    .await;

    assert!(
        tls_detected,
        "TLS rotation in dynamically-updated path was not detected"
    );
}

/// A rotated overlay must reload the merged configuration, not just fire an
/// event: the new value has to reach the subscriber.
#[tokio::test]
#[cfg(unix)]
async fn overlay_rotation_reloads_with_the_new_value() {
    let base_dir = TempDir::new().unwrap();
    let overlay_dir = TempDir::new().unwrap();

    let base_path = base_dir.path().join("config.toml");
    fs::write(&base_path, MINIMAL_CONFIG_TOML).unwrap();
    create_k8s_mount(
        overlay_dir.path(),
        &[("overlay.toml", "[global]\nupdate_pull_time = false\n")],
    );
    let overlay_path = overlay_dir.path().join("overlay.toml");

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        &[base_path.clone(), overlay_path.clone()],
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

    rotate_k8s_mount(
        overlay_dir.path(),
        &[("overlay.toml", "[global]\nupdate_pull_time = true\n")],
        "2024_01_02_00_00_00.000000000",
    );

    let applied = wait_for_condition(
        || {
            notifier
                .config_changes
                .lock()
                .unwrap()
                .last()
                .is_some_and(|cfg| cfg.global.update_pull_time)
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(applied, "rotated overlay value was not applied");
}

/// Config files often live in separate mounts, so every directory holding one
/// has to be watched, not just the first.
#[tokio::test]
async fn a_change_to_the_overlay_directory_is_detected() {
    let base_dir = TempDir::new().unwrap();
    let overlay_dir = TempDir::new().unwrap();

    let base_path = base_dir.path().join("config.toml");
    fs::write(&base_path, MINIMAL_CONFIG_TOML).unwrap();
    let overlay_path = overlay_dir.path().join("overlay.toml");
    fs::write(&overlay_path, "[global]\nupdate_pull_time = false\n").unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        &[base_path.clone(), overlay_path.clone()],
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

    let mut file = File::create(&overlay_path).unwrap();
    file.write_all(b"[global]\nupdate_pull_time = true\n")
        .unwrap();
    file.sync_all().unwrap();

    let applied = wait_for_condition(
        || {
            notifier
                .config_changes
                .lock()
                .unwrap()
                .last()
                .is_some_and(|cfg| cfg.global.update_pull_time)
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(applied, "overlay directory was not watched");
}

/// A rotation that briefly unlinks the overlay must not leave the watcher dead:
/// the reload fails soft and the next write still applies.
#[tokio::test]
async fn a_vanished_overlay_does_not_kill_the_watcher() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().join("config.toml");
    fs::write(&base_path, MINIMAL_CONFIG_TOML).unwrap();
    let overlay_path = temp_dir.path().join("overlay.toml");
    fs::write(&overlay_path, "[global]\nupdate_pull_time = false\n").unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        &[base_path.clone(), overlay_path.clone()],
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    sleep(Duration::from_millis(500)).await;

    fs::remove_file(&overlay_path).unwrap();
    sleep(Duration::from_millis(500)).await;

    fs::write(&overlay_path, "[global]\nupdate_pull_time = true\n").unwrap();

    let applied = wait_for_condition(
        || {
            notifier
                .config_changes
                .lock()
                .unwrap()
                .last()
                .is_some_and(|cfg| cfg.global.update_pull_time)
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(
        applied,
        "watcher did not recover after the overlay vanished"
    );
}

/// inotify names the unresolved path while the watcher knew only the resolved
/// one, so matching a single form drops the event and hot reload silently stops
/// under a Kubernetes Secret mount.
#[cfg(unix)]
#[test]
fn watched_config_matches_a_file_reported_through_its_symlink() {
    let temp_dir = TempDir::new().unwrap();
    let real = temp_dir.path().join("real");
    fs::create_dir(&real).unwrap();
    fs::write(real.join("config.toml"), MINIMAL_CONFIG_TOML).unwrap();
    let link = temp_dir.path().join("link");
    symlink(&real, &link).unwrap();

    // The operator names the path through the symlink.
    let config = WatchedConfig::new(vec![link.join("config.toml")]);

    // The event names it the same way, as inotify reports it.
    let event = make_event(
        EventKind::Modify(ModifyKind::Data(DataChange::Content)),
        vec![link.join("config.toml")],
    );
    assert_eq!(
        classify_event(
            &event,
            &config.matchable_paths,
            &config.dirs.matchable,
            &HashSet::new()
        ),
        ChangeKind::Config,
    );

    // And the resolved form fsevents reports must match just as well.
    let resolved = make_event(
        EventKind::Modify(ModifyKind::Data(DataChange::Content)),
        vec![fs::canonicalize(real.join("config.toml")).unwrap()],
    );
    assert_eq!(
        classify_event(
            &resolved,
            &config.matchable_paths,
            &config.dirs.matchable,
            &HashSet::new()
        ),
        ChangeKind::Config,
    );
}

/// The `..data` symlink a Kubernetes mount swaps on update sits in the config
/// directory, so that directory has to match under either spelling.
#[cfg(unix)]
#[test]
fn watched_config_matches_a_k8s_data_symlink_through_a_symlinked_dir() {
    let temp_dir = TempDir::new().unwrap();
    let real = temp_dir.path().join("real");
    fs::create_dir(&real).unwrap();
    fs::write(real.join("config.toml"), MINIMAL_CONFIG_TOML).unwrap();
    let link = temp_dir.path().join("link");
    symlink(&real, &link).unwrap();

    let config = WatchedConfig::new(vec![link.join("config.toml")]);
    let event = make_event(
        EventKind::Modify(ModifyKind::Data(DataChange::Content)),
        vec![link.join("..data")],
    );

    assert_eq!(
        classify_event(
            &event,
            &config.matchable_paths,
            &config.dirs.matchable,
            &HashSet::new()
        ),
        ChangeKind::Config,
    );
}

/// TLS directories carry every spelling too, so certificate rotation behind a
/// symlinked mount is still detected.
#[cfg(unix)]
#[test]
fn watched_dirs_match_a_symlinked_path_and_its_target() {
    let temp_dir = TempDir::new().unwrap();
    let real = temp_dir.path().join("real");
    fs::create_dir(&real).unwrap();
    let link = temp_dir.path().join("link");
    symlink(&real, &link).unwrap();

    let dirs = WatchedDirs::new(path_set(&link));

    assert!(
        dirs.matchable.contains(&link),
        "the operator's own path must match: {:?}",
        dirs.matchable
    );
    assert!(
        dirs.matchable.contains(&fs::canonicalize(&real).unwrap()),
        "the resolved target must match too: {:?}",
        dirs.matchable
    );
}

/// A relative path is reported absolute, so both forms have to match.
#[test]
fn watched_dirs_keep_the_raw_form_of_a_relative_path() {
    let dirs = WatchedDirs::new(path_set(Path::new(".")));

    assert!(
        dirs.matchable.contains(Path::new(".")),
        "the operator's own path must match: {:?}",
        dirs.matchable
    );
    assert!(
        dirs.matchable.contains(&absolute(".").unwrap()),
        "the absolutized form must match: {:?}",
        dirs.matchable
    );
}
