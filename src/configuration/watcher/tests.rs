use std::{fs, io::Write, sync::Mutex, time::Duration};

use notify::{
    EventKind,
    event::{AccessKind, ModifyKind},
};
use tempfile::TempDir;

use super::{classify::is_k8s_data_symlink, *};
use crate::{
    configuration::listeners::ServerTlsConfig,
    test_fixtures::configuration::{MINIMAL_CONFIG_TOML, config_toml},
};

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
    fs::write(&config_path, MINIMAL_CONFIG_TOML).unwrap();

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
    create_k8s_mount(temp_dir.path(), &[("config.toml", MINIMAL_CONFIG_TOML)]);

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
    fs::write(&config_path, MINIMAL_CONFIG_TOML).unwrap();

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

#[test]
fn is_k8s_data_symlink_recognizes_dotdot_data_in_config_dir() {
    let config_dir = Path::new("/etc/registry");
    let path = config_dir.join("..data");
    assert!(is_k8s_data_symlink(&path, config_dir));
}

#[test]
fn is_k8s_data_symlink_rejects_other_filenames_in_config_dir() {
    let config_dir = Path::new("/etc/registry");
    let path = config_dir.join("config.toml");
    assert!(!is_k8s_data_symlink(&path, config_dir));
}

#[test]
fn is_k8s_data_symlink_rejects_dotdot_data_in_other_directory() {
    let config_dir = Path::new("/etc/registry");
    let path = Path::new("/var/lib/other/..data");
    assert!(!is_k8s_data_symlink(path, config_dir));
}

#[test]
fn is_k8s_data_symlink_rejects_path_with_no_filename() {
    let config_dir = Path::new("/etc/registry");
    assert!(!is_k8s_data_symlink(Path::new("/"), config_dir));
}

#[test]
fn resolve_tls_dir_absolute_path_returns_parent() {
    let config_dir = Path::new("/etc/registry");
    let path = Path::new("/var/secrets/tls/server.pem");
    assert_eq!(
        resolve_tls_dir(config_dir, path),
        Some(PathBuf::from("/var/secrets/tls")),
    );
}

#[test]
fn resolve_tls_dir_relative_path_joins_with_config_dir() {
    let config_dir = Path::new("/etc/registry");
    let path = Path::new("tls/server.pem");
    assert_eq!(
        resolve_tls_dir(config_dir, path),
        Some(PathBuf::from("/etc/registry/tls")),
    );
}

#[test]
fn resolve_tls_dir_absolute_root_path_has_no_parent() {
    let config_dir = Path::new("/etc/registry");
    assert_eq!(resolve_tls_dir(config_dir, Path::new("/")), None);
}

#[test]
fn resolve_tls_dir_relative_bare_filename_lands_in_config_dir() {
    let config_dir = Path::new("/etc/registry");
    let path = Path::new("server.pem");
    assert_eq!(
        resolve_tls_dir(config_dir, path),
        Some(PathBuf::from("/etc/registry")),
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

    // Send a burst of 10 events that classify as Config, all sent before
    // the coalescer has a chance to time out, simulating e.g. an editor
    // write, truncate, write sequence.
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
// the `cached.is_none()` branch in `ensure_config_cached` / `reload_tls`.
// Calling those helpers directly does.

#[test]
fn ensure_config_cached_loads_from_disk_when_cache_is_empty() {
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
    let resolved = ensure_config_cached(&mut cached, &config_path);

    assert!(resolved.is_some(), "must load from disk when cache empty");
    assert!(
        cached.is_some(),
        "must populate cache after successful load"
    );
}

#[test]
fn ensure_config_cached_returns_none_when_cache_empty_and_disk_invalid() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    fs::write(&config_path, b"invalid toml [[[").unwrap();

    let mut cached: Option<Configuration> = None;
    let resolved = ensure_config_cached(&mut cached, &config_path);

    assert!(resolved.is_none());
    assert!(cached.is_none(), "must not populate cache on load failure");
}

#[test]
fn ensure_config_cached_returns_cached_without_reading_disk() {
    let temp_dir = TempDir::new().unwrap();
    // Cache is preloaded with a non-TLS config.
    let cached_cfg: Configuration =
        Configuration::load_from_str("[server]\nbind_address = \"127.0.0.1\"").unwrap();
    let mut cached: Option<Configuration> = Some(cached_cfg);

    // Point at a non-existent path; must not be touched.
    let bogus_path = temp_dir.path().join("does-not-exist.toml");
    let resolved = ensure_config_cached(&mut cached, &bogus_path);

    assert!(resolved.is_some(), "must reuse cached value");
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
    reload_tls(&mut cached, &config_path, &notifier);

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
    reload_tls(&mut cached, &config_path, &notifier);

    assert_eq!(notifier.tls_change_count(), 0);
    assert!(cached.is_none());
}

#[tokio::test]
async fn reload_tls_does_not_notify_when_server_is_not_tls() {
    let temp_dir = TempDir::new().unwrap();
    // Insecure server config, no TLS section.
    let cached_cfg: Configuration =
        Configuration::load_from_str("[server]\nbind_address = \"127.0.0.1\"").unwrap();

    let notifier = TestNotifier::new();
    let mut cached: Option<Configuration> = Some(cached_cfg);
    let bogus_path = temp_dir.path().join("does-not-exist.toml");
    reload_tls(&mut cached, &bogus_path, &notifier);

    assert_eq!(notifier.tls_change_count(), 0);
}

/// `ConfigWatcher::new` must return `Err(Error::NotReadable)` immediately
/// when the config file does not exist at all.  The caller (the `server`
/// command) relies on this to abort startup with a clear error message
/// rather than silently watching a path that will never fire.
#[tokio::test]
async fn startup_with_nonexistent_config_returns_err() {
    let temp_dir = TempDir::new().unwrap();
    let missing = temp_dir.path().join("does-not-exist.toml");
    let notifier = Arc::new(TestNotifier::new());

    let result = ConfigWatcher::new(
        missing.to_str().unwrap(),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    );

    assert!(
        matches!(result, Err(Error::NotReadable(_))),
        "expected Err(NotReadable), got {:?}",
        result.err()
    );
}

/// When the config file *exists* but contains invalid TOML,
/// `ConfigWatcher::new` must return `Ok`: the watcher starts, logs a
/// warning, and waits for a corrected file to appear.  This is the
/// intentional "soft startup" behaviour: if the operator wrote a bad
/// config, the running registry keeps serving traffic while the watcher
/// waits for a fix, rather than killing the process.
///
/// After writing a valid config the notifier must receive at least one
/// reload callback, confirming the watcher did not stall.
#[tokio::test]
async fn startup_with_invalid_toml_recovers_on_valid_write() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    fs::write(&config_path, b"this is not valid toml [[[").unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        config_path.to_str().unwrap(),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .expect("ConfigWatcher::new must succeed even with invalid TOML at startup");

    // Give the background task time to initialise the notify watcher before
    // writing the valid config.  The existing integration tests use 500 ms
    // for a valid-startup case; we use the same budget here because the
    // background task setup path is identical.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now write a valid config; the watcher must detect it and call
    // notify_config_change at least once.
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

/// A rapid burst of writes to the config file (simulating an editor
/// save sequence) must produce at most a small number of reload
/// notifications (far fewer than the number of writes) because the
/// debounce window coalesces consecutive events.
///
/// We write 10 times in a tight loop and assert the notifier is called
/// no more than 3 times.  In practice the debouncer collapses everything
/// into one reload; we allow a small upper bound to tolerate the OS
/// delivering events in two distinct quiet gaps on slow CI runners.
#[tokio::test]
async fn burst_config_writes_produce_bounded_reloads() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    fs::write(&config_path, MINIMAL_CONFIG_TOML).unwrap();

    let notifier = Arc::new(TestNotifier::new());
    let _watcher = ConfigWatcher::new(
        config_path.to_str().unwrap(),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    // Wait for the watcher background task to initialise its inotify fd
    // before the burst; otherwise the first few events may be missed
    // entirely, producing 0 reloads instead of the bounded > 0 we want.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write a valid config 10 times as fast as the OS allows.
    for i in 0_u8..10 {
        let content = format!("[server]\nbind_address = \"10.0.0.{i}\"\n");
        let mut file = fs::File::create(&config_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    // Wait long enough for debounce to settle (DEBOUNCE = 100 ms; give it
    // several multiples to ensure all coalesced reloads have fired).
    tokio::time::sleep(Duration::from_millis(800)).await;

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

/// When the configuration references a TLS directory that does not exist
/// on disk, the watcher must log a warning and continue rather than
/// failing.  A subsequent ordinary config-file write must still trigger
/// a reload notification, confirming the watcher loop is alive.
///
/// This exercises the `warn!("Failed to watch TLS directory ...")` branch
/// in `watch_config_loop`.
#[tokio::test]
async fn missing_tls_dir_does_not_prevent_config_reload() {
    let temp_dir = TempDir::new().unwrap();

    // Point the TLS config at directories that do not exist.
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
    // Must not return Err even though the TLS dir doesn't exist.
    let _watcher = ConfigWatcher::new(
        config_path.to_str().unwrap(),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .expect("watcher must start even when TLS directory is absent");

    // Give the background task time to initialise the watcher.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write a fresh valid config; watcher must still detect it.
    let updated = "[server]\nbind_address = \"192.0.2.1\"\n";
    let mut file = fs::File::create(&config_path).unwrap();
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

/// When a config reload changes the TLS certificate/key paths to point to
/// a new directory, the watcher must restart its directory watches and
/// subsequently detect cert rotations in the new location.
///
/// Sequence:
///   1. Start with TLS paths in `tls_v1/`.
///   2. Reload config pointing to `tls_v2/` (triggers `ChangeKind::Config`
///      and a `break` in the inner loop to re-arm watches).
///   3. Rotate the cert in `tls_v2/`; expect a TLS notification.
#[tokio::test]
async fn dynamic_tls_path_change_rewatches_new_directory() {
    let temp_dir = TempDir::new().unwrap();

    // --- initial TLS dir ---
    let tls_v1 = temp_dir.path().join("tls_v1");
    fs::create_dir_all(&tls_v1).unwrap();
    let cert_v1 = tls_v1.join("server.pem");
    let key_v1 = tls_v1.join("server.key");
    fs::write(&cert_v1, "cert-v1").unwrap();
    fs::write(&key_v1, "key-v1").unwrap();

    // --- new TLS dir (not yet in config) ---
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
        config_path.to_str().unwrap(),
        Arc::clone(&notifier) as Arc<dyn ConfigNotifier>,
    )
    .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Step 2: update config to point at tls_v2, which fires ChangeKind::Config
    // and causes the inner loop to break and re-arm watches for tls_v2.
    fs::write(
        &config_path,
        minimal_tls_config(cert_v2.to_str().unwrap(), key_v2.to_str().unwrap()),
    )
    .unwrap();

    // Wait for the config-reload notification and for the watcher to
    // re-arm (the inner loop breaks and the outer loop re-enters).
    let config_reloaded = wait_for_condition(
        || notifier.config_change_count() >= 1,
        Duration::from_secs(5),
    )
    .await;
    assert!(
        config_reloaded,
        "config change to new TLS dir was not detected"
    );

    // Give the outer loop time to re-arm the notify watcher for tls_v2.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let tls_before = notifier.tls_change_count();

    // Step 3: rotate the cert in the *new* dir; the watcher must notice.
    let mut file = fs::File::create(&cert_v2).unwrap();
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
