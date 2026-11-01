use std::collections::HashSet;
use std::path::{Path, PathBuf};

use notify::{Event, EventKind};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChangeKind {
    Config,
    Tls,
    Irrelevant,
}

/// Detects K8s ConfigMap/Secret-style atomic-update symlinks: directories mounted
/// from those resources contain `..data` symlinks that point to a versioned subdir,
/// and content updates manifest as a swap of the symlink rather than a write to the
/// target file. Treating modifications of `<config_dir>/..data` as config changes
/// keeps hot-reload working under K8s.
pub fn is_k8s_data_symlink(path: &Path, config_dirs: &HashSet<PathBuf>) -> bool {
    path.file_name().is_some_and(|n| n == "..data")
        && path
            .parent()
            .is_some_and(|parent| config_dirs.contains(parent))
}

/// Each set holds every spelling of a watched path, so one reached through a
/// symlink matches whichever form the platform's `notify` backend reports.
pub fn classify_event(
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

    let affects_config = event
        .paths
        .iter()
        .any(|p| config_paths.contains(p) || is_k8s_data_symlink(p, config_dirs));
    if affects_config {
        return ChangeKind::Config;
    }

    let affects_tls = event
        .paths
        .iter()
        .any(|p| p.parent().is_some_and(|d| tls_dirs.contains(d)));
    if affects_tls {
        return ChangeKind::Tls;
    }

    ChangeKind::Irrelevant
}

/// Combines two `ChangeKind` values, preferring the stronger action.
/// `Config` dominates `Tls`; a config edit covers TLS-path changes too.
pub fn merge_change_kind(a: ChangeKind, b: ChangeKind) -> ChangeKind {
    match (a, b) {
        (ChangeKind::Config, _) | (_, ChangeKind::Config) => ChangeKind::Config,
        (ChangeKind::Tls, _) | (_, ChangeKind::Tls) => ChangeKind::Tls,
        _ => ChangeKind::Irrelevant,
    }
}
