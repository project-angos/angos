use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

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
pub fn is_k8s_data_symlink(path: &Path, config_dir: &Path) -> bool {
    path.file_name().is_some_and(|n| n == "..data") && path.parent() == Some(config_dir)
}

pub fn classify_event(
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

    let affects_config = event
        .paths
        .iter()
        .any(|p| p == canonical_config_path || is_k8s_data_symlink(p, canonical_config_dir));
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
pub fn merge_change_kind(a: ChangeKind, b: ChangeKind) -> ChangeKind {
    match (a, b) {
        (ChangeKind::Config, _) | (_, ChangeKind::Config) => ChangeKind::Config,
        (ChangeKind::Tls, _) | (_, ChangeKind::Tls) => ChangeKind::Tls,
        _ => ChangeKind::Irrelevant,
    }
}
