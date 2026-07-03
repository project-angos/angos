use super::common::{build_endpoint, create_test_config};
use crate::{configuration::RegexPattern, event_webhook::event::EventKind};

#[test]
fn matches_event_no_filter_matches_all_repositories() {
    let endpoint = build_endpoint(create_test_config(vec![EventKind::ManifestPush], None));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "myapp/backend"));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "other/thing"));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "anything"));
}

#[test]
fn matches_event_multiple_filters_matches_if_any_pattern_matches() {
    let endpoint = build_endpoint(create_test_config(
        vec![EventKind::ManifestPush],
        Some(vec![
            RegexPattern::compile("^myapp/.*").unwrap(),
            RegexPattern::compile("^library/.*").unwrap(),
        ]),
    ));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "myapp/backend"));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "library/nginx"));
    assert!(!endpoint.matches_event(&EventKind::ManifestPush, "other/repo"));
}

#[test]
fn matches_event_filters_by_event_kind() {
    let endpoint = build_endpoint(create_test_config(vec![EventKind::ManifestPush], None));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "myapp/backend"));
    assert!(!endpoint.matches_event(&EventKind::BlobPush, "myapp/backend"));
    assert!(!endpoint.matches_event(&EventKind::TagCreate, "myapp/backend"));
}

#[test]
fn matches_event_multiple_event_kinds() {
    let endpoint = build_endpoint(create_test_config(
        vec![EventKind::ManifestPush, EventKind::TagCreate],
        None,
    ));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "repo"));
    assert!(endpoint.matches_event(&EventKind::TagCreate, "repo"));
    assert!(!endpoint.matches_event(&EventKind::ManifestDelete, "repo"));
    assert!(!endpoint.matches_event(&EventKind::BlobPush, "repo"));
}

#[test]
fn matches_event_both_event_kind_and_repository_must_match() {
    let endpoint = build_endpoint(create_test_config(
        vec![EventKind::ManifestPush],
        Some(vec![RegexPattern::compile("^myapp/.*").unwrap()]),
    ));
    assert!(endpoint.matches_event(&EventKind::ManifestPush, "myapp/backend"));
    assert!(!endpoint.matches_event(&EventKind::BlobPush, "myapp/backend"));
    assert!(!endpoint.matches_event(&EventKind::ManifestPush, "other/repo"));
    assert!(!endpoint.matches_event(&EventKind::BlobPush, "other/repo"));
}
