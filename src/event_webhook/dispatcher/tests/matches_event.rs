use super::common::{build_endpoint, create_test_webhook_config};
use crate::configuration::RegexPattern;
use crate::event_webhook::config::DeliveryPolicy;
use crate::event_webhook::event::EventKind::{BlobPush, ManifestPush};

#[test]
fn matches_event_requires_the_kind_and_any_repository_filter() {
    let config = || {
        create_test_webhook_config(
            "https://example.com/webhook",
            DeliveryPolicy::Optional,
            None,
            0,
        )
    };
    let unfiltered = build_endpoint(config());
    assert!(unfiltered.matches_event(&ManifestPush, "anything"));
    assert!(!unfiltered.matches_event(&BlobPush, "anything"));

    let mut filtered = config();
    filtered.repository_filter = Some(vec![
        RegexPattern::compile("^myapp/.*").unwrap(),
        RegexPattern::compile("^library/.*").unwrap(),
    ]);
    let filtered = build_endpoint(filtered);
    assert!(filtered.matches_event(&ManifestPush, "myapp/backend"));
    assert!(filtered.matches_event(&ManifestPush, "library/nginx"));
    assert!(!filtered.matches_event(&ManifestPush, "other/repo"));
    assert!(!filtered.matches_event(&BlobPush, "myapp/backend"));
}
