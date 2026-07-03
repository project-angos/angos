//! Webhook event fixtures.

use chrono::Utc;
use uuid::Uuid;

use crate::{
    event_webhook::event::{Event, EventKind},
    oci::Namespace,
};

/// A `manifest.push` [`Event`] with the shared test skeleton; the caller pins
/// the fields its assertions read.
///
/// # Panics
/// When `namespace` is not a valid OCI namespace.
pub fn manifest_push_event(namespace: &str, repository: &str, tag: Option<&str>) -> Event {
    Event {
        id: Uuid::new_v4(),
        timestamp: Utc::now(),
        kind: EventKind::ManifestPush,
        namespace: Namespace::new(namespace).unwrap(),
        digest: Some("sha256:abc123".to_string()),
        reference: Some("sha256:abc123".to_string()),
        tag: tag.map(str::to_string),
        actor: None,
        repository: repository.to_string(),
    }
}
