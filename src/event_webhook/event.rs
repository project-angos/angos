use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::identity::ClientIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    #[serde(rename = "manifest.push")]
    ManifestPush,
    #[serde(rename = "manifest.delete")]
    ManifestDelete,
    #[serde(rename = "blob.push")]
    BlobPush,
    #[serde(rename = "tag.create")]
    TagCreate,
    #[serde(rename = "tag.delete")]
    TagDelete,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventActor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
}

impl From<ClientIdentity> for EventActor {
    fn from(identity: ClientIdentity) -> Self {
        Self {
            id: identity.id,
            username: identity.username,
            client_ip: identity.client_ip,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub kind: EventKind,
    pub namespace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<EventActor>,
    pub repository: String,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use crate::event_webhook::event::{Event, EventActor, EventKind};
    use crate::identity::ClientIdentity;

    #[test]
    fn serialize_event_kind_manifest_push() {
        let kind = EventKind::ManifestPush;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""manifest.push""#);
    }

    #[test]
    fn serialize_event_kind_manifest_delete() {
        let kind = EventKind::ManifestDelete;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""manifest.delete""#);
    }

    #[test]
    fn serialize_event_kind_blob_push() {
        let kind = EventKind::BlobPush;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""blob.push""#);
    }

    #[test]
    fn serialize_event_kind_tag_create() {
        let kind = EventKind::TagCreate;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""tag.create""#);
    }

    #[test]
    fn serialize_event_kind_tag_delete() {
        let kind = EventKind::TagDelete;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""tag.delete""#);
    }

    #[test]
    fn deserialize_event_kind_all_variants() {
        let cases = [
            (r#""manifest.push""#, EventKind::ManifestPush),
            (r#""manifest.delete""#, EventKind::ManifestDelete),
            (r#""blob.push""#, EventKind::BlobPush),
            (r#""tag.create""#, EventKind::TagCreate),
            (r#""tag.delete""#, EventKind::TagDelete),
        ];

        for (input, expected) in cases {
            let result: EventKind = serde_json::from_str(input).unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn deserialize_event_kind_round_trip() {
        let variants = [
            EventKind::ManifestPush,
            EventKind::ManifestDelete,
            EventKind::BlobPush,
            EventKind::TagCreate,
            EventKind::TagDelete,
        ];

        for kind in variants {
            let json = serde_json::to_string(&kind).unwrap();
            let deserialized: EventKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, deserialized);
        }
    }

    #[test]
    fn deserialize_event_kind_unknown_string_fails() {
        let result = serde_json::from_str::<EventKind>(r#""unknown.event""#);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_event_kind_empty_string_fails() {
        let result = serde_json::from_str::<EventKind>(r#""""#);
        assert!(result.is_err());
    }

    #[test]
    fn event_actor_from_client_identity_with_all_fields() {
        let identity = ClientIdentity {
            id: Some("user-123".to_string()),
            username: Some("alice".to_string()),
            client_ip: Some("192.168.1.1".to_string()),
            ..Default::default()
        };

        let actor = EventActor::from(identity);
        assert_eq!(actor.id, Some("user-123".to_string()));
        assert_eq!(actor.username, Some("alice".to_string()));
        assert_eq!(actor.client_ip, Some("192.168.1.1".to_string()));
    }

    #[test]
    fn event_actor_from_client_identity_with_no_fields() {
        let identity = ClientIdentity::default();

        let actor = EventActor::from(identity);
        assert_eq!(actor.id, None);
        assert_eq!(actor.username, None);
        assert_eq!(actor.client_ip, None);
    }

    #[test]
    fn event_serializes_all_fields_to_json() {
        let id = Uuid::new_v4();
        let timestamp = Utc::now();
        let actor = EventActor {
            id: Some("user-1".to_string()),
            username: Some("bob".to_string()),
            client_ip: Some("10.0.0.1".to_string()),
        };

        let event = Event {
            id,
            timestamp,
            kind: EventKind::ManifestPush,
            namespace: "library/nginx".to_string(),
            digest: Some("sha256:abc123".to_string()),
            reference: Some("sha256:abc123".to_string()),
            tag: Some("latest".to_string()),
            actor: Some(actor),
            repository: "docker-hub".to_string(),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["id"], id.to_string());
        assert_eq!(json["kind"], "manifest.push");
        assert_eq!(json["namespace"], "library/nginx");
        assert_eq!(json["digest"], "sha256:abc123");
        assert_eq!(json["reference"], "sha256:abc123");
        assert_eq!(json["tag"], "latest");
        assert_eq!(json["repository"], "docker-hub");
        assert_eq!(json["actor"]["id"], "user-1");
        assert_eq!(json["actor"]["username"], "bob");
        assert_eq!(json["actor"]["client_ip"], "10.0.0.1");
        assert!(json["timestamp"].is_string());
    }

    #[test]
    fn event_serializes_without_optional_fields() {
        let id = Uuid::new_v4();
        let timestamp = Utc::now();

        let event = Event {
            id,
            timestamp,
            kind: EventKind::BlobPush,
            namespace: "myapp/backend".to_string(),
            digest: None,
            reference: None,
            tag: None,
            actor: None,
            repository: "internal".to_string(),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["kind"], "blob.push");
        assert_eq!(json["namespace"], "myapp/backend");
        assert_eq!(json["repository"], "internal");
        assert!(json.get("digest").is_none());
        assert!(json.get("reference").is_none());
        assert!(json.get("tag").is_none());
        assert!(json.get("actor").is_none());
    }

    #[test]
    fn event_serializes_with_partial_actor() {
        let event = Event {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            kind: EventKind::TagDelete,
            namespace: "myapp".to_string(),
            digest: None,
            reference: None,
            tag: Some("v1.0".to_string()),
            actor: Some(EventActor {
                id: None,
                username: Some("ci-bot".to_string()),
                client_ip: None,
            }),
            repository: "production".to_string(),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["tag"], "v1.0");
        assert!(json["actor"].get("id").is_none());
        assert_eq!(json["actor"]["username"], "ci-bot");
        assert!(json["actor"].get("client_ip").is_none());
    }
}
