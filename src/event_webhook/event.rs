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

impl EventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::ManifestPush => "manifest.push",
            EventKind::ManifestDelete => "manifest.delete",
            EventKind::BlobPush => "blob.push",
            EventKind::TagCreate => "tag.create",
            EventKind::TagDelete => "tag.delete",
        }
    }
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

impl Event {
    pub fn new(kind: EventKind, namespace: String, repository: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            kind,
            namespace,
            repository,
            digest: None,
            reference: None,
            tag: None,
            actor: None,
        }
    }

    pub fn digest(mut self, digest: Option<String>) -> Self {
        self.digest = digest;
        self
    }

    pub fn reference(mut self, reference: Option<String>) -> Self {
        self.reference = reference;
        self
    }

    pub fn tag(mut self, tag: Option<String>) -> Self {
        self.tag = tag;
        self
    }

    pub fn actor(mut self, actor: Option<EventActor>) -> Self {
        self.actor = actor;
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        event_webhook::event::{EventActor, EventKind},
        identity::ClientIdentity,
    };

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
    fn event_kind_as_str_covers_all_variants() {
        assert_eq!(EventKind::ManifestPush.as_str(), "manifest.push");
        assert_eq!(EventKind::ManifestDelete.as_str(), "manifest.delete");
        assert_eq!(EventKind::BlobPush.as_str(), "blob.push");
        assert_eq!(EventKind::TagCreate.as_str(), "tag.create");
        assert_eq!(EventKind::TagDelete.as_str(), "tag.delete");
    }
}
