use std::{collections::HashMap, mem};

use serde::{Deserialize, Deserializer, Serialize};

use crate::types::constants::IN_TOTO_PREDICATE_TYPE;
use crate::types::{Descriptor, Digest, Error, MediaType};

/// OCI image-spec manifest `schemaVersion`. Pinned on ingress by
/// [`Manifest::from_pushed`], never on a read, so angos keeps reading what it
/// once accepted.
pub const OCI_MANIFEST_SCHEMA_VERSION: i32 = 2;

/// The descriptor payload of a manifest. The image spec makes the two shapes
/// mutually exclusive: an image manifest carries a config and its layers, an
/// index carries the child manifests it lists.
///
/// Flattened into the manifest object, so `Index` is chosen for a document
/// carrying `manifests` and `Image` for every other one. Each variant emits
/// only its own array, which is the one the spec requires of that shape.
#[derive(Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum Content {
    Index {
        manifests: Vec<Descriptor>,
    },
    Image {
        /// Boxed: a descriptor is by far the largest thing either variant
        /// carries, and inline it would make every `Content` the size of one.
        #[serde(skip_serializing_if = "Option::is_none")]
        config: Option<Box<Descriptor>>,
        layers: Vec<Descriptor>,
    },
}

/// Every array either shape may carry, read before the shape is chosen.
#[derive(Deserialize)]
struct ContentFields {
    manifests: Option<Vec<Descriptor>>,
    config: Option<Box<Descriptor>>,
    #[serde(default)]
    layers: Vec<Descriptor>,
}

impl<'de> Deserialize<'de> for Content {
    /// The shape follows `manifests` alone: an index whose child does not parse
    /// fails, rather than reading as an image manifest with nothing to pin.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fields = ContentFields::deserialize(deserializer)?;
        Ok(match fields.manifests {
            Some(manifests) => Content::Index { manifests },
            None => Content::Image {
                config: fields.config,
                layers: fields.layers,
            },
        })
    }
}

impl Content {
    /// The config descriptor of an image manifest; an index carries none.
    #[must_use]
    pub fn config(&self) -> Option<&Descriptor> {
        match self {
            Content::Image { config, .. } => config.as_deref(),
            Content::Index { .. } => None,
        }
    }
}

impl Default for Content {
    fn default() -> Self {
        Content::Image {
            config: None,
            layers: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub schema_version: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<MediaType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<Descriptor>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub annotations: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<MediaType>,
    #[serde(flatten)]
    pub content: Content,
}

impl Manifest {
    /// Deserializes leniently, like `media_type` being optional: angos must
    /// keep reading manifests it once accepted. Ingress checks the strict rules
    /// through [`Self::from_pushed`].
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not a JSON manifest object.
    pub fn from_slice(s: &[u8]) -> Result<Self, Error> {
        Ok(serde_json::from_slice(s)?)
    }

    /// Parses a manifest a client is pushing, applying the ingress rules
    /// [`Self::from_slice`] stays lenient about: image and index content are
    /// mutually exclusive in the spec (the hybrid would store a document
    /// neither shape describes), `schemaVersion` is pinned, and the declared
    /// `mediaType` must agree with the `Content-Type` it was sent under.
    ///
    /// The shapes are read off the raw object because the flattened [`Content`]
    /// keeps only the one it selects, which is what lets an already-stored
    /// hybrid stay readable.
    ///
    /// # Errors
    ///
    /// Returns an error when the bytes are not a JSON manifest object, or when
    /// the object breaks one of those rules.
    pub fn from_pushed(s: &[u8], content_type: Option<&MediaType>) -> Result<Self, Error> {
        let raw: serde_json::Value = serde_json::from_slice(s)?;
        if declares_both_shapes(&raw) {
            return Err(Error::InvalidManifest(
                "a manifest carries either config/layers or manifests, never both".to_string(),
            ));
        }
        let manifest: Self = serde_json::from_value(raw)?;
        if manifest.schema_version != OCI_MANIFEST_SCHEMA_VERSION {
            return Err(Error::InvalidManifest(format!(
                "unsupported schemaVersion {}, expected {OCI_MANIFEST_SCHEMA_VERSION}",
                manifest.schema_version
            )));
        }
        if !manifest.media_type_matches(content_type) {
            return Err(Error::InvalidManifest(
                "Expected manifest media type mismatch".to_string(),
            ));
        }
        Ok(manifest)
    }

    /// Whether the declared `mediaType` agrees with the `Content-Type` the
    /// manifest travelled under. Either side being absent is no disagreement:
    /// the spec leaves the manifest's own field optional.
    #[must_use]
    pub fn media_type_matches(&self, content_type: Option<&MediaType>) -> bool {
        match (self.media_type.as_ref(), content_type) {
            (Some(declared), Some(sent)) => declared == sent,
            _ => true,
        }
    }

    /// Returns `true` if `artifact_type` equals either the manifest's top-level
    /// `artifactType` field or, per the OCI Referrers API spec, the config's
    /// `mediaType` fallback.
    #[must_use]
    pub fn has_artifact_type(&self, artifact_type: &MediaType) -> bool {
        self.artifact_type.as_ref() == Some(artifact_type)
            || self
                .content
                .config()
                .is_some_and(|c| c.media_type == *artifact_type)
    }

    /// Returns whether this manifest's `artifact_type` (or config `mediaType`
    /// fallback) matches the given filter. A `None` filter matches anything.
    #[must_use]
    pub fn artifact_type_matches(&self, filter: Option<&MediaType>) -> bool {
        filter.is_none_or(|want| self.has_artifact_type(want))
    }

    /// The media type describing this manifest: its own `mediaType`, else the
    /// one its shape implies. Keyed on the children rather than the shape: a
    /// document whose `manifests` array is present but empty has always been
    /// served as an image manifest, and a stored one must keep the
    /// `Content-Type` it has always had.
    #[must_use]
    pub fn described_media_type(&self) -> MediaType {
        if let Some(media_type) = self.media_type.clone() {
            return media_type;
        }
        match &self.content {
            Content::Index { manifests } if !manifests.is_empty() => MediaType::oci_index(),
            _ => MediaType::oci_manifest(),
        }
    }

    /// Builds a `Descriptor` for this manifest, moving the (potentially large)
    /// annotations map out of `self` rather than cloning it. A manifest with no
    /// `mediaType` is described by the one its shape implies, so it is listed
    /// rather than dropped. Filter-mismatch is a separate concern; callers that
    /// need filtering call `artifact_type_matches` first.
    pub fn take_descriptor(&mut self, digest: Digest, size: u64) -> Descriptor {
        let media_type = self.described_media_type();
        // Per the OCI Referrers API a referrer's `artifactType` is the manifest's
        // own field, falling back to the config `mediaType` for image manifests
        // without one; an empty value would drop the entry from filtered lists.
        let artifact_type = self
            .artifact_type
            .clone()
            .or_else(|| self.content.config().map(|c| c.media_type.clone()));
        Descriptor {
            media_type,
            annotations: mem::take(&mut self.annotations),
            artifact_type,
            platform: None,
            digest,
            size,
        }
    }

    /// An OCI image index listing `manifests`, typed as one so it serializes
    /// under the media type a client negotiates the listing on.
    #[must_use]
    pub fn oci_index(manifests: Vec<Descriptor>) -> Self {
        Self {
            media_type: Some(MediaType::oci_index()),
            content: Content::Index { manifests },
            ..Self::default()
        }
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            schema_version: OCI_MANIFEST_SCHEMA_VERSION,
            media_type: None,
            subject: None,
            annotations: HashMap::new(),
            artifact_type: None,
            content: Content::default(),
        }
    }
}

/// Construction shortcuts for fixtures: nothing in the serving paths builds a
/// manifest, they only parse.
#[cfg(feature = "test-util")]
impl Manifest {
    /// An image manifest carrying `config` and `layers`, every other field left
    /// at its default.
    #[must_use]
    pub fn image(config: Option<Descriptor>, layers: Vec<Descriptor>) -> Self {
        Self {
            content: Content::Image {
                config: config.map(Box::new),
                layers,
            },
            ..Self::default()
        }
    }

    /// An index listing `manifests`, every other field left at its default.
    #[must_use]
    pub fn index(manifests: Vec<Descriptor>) -> Self {
        Self {
            content: Content::Index { manifests },
            ..Self::default()
        }
    }
}

/// Whether the raw manifest object carries both an image payload and an index
/// one, which the image spec forbids.
fn declares_both_shapes(raw: &serde_json::Value) -> bool {
    let non_empty_array = |field: &str| {
        raw.get(field)
            .and_then(serde_json::Value::as_array)
            .is_some_and(|entries| !entries.is_empty())
    };
    let image = raw.get("config").is_some_and(|c| !c.is_null()) || non_empty_array("layers");
    image && non_empty_array("manifests")
}

impl Manifest {
    /// The in-toto predicate type annotation from the first layer carrying
    /// one; `None` for an index, or an image whose layers carry none.
    #[must_use]
    pub fn in_toto_predicate_type(&self) -> Option<&str> {
        let Content::Image { layers, .. } = &self.content else {
            return None;
        };
        layers
            .iter()
            .find_map(|layer| layer.annotations.get(IN_TOTO_PREDICATE_TYPE))
            .map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::types::Digest;
    use crate::types::constants::{IN_TOTO_PREDICATE_TYPE, OCI_INDEX_MEDIA_TYPE};
    use crate::types::manifest::*;

    const VALID_HASH: &str = "99c9d5e2bdc7ef0223f56c845a695ea0f8f11f5b55ea6f74e1f7df0d4f90026c";
    const MEDIA_TYPE_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
    const MEDIA_TYPE_CONFIG: &str = "application/vnd.oci.image.config.v1+json";
    const ARTIFACT_TYPE_INDEX: &str = "application/vnd.oci.image.index.v1+json";

    fn valid_digest() -> Digest {
        Digest::sha256(VALID_HASH).unwrap()
    }

    fn media_type(value: &str) -> MediaType {
        MediaType::new(value).unwrap()
    }

    fn demo_manifest() -> Manifest {
        Manifest {
            media_type: Some(media_type(MEDIA_TYPE_MANIFEST)),
            artifact_type: Some(media_type(ARTIFACT_TYPE_INDEX)),
            ..Manifest::image(
                Some(Descriptor {
                    media_type: media_type(MEDIA_TYPE_CONFIG),
                    digest: valid_digest(),
                    size: 1234,
                    annotations: HashMap::new(),
                    artifact_type: None,
                    platform: None,
                }),
                vec![Descriptor {
                    media_type: media_type("application/vnd.oci.image.layer.v1.tar"),
                    digest: valid_digest(),
                    size: 5678,
                    annotations: HashMap::new(),
                    artifact_type: None,
                    platform: None,
                }],
            )
        }
    }

    /// A `null` config or subject is not something a client would ever send,
    /// and `layers` is required on an image manifest however empty it is.
    #[test]
    fn serializing_omits_absent_fields_but_keeps_the_required_arrays() {
        let json = serde_json::to_value(Manifest::default()).unwrap();
        let object = json
            .as_object()
            .expect("a manifest serializes to an object");

        assert!(
            !object.values().any(Value::is_null),
            "no field may serialize as null: {json}"
        );
        assert!(
            !object.contains_key("annotations"),
            "an empty annotations map must be omitted: {json}"
        );
        assert!(
            object.contains_key("layers"),
            "an image manifest must keep its spec-required layers array: {json}"
        );
        assert!(
            !object.contains_key("manifests"),
            "an image manifest must not advertise an index array: {json}"
        );
    }

    #[test]
    fn serializing_keeps_the_fields_that_are_set() {
        let json = serde_json::to_value(demo_manifest()).unwrap();

        assert_eq!(json["mediaType"], MEDIA_TYPE_MANIFEST);
        assert_eq!(json["artifactType"], ARTIFACT_TYPE_INDEX);
        assert_eq!(json["config"]["size"], 1234);
        assert_eq!(json["layers"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn test_has_artifact_type_top_level_field() {
        let manifest = demo_manifest();
        assert!(manifest.has_artifact_type(&media_type(ARTIFACT_TYPE_INDEX)));
    }

    #[test]
    fn test_has_artifact_type_config_media_type_fallback() {
        let manifest = demo_manifest();
        assert!(manifest.has_artifact_type(&media_type(MEDIA_TYPE_CONFIG)));
    }

    #[test]
    fn test_has_artifact_type_no_match() {
        let manifest = demo_manifest();
        assert!(!manifest.has_artifact_type(&media_type("application/vnd.example.unknown")));
    }

    #[test]
    fn test_has_artifact_type_none_artifact_type_none_config() {
        let manifest = Manifest::default();
        assert!(!manifest.has_artifact_type(&media_type("application/vnd.anything")));
    }

    // take_descriptor: the manifest's own media type describes it
    #[test]
    fn test_take_descriptor_with_media_type_returns_descriptor() {
        let mut manifest = demo_manifest();
        let digest = valid_digest();
        let d = manifest.take_descriptor(digest.clone(), 999);
        assert_eq!(d.media_type, MEDIA_TYPE_MANIFEST);
        assert_eq!(d.digest, digest);
        assert_eq!(d.size, 999);
        assert_eq!(d.artifact_type.as_deref(), Some(ARTIFACT_TYPE_INDEX));
    }

    // take_descriptor: an image manifest without its own artifactType advertises
    // the config mediaType (OCI Referrers API fallback), not an empty value.
    #[test]
    fn test_take_descriptor_falls_back_to_config_media_type() {
        let mut manifest = demo_manifest();
        manifest.artifact_type = None;
        let d = manifest.take_descriptor(valid_digest(), 1);
        assert_eq!(d.artifact_type.as_deref(), Some(MEDIA_TYPE_CONFIG));
    }

    /// A manifest carrying no `mediaType` is described by the type its shape
    /// implies, so a referrer listing names it rather than dropping it.
    #[test]
    fn an_oci_index_serializes_as_the_referrers_document() {
        let index = Manifest::oci_index(vec![Descriptor {
            media_type: media_type(MEDIA_TYPE_MANIFEST),
            digest: valid_digest(),
            size: 7,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: None,
        }]);
        let json: Value = serde_json::to_value(&index).unwrap();

        assert_eq!(json["schemaVersion"], 2);
        assert_eq!(json["mediaType"], OCI_INDEX_MEDIA_TYPE);
        assert_eq!(json["manifests"].as_array().unwrap().len(), 1);
        assert!(json.get("config").is_none() && json.get("layers").is_none());
    }

    #[test]
    fn test_take_descriptor_recovers_an_absent_media_type() {
        let image = Manifest {
            media_type: None,
            ..Manifest::default()
        }
        .take_descriptor(valid_digest(), 0);
        assert_eq!(image.media_type, MediaType::oci_manifest());

        let child = Descriptor {
            media_type: media_type(MEDIA_TYPE_MANIFEST),
            digest: valid_digest(),
            size: 1,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: None,
        };
        let index = Manifest {
            media_type: None,
            ..Manifest::index(vec![child])
        }
        .take_descriptor(valid_digest(), 0);
        assert_eq!(index.media_type, MediaType::oci_index());
    }

    /// The filter is optional and, when given, defers to
    /// [`Manifest::has_artifact_type`]: an absent one lists everything, and a
    /// present one matches the manifest's own type or its config fallback.
    #[test]
    fn artifact_type_matches_only_filters_when_asked() {
        let manifest = demo_manifest();

        assert!(manifest.artifact_type_matches(None));
        assert!(manifest.artifact_type_matches(Some(&media_type(ARTIFACT_TYPE_INDEX))));
        assert!(manifest.artifact_type_matches(Some(&media_type(MEDIA_TYPE_CONFIG))));
        assert!(
            !manifest.artifact_type_matches(Some(&media_type("application/vnd.example.other")))
        );
    }

    /// A stored hybrid predates the ingress refusal and must stay readable: it
    /// reads as the index whose children it lists.
    #[test]
    fn a_stored_hybrid_reads_as_the_index_it_lists() {
        let body = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "config": { "mediaType": MEDIA_TYPE_CONFIG, "digest": format!("sha256:{VALID_HASH}"), "size": 1 },
            "layers": [{ "mediaType": MEDIA_TYPE_CONFIG, "digest": format!("sha256:{VALID_HASH}"), "size": 2 }],
            "manifests": [{ "mediaType": MEDIA_TYPE_MANIFEST, "digest": format!("sha256:{VALID_HASH}"), "size": 3 }],
        }))
        .unwrap();

        let manifest = Manifest::from_slice(&body).expect("a stored hybrid must stay readable");
        let Content::Index { manifests } = &manifest.content else {
            panic!("a hybrid must read as an index, got {:?}", manifest.content);
        };
        assert_eq!(manifests.len(), 1);
    }

    /// The same document is refused on push, so no new one can be stored.
    #[test]
    fn pushing_a_hybrid_is_refused() {
        let body = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "layers": [{ "mediaType": MEDIA_TYPE_CONFIG, "digest": format!("sha256:{VALID_HASH}"), "size": 2 }],
            "manifests": [{ "mediaType": MEDIA_TYPE_MANIFEST, "digest": format!("sha256:{VALID_HASH}"), "size": 3 }],
        }))
        .unwrap();

        assert!(
            matches!(
                Manifest::from_pushed(&body, None),
                Err(Error::InvalidManifest(_))
            ),
            "a document carrying both shapes must not enter the store"
        );
    }

    /// A conforming image manifest and a conforming index both survive push.
    #[test]
    fn pushing_either_shape_alone_is_accepted() {
        for body in [
            serde_json::json!({
                "schemaVersion": 2,
                "config": { "mediaType": MEDIA_TYPE_CONFIG, "digest": format!("sha256:{VALID_HASH}"), "size": 1 },
                "layers": [],
            }),
            serde_json::json!({
                "schemaVersion": 2,
                "manifests": [{ "mediaType": MEDIA_TYPE_MANIFEST, "digest": format!("sha256:{VALID_HASH}"), "size": 3 }],
            }),
        ] {
            let raw = serde_json::to_vec(&body).unwrap();
            assert!(
                Manifest::from_pushed(&raw, None).is_ok(),
                "a single-shape manifest must be accepted: {body}"
            );
        }
    }

    /// A child that does not parse fails the index rather than demoting it to
    /// an image manifest with no reference to check or pin.
    #[test]
    fn an_index_with_an_invalid_child_is_refused() {
        let body = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": OCI_INDEX_MEDIA_TYPE,
            "manifests": [
                { "mediaType": MEDIA_TYPE_MANIFEST, "digest": format!("sha256:{VALID_HASH}"), "size": 3 },
                { "mediaType": MEDIA_TYPE_MANIFEST, "digest": format!("sha256:{VALID_HASH}"), "size": 3, "platform": { "os": "linux" } },
            ],
        }))
        .unwrap();

        assert!(Manifest::from_pushed(&body, None).is_err());
        assert!(Manifest::from_slice(&body).is_err());
    }

    /// The pushed `Content-Type` and the body's own `mediaType` must agree,
    /// but neither side is required to speak: a body declaring none travels
    /// under any content type, and a parameterized header still matches the
    /// bare type it carries.
    #[test]
    fn pushing_a_media_type_the_content_type_contradicts_is_refused() {
        let declared = |media_type: Option<&str>| {
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": media_type,
            }))
            .unwrap()
        };
        let sent = MediaType::from_content_type(&format!("{MEDIA_TYPE_MANIFEST}; charset=utf-8"))
            .expect("a header may carry a parameter section");

        assert!(
            matches!(
                Manifest::from_pushed(&declared(Some(OCI_INDEX_MEDIA_TYPE)), Some(&sent)),
                Err(Error::InvalidManifest(_))
            ),
            "a body contradicting its Content-Type must not enter the store"
        );
        assert!(
            Manifest::from_pushed(&declared(Some(MEDIA_TYPE_MANIFEST)), Some(&sent)).is_ok(),
            "a Content-Type parameter is the sender's business, not a mismatch"
        );
        assert!(
            Manifest::from_pushed(&declared(None), Some(&sent)).is_ok(),
            "a body declaring no mediaType agrees with any Content-Type"
        );
    }

    /// The schema version the image spec defines is pinned on push, and the
    /// refusal names what it found next to what it wanted.
    #[test]
    fn pushing_a_foreign_schema_version_is_refused() {
        let body = serde_json::to_vec(&json!({
            "schemaVersion": 1,
            "mediaType": MEDIA_TYPE_MANIFEST,
        }))
        .unwrap();

        let Err(Error::InvalidManifest(message)) = Manifest::from_pushed(&body, None) else {
            panic!("a foreign schemaVersion must not enter the store");
        };
        assert_eq!(message, "unsupported schemaVersion 1, expected 2");
    }

    /// Reads stay lenient so a scrub or a replication push never trips over an
    /// object an older angos accepted; only the write paths reject it.
    #[test]
    fn a_stored_foreign_schema_version_is_still_parsed() {
        let body = serde_json::to_vec(&json!({
            "schemaVersion": 1,
            "mediaType": MEDIA_TYPE_MANIFEST,
        }))
        .unwrap();

        let parsed = Manifest::from_slice(&body)
            .expect("a stored manifest must stay readable whatever its schemaVersion");
        assert_eq!(parsed.media_type.as_deref(), Some(MEDIA_TYPE_MANIFEST));
    }

    /// An index round-trips through the wire form without gaining an image's
    /// `layers` array, and back to the same value.
    #[test]
    fn each_shape_round_trips_through_its_own_wire_form() {
        let index = Manifest::index(vec![Descriptor {
            media_type: media_type(MEDIA_TYPE_MANIFEST),
            digest: valid_digest(),
            size: 7,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: None,
        }]);

        let json = serde_json::to_value(&index).unwrap();
        let object = json.as_object().expect("an index serializes to an object");
        assert!(
            object.contains_key("manifests"),
            "index keeps manifests: {json}"
        );
        assert!(
            !object.contains_key("layers"),
            "index must not emit layers: {json}"
        );

        let parsed: Manifest = serde_json::from_value(json).unwrap();
        assert_eq!(
            parsed, index,
            "an index must survive a round trip unchanged"
        );
    }

    /// Bodies as the real clients emit them. Each must be accepted on push and
    /// land in the shape its spec describes, so the ecosystem keeps working.
    #[test]
    fn real_client_manifests_push_and_classify() {
        const DIGEST: &str =
            "sha256:99c9d5e2bdc7ef0223f56c845a695ea0f8f11f5b55ea6f74e1f7df0d4f90026c";

        // docker push (schema 2), the shape `docker buildx` and `docker push` send.
        let docker_image = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "digest": DIGEST, "size": 7023,
            },
            "layers": [{
                "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "digest": DIGEST, "size": 32654,
            }],
        });

        // docker manifest list, the multi-arch shape a `docker pull` resolves.
        let docker_list = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [{
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": DIGEST, "size": 7143,
                "platform": { "architecture": "amd64", "os": "linux" },
            }],
        });

        // helm push, an image manifest whose config names the chart type.
        let helm_chart = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.cncf.helm.config.v1+json",
                "digest": DIGEST, "size": 233,
            },
            "layers": [{
                "mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
                "digest": DIGEST, "size": 4096,
            }],
        });

        // oras attach, an artifact carrying `subject` and `artifactType` with the
        // empty config descriptor.
        let oras_artifact = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "artifactType": "application/vnd.example.sbom.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.empty.v1+json",
                "digest": DIGEST, "size": 2,
            },
            "layers": [{
                "mediaType": "application/vnd.example.sbom.v1+json",
                "digest": DIGEST, "size": 1024,
            }],
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": DIGEST, "size": 7023,
            },
            "annotations": { "org.opencontainers.image.created": "2024-01-01T00:00:00Z" },
        });

        // An OCI index carrying a subject, as an attestation index does.
        let oci_index = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [{
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": DIGEST, "size": 7023,
            }],
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": DIGEST, "size": 7023,
            },
        });

        let images = [
            ("docker image", docker_image),
            ("helm chart", helm_chart),
            ("oras artifact", oras_artifact),
        ];
        for (name, body) in images {
            let raw = serde_json::to_vec(&body).unwrap();
            let manifest = Manifest::from_pushed(&raw, None)
                .unwrap_or_else(|e| panic!("{name} must be accepted on push: {e}"));
            assert!(
                matches!(manifest.content, Content::Image { .. }),
                "{name} must classify as an image manifest, got {:?}",
                manifest.content
            );
        }

        for (name, body) in [
            ("docker manifest list", docker_list),
            ("oci index", oci_index),
        ] {
            let raw = serde_json::to_vec(&body).unwrap();
            let manifest = Manifest::from_pushed(&raw, None)
                .unwrap_or_else(|e| panic!("{name} must be accepted on push: {e}"));
            assert!(
                matches!(manifest.content, Content::Index { .. }),
                "{name} must classify as an index, got {:?}",
                manifest.content
            );
        }
    }

    /// An image manifest with no `layers` key at all still parses: the spec
    /// requires the array, but angos keeps reading what it once accepted.
    #[test]
    fn an_image_manifest_without_layers_still_parses() {
        let body = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MEDIA_TYPE_MANIFEST,
        }))
        .unwrap();
        let manifest = Manifest::from_pushed(&body, None).expect("a config-less image must parse");
        assert!(matches!(manifest.content, Content::Image { .. }));
    }

    #[test]
    fn in_toto_predicate_type_is_none_for_an_index() {
        let manifest = manifest_with_layer_annotations(None);
        assert_eq!(manifest.in_toto_predicate_type(), None);
    }

    #[test]
    fn in_toto_predicate_type_is_none_when_no_layer_carries_one() {
        let manifest = manifest_with_layer_annotations(Some(vec![HashMap::from([(
            "some.other.key".to_string(),
            "value".to_string(),
        )])]));
        assert_eq!(manifest.in_toto_predicate_type(), None);
    }

    #[test]
    fn in_toto_predicate_type_takes_the_first_layer_carrying_one() {
        let manifest = manifest_with_layer_annotations(Some(vec![
            HashMap::from([(
                IN_TOTO_PREDICATE_TYPE.to_string(),
                "https://slsa.dev/provenance/v0.2".to_string(),
            )]),
            HashMap::from([(
                IN_TOTO_PREDICATE_TYPE.to_string(),
                "https://slsa.dev/provenance/v1".to_string(),
            )]),
        ]));
        assert_eq!(
            manifest.in_toto_predicate_type(),
            Some("https://slsa.dev/provenance/v0.2"),
        );
    }

    /// `None` builds an index, `Some` an image with one layer per annotation map.
    fn manifest_with_layer_annotations(layers: Option<Vec<HashMap<String, String>>>) -> Manifest {
        let content = match layers {
            None => Content::Index { manifests: vec![] },
            Some(layers) => Content::Image {
                config: None,
                layers: layers
                    .into_iter()
                    .map(|annotations| Descriptor {
                        media_type: MediaType::new(MEDIA_TYPE_CONFIG).unwrap(),
                        digest: valid_digest(),
                        size: 1,
                        annotations,
                        artifact_type: None,
                        platform: None,
                    })
                    .collect(),
            },
        };
        Manifest {
            schema_version: OCI_MANIFEST_SCHEMA_VERSION,
            media_type: None,
            subject: None,
            annotations: HashMap::new(),
            artifact_type: None,
            content,
        }
    }
}
