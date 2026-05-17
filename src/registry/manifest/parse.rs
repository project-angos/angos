use tracing::warn;

use super::response::ManifestMeta;
use crate::{
    oci::{Digest, Manifest},
    registry::{Error, metadata_store::link_kind::LinkKind},
};

pub struct ParsedManifestDigests {
    pub subject: Option<Digest>,
    pub config: Option<Digest>,
    pub layers: Vec<Digest>,
    pub manifests: Vec<Digest>,
}

impl ParsedManifestDigests {
    pub fn links_for_revision(&self, revision: &Digest) -> Vec<(LinkKind, Digest)> {
        let mut links = self.referenced_links_for_revision(revision);

        if let Some(subject) = &self.subject {
            links.push((
                LinkKind::Referrer(subject.clone(), revision.clone()),
                revision.clone(),
            ));
        }

        links
    }

    pub fn referenced_links_for_revision(&self, revision: &Digest) -> Vec<(LinkKind, Digest)> {
        let config = self
            .config
            .iter()
            .map(|digest| (LinkKind::Config(digest.clone()), digest.clone()));
        let layers = self
            .layers
            .iter()
            .map(|digest| (LinkKind::Layer(digest.clone()), digest.clone()));
        let manifests = self.manifests.iter().map(|digest| {
            (
                LinkKind::Manifest(revision.clone(), digest.clone()),
                digest.clone(),
            )
        });

        config.chain(layers).chain(manifests).collect()
    }
}

fn validate_media_type_match(
    manifest: &Manifest,
    content_type: Option<&String>,
) -> Result<(), Error> {
    if content_type.is_some()
        && manifest.media_type.is_some()
        && manifest.media_type.as_ref() != content_type
    {
        warn!(
            "Manifest media type mismatch: {content_type:?} (expected) != {:?} (found)",
            manifest.media_type
        );
        return Err(Error::ManifestInvalid(
            "Expected manifest media type mismatch".to_string(),
        ));
    }
    Ok(())
}

/// Constructs a `ManifestMeta` from raw manifest body bytes for a known target
/// digest. Centralises the deserialize-and-project step so the I/O fallback in
/// `head_local_manifest` and any future caller share one implementation.
pub fn manifest_meta_from_body(target: &Digest, bytes: &[u8]) -> Result<ManifestMeta, Error> {
    let manifest = serde_json::from_slice::<Manifest>(bytes)?;
    Ok(ManifestMeta {
        media_type: manifest.media_type,
        digest: target.clone(),
        size: bytes.len() as u64,
    })
}

/// Deserialize a manifest body and verify its declared media type matches the
/// optional `content_type` hint. Centralises the JSON-to-`Manifest` conversion
/// so both `parse_manifest_digests` (digest projection) and `put_manifest`
/// (full-manifest writer) share one error payload.
pub fn parse_and_validate_manifest(
    body: &[u8],
    content_type: Option<&String>,
) -> Result<Manifest, Error> {
    let manifest: Manifest = serde_json::from_slice(body).map_err(|e| {
        warn!("Failed to deserialize manifest: {e}");
        Error::ManifestInvalid(format!("invalid manifest JSON: {e}"))
    })?;
    validate_media_type_match(&manifest, content_type)?;
    Ok(manifest)
}

pub fn parse_manifest_digests(
    body: &[u8],
    content_type: Option<&String>,
) -> Result<ParsedManifestDigests, Error> {
    let manifest = parse_and_validate_manifest(body, content_type)?;

    let subject = manifest.subject.map(|subject| subject.digest);

    let config = manifest.config.map(|config| config.digest);

    let layers = manifest
        .layers
        .into_iter()
        .map(|layer| layer.digest)
        .collect::<Vec<_>>();

    let manifests = manifest
        .manifests
        .into_iter()
        .map(|m| m.digest)
        .collect::<Vec<_>>();

    Ok(ParsedManifestDigests {
        subject,
        config,
        layers,
        manifests,
    })
}
