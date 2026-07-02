use tracing::warn;

use crate::{
    oci::{Digest, Manifest, MediaType},
    registry::{Error, manifest::response::ManifestMeta},
};

pub struct ParsedManifestDigests {
    /// The body's declared `mediaType`, surfaced from the single parse.
    pub media_type: Option<MediaType>,
    /// The body's declared `artifactType`, for building a referrer descriptor
    /// without re-parsing.
    pub artifact_type: Option<MediaType>,
    pub subject: Option<Digest>,
    pub config: Option<Digest>,
    pub layers: Vec<Digest>,
    pub manifests: Vec<Digest>,
}

fn validate_media_type_match(
    manifest: &Manifest,
    content_type: Option<&MediaType>,
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

/// Build a `ManifestMeta` from raw body bytes for a known target digest.
pub fn manifest_meta_from_body(target: &Digest, bytes: &[u8]) -> Result<ManifestMeta, Error> {
    let manifest = serde_json::from_slice::<Manifest>(bytes)?;
    Ok(ManifestMeta {
        media_type: manifest.media_type,
        digest: target.clone(),
        size: bytes.len() as u64,
    })
}

/// Deserialize a manifest body and verify its declared media type matches the
/// optional `content_type` hint.
pub fn parse_and_validate_manifest(
    body: &[u8],
    content_type: Option<&MediaType>,
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
    content_type: Option<&MediaType>,
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
        media_type: manifest.media_type,
        artifact_type: manifest.artifact_type,
        subject,
        config,
        layers,
        manifests,
    })
}
