use crate::types::Error;
use crate::types::constants::{
    DOCKER_MANIFEST_LIST_MEDIA_TYPE, DOCKER_MANIFEST_MEDIA_TYPE, OCI_EMPTY_MEDIA_TYPE,
    OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE,
};

/// An RFC 6838 `restricted-name`: an alphanumeric first char, then alphanumerics
/// and `!#$&-^_.+`, at most 127 chars. The `+suffix` of a structured syntax
/// (e.g. `+json`) is covered by the `+`.
pub fn is_restricted_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= 127
        && bytes
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || b"!#$&^_.+-".contains(&byte))
}

/// A validated media type as carried by a manifest's `mediaType`, a descriptor,
/// and the `Content-Type` of a manifest request. The private field forces
/// construction through the validating constructors so a stored or wire value is
/// always a bare RFC 6838 media type. Only a header carries a parameter section,
/// which [`Self::from_content_type`] drops; a `mediaType` or an `artifactType`
/// naming one is malformed rather than reduced.
#[derive(Clone, Debug, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct MediaType(String);

impl MediaType {
    /// # Errors
    ///
    /// Returns an error when `s` is not an RFC 6838 `type/subtype` media type,
    /// each a restricted name.
    pub fn new(s: &str) -> Result<Self, Error> {
        if s.split_once('/')
            .is_some_and(|(kind, subtype)| is_restricted_name(kind) && is_restricted_name(subtype))
        {
            Ok(Self(s.to_owned()))
        } else {
            Err(Error::MediaType(s.to_string()))
        }
    }

    /// The media type of a `Content-Type` header, dropping the parameter
    /// section (`; charset=utf-8`) a sender may attach: the spec has a registry
    /// ignore parameters there and never send them back.
    ///
    /// # Errors
    ///
    /// Returns an error when what precedes the parameters is not a media type.
    pub fn from_content_type(value: &str) -> Result<Self, Error> {
        match value.split_once(';') {
            Some((essence, _)) => Self::new(essence.trim_end()),
            None => Self::new(value),
        }
    }

    /// The OCI image-manifest media type. Infallible: the value is a validated
    /// compile-time constant needing no fallible re-validation. Serves as the
    /// `Content-Type` fallback for a manifest whose link and body both lack one.
    #[must_use]
    pub fn oci_manifest() -> Self {
        Self(OCI_MANIFEST_MEDIA_TYPE.to_owned())
    }

    /// The OCI image-index (manifest list) media type, infallible like
    /// [`Self::oci_manifest`].
    #[must_use]
    pub fn oci_index() -> Self {
        Self(OCI_INDEX_MEDIA_TYPE.to_owned())
    }

    /// The OCI empty-descriptor media type, infallible like
    /// [`Self::oci_manifest`].
    #[must_use]
    pub fn oci_empty() -> Self {
        Self(OCI_EMPTY_MEDIA_TYPE.to_owned())
    }

    /// The Docker v2 schema-2 manifest media type, infallible like
    /// [`Self::oci_manifest`].
    #[must_use]
    pub fn docker_manifest() -> Self {
        Self(DOCKER_MANIFEST_MEDIA_TYPE.to_owned())
    }

    /// The Docker v2 manifest-list media type, infallible like
    /// [`Self::oci_manifest`].
    #[must_use]
    pub fn docker_manifest_list() -> Self {
        Self(DOCKER_MANIFEST_LIST_MEDIA_TYPE.to_owned())
    }
}

str_newtype!(MediaType);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_media_types() {
        for input in [
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.layer.v1.tar+gzip",
            "application/vnd.oci.image.layer.v1.tar",
            "application/json",
            "text/plain",
            "application/octet-stream",
        ] {
            assert!(MediaType::new(input).is_ok(), "'{input}' must be accepted");
        }
    }

    /// A parameter section belongs to a header alone: it is dropped there, so a
    /// `Content-Type` carrying one compares equal to and is served as the bare
    /// media type. A `mediaType` or an `artifactType` naming one is malformed,
    /// not silently reduced.
    #[test]
    fn test_content_type_parameters_are_dropped_only_for_a_header() {
        for input in [
            "application/json; charset=utf-8",
            "application/json;charset=utf-8",
            "application/json\t; charset=utf-8; boundary=x",
        ] {
            assert_eq!(
                MediaType::from_content_type(input).unwrap(),
                "application/json"
            );
            assert!(MediaType::new(input).is_err(), "'{input}' is not bare");
            assert!(MediaType::try_from(input.to_string()).is_err());
        }
    }

    #[test]
    fn test_invalid_media_types_rejected() {
        for input in [
            "",
            "application",
            "application/",
            "/json",
            "appl ication/json",
            "application/vnd oci",
        ] {
            assert!(MediaType::new(input).is_err(), "'{input}' must be rejected");
        }
    }

    #[test]
    fn test_deserialize_invalid_rejected() {
        let result: Result<MediaType, _> = serde_json::from_str("\"not a media type\"");
        assert!(result.is_err());
    }
}
