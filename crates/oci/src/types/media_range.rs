use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use crate::types::media_type::is_restricted_name;
use crate::types::{Error, MediaType};

/// An RFC 9110 media-range: `*/*`, `type/*` or `type/subtype`, each optionally
/// followed by a single-line parameter section (a `q` weight above all). Only
/// the two wildcard forms differ from the media-type grammar, which is why an
/// `Accept` value cannot be a [`MediaType`].
fn is_media_range(s: &str) -> bool {
    let essence = match s.split_once(';') {
        Some((_, parameters)) if parameters.contains('\n') => return false,
        Some((essence, _)) => essence.trim_end_matches([' ', '\t']),
        None => s,
    };
    essence == "*/*"
        || essence.split_once('/').is_some_and(|(kind, subtype)| {
            is_restricted_name(kind) && (subtype == "*" || is_restricted_name(subtype))
        })
}

/// One member of an `Accept` header: what a client will take, which a
/// [`MediaType`] cannot express because `*/*` and `type/*` are not media types.
/// The private field forces construction through the validating constructors, so
/// a value forwarded to an upstream is always a well-formed media range.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MediaRange(String);

impl MediaRange {
    /// # Errors
    ///
    /// Returns [`Error::MediaRange`] when `s` is not a media range.
    pub fn new(s: &str) -> Result<Self, Error> {
        if is_media_range(s) {
            Ok(Self(s.to_owned()))
        } else {
            Err(Error::MediaRange(s.to_string()))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Every media type is a media range, so no re-validation is needed.
impl From<MediaType> for MediaRange {
    fn from(media_type: MediaType) -> Self {
        Self(media_type.to_string())
    }
}

impl FromStr for MediaRange {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Display for MediaRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Manifest media types stamped as `Accept` headers on every manifest probe.
/// Without them a content-negotiating registry may return a converted
/// representation whose digest never matches the one asked for.
#[must_use]
pub fn manifest_accept_types() -> Vec<MediaRange> {
    [
        MediaType::oci_manifest(),
        MediaType::oci_index(),
        MediaType::docker_manifest(),
        MediaType::docker_manifest_list(),
    ]
    .map(MediaRange::from)
    .to_vec()
}

#[cfg(test)]
mod tests {
    use crate::types::MediaType;
    use crate::types::media_range::MediaRange;

    /// The wildcard forms are why an `Accept` member is not a `MediaType`:
    /// clients send `*/*` by default, and it must survive to the upstream.
    #[test]
    fn the_wildcard_forms_are_accepted() {
        for raw in ["*/*", "application/*", "application/json"] {
            assert!(
                MediaRange::new(raw).is_ok(),
                "'{raw}' must parse as a media range"
            );
        }
    }

    #[test]
    fn parameters_are_kept_verbatim() {
        let range = MediaRange::new("application/json;q=0.9").unwrap();
        assert_eq!(range.as_str(), "application/json;q=0.9");
    }

    #[test]
    fn a_wildcard_type_with_a_concrete_subtype_is_refused() {
        for raw in ["*/json", "", "application", "/json", "application/"] {
            assert!(
                MediaRange::new(raw).is_err(),
                "'{raw}' must not parse as a media range"
            );
        }
    }

    /// The `From` impl skips the media-range grammar on the strength of every
    /// media type already satisfying it. That claim is what is checked here: a
    /// media-range grammar tightened past the media-type one would make the
    /// bypass produce values `new` itself rejects.
    #[test]
    fn a_media_type_converts_without_re_validation() {
        for media_type in [
            MediaType::oci_manifest(),
            MediaType::oci_index(),
            MediaType::docker_manifest(),
            MediaType::docker_manifest_list(),
        ] {
            let range = MediaRange::from(media_type.clone());
            assert_eq!(range.as_str(), media_type.as_ref());
            assert!(
                MediaRange::new(range.as_str()).is_ok(),
                "'{media_type}' must satisfy the media-range grammar the bypass assumes"
            );
        }
    }
}
