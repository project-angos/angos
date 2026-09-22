//! The live tags of a namespace with the times the `top_pushed` and
//! `top_pulled` rankings order them by, read once per namespace by `prune`,
//! `reconcile scan` and `reconcile index`, and the per-image judgement the
//! scan and index policies share.

use std::cmp::Reverse;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;

use angos_oci::{Digest, Namespace, Tag};

use crate::{
    command::maintenance::error::Error,
    policy::{ImagePolicy, ManifestImage},
    registry::{
        Error as RegistryError,
        metadata_store::{LinkKind, LinkMetadata, MetadataStore},
    },
};

/// Fan-out for the per-tag link-metadata reads feeding the rankings.
/// Fixed rather than derived from `--concurrency`, which already bounds the
/// namespace walk: one knob for both would square the in-flight reads.
pub const TAG_METADATA_CONCURRENCY: usize = 16;

pub struct TagWithMetadata {
    pub name: Tag,
    pub metadata: LinkMetadata,
    /// The tag's last pull, read from its access entries rather than carried
    /// on the metadata the tag resolves to.
    pub pulled_at: Option<DateTime<Utc>>,
}

/// Reads every live tag's link metadata and last pull, up to
/// `TAG_METADATA_CONCURRENCY` at a time.
pub async fn live_tags(
    metadata_store: &MetadataStore,
    namespace: &Namespace,
) -> Result<Vec<TagWithMetadata>, Error> {
    // The listing resolves each tag, so only the pull history is read.
    metadata_store
        .stream_live_tags(namespace, None)
        .err_into::<Error>()
        .map_ok(|(tag, metadata)| async move {
            let pulled_at = metadata_store
                .read_access_time(namespace, &LinkKind::Tag(tag.clone()))
                .await?;
            Ok(TagWithMetadata {
                name: tag,
                metadata,
                pulled_at,
            })
        })
        .try_buffered(TAG_METADATA_CONCURRENCY)
        .try_collect()
        .await
}

/// Ranks tags most recent first, leaving out those carrying no such time.
/// A never-pulled tag is not one of the "n most recently pulled", and
/// ranking it would make `top_pulled(n)` retain untouched tags forever.
pub fn rank_by(
    tags: &[TagWithMetadata],
    key: impl Fn(&TagWithMetadata) -> Option<DateTime<Utc>>,
) -> Vec<String> {
    let mut ranked: Vec<(Reverse<DateTime<Utc>>, String)> = tags
        .iter()
        .filter_map(|t| Some((Reverse(key(t)?), t.name.to_string())))
        .collect();
    ranked.sort_by_key(|t| t.0);
    ranked.into_iter().map(|(_, name)| name).collect()
}

/// A namespace's tags with both rankings, what an image policy is judged
/// against.
pub struct Rankings {
    pub tags: Vec<TagWithMetadata>,
    pub last_pushed: Vec<String>,
    pub last_pulled: Vec<String>,
}

impl Rankings {
    pub async fn read(
        metadata_store: &MetadataStore,
        namespace: &Namespace,
    ) -> Result<Self, Error> {
        let tags = live_tags(metadata_store, namespace).await?;
        Ok(Self {
            last_pushed: rank_by(&tags, |t| t.metadata.created_at),
            last_pulled: rank_by(&tags, |t| t.pulled_at),
            tags,
        })
    }

    /// Whether `policy` applies to the image `digest`, its newest report at
    /// `scanned_at`: every tag pointing at it is tried in turn, and an
    /// untagged image once with `image.tag == null`, the way retention
    /// judges each.
    pub async fn applies(
        &self,
        policy: &ImagePolicy,
        metadata_store: &MetadataStore,
        namespace: &Namespace,
        digest: &Digest,
        scanned_at: i64,
    ) -> Result<bool, Error> {
        let now = Utc::now();
        let image = |tag: Option<String>,
                     pushed_at: Option<DateTime<Utc>>,
                     pulled_at: Option<DateTime<Utc>>| {
            let mut image = ManifestImage::new(namespace, tag, pushed_at, pulled_at, now);
            image.scanned_at = scanned_at;
            image
        };
        let mut images: Vec<ManifestImage> = self
            .tags
            .iter()
            .filter(|tag| tag.metadata.target == *digest)
            .map(|tag| {
                image(
                    Some(tag.name.to_string()),
                    tag.metadata.created_at,
                    tag.pulled_at,
                )
            })
            .collect();
        if images.is_empty() {
            let revision = LinkKind::Digest(digest.clone());
            let pushed_at = match metadata_store.read_link(namespace, &revision).await {
                Ok(metadata) => metadata.created_at,
                Err(RegistryError::NotFound) => None,
                Err(e) => return Err(e.into()),
            };
            let pulled_at = metadata_store
                .read_access_time(namespace, &revision)
                .await?;
            images.push(image(None, pushed_at, pulled_at));
        }
        Ok(images
            .iter()
            .any(|image| policy.applies(image, &self.last_pushed, &self.last_pulled)))
    }
}
