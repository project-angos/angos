//! The live tags of a namespace with the times the `top_pushed` and
//! `top_pulled` rankings order them by, read once per namespace by `prune`
//! and by `reconcile scan`.

use std::cmp::Reverse;

use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;

use angos_oci::{Namespace, Tag};

use crate::{
    command::maintenance::error::Error,
    registry::metadata_store::{LinkKind, LinkMetadata, MetadataStore},
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
