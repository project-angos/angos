use std::{future::Future, pin::Pin, sync::Arc};

use futures_util::stream::{self, Stream, TryStreamExt};

use crate::{
    command::scrub::error::Error,
    oci::{Digest, Tag},
    registry::{blob_store, metadata_store::MetadataStore},
};

const PAGE_SIZE: u16 = 100;

/// Streamed `Result<T, Error>` items, lazily paginated. Never buffers more
/// than one page (`PAGE_SIZE` items) of `T`.
pub type ResultStream<'a, T> = Pin<Box<dyn Stream<Item = Result<T, Error>> + Send + 'a>>;

pub fn revisions<'a>(
    metadata_store: &'a Arc<MetadataStore>,
    namespace: &'a str,
) -> ResultStream<'a, Digest> {
    Box::pin(paginated(move |marker| async move {
        metadata_store
            .list_revisions(namespace, PAGE_SIZE, marker)
            .await
            .map_err(Error::from)
    }))
}

pub fn tags<'a>(
    metadata_store: &'a Arc<MetadataStore>,
    namespace: &'a str,
) -> ResultStream<'a, Tag> {
    Box::pin(paginated(move |marker| async move {
        metadata_store
            .list_tags(namespace, PAGE_SIZE, marker)
            .await
            .map_err(Error::from)
    }))
}

pub fn unparsed_tags<'a>(
    metadata_store: &'a Arc<MetadataStore>,
    namespace: &'a str,
) -> ResultStream<'a, String> {
    Box::pin(paginated(move |marker| async move {
        metadata_store
            .list_tag_names(namespace, PAGE_SIZE, marker)
            .await
            .map_err(Error::from)
    }))
}

pub fn uploads<'a>(
    blob_store: &'a Arc<blob_store::BlobStore>,
    namespace: &'a str,
) -> ResultStream<'a, String> {
    Box::pin(paginated(move |marker| async move {
        blob_store
            .list_uploads(namespace, PAGE_SIZE, marker)
            .await
            .map_err(Error::from)
    }))
}

pub fn blobs(blob_store: &Arc<blob_store::BlobStore>) -> ResultStream<'_, Digest> {
    Box::pin(paginated(move |marker| async move {
        blob_store
            .list_blobs(PAGE_SIZE, marker)
            .await
            .map_err(Error::from)
    }))
}

pub fn namespaces(metadata_store: &Arc<MetadataStore>) -> ResultStream<'_, String> {
    Box::pin(paginated(move |marker| async move {
        metadata_store
            .list_namespaces(PAGE_SIZE, marker)
            .await
            .map_err(Error::from)
    }))
}

pub fn upload_namespaces(metadata_store: &Arc<MetadataStore>) -> ResultStream<'_, String> {
    Box::pin(paginated(move |marker| async move {
        metadata_store
            .list_upload_namespaces(PAGE_SIZE, marker)
            .await
            .map_err(Error::from)
    }))
}

/// Drives a paginated source as a stream. Each yielded item is one entry from
/// the underlying source; pages are fetched lazily, one at a time.
fn paginated<T, F, Fut>(fetch: F) -> impl Stream<Item = Result<T, Error>>
where
    T: Send,
    F: FnMut(Option<String>) -> Fut + Send,
    Fut: Future<Output = Result<(Vec<T>, Option<String>), Error>> + Send,
{
    stream::try_unfold(
        (Some(None::<String>), fetch),
        |(state, mut fetch)| async move {
            let Some(marker) = state else {
                return Ok::<_, Error>(None);
            };
            let (items, next_marker) = fetch(marker).await?;
            let next_state = next_marker.map(Some);
            let page = stream::iter(items.into_iter().map(Ok::<T, Error>));
            Ok(Some((page, (next_state, fetch))))
        },
    )
    .try_flatten()
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::*;
    use crate::{
        oci::{Digest, Namespace},
        registry::{
            metadata_store::{LinkKind, LinkOperation},
            test_utils::backends,
        },
    };

    #[tokio::test]
    async fn revisions_streams_each_stored_revision() {
        for test_case in backends() {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = &Namespace::new("list-all-test/revisions").unwrap();

            let (digest, _) =
                crate::registry::test_utils::create_test_blob(registry, namespace, b"manifest")
                    .await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let mut stream = revisions(&metadata_store, namespace);
            let mut collected: Vec<Digest> = Vec::new();
            while let Some(item) = stream.next().await {
                collected.push(item.unwrap());
            }

            assert_eq!(collected, vec![digest]);
            test_case.cleanup().await;
        }
    }

    #[tokio::test]
    async fn namespaces_streams_each_stored_namespace() {
        for test_case in backends() {
            let registry = test_case.registry();
            let metadata_store = test_case.metadata_store();
            let namespace = &Namespace::new("list-all-test/namespaces").unwrap();

            let (digest, _) =
                crate::registry::test_utils::create_test_blob(registry, namespace, b"manifest")
                    .await;
            metadata_store
                .update_links(
                    namespace,
                    &[LinkOperation::create(
                        LinkKind::Digest(digest.clone()),
                        digest.clone(),
                    )],
                )
                .await
                .unwrap();

            let mut stream = namespaces(&metadata_store);
            let mut collected: Vec<String> = Vec::new();
            while let Some(item) = stream.next().await {
                collected.push(item.unwrap());
            }

            assert!(collected.contains(&namespace.to_string()));
            test_case.cleanup().await;
        }
    }
}
