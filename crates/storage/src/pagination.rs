//! The shared driver for token-paginated listings.

use std::future::Future;

use futures_util::stream::{self, Stream, TryStreamExt};

/// Drives a token-paginated source as a lazy stream. Each yielded item is one
/// entry from the underlying source; pages are fetched one at a time as the
/// stream is polled, so at most one page of items is ever buffered.
///
/// `fetch` receives the current continuation token (`None` on the first call)
/// and returns one page of items plus the next token (`None` ends the stream).
/// Consumers compose reduction and bounded fan-out with `TryStreamExt`
/// combinators (`try_fold`, `try_buffer_unordered`, `try_for_each_concurrent`,
/// `try_collect`).
pub fn paginated<T, E, F, Fut>(fetch: F) -> impl Stream<Item = Result<T, E>>
where
    T: Send,
    F: FnMut(Option<String>) -> Fut + Send,
    Fut: Future<Output = Result<(Vec<T>, Option<String>), E>> + Send,
{
    stream::try_unfold(
        (Some(None::<String>), fetch),
        |(state, mut fetch)| async move {
            let Some(marker) = state else {
                return Ok::<_, E>(None);
            };
            let (items, next_marker) = fetch(marker).await?;
            let next_state = next_marker.map(Some);
            let page = stream::iter(items.into_iter().map(Ok::<T, E>));
            Ok(Some((page, (next_state, fetch))))
        },
    )
    .try_flatten()
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::pin::pin;

    use futures_util::StreamExt;

    use super::*;

    /// Three pages of two items each, keyed by the token the fetch received.
    async fn fixture_fetch(
        token: Option<String>,
    ) -> Result<(Vec<u32>, Option<String>), Infallible> {
        Ok(match token.as_deref() {
            None => (vec![1, 2], Some("a".to_string())),
            Some("a") => (vec![3, 4], Some("b".to_string())),
            _ => (vec![5, 6], None),
        })
    }

    #[tokio::test]
    async fn paginated_streams_every_item_across_pages() {
        let items: Vec<u32> = paginated(fixture_fetch)
            .map(|item| item.unwrap())
            .collect()
            .await;
        assert_eq!(items, [1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn paginated_yields_nothing_for_an_empty_source() {
        let items: Vec<Result<u32, Infallible>> = paginated(|_| async { Ok((Vec::new(), None)) })
            .collect()
            .await;
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn paginated_surfaces_a_page_error_after_prior_items() {
        let mut stream = pin!(paginated(|token: Option<String>| async move {
            match token {
                None => Ok((vec![1u32], Some("next".to_string()))),
                Some(_) => Err("boom"),
            }
        }));
        assert_eq!(stream.next().await, Some(Ok(1)));
        assert_eq!(stream.next().await, Some(Err("boom")));
    }
}
