use std::future::Future;

use crate::oci::Algorithm;

/// Paginate across the per-algorithm shards of a sharded store
/// (`<root>/<algorithm>/...`), resuming via an opaque `"<algorithm>:<cursor>"`
/// token; algorithms never interleave in sort order, so each call lists at most
/// one bounded page per prefix rather than re-enumerating every entry.
///
/// `fetch(algorithm, limit, cursor)` returns up to `limit` items from that
/// prefix plus the backend's next cursor (`None` once exhausted); an unknown
/// algorithm in the token restarts from the first prefix.
pub async fn paginate_by_algorithm<T, E, Fetch, FetchFut>(
    n: u16,
    continuation_token: Option<String>,
    mut fetch: Fetch,
) -> Result<(Vec<T>, Option<String>), E>
where
    Fetch: FnMut(Algorithm, u16, Option<String>) -> FetchFut,
    FetchFut: Future<Output = Result<(Vec<T>, Option<String>), E>>,
{
    let algorithms = Algorithm::supported_algorithms();
    let (mut index, mut cursor) = continuation_token
        .as_deref()
        .and_then(|token| {
            let (name, cursor) = match token.split_once(':') {
                Some((name, cursor)) => (name, (!cursor.is_empty()).then(|| cursor.to_string())),
                None => (token, None),
            };
            Some((algorithms.iter().position(|a| a.as_str() == name)?, cursor))
        })
        .unwrap_or((0, None));

    let target = usize::from(n);
    let mut items = Vec::with_capacity(target);
    let mut next_token = None;
    while index < algorithms.len() {
        let algorithm = algorithms[index];
        let limit = u16::try_from(target - items.len()).unwrap_or(u16::MAX);
        let (page, page_cursor) = fetch(algorithm, limit, cursor.take()).await?;
        items.extend(page);
        match page_cursor {
            Some(token) => cursor = Some(token),
            None => index += 1,
        }
        if items.len() >= target {
            next_token = match &cursor {
                Some(token) => Some(format!("{algorithm}:{token}")),
                None => algorithms.get(index).map(ToString::to_string),
            };
            break;
        }
    }

    Ok((items, next_token))
}

/// Collects all items from a paginated source into a single `Vec`. `fetch`
/// receives the current continuation token and returns the next page plus the
/// next token (`None` stops iteration).
pub async fn collect_all_pages<T, E, Fetch, FetchFut>(mut fetch: Fetch) -> Result<Vec<T>, E>
where
    Fetch: FnMut(Option<String>) -> FetchFut,
    FetchFut: std::future::Future<Output = Result<(Vec<T>, Option<String>), E>>,
{
    let mut all = Vec::new();
    let mut marker = None;
    loop {
        let (items, next_marker) = fetch(marker).await?;
        all.extend(items);
        if next_marker.is_none() {
            break;
        }
        marker = next_marker;
    }
    Ok(all)
}

/// Slices `items[start_idx..]` into a single page of at most `n` entries.
///
/// Returns the page and a continuation token (the last entry's `ToString`)
/// when more items remain after this page; otherwise the token is `None`.
fn slice_page<T: Clone + ToString>(
    items: &[T],
    start_idx: usize,
    n: u16,
) -> (Vec<T>, Option<String>) {
    let start_idx = start_idx.min(items.len());
    let end_idx = (start_idx + n as usize).min(items.len());
    let result = items[start_idx..end_idx].to_vec();

    let next_token = if end_idx < items.len() {
        result.last().map(ToString::to_string)
    } else {
        None
    };

    (result, next_token)
}

pub fn paginate<T: Clone + ToString>(
    items: &[T],
    n: u16,
    continuation_token: Option<&str>,
) -> (Vec<T>, Option<String>) {
    let start_idx = continuation_token
        .and_then(|token| items.iter().position(|item| item.to_string() == token))
        .map_or(0, |pos| pos + 1);
    slice_page(items, start_idx, n)
}

pub fn paginate_sorted<T: Clone + ToString + Ord>(
    items: &[T],
    n: u16,
    last: Option<&str>,
) -> (Vec<T>, Option<String>) {
    let start_idx = last.map_or(0, |last_item| {
        items
            .iter()
            .position(|item| item.to_string().as_str() > last_item)
            .unwrap_or(items.len())
    });
    slice_page(items, start_idx, n)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Per-algorithm fixture: sha256 holds [a, b, c], sha512 holds [d, e]. `fetch`
    // lists up to `limit` entries from the named prefix after `cursor`.
    async fn fixture_fetch(
        algorithm: Algorithm,
        limit: u16,
        cursor: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), std::convert::Infallible> {
        let items: &[&str] = match algorithm {
            Algorithm::Sha256 => &["a", "b", "c"],
            Algorithm::Sha512 => &["d", "e"],
        };
        let start = match cursor.as_deref() {
            Some(c) => items
                .iter()
                .position(|i| *i == c)
                .map_or(items.len(), |p| p + 1),
            None => 0,
        };
        let end = (start + usize::from(limit)).min(items.len());
        let page: Vec<String> = items[start..end].iter().map(|s| (*s).to_string()).collect();
        let next = (end < items.len()).then(|| page.last().cloned()).flatten();
        Ok((page, next))
    }

    #[tokio::test]
    async fn paginate_by_algorithm_streams_across_prefixes() {
        let mut all = Vec::new();
        let mut marker = None;
        loop {
            let (page, next) = paginate_by_algorithm(1, marker, fixture_fetch)
                .await
                .unwrap();
            assert!(page.len() <= 1);
            all.extend(page);
            match next {
                Some(next_marker) => marker = Some(next_marker),
                None => break,
            }
        }
        assert_eq!(all, ["a", "b", "c", "d", "e"]);
    }

    #[tokio::test]
    async fn paginate_by_algorithm_fills_pages_across_the_boundary() {
        // A page larger than the first prefix spills into the next one.
        let (page, token) = paginate_by_algorithm(4, None, fixture_fetch).await.unwrap();
        assert_eq!(page, ["a", "b", "c", "d"]);
        assert_eq!(token, Some("sha512:d".to_string()));

        let (page, token) = paginate_by_algorithm(4, token, fixture_fetch)
            .await
            .unwrap();
        assert_eq!(page, ["e"]);
        assert!(token.is_none());
    }

    #[tokio::test]
    async fn paginate_by_algorithm_unknown_token_restarts() {
        let (page, _) = paginate_by_algorithm(2, Some("blake3:x".to_string()), fixture_fetch)
            .await
            .unwrap();
        assert_eq!(page, ["a", "b"]);
    }

    #[test]
    fn test_paginate_empty() {
        let items: Vec<String> = vec![];
        let (result, token) = paginate(&items, 10, None);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn test_paginate_all_items() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate(&items, 10, None);
        assert_eq!(result.len(), 3);
        assert!(token.is_none());
    }

    #[test]
    fn test_paginate_first_page() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate(&items, 2, None);
        assert_eq!(result, vec!["a", "b"]);
        assert_eq!(token, Some("b".to_string()));
    }

    #[test]
    fn test_paginate_second_page() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate(&items, 2, Some("b"));
        assert_eq!(result, vec!["c"]);
        assert!(token.is_none());
    }

    #[test]
    fn test_paginate_invalid_token() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate(&items, 2, Some("invalid"));
        assert_eq!(result, vec!["a", "b"]);
        assert_eq!(token, Some("b".to_string()));
    }

    #[test]
    fn test_paginate_sorted_empty() {
        let items: Vec<String> = vec![];
        let (result, token) = paginate_sorted(&items, 10, None);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn test_paginate_sorted_all_items() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate_sorted(&items, 10, None);
        assert_eq!(result.len(), 3);
        assert!(token.is_none());
    }

    #[test]
    fn test_paginate_sorted_first_page() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate_sorted(&items, 2, None);
        assert_eq!(result, vec!["a", "b"]);
        assert_eq!(token, Some("b".to_string()));
    }

    #[test]
    fn test_paginate_sorted_second_page() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate_sorted(&items, 2, Some("b"));
        assert_eq!(result, vec!["c"]);
        assert!(token.is_none());
    }

    #[test]
    fn test_paginate_sorted_with_greater_than_semantics() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = paginate_sorted(&items, 10, Some("a"));
        assert_eq!(result, vec!["b", "c"]);
        assert!(token.is_none());
    }

    #[test]
    fn test_slice_page_empty_input() {
        let items: Vec<String> = vec![];
        let (result, token) = slice_page(&items, 0, 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn test_slice_page_zero_size() {
        let items = vec!["a".to_string(), "b".to_string()];
        let (result, token) = slice_page(&items, 0, 0);
        assert!(result.is_empty());
        // No items emitted, but more remain after start_idx. The contract here
        // is that an empty page implies no last-element token.
        assert!(token.is_none());
    }

    #[test]
    fn test_slice_page_start_past_end() {
        let items = vec!["a".to_string(), "b".to_string()];
        let (result, token) = slice_page(&items, 5, 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn test_slice_page_partial_page() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = slice_page(&items, 0, 2);
        assert_eq!(result, vec!["a", "b"]);
        assert_eq!(token, Some("b".to_string()));
    }

    #[test]
    fn test_slice_page_exact_remaining() {
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (result, token) = slice_page(&items, 1, 2);
        assert_eq!(result, vec!["b", "c"]);
        assert!(token.is_none());
    }
}
