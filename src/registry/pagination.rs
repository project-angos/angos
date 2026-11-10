use std::num::NonZeroUsize;

use angos_storage::Page;

/// Fan-out for the concurrent namespace walk: up to this many directories are
/// scanned at once, refilled from the backlog as scans complete, to hide
/// per-request backend latency, which dominates on S3.
pub const NAMESPACE_WALK_CONCURRENCY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(128 - 1);

/// Concurrent reads an admin listing keeps in flight per request.
pub const LISTING_READ_CONCURRENCY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(16 - 1);

/// Slices `items` into a single page of at most `n` entries.
///
/// Returns the page and a continuation token (the last entry's `ToString`)
/// when more items remain after this page; otherwise the token is `None`.
pub fn slice_page<T: Clone + ToString>(items: &[T], n: u16) -> Page<T> {
    let end_idx = items.len().min(n.into());
    let items_page = items[..end_idx].to_vec();

    let next_token = if end_idx < items.len() {
        items_page.last().map(ToString::to_string)
    } else {
        None
    };

    Page {
        items: items_page,
        next_token,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_page_bounds_the_page_and_tokens_only_a_truncated_one() {
        let items = ["a", "b", "c"].map(String::from);
        for (n, page, token) in [
            (0, &[][..], None),
            (2, &["a", "b"], Some("b")),
            (3, &["a", "b", "c"], None),
            (10, &["a", "b", "c"], None),
        ] {
            let Page { items, next_token } = slice_page(&items, n);
            assert_eq!(items, page, "n {n}");
            assert_eq!(next_token.as_deref(), token, "n {n}");
        }
    }
}
