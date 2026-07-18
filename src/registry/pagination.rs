use std::future::Future;

use angos_storage::ChildrenPage;
use futures_util::stream::{self, StreamExt};

/// Fan-out for the concurrent namespace walk. The tree is shallow and wide (one
/// node per repository path segment), so a directory's siblings are listed in
/// parallel to hide per-request backend latency, which dominates on S3.
pub const NAMESPACE_WALK_CONCURRENCY: usize = 32;

/// One scanned directory: its namespace name when it holds the `marker` child,
/// and the sub-directories to descend into.
struct DirectoryScan {
    namespace: Option<String>,
    children: Vec<(String, String)>,
}

/// Walk the repository tree under `root_path` and yield every path that has a
/// `marker` child: `_manifests` for the content catalog on the metadata store,
/// `_uploads` for upload-namespace discovery on the blob store. `_`-prefixed
/// children are never descended into, so manifest/upload/blob substructure is
/// not mistaken for nested namespaces.
///
/// `root_prefix` seeds the namespace name of `root_path` (empty for a
/// whole-store walk, `"{repository}/"` to restrict the walk to one repository's
/// subtree while keeping the repository segment in the returned names). The walk
/// runs breadth-first, listing each level's directories concurrently, so the
/// returned order is unspecified and callers that need ordering must sort.
///
/// `list_children(path, token)` returns one page of `path`'s immediate
/// children on whichever store is being walked.
pub async fn collect_namespaces_with_marker<E, List, ListFut>(
    root_path: &str,
    root_prefix: &str,
    marker: &str,
    concurrency: usize,
    list_children: List,
) -> Result<Vec<String>, E>
where
    List: Fn(String, Option<String>) -> ListFut,
    ListFut: Future<Output = Result<ChildrenPage, E>>,
{
    let mut namespaces = Vec::new();
    let mut frontier = vec![(root_path.to_string(), root_prefix.to_string())];

    while !frontier.is_empty() {
        let scanned: Vec<Result<DirectoryScan, E>> = stream::iter(frontier.drain(..))
            .map(|(path, prefix)| scan_directory(&list_children, marker, path, prefix))
            .buffer_unordered(concurrency.max(1))
            .collect()
            .await;

        let mut next_frontier = Vec::new();
        for scan in scanned {
            let scan = scan?;
            if let Some(namespace) = scan.namespace {
                namespaces.push(namespace);
            }
            next_frontier.extend(scan.children);
        }
        frontier = next_frontier;
    }

    Ok(namespaces)
}

/// Page through `path`'s immediate children, deciding whether it is a namespace
/// and collecting the sub-directories to descend into.
async fn scan_directory<E, List, ListFut>(
    list_children: &List,
    marker: &str,
    path: String,
    prefix: String,
) -> Result<DirectoryScan, E>
where
    List: Fn(String, Option<String>) -> ListFut,
    ListFut: Future<Output = Result<ChildrenPage, E>>,
{
    let mut token = None;
    let mut is_namespace = false;
    let mut children = Vec::new();

    loop {
        let page = list_children(path.clone(), token).await?;

        for entry in &page.sub_prefixes {
            if entry == marker {
                is_namespace = true;
                continue;
            }
            if entry.starts_with('_') {
                continue;
            }
            children.push((format!("{path}/{entry}"), format!("{prefix}{entry}/")));
        }

        token = page.next_token;
        if token.is_none() {
            break;
        }
    }

    let namespace = is_namespace
        .then(|| prefix.strip_suffix('/').unwrap_or(&prefix).to_string())
        .filter(|namespace| !namespace.is_empty());

    Ok(DirectoryScan {
        namespace,
        children,
    })
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
