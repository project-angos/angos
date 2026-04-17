/// Iterates through all pages from a paginated source, calling `process` for each page of items.
///
/// `fetch` receives the current continuation token and returns the next page plus an optional
/// next token. When the token is `None`, iteration stops.
pub async fn for_each_page<T, E, Fetch, FetchFut, Process, ProcessFut>(
    mut fetch: Fetch,
    mut process: Process,
) -> Result<(), E>
where
    Fetch: FnMut(Option<String>) -> FetchFut,
    FetchFut: std::future::Future<Output = Result<(Vec<T>, Option<String>), E>>,
    Process: FnMut(Vec<T>) -> ProcessFut,
    ProcessFut: std::future::Future<Output = Result<(), E>>,
{
    let mut marker = None;
    loop {
        let (items, next_marker) = fetch(marker).await?;
        process(items).await?;
        if next_marker.is_none() {
            break;
        }
        marker = next_marker;
    }
    Ok(())
}

/// Collects all items from a paginated source into a single `Vec`.
///
/// `fetch` receives the current continuation token and returns the next page plus an optional
/// next token. When the token is `None`, iteration stops.
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

pub fn paginate<T: Clone + ToString>(
    items: &[T],
    n: u16,
    continuation_token: Option<&str>,
) -> (Vec<T>, Option<String>) {
    let start_idx = continuation_token
        .and_then(|token| items.iter().position(|item| item.to_string() == token))
        .map_or(0, |pos| pos + 1);

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

    let end_idx = (start_idx + n as usize).min(items.len());
    let result = items[start_idx..end_idx].to_vec();

    let next_token = if end_idx < items.len() {
        result.last().map(ToString::to_string)
    } else {
        None
    };

    (result, next_token)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
