/// One-deep lookahead over paginated results.
///
/// Fetches the next page concurrently with processing the current one via
/// `tokio::join!`, hiding the LIST round-trip behind per-page work.
///
/// `$first` is the already-awaited first page. On each iteration the page is
/// destructured by `$page`, which must produce two expressions: a *process*
/// future (`Result<(), E>`) and an optional *next-page* future
/// (`Option<impl Future<Output = Result<Page, E>>>`).
///
/// ```ignore
/// pipeline_pages!(
///     store.list(1000, None).await?,
///     |(items, next_token)| (
///         async { /* process items */ Ok::<(), Error>(()) },
///         next_token.map(|t| store.list(1000, Some(t))),
///     )
/// )?;
/// ```
macro_rules! pipeline_pages {
    ($first:expr, |$page:pat_param| ($process:expr, $next:expr $(,)?)) => {
        '__pipeline: {
            let mut __current = $first;
            loop {
                let $page = __current;
                let __next_fut = $next;
                let __process_fut = $process;
                if let Some(__next_fut) = __next_fut {
                    let (__process_res, __next_res) = tokio::join!(__process_fut, __next_fut);
                    if let Err(__e) = __process_res {
                        break '__pipeline Err(__e.into());
                    }
                    match __next_res {
                        Ok(__page) => __current = __page,
                        Err(__e) => break '__pipeline Err(__e.into()),
                    }
                } else {
                    break '__pipeline __process_fut.await;
                }
            }
        }
    };
}

pub(crate) use pipeline_pages;

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
