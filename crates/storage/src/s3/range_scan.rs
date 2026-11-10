//! Whole-listing scans that split S3's byte-ordered key space into disjoint
//! ranges walked concurrently, each paging only until it passes its bound.
//!
//! The children scan uses `[lo, hi)` ranges whose boundaries all sort above
//! `/`, so a child `name` stays with its `name/...` keys; an object named
//! exactly `hi` is kept by the range below, the only one that sees it. The flat
//! scan uses `(lo, hi]`, matching S3's exclusive `start-after`.

use std::future;

use futures_util::stream::{self, StreamExt, TryStreamExt};

use crate::error::Error;
use crate::pagination::paginated;
use crate::s3::Backend;
use crate::{Children, ChildrenPage, KeyStream, ObjectStore};

const RANGE_PAGE_SIZE: u16 = 1000;

/// Every immediate child of `prefix`; a truncated first page fans out into
/// per-first-character ranges.
pub async fn scan_all_children(
    backend: &Backend,
    prefix: &str,
    concurrency: usize,
) -> Result<Children, Error> {
    let ChildrenPage {
        sub_prefixes,
        objects,
        next_token,
    } = children_page(backend, prefix, None, None).await?;
    if next_token.is_none() {
        return Ok(Children {
            sub_prefixes,
            objects,
        });
    }

    let ranges = partition_ranges(None, split_bounds(""), None);
    let parts: Vec<Children> = stream::iter(ranges)
        .map(|(lo, hi)| async move {
            scan_children_range_split(backend, prefix, lo.as_deref(), hi.as_deref(), concurrency)
                .await
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
    Ok(merge_parts(parts))
}

/// `after` only applies to a chain's first page.
async fn children_page(
    backend: &Backend,
    prefix: &str,
    token: Option<String>,
    after: Option<&str>,
) -> Result<ChildrenPage, Error> {
    let listing = backend
        .client
        .list_prefixes(prefix, RANGE_PAGE_SIZE, token, after.map(str::to_string))
        .await?;
    Ok(ChildrenPage {
        sub_prefixes: listing.prefixes,
        objects: listing.objects,
        next_token: listing.next_token,
    })
}

/// Drains the child-name range `[lo, hi)` serially.
async fn scan_children_range(
    backend: &Backend,
    prefix: &str,
    lo: Option<&str>,
    hi: Option<&str>,
) -> Result<Children, Error> {
    let ChildrenPage {
        mut sub_prefixes,
        mut objects,
        next_token: mut next,
    } = children_page(backend, prefix, None, lo).await?;
    let mut stop = retain_range(&mut sub_prefixes, &mut objects, lo, hi);
    while let Some(token) = next {
        if stop {
            break;
        }
        let mut page = children_page(backend, prefix, Some(token), None).await?;
        stop = retain_range(&mut page.sub_prefixes, &mut page.objects, lo, hi);
        sub_prefixes.append(&mut page.sub_prefixes);
        objects.append(&mut page.objects);
        next = page.next_token;
    }
    Ok(Children {
        sub_prefixes,
        objects,
    })
}

/// [`scan_children_range`], but a range whose first page truncates is re-split
/// once on the next character, for skewed names sharing a first character.
async fn scan_children_range_split(
    backend: &Backend,
    prefix: &str,
    lo: Option<&str>,
    hi: Option<&str>,
    concurrency: usize,
) -> Result<Children, Error> {
    let ChildrenPage {
        mut sub_prefixes,
        mut objects,
        next_token,
    } = children_page(backend, prefix, None, lo).await?;
    let stop = retain_range(&mut sub_prefixes, &mut objects, lo, hi);
    if next_token.is_none() || stop {
        return Ok(Children {
            sub_prefixes,
            objects,
        });
    }
    // The range below the first boundary has no name to anchor sub-splits on;
    // only names below '0' land there, so finish it serially.
    let Some(lo) = lo else {
        return scan_children_range(backend, prefix, None, hi).await;
    };

    let ranges = partition_ranges(
        Some(lo.to_string()),
        split_bounds(lo),
        hi.map(str::to_string),
    );
    let parts: Vec<Children> = stream::iter(ranges)
        .map(|(sub_lo, sub_hi)| async move {
            scan_children_range(backend, prefix, sub_lo.as_deref(), sub_hi.as_deref()).await
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
    Ok(merge_parts(parts))
}

fn merge_parts(parts: Vec<Children>) -> Children {
    let mut merged = Children::default();
    for part in parts {
        merged.sub_prefixes.extend(part.sub_prefixes);
        merged.objects.extend(part.objects);
    }
    merged
}

/// Keeps `[lo, hi)` plus an object named exactly `hi`. Returns whether any child
/// at or past `hi` appeared, which ends the paging.
fn retain_range(
    sub_prefixes: &mut Vec<String>,
    objects: &mut Vec<String>,
    lo: Option<&str>,
    hi: Option<&str>,
) -> bool {
    let from_lo = |name: &str| lo.is_none_or(|lo| name >= lo);
    let below_hi = |name: &str| hi.is_none_or(|hi| name < hi);
    let saw_hi = hi.is_some_and(|hi| {
        sub_prefixes
            .iter()
            .chain(objects.iter())
            .any(|name| name.as_str() >= hi)
    });
    sub_prefixes.retain(|name| from_lo(name) && below_hi(name));
    objects.retain(|name| from_lo(name) && (below_hi(name) || hi == Some(name.as_str())));
    saw_hi
}

/// Every key under `prefix`, unordered; a truncated first page fans out into
/// `(lo, hi]` ranges. Keys sharing a long common prefix still drain serially.
pub fn scan_all_keys<'a>(
    backend: &'a Backend,
    prefix: &'a str,
    concurrency: usize,
) -> KeyStream<'a> {
    Box::pin(
        stream::once(async move {
            let page = backend.list(prefix, RANGE_PAGE_SIZE, None).await?;
            if page.next_token.is_none() {
                let settled: KeyStream<'a> = Box::pin(stream::iter(page.items.into_iter().map(Ok)));
                return Ok::<KeyStream<'a>, Error>(settled);
            }
            let ranges = partition_ranges(None, split_bounds(""), None);
            let fanned = stream::iter(ranges)
                .map(move |(lo, hi)| drain_key_range(backend, prefix, lo, hi))
                .flatten_unordered(concurrency);
            Ok(Box::pin(fanned) as KeyStream<'a>)
        })
        .try_flatten(),
    )
}

/// Streams the key range `(lo, hi]`.
fn drain_key_range<'a>(
    backend: &'a Backend,
    prefix: &'a str,
    lo: Option<String>,
    hi: Option<String>,
) -> KeyStream<'a> {
    let pages =
        paginated(move |token| backend.list_after(prefix, RANGE_PAGE_SIZE, token, lo.clone()));
    let bounded = pages.try_take_while(move |key| {
        let within = hi.as_deref().is_none_or(|hi| key.as_str() <= hi);
        future::ready(Ok(within))
    });
    Box::pin(bounded)
}

/// One boundary per character keys commonly start with, in ASCII order, each
/// above `/`.
fn split_bounds(base: &str) -> Vec<String> {
    ('0'..='9')
        .chain('A'..='Z')
        .chain(['_'])
        .chain('a'..='z')
        .map(|c| format!("{base}{c}"))
        .collect()
}

/// The consecutive `(lo, hi)` ranges delimited by `bounds`, `None` being open.
fn partition_ranges(
    below: Option<String>,
    bounds: Vec<String>,
    above: Option<String>,
) -> Vec<(Option<String>, Option<String>)> {
    let mut lows = vec![below];
    lows.extend(bounds.iter().cloned().map(Some));
    let mut highs: Vec<Option<String>> = bounds.into_iter().map(Some).collect();
    highs.push(above);
    lows.into_iter().zip(highs).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// How many `(lo, hi]` ranges contain `key`.
    fn hits(ranges: &[(Option<String>, Option<String>)], key: &str) -> usize {
        ranges
            .iter()
            .filter(|(lo, hi)| {
                lo.as_deref().is_none_or(|lo| key > lo) && hi.as_deref().is_none_or(|hi| key <= hi)
            })
            .count()
    }

    #[test]
    fn split_bounds_ascend_under_a_common_prefix() {
        let bounds = split_bounds("x");
        assert!(bounds.iter().all(|b| b.starts_with('x')));
        assert!(bounds.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn every_key_lands_in_exactly_one_range() {
        let ranges = partition_ranges(None, split_bounds(""), None);
        // Boundary values, keys just around them, deep keys, and bytes outside
        // the boundary alphabet on both ends.
        let samples = [
            "",
            "!bang",
            "0",
            "0a",
            "9",
            "A",
            "Z",
            "_",
            "a",
            "m",
            "z",
            "za",
            "z~",
            "sha256/00/data",
            "sha256/ff/data",
            "~tilde",
        ];
        for key in samples {
            assert_eq!(
                hits(&ranges, key),
                1,
                "{key:?} must land in exactly one range"
            );
        }
    }
}
