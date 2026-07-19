//! Concurrent range-scan of an S3 listing: the single home of the key-ordering
//! model shared by the children scan and the flat whole-store scan.
//!
//! S3 stores keys in raw byte order with no directory concept. Enumerating a
//! large listing through one serial continuation-token chain is slow, so both
//! scans split the key space into disjoint ranges ([`partition_ranges`],
//! delimited by [`split_bounds`]) and walk them concurrently, each range bounded
//! by the slowest of its siblings rather than by their sum.
//!
//! Two scans share that partition with different range semantics:
//!
//! - [`scan_all_children`] groups children on the `/` delimiter and treats each
//!   range as half-open `[lo, hi)`. Split boundaries each end in a character
//!   above `/` (0x2F) so a child `name` stays in the same range as its
//!   `name/...` keys and its `name-...`/`name....` extensions; a boundary never
//!   falls between a prefix and its key family. An object named exactly `hi` is
//!   kept in `[lo, hi)` ([`retain_range`]) because its bare key sorts at the
//!   upper neighbour's exclusive scan start, so only this range ever sees it.
//! - [`scan_all_keys`] streams every raw key and treats each range as
//!   `(lo, hi]`: exclusive on `lo` (S3 `start-after` skips the boundary itself)
//!   and inclusive on `hi`. A key equal to a boundary lands in the range below
//!   it and is skipped by the range above, so the partition neither misses nor
//!   duplicates a key with no delimiter reasoning required.
//!
//! Both stop paging a range early: keys emit in ascending byte order, so once a
//! key at or past `hi` appears no in-range key can follow on a later page.

use std::future;

use futures_util::stream::{self, StreamExt, TryStreamExt};

use angos_s3_client::Backend as S3Backend;

use crate::{KeyStream, error::Error, pagination::paginated};

/// One S3 list page per range scan.
const RANGE_PAGE_SIZE: u16 = 1000;

/// Max concurrent range chains a truncated scan fans out to.
const RANGE_CONCURRENCY: usize = 16;

// children scan

/// Enumerate every immediate child of `prefix` as `(sub_prefixes, objects)`. A
/// directory that fits in one page settles with a single list; a truncated one
/// is rescanned as disjoint per-first-character name ranges walked concurrently,
/// so the enumeration is not one serial token chain.
pub async fn scan_all_children(
    client: &S3Backend,
    prefix: &str,
) -> Result<(Vec<String>, Vec<String>), Error> {
    // Probe: a single page settles any listing that fits in one.
    let (sub_prefixes, objects, next_token) = children_page(client, prefix, None, None).await?;
    if next_token.is_none() {
        return Ok((sub_prefixes, objects));
    }

    let ranges = partition_ranges(None, split_bounds(""), None);
    let parts: Vec<(Vec<String>, Vec<String>)> = stream::iter(ranges)
        .map(|(lo, hi)| async move {
            scan_children_range_split(client, prefix, lo.as_deref(), hi.as_deref()).await
        })
        .buffer_unordered(RANGE_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(merge_parts(parts))
}

/// One page of `prefix`'s children. `after` is a raw child-name suffix the
/// listing starts strictly after (only meaningful on the first page of a chain;
/// a continuation token carries the position afterwards).
async fn children_page(
    client: &S3Backend,
    prefix: &str,
    token: Option<String>,
    after: Option<&str>,
) -> Result<(Vec<String>, Vec<String>, Option<String>), Error> {
    Ok(client
        .list_prefixes(
            prefix,
            "/",
            RANGE_PAGE_SIZE,
            token,
            after.map(str::to_string),
        )
        .await?)
}

/// Drain the child-name range `[lo, hi)` serially: page from `lo`, keep in-range
/// children, and stop paging once a child at or past `hi` appears.
async fn scan_children_range(
    client: &S3Backend,
    prefix: &str,
    lo: Option<&str>,
    hi: Option<&str>,
) -> Result<(Vec<String>, Vec<String>), Error> {
    let (mut sub_prefixes, mut objects, mut next) = children_page(client, prefix, None, lo).await?;
    let mut stop = retain_range(&mut sub_prefixes, &mut objects, lo, hi);
    while let Some(token) = next {
        if stop {
            break;
        }
        let (mut page_prefixes, mut page_objects, page_next) =
            children_page(client, prefix, Some(token), None).await?;
        stop = retain_range(&mut page_prefixes, &mut page_objects, lo, hi);
        sub_prefixes.append(&mut page_prefixes);
        objects.append(&mut page_objects);
        next = page_next;
    }
    Ok((sub_prefixes, objects))
}

/// Like [`scan_children_range`], but a range whose first page truncates is
/// re-split once on the next name character and its sub-ranges scanned
/// concurrently, capping the serial chain a skewed name distribution (most
/// children starting with one character) would otherwise produce.
async fn scan_children_range_split(
    client: &S3Backend,
    prefix: &str,
    lo: Option<&str>,
    hi: Option<&str>,
) -> Result<(Vec<String>, Vec<String>), Error> {
    let (mut sub_prefixes, mut objects, next) = children_page(client, prefix, None, lo).await?;
    let stop = retain_range(&mut sub_prefixes, &mut objects, lo, hi);
    if next.is_none() || stop {
        return Ok((sub_prefixes, objects));
    }
    // The range below the first boundary has no name to anchor sub-splits on;
    // only names below '0' land there, so finish it serially.
    let Some(lo) = lo else {
        return scan_children_range(client, prefix, None, hi).await;
    };

    let ranges = partition_ranges(
        Some(lo.to_string()),
        split_bounds(lo),
        hi.map(str::to_string),
    );
    let parts: Vec<(Vec<String>, Vec<String>)> = stream::iter(ranges)
        .map(|(sub_lo, sub_hi)| async move {
            scan_children_range(client, prefix, sub_lo.as_deref(), sub_hi.as_deref()).await
        })
        .buffer_unordered(RANGE_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(merge_parts(parts))
}

/// Concatenate per-range `(sub_prefixes, objects)` results into one pair.
fn merge_parts(parts: Vec<(Vec<String>, Vec<String>)>) -> (Vec<String>, Vec<String>) {
    let mut sub_prefixes = Vec::new();
    let mut objects = Vec::new();
    for (range_prefixes, range_objects) in parts {
        sub_prefixes.extend(range_prefixes);
        objects.extend(range_objects);
    }
    (sub_prefixes, objects)
}

/// Keep the children in the name range `[lo, hi)`, plus an object named exactly
/// `hi` (see the module docs). Returns whether any child at or past `hi`
/// appeared, which tells the caller to stop paging.
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

// flat scan

/// Stream every key under `prefix`, prefix-relative and unordered. A listing
/// that fits in one page settles from that page; a truncated one fans out into
/// disjoint `(lo, hi]` key ranges walked concurrently, each a serial page chain
/// that stops early once it passes its upper bound.
///
/// Keys sharing a long common prefix (e.g. a single content-addressed subtree)
/// all fall in one range, which then drains as a serial chain; a keyspace that
/// diverges near the front (many namespaces) parallelises across ranges.
pub fn scan_all_keys<'a>(client: &'a S3Backend, prefix: &'a str) -> KeyStream<'a> {
    Box::pin(
        stream::once(async move {
            // Probe: a single page settles any listing that fits in one.
            let (keys, next) = key_page(client, prefix, None, None).await?;
            if next.is_none() {
                let settled: KeyStream<'a> = Box::pin(stream::iter(keys.into_iter().map(Ok)));
                return Ok::<KeyStream<'a>, Error>(settled);
            }
            let ranges = partition_ranges(None, split_bounds(""), None);
            let fanned = stream::iter(ranges)
                .map(move |(lo, hi)| drain_key_range(client, prefix, lo, hi))
                .flatten_unordered(RANGE_CONCURRENCY);
            Ok(Box::pin(fanned) as KeyStream<'a>)
        })
        .try_flatten(),
    )
}

/// One flat listing page under `prefix`. `after` is a prefix-relative key the
/// listing starts strictly after (only meaningful on a chain's first page).
async fn key_page(
    client: &S3Backend,
    prefix: &str,
    token: Option<String>,
    after: Option<&str>,
) -> Result<(Vec<String>, Option<String>), Error> {
    Ok(client
        .list_objects(prefix, RANGE_PAGE_SIZE, token, after.map(str::to_string))
        .await?)
}

/// Stream the key range `(lo, hi]`: page from just after `lo`, then stop at the
/// first key past `hi` (keys ascend, so none in range can follow it).
fn drain_key_range<'a>(
    client: &'a S3Backend,
    prefix: &'a str,
    lo: Option<String>,
    hi: Option<String>,
) -> KeyStream<'a> {
    let pages = paginated(move |token| {
        let after = if token.is_none() { lo.clone() } else { None };
        async move { key_page(client, prefix, token, after.as_deref()).await }
    });
    let bounded = pages.try_take_while(move |key| {
        let within = hi.as_deref().is_none_or(|hi| key.as_str() <= hi);
        future::ready(Ok(within))
    });
    Box::pin(bounded)
}

// shared range partition

/// Boundary names splitting the key space after `base`: one boundary per
/// character keys commonly start with, in ASCII order. Each boundary sorts above
/// `/`, which the children scan relies on (see the module docs).
fn split_bounds(base: &str) -> Vec<String> {
    ('0'..='9')
        .chain('A'..='Z')
        .chain(['_'])
        .chain('a'..='z')
        .map(|c| format!("{base}{c}"))
        .collect()
}

/// The consecutive ranges delimited by `bounds`, from `below` up to `above`
/// (`None` = open end), as `(lo, hi)` pairs. Keys starting outside the boundary
/// alphabet fall into the surrounding range, so the ranges cover every key.
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

    /// How many `(lo, hi]` ranges contain `key` under the flat scan's
    /// exclusive-low, inclusive-high rule.
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
    fn ranges_are_contiguous_and_open_ended() {
        let ranges = partition_ranges(None, split_bounds(""), None);
        assert_eq!(ranges.first().map(|r| &r.0), Some(&None));
        assert_eq!(ranges.last().map(|r| &r.1), Some(&None));
        assert!(ranges.windows(2).all(|w| w[0].1 == w[1].0));
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
