//! Concurrent range-scan of an S3 "directory": the single home of the
//! child-name ordering model.
//!
//! S3 stores keys in raw byte order with no directory concept;
//! [`angos_s3_client::Backend::list_prefixes`] groups children on the `/`
//! delimiter and returns them in that key order. Enumerating a large directory
//! through one serial continuation-token chain is slow, so [`scan_all_children`]
//! splits the child-name space into disjoint half-open ranges and walks them
//! concurrently.
//!
//! ## Why the ranges neither miss nor duplicate a child
//!
//! - **Split boundaries** ([`child_split_bounds`]) each end in a character that
//!   sorts above `/` (0x2F). A child `name` therefore stays in the same range as
//!   its `name/...` keys and its `name-...`/`name....` extensions (whose next
//!   byte sorts below `/`): a boundary never falls between a prefix and its key
//!   family.
//! - **Upper bound** ([`retain_range`]): a range `[lo, hi)` is half-open on
//!   names, but an object named exactly `hi` is kept here, because its bare key
//!   sorts at the upper neighbour's exclusive scan start, so only this range
//!   ever sees it.
//! - **Early stop**: children emit grouped by leading bytes, so once a name at
//!   or past `hi` appears no in-range child can follow on a later page; the scan
//!   stops paging that range.

use futures_util::stream::{self, StreamExt, TryStreamExt};

use angos_s3_client::Backend as S3Backend;

use crate::error::Error;

/// One S3 list page per range scan.
const CHILDREN_PAGE_SIZE: u16 = 1000;

/// Max concurrent range chains a truncated directory scan fans out to.
const CHILDREN_RANGE_CONCURRENCY: usize = 16;

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

    let ranges = child_ranges(None, child_split_bounds(""), None);
    let parts: Vec<(Vec<String>, Vec<String>)> = stream::iter(ranges)
        .map(|(lo, hi)| async move {
            scan_children_range_split(client, prefix, lo.as_deref(), hi.as_deref()).await
        })
        .buffer_unordered(CHILDREN_RANGE_CONCURRENCY)
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
            CHILDREN_PAGE_SIZE,
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

    let ranges = child_ranges(
        Some(lo.to_string()),
        child_split_bounds(lo),
        hi.map(str::to_string),
    );
    let parts: Vec<(Vec<String>, Vec<String>)> = stream::iter(ranges)
        .map(|(sub_lo, sub_hi)| async move {
            scan_children_range(client, prefix, sub_lo.as_deref(), sub_hi.as_deref()).await
        })
        .buffer_unordered(CHILDREN_RANGE_CONCURRENCY)
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

/// Boundary names splitting the child namespace after `base`: one boundary per
/// character children commonly start with, in ASCII order. See the module docs
/// for why every boundary sorts above `/`.
fn child_split_bounds(base: &str) -> Vec<String> {
    ('0'..='9')
        .chain('A'..='Z')
        .chain(['_'])
        .chain('a'..='z')
        .map(|c| format!("{base}{c}"))
        .collect()
}

/// The half-open child-name ranges delimited by `bounds`, from `below` up to
/// `above` (`None` = open end). Children starting outside the boundary alphabet
/// fall into the surrounding range, so the ranges cover every name.
fn child_ranges(
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
