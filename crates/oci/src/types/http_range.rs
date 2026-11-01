//! Byte ranges on the wire: the window a client asks for in a `Range` header,
//! and the window a `206` reports in `Content-Range`. Parsing and formatting
//! both live here so the registry, the request layer, and the upstream client
//! read and write one grammar.

use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;
use std::str::FromStr;

use http::header::{HeaderValue, InvalidHeaderValue};

static BYTES_RANGE_PREFIX: &str = "bytes=";
static BYTES_CONTENT_PREFIX: &str = "bytes ";
/// The `Content-Range` complete length of a body whose total size is unknown.
static UNKNOWN_TOTAL_LENGTH: &str = "*";

/// Why a range could not be read or served. Every variant is a `416` to the
/// client, so callers map it onto their own not-satisfiable error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Malformed(String),
    #[error("the requested range cannot be satisfied")]
    Unsatisfiable,
}

fn malformed(value: &str) -> Error {
    Error::Malformed(format!("Invalid Range header format: '{value}'"))
}

/// An unsigned decimal of ASCII digits only: `u64::from_str` also takes a
/// leading `+`, which no range grammar allows.
struct Decimal(u64);

impl FromStr for Decimal {
    /// `None` when the value is not all digits, the overflow otherwise.
    type Err = Option<ParseIntError>;

    fn from_str(number: &str) -> Result<Self, Self::Err> {
        if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(None);
        }
        number.parse().map(Decimal).map_err(Some)
    }
}

impl Decimal {
    /// Reads the `part` of header `value` that `number` spells. A value that is
    /// not digits is malformed as a whole; an overflow names its part.
    fn read(number: &str, part: &str, value: &str) -> Result<u64, Error> {
        match number.parse() {
            Ok(Decimal(number)) => Ok(number),
            Err(None) => Err(malformed(value)),
            Err(Some(error)) => Err(Error::Malformed(format!(
                "Error parsing '{part}' in Range header: {error}"
            ))),
        }
    }
}

/// Splits the `bytes=` unit off a `Range` value. Checked rather than
/// positional: the prefix length can land inside a multi-byte character, which
/// `split_at` panics on.
fn strip_bytes_prefix(value: &str) -> Option<&str> {
    let (prefix, range_value) = value.split_at_checked(BYTES_RANGE_PREFIX.len())?;

    prefix
        .eq_ignore_ascii_case(BYTES_RANGE_PREFIX)
        .then_some(range_value)
}

/// An explicit byte window: where it starts, and where it ends when an end is
/// named. The `Range` header's `start-end` form and a `Content-Range` are the
/// same window, so both parse to this.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteWindow {
    pub start: u64,
    pub end: Option<u64>,
}

impl ByteWindow {
    /// The inclusive bounds of the window a chunk declares, which end-5 spells
    /// `<start>-<end>` and pins to `^[0-9]+-[0-9]+$`. Both ends are required,
    /// so the open-ended form only a `Range` may use is malformed here, as is
    /// the `bytes=` unit that belongs to `Range` alone.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a value that is not that form, with a
    /// non-numeric bound, without an end, or with an end before its start.
    pub fn chunk_bounds(value: &str) -> Result<Self, Error> {
        match Self::parse(value)? {
            ByteWindow { end: None, .. } => Err(malformed(value)),
            window => Ok(window),
        }
    }

    /// Parses a `start-end` value where `end` is optional (`100-200`, `0-`),
    /// which is the explicit form of a `Range`. A `Content-Range` requires both
    /// ends and goes through [`Self::chunk_bounds`].
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a value that is not that form, with a
    /// non-numeric bound, or with an end before its start.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let (start, end) = value.split_once('-').ok_or_else(|| malformed(value))?;

        let start = Decimal::read(start, "start", value)?;
        if end.is_empty() {
            return Ok(ByteWindow { start, end: None });
        }

        let end = Decimal::read(end, "end", value)?;
        if start > end {
            return Err(Error::Malformed(format!(
                "Invalid Range header: start ({start}) > end ({end})"
            )));
        }

        Ok(ByteWindow {
            start,
            end: Some(end),
        })
    }

    /// Whether `received` bytes is what this window declared. Only a `Range`
    /// window names no end, and it declares nothing to disagree with.
    #[must_use]
    pub fn covers(self, received: u64) -> bool {
        // Compared without the `+ 1` a window ending at `u64::MAX` would
        // overflow: it declares one byte more than a `u64` can count, so
        // nothing covers it.
        self.end
            .is_none_or(|end| received.checked_sub(1) == Some(end.saturating_sub(self.start)))
    }

    /// Whether this window resumes at `committed`: a gap or a rewind is an
    /// out-of-order chunk, not a digest mismatch.
    #[must_use]
    pub fn starts_at(self, committed: u64) -> bool {
        self.start == committed
    }
}

/// A single byte range as a client asks for it: an explicit window, or a suffix
/// length anchored to the end of the body. A suffix is its own variant because
/// its number is a length rather than a position, and cannot be resolved to a
/// window without knowing how long the body is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestRange {
    FromTo(ByteWindow),
    Suffix(u64),
}

impl RequestRange {
    /// Parses a `Range` header value. `None` is a multi-range request, which is
    /// answered with the whole body rather than refused.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a value without the `bytes=` unit, with a
    /// non-numeric bound, or with an end before its start.
    pub fn parse(value: &str) -> Result<Option<Self>, Error> {
        let Some(range_value) = strip_bytes_prefix(value) else {
            return Err(malformed(value));
        };

        if range_value.contains(',') {
            return Ok(None);
        }

        if let Some(suffix) = range_value.strip_prefix('-') {
            let suffix = Decimal::read(suffix, "suffix", value)?;
            return Ok(Some(RequestRange::Suffix(suffix)));
        }

        ByteWindow::parse(range_value)
            .map(RequestRange::FromTo)
            .map(Some)
    }

    /// Resolves the requested window against a known body length, clamping an
    /// end past the last byte and a suffix longer than the body. `None` means
    /// the range is to be ignored, as an empty body has no window to serve.
    ///
    /// # Errors
    ///
    /// [`Error::Unsatisfiable`] for a start at or past the body length, an
    /// inverted window, or a zero-length suffix.
    pub fn resolve(self, total_length: u64) -> Result<Option<ResponseRange>, Error> {
        if total_length == 0 {
            return Ok(None);
        }

        let last_byte = total_length - 1;
        let (start, end) = match self {
            RequestRange::FromTo(ByteWindow { start, end }) => {
                if start >= total_length {
                    return Err(Error::Unsatisfiable);
                }

                let end = end.unwrap_or(last_byte).min(last_byte);
                if end < start {
                    return Err(Error::Unsatisfiable);
                }

                (start, end)
            }
            RequestRange::Suffix(suffix_length) => {
                if suffix_length == 0 {
                    return Err(Error::Unsatisfiable);
                }

                let length = suffix_length.min(total_length);
                (total_length - length, last_byte)
            }
        };

        Ok(Some(ResponseRange {
            start,
            end,
            total_length: Some(total_length),
        }))
    }
}

impl Display for RequestRange {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RequestRange::FromTo(ByteWindow {
                start,
                end: Some(end),
            }) => write!(formatter, "{BYTES_RANGE_PREFIX}{start}-{end}"),
            RequestRange::FromTo(ByteWindow { start, end: None }) => {
                write!(formatter, "{BYTES_RANGE_PREFIX}{start}-")
            }
            RequestRange::Suffix(length) => write!(formatter, "{BYTES_RANGE_PREFIX}-{length}"),
        }
    }
}

impl TryFrom<RequestRange> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(range: RequestRange) -> Result<Self, Self::Error> {
        HeaderValue::try_from(range.to_string())
    }
}

/// The window a `206` serves. `total_length` is absent when the complete length
/// is unknown, which `Content-Range` spells `*`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResponseRange {
    pub start: u64,
    pub end: u64,
    pub total_length: Option<u64>,
}

impl ResponseRange {
    /// The number of bytes served, `end` being inclusive.
    #[must_use]
    pub fn length(self) -> u64 {
        self.end - self.start + 1
    }
}

impl FromStr for ResponseRange {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let window = value
            .strip_prefix(BYTES_CONTENT_PREFIX)
            .ok_or_else(|| malformed(value))?;
        let (window, total_length) = window.split_once('/').ok_or_else(|| malformed(value))?;
        let total_length = if total_length == UNKNOWN_TOTAL_LENGTH {
            None
        } else {
            Some(Decimal::read(total_length, "complete length", value)?)
        };

        // A served window always names its last byte, like a chunk's.
        let (start, end) = match ByteWindow::parse(window)? {
            ByteWindow {
                start,
                end: Some(end),
            } => (start, end),
            ByteWindow { end: None, .. } => return Err(malformed(window)),
        };

        Ok(ResponseRange {
            start,
            end,
            total_length,
        })
    }
}

impl Display for ResponseRange {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{BYTES_CONTENT_PREFIX}{}-{}/",
            self.start, self.end
        )?;
        match self.total_length {
            Some(total_length) => write!(formatter, "{total_length}"),
            None => write!(formatter, "{UNKNOWN_TOTAL_LENGTH}"),
        }
    }
}

impl TryFrom<ResponseRange> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(range: ResponseRange) -> Result<Self, Self::Error> {
        HeaderValue::try_from(range.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reads_every_requested_form() {
        assert_eq!(
            RequestRange::parse("bytes=0-499").unwrap(),
            Some(RequestRange::FromTo(ByteWindow {
                start: 0,
                end: Some(499),
            }))
        );
        assert_eq!(
            RequestRange::parse("bytes=5-").unwrap(),
            Some(RequestRange::FromTo(ByteWindow {
                start: 5,
                end: None,
            }))
        );
        assert_eq!(
            RequestRange::parse("bytes=-64").unwrap(),
            Some(RequestRange::Suffix(64))
        );
    }

    /// A multi-range request is served whole rather than refused.
    #[test]
    fn parse_ignores_multiple_ranges() {
        assert_eq!(RequestRange::parse("bytes=0-10,20-30").unwrap(), None);
    }

    #[test]
    fn parse_rejects_a_value_without_the_bytes_unit() {
        assert!(matches!(
            RequestRange::parse("100-200"),
            Err(Error::Malformed(_))
        ));
    }

    /// The unit is stripped by byte offset, which lands inside a multi-byte
    /// character here. A header value never carries one, but this is a `pub`
    /// parser, and refusing beats panicking.
    #[test]
    fn parse_refuses_a_value_that_is_not_ascii() {
        for value in ["bytes€", "byte€=0-5", "€"] {
            assert!(
                matches!(RequestRange::parse(value), Err(Error::Malformed(_))),
                "'{value}' must be refused, not panicked on"
            );
        }
    }

    /// end-5 pins a chunk's `Content-Range` to `^[0-9]+-[0-9]+$`: both ends are
    /// required, the `bytes=` unit is the `Range` header's, and neither the
    /// open-ended form nor a prefixed one is that grammar. `parse` stays
    /// permissive because a `Range` may spell `5-`.
    #[test]
    fn chunk_bounds_pin_the_grammar_end_5_defines() {
        assert_eq!(
            ByteWindow::chunk_bounds("0-41").unwrap(),
            ByteWindow {
                start: 0,
                end: Some(41)
            }
        );
        for value in ["bytes=0-41", "0-", "41"] {
            assert!(
                matches!(ByteWindow::chunk_bounds(value), Err(Error::Malformed(_))),
                "'{value}' is not the chunk grammar"
            );
        }

        assert_eq!(
            ByteWindow::parse("0-").unwrap(),
            ByteWindow {
                start: 0,
                end: None,
            }
        );
    }

    /// A window ending at the maximum offset declares one byte more than a
    /// `u64` can count, so no length covers it. The header is client-supplied,
    /// and computing the length by adding one would overflow.
    #[test]
    fn a_window_ending_at_the_maximum_offset_is_covered_by_nothing() {
        let window = ByteWindow {
            start: 0,
            end: Some(u64::MAX),
        };

        assert!(!window.covers(0));
        assert!(!window.covers(u64::MAX));
    }

    /// A chunk that names its last byte declares how many bytes it carries. The
    /// open-ended window only a `Range` can produce declares nothing.
    #[test]
    fn chunk_range_covers_only_the_length_it_declared() {
        let window = ByteWindow {
            start: 10,
            end: Some(19),
        };
        assert!(window.covers(10));
        assert!(!window.covers(9));
        assert!(!window.covers(11));

        let open = ByteWindow {
            start: 10,
            end: None,
        };
        assert!(open.covers(0));
        assert!(open.covers(4096));
    }

    /// A chunk resumes a session only at the offset the session stands at.
    #[test]
    fn chunk_range_starts_only_where_the_session_stands() {
        let window = ByteWindow {
            start: 10,
            end: Some(19),
        };
        assert!(window.starts_at(10));
        assert!(!window.starts_at(9));
        assert!(!window.starts_at(11));
    }

    #[test]
    fn parse_rejects_an_inverted_window() {
        let error = ByteWindow::parse("500-499").expect_err("inverted window must be refused");
        assert!(
            error.to_string().contains("start (500) > end (499)"),
            "the error must name both bounds, got: {error}"
        );
    }

    #[test]
    fn request_range_round_trips_through_its_header_value() {
        for range in [
            RequestRange::FromTo(ByteWindow {
                start: 5,
                end: Some(10),
            }),
            RequestRange::FromTo(ByteWindow {
                start: 5,
                end: None,
            }),
            RequestRange::Suffix(64),
        ] {
            let header =
                HeaderValue::try_from(range).expect("a range must format as a header value");
            assert_eq!(
                RequestRange::parse(header.to_str().unwrap()).unwrap(),
                Some(range)
            );
        }
    }

    #[test]
    fn resolve_clamps_to_the_body_length() {
        assert_eq!(
            RequestRange::FromTo(ByteWindow {
                start: 4,
                end: Some(99),
            })
            .resolve(10)
            .unwrap(),
            Some(ResponseRange {
                start: 4,
                end: 9,
                total_length: Some(10)
            })
        );
        assert_eq!(
            RequestRange::FromTo(ByteWindow {
                start: 4,
                end: None,
            })
            .resolve(10)
            .unwrap(),
            Some(ResponseRange {
                start: 4,
                end: 9,
                total_length: Some(10)
            })
        );
        assert_eq!(
            RequestRange::Suffix(99).resolve(10).unwrap(),
            Some(ResponseRange {
                start: 0,
                end: 9,
                total_length: Some(10)
            })
        );
    }

    #[test]
    fn resolve_reads_a_suffix_from_the_end() {
        assert_eq!(
            RequestRange::Suffix(4).resolve(10).unwrap(),
            Some(ResponseRange {
                start: 6,
                end: 9,
                total_length: Some(10)
            })
        );
    }

    #[test]
    fn resolve_ignores_ranges_for_an_empty_body() {
        assert!(matches!(
            RequestRange::FromTo(ByteWindow {
                start: 0,
                end: None,
            })
            .resolve(0),
            Ok(None)
        ));
        assert!(matches!(RequestRange::Suffix(1).resolve(0), Ok(None)));
    }

    #[test]
    fn resolve_refuses_a_window_outside_the_body() {
        assert!(matches!(
            RequestRange::FromTo(ByteWindow {
                start: 10,
                end: None,
            })
            .resolve(10),
            Err(Error::Unsatisfiable)
        ));
        assert!(matches!(
            RequestRange::Suffix(0).resolve(10),
            Err(Error::Unsatisfiable)
        ));
    }

    #[test]
    fn response_range_round_trips_through_its_header_value() {
        for range in [
            ResponseRange {
                start: 5,
                end: 10,
                total_length: Some(100),
            },
            ResponseRange {
                start: 0,
                end: 9,
                total_length: None,
            },
        ] {
            let header =
                HeaderValue::try_from(range).expect("a range must format as a header value");
            assert_eq!(
                header.to_str().unwrap().parse::<ResponseRange>().unwrap(),
                range
            );
        }
    }

    #[test]
    fn response_range_spells_an_unknown_total_length() {
        let range = ResponseRange {
            start: 5,
            end: 10,
            total_length: None,
        };
        assert_eq!(range.to_string(), "bytes 5-10/*");
    }

    /// An upstream `Content-Range` that names no last byte cannot describe a served
    /// window, so it must not be forwarded as one.
    #[test]
    fn response_range_rejects_an_open_ended_window() {
        assert!("bytes 5-/100".parse::<ResponseRange>().is_err());
        assert!("5-10/100".parse::<ResponseRange>().is_err());
    }

    /// `u64::from_str` takes a leading `+`; no range grammar does.
    #[test]
    fn numbers_are_ascii_digits_only() {
        assert!(RequestRange::parse("bytes=+5-10").is_err());
        assert!(RequestRange::parse("bytes=-+5").is_err());
        assert!("bytes 5-10/+100".parse::<ResponseRange>().is_err());
    }
}
