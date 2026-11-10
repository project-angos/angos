//! Byte ranges on the wire: `Range` requests and `Content-Range` answers.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use http::header::{HeaderValue, InvalidHeaderValue};

static BYTES_RANGE_PREFIX: &str = "bytes=";
static BYTES_CONTENT_PREFIX: &str = "bytes ";
static UNKNOWN_TOTAL_LENGTH: &str = "*";

/// Every variant is a `416` to the client.
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

/// Reads the `part` of header `value` that `number` spells, as ASCII digits
/// only: `u64::from_str` also takes a leading `+`. An overflow names its part.
fn read_decimal(number: &str, part: &str, value: &str) -> Result<u64, Error> {
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(malformed(value));
    }
    number.parse().map_err(|error| {
        Error::Malformed(format!("Error parsing '{part}' in Range header: {error}"))
    })
}

/// An explicit byte window, as a `Range` or a `Content-Range` names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteWindow {
    pub start: u64,
    pub end: Option<u64>,
}

impl ByteWindow {
    /// The window a chunk declares, which end-5 pins to `^[0-9]+-[0-9]+$`.
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

    /// Parses `start-end` with an optional end (`100-200`, `0-`).
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a value that is not that form, with a
    /// non-numeric bound, or with an end before its start.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let (start, end) = value.split_once('-').ok_or_else(|| malformed(value))?;

        let start = read_decimal(start, "start", value)?;
        if end.is_empty() {
            return Ok(ByteWindow { start, end: None });
        }

        let end = read_decimal(end, "end", value)?;
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

    /// Whether `received` bytes is what this window declared; an open window
    /// declares nothing.
    #[must_use]
    pub fn covers(self, received: u64) -> bool {
        // No `+ 1`: a window ending at `u64::MAX` would overflow it.
        self.end
            .is_none_or(|end| received.checked_sub(1) == Some(end.saturating_sub(self.start)))
    }
}

/// A requested range; a suffix is a length, resolvable only against the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestRange {
    FromTo(ByteWindow),
    Suffix(u64),
}

impl RequestRange {
    /// `None` is a multi-range request, answered with the whole body.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a value without the `bytes=` unit, with a
    /// non-numeric bound, or with an end before its start.
    pub fn parse(value: &str) -> Result<Option<Self>, Error> {
        // Checked: the prefix length can land inside a multi-byte character.
        let Some(range_value) =
            value
                .split_at_checked(BYTES_RANGE_PREFIX.len())
                .and_then(|(prefix, rest)| {
                    prefix
                        .eq_ignore_ascii_case(BYTES_RANGE_PREFIX)
                        .then_some(rest)
                })
        else {
            return Err(malformed(value));
        };

        if range_value.contains(',') {
            return Ok(None);
        }

        if let Some(suffix) = range_value.strip_prefix('-') {
            let suffix = read_decimal(suffix, "suffix", value)?;
            return Ok(Some(RequestRange::Suffix(suffix)));
        }

        ByteWindow::parse(range_value)
            .map(RequestRange::FromTo)
            .map(Some)
    }

    /// Resolves against the body length, clamping to it. `None` ignores the
    /// range, as an empty body has no window.
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

/// The window a `206` serves; an unknown total is spelled `*`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResponseRange {
    pub start: u64,
    pub end: u64,
    pub total_length: Option<u64>,
}

impl ResponseRange {
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
            Some(read_decimal(total_length, "complete length", value)?)
        };

        // A served window always names its last byte, like a chunk's.
        let ByteWindow {
            start,
            end: Some(end),
        } = ByteWindow::parse(window)?
        else {
            return Err(malformed(window));
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

    #[test]
    fn parse_refuses_a_value_that_is_not_ascii() {
        for value in ["bytes€", "byte€=0-5", "€"] {
            assert!(
                matches!(RequestRange::parse(value), Err(Error::Malformed(_))),
                "'{value}' must be refused, not panicked on"
            );
        }
    }

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

    #[test]
    fn a_window_ending_at_the_maximum_offset_is_covered_by_nothing() {
        let window = ByteWindow {
            start: 0,
            end: Some(u64::MAX),
        };

        assert!(!window.covers(0));
        assert!(!window.covers(u64::MAX));
    }

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
            RequestRange::Suffix(4).resolve(10).unwrap(),
            Some(ResponseRange {
                start: 6,
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

    #[test]
    fn response_range_rejects_an_open_ended_window() {
        assert!("bytes 5-/100".parse::<ResponseRange>().is_err());
        assert!("5-10/100".parse::<ResponseRange>().is_err());
    }

    #[test]
    fn numbers_are_ascii_digits_only() {
        assert!(RequestRange::parse("bytes=+5-10").is_err());
        assert!(RequestRange::parse("bytes=-+5").is_err());
        assert!("bytes 5-10/+100".parse::<ResponseRange>().is_err());
    }
}
