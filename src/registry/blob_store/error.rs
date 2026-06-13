use std::{fmt, io, num::TryFromIntError, string::FromUtf8Error};

use sha2::digest::common::hazmat::DeserializeStateError;

use crate::oci;
use angos_s3_client as s3_client;
use angos_tx_engine::StorageError;

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    DataStore(s3_client::Error),
    HashSerialization(String),
    JSONSerialization(String),
    StorageBackend(String),
    UploadBodyRead(String),
    UploadBodySize { expected: u64, actual: u64 },
    InvalidFormat(String),
    UploadNotFound,
    BlobNotFound,
    ReferenceNotFound,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::DataStore(err) => write!(f, "Data store error: {err}"),
            Error::HashSerialization(e) => write!(f, "Hash state management error: {e}"),
            Error::JSONSerialization(e) => write!(f, "JSON serialization error: {e}"),
            Error::StorageBackend(e) => write!(f, "Storage backend error: {e}"),
            Error::UploadBodyRead(e) => write!(f, "Upload body read error: {e}"),
            Error::UploadBodySize { expected, actual } => {
                write!(
                    f,
                    "Upload body size mismatch: expected {expected} bytes, read {actual}"
                )
            }
            Error::InvalidFormat(e) => write!(f, "Reference format error: {e}"),
            Error::UploadNotFound => write!(f, "Upload not found"),
            Error::BlobNotFound => write!(f, "Blob not found"),
            Error::ReferenceNotFound => write!(f, "Reference not found"),
        }
    }
}

impl From<s3_client::Error> for Error {
    fn from(err: s3_client::Error) -> Self {
        match err {
            s3_client::Error::NotFound(_) => Error::ReferenceNotFound,
            _ => Error::DataStore(err),
        }
    }
}

impl From<DeserializeStateError> for Error {
    fn from(e: DeserializeStateError) -> Self {
        Error::HashSerialization(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::JSONSerialization(e.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Error::InvalidFormat(e.to_string())
    }
}

// FS and generic IO errors

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::NotFound {
            Error::ReferenceNotFound
        } else {
            Error::StorageBackend(e.to_string())
        }
    }
}

impl From<TryFromIntError> for Error {
    fn from(e: TryFromIntError) -> Self {
        Error::InvalidFormat(e.to_string())
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(e: chrono::format::ParseError) -> Self {
        Error::InvalidFormat(e.to_string())
    }
}

impl From<oci::Error> for Error {
    fn from(e: oci::Error) -> Self {
        Error::InvalidFormat(e.to_string())
    }
}

/// Blanket conversion from a storage-layer error into a blob-store error.
///
/// # Trap: `StorageError::NotFound` maps to `Error::ReferenceNotFound`
///
/// This mapping is intentionally conservative: it assumes the missing resource
/// is a *reference* (manifest link, tag, etc.) because that is the most common
/// hot path. It is **wrong** for blob and upload call sites, where the correct
/// variants are [`Error::BlobNotFound`] and [`Error::UploadNotFound`]
/// respectively.
///
/// Any call site that operates on blob or upload keys **must** intercept
/// `StorageError::NotFound` explicitly before relying on `?` to reach this
/// impl, otherwise the caller receives a misleading `ReferenceNotFound` error.
/// Search for `StorageError::NotFound =>` in the blob-store modules to see how
/// the correct pattern is applied.
impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        match e {
            // Fallback mapping, correct only for reference/manifest paths.
            // Blob and upload call sites must match this variant explicitly
            // before using `?`; see the doc comment on this impl for details.
            StorageError::NotFound => Error::ReferenceNotFound,
            StorageError::PreconditionFailed => {
                Error::StorageBackend("Precondition failed".to_string())
            }
            StorageError::Backend(msg) => Error::StorageBackend(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::digest::common::hazmat::DeserializeStateError;

    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            format!(
                "{}",
                Error::DataStore(s3_client::Error::Io("IO error".to_string()))
            ),
            "Data store error: IO error: IO error"
        );
        assert_eq!(
            format!("{}", Error::HashSerialization("Invalid state".to_string())),
            "Hash state management error: Invalid state"
        );
        assert_eq!(
            format!("{}", Error::JSONSerialization("Parse error".to_string())),
            "JSON serialization error: Parse error"
        );
        assert_eq!(
            format!(
                "{}",
                Error::StorageBackend("S3 connection failed".to_string())
            ),
            "Storage backend error: S3 connection failed"
        );
        assert_eq!(
            format!("{}", Error::UploadBodyRead("connection reset".to_string())),
            "Upload body read error: connection reset"
        );
        assert_eq!(
            format!(
                "{}",
                Error::UploadBodySize {
                    expected: 10,
                    actual: 8
                }
            ),
            "Upload body size mismatch: expected 10 bytes, read 8"
        );
        assert_eq!(
            format!("{}", Error::InvalidFormat("Bad UTF-8".to_string())),
            "Reference format error: Bad UTF-8"
        );
        assert_eq!(format!("{}", Error::UploadNotFound), "Upload not found");
        assert_eq!(format!("{}", Error::BlobNotFound), "Blob not found");
        assert_eq!(
            format!("{}", Error::ReferenceNotFound),
            "Reference not found"
        );
    }

    #[test]
    fn test_from_data_store_error_not_found() {
        assert_eq!(
            Error::from(s3_client::Error::NotFound("test.txt".to_string())),
            Error::ReferenceNotFound
        );
    }

    #[test]
    fn test_from_data_store_error_other() {
        assert!(matches!(
            Error::from(s3_client::Error::Io("test".to_string())),
            Error::DataStore(_)
        ));
    }

    #[test]
    fn test_from_io_error_not_found() {
        assert_eq!(
            Error::from(io::Error::new(io::ErrorKind::NotFound, "file not found")),
            Error::ReferenceNotFound
        );
    }

    #[test]
    fn test_from_io_error_other() {
        assert!(matches!(
            Error::from(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "permission denied"
            )),
            Error::StorageBackend(_)
        ));
    }

    #[test]
    fn test_from_deserialize_state_error() {
        assert!(matches!(
            Error::from(DeserializeStateError),
            Error::HashSerialization(_)
        ));
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_error = serde_json::from_str::<serde_json::Value>("{invalid}").unwrap_err();
        assert!(matches!(
            Error::from(json_error),
            Error::JSONSerialization(_)
        ));
    }

    #[test]
    fn test_from_utf8_error() {
        let utf8_error = String::from_utf8(vec![0, 159, 146, 150]).unwrap_err();
        assert!(matches!(Error::from(utf8_error), Error::InvalidFormat(_)));
    }

    #[test]
    fn test_from_try_from_int_error() {
        let int_error: Result<u8, TryFromIntError> = 256u16.try_into();
        assert!(matches!(
            Error::from(int_error.unwrap_err()),
            Error::InvalidFormat(_)
        ));
    }

    #[test]
    fn test_from_chrono_parse_error() {
        let parse_error = chrono::DateTime::parse_from_rfc3339("invalid").unwrap_err();
        assert!(matches!(Error::from(parse_error), Error::InvalidFormat(_)));
    }

    #[test]
    fn test_from_oci_error() {
        let oci_error = oci::Error::InvalidDigest("bad".to_string());
        assert!(matches!(Error::from(oci_error), Error::InvalidFormat(_)));
    }

    #[test]
    fn test_from_storage_error_not_found() {
        // Verifies the *fallback* mapping documented on `impl From<StorageError>
        // for Error`: `NotFound` converts to `ReferenceNotFound`, which is
        // intentionally conservative and correct only for reference/manifest
        // paths. Blob and upload call sites must intercept `StorageError::NotFound`
        // explicitly before using `?`; see the doc comment on the impl for the
        // full trap description.
        assert_eq!(
            Error::from(StorageError::NotFound),
            Error::ReferenceNotFound
        );
    }

    #[test]
    fn test_from_storage_error_precondition_failed() {
        assert!(matches!(
            Error::from(StorageError::PreconditionFailed),
            Error::StorageBackend(_)
        ));
    }

    #[test]
    fn test_from_storage_error_backend() {
        assert!(matches!(
            Error::from(StorageError::Backend("oops".to_string())),
            Error::StorageBackend(_)
        ));
    }
}
