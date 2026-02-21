use std::{fmt, fmt::Debug, io, num::TryFromIntError, string::FromUtf8Error};

use aws_sdk_s3::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{get_object::GetObjectError, head_object::HeadObjectError},
    primitives::ByteStreamError,
};
use sha2::digest::crypto_common::hazmat::DeserializeStateError;
use tracing::error;

use crate::{oci, registry::data_store};

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    DataStore(data_store::Error),
    HashSerialization(String),
    JSONSerialization(String),
    StorageBackend(String),
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
            Error::InvalidFormat(e) => write!(f, "Reference format error: {e}"),
            Error::UploadNotFound => write!(f, "Upload not found"),
            Error::BlobNotFound => write!(f, "Blob not found"),
            Error::ReferenceNotFound => write!(f, "Reference not found"),
        }
    }
}

impl From<data_store::Error> for Error {
    fn from(err: data_store::Error) -> Self {
        match err {
            data_store::Error::NotFound(_) => Error::ReferenceNotFound,
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
            error!("IO error: {e:?}");
            Error::StorageBackend(e.to_string())
        }
    }
}

impl From<TryFromIntError> for Error {
    fn from(e: TryFromIntError) -> Self {
        error!("TryFromIntError: {e:?}");
        Error::InvalidFormat(e.to_string())
    }
}

// S3 errors

impl From<HeadObjectError> for Error {
    fn from(e: HeadObjectError) -> Self {
        error!("HeadObjectError: {e:?}");
        Error::StorageBackend(format!("HeadObjectError: {e:?}"))
    }
}

impl From<GetObjectError> for Error {
    fn from(e: GetObjectError) -> Self {
        error!("GetObjectError: {e:?}");
        Error::StorageBackend(format!("GetObjectError: {e:?}"))
    }
}

impl<T> From<SdkError<T, HttpResponse>> for Error
where
    T: Debug,
{
    fn from(e: SdkError<T, HttpResponse>) -> Self {
        error!("SdkError: {e:?}");
        Error::StorageBackend(format!("SdkError: {e:?}"))
    }
}

impl From<ByteStreamError> for Error {
    fn from(e: ByteStreamError) -> Self {
        error!("ByteStreamError: {e:?}");
        Error::StorageBackend(format!("ByteStreamError: {e:?}"))
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

#[cfg(test)]
mod tests {
    use aws_sdk_s3::{
        error::SdkError,
        operation::{
            get_object::GetObjectError, head_object::HeadObjectError, put_object::PutObjectError,
        },
        primitives::ByteStreamError,
    };
    use sha2::digest::crypto_common::hazmat::DeserializeStateError;

    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            format!(
                "{}",
                Error::DataStore(data_store::Error::Io("IO error".to_string()))
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
            Error::from(data_store::Error::NotFound("test.txt".to_string())),
            Error::ReferenceNotFound
        );
    }

    #[test]
    fn test_from_data_store_error_other() {
        assert!(matches!(
            Error::from(data_store::Error::Io("test".to_string())),
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
        let oci_error = oci::Error::InvalidFormat("bad".to_string());
        assert!(matches!(Error::from(oci_error), Error::InvalidFormat(_)));
    }

    #[test]
    fn test_from_head_object_error() {
        let head_error =
            HeadObjectError::NotFound(aws_sdk_s3::types::error::NotFound::builder().build());
        assert!(matches!(Error::from(head_error), Error::StorageBackend(_)));
    }

    #[test]
    fn test_from_get_object_error() {
        let get_error =
            GetObjectError::NoSuchKey(aws_sdk_s3::types::error::NoSuchKey::builder().build());
        assert!(matches!(Error::from(get_error), Error::StorageBackend(_)));
    }

    #[test]
    fn test_from_byte_stream_error() {
        let stream_error =
            ByteStreamError::from(io::Error::new(io::ErrorKind::UnexpectedEof, "stream error"));
        assert!(matches!(
            Error::from(stream_error),
            Error::StorageBackend(_)
        ));
    }

    #[test]
    fn test_from_sdk_error() {
        let sdk_error: SdkError<PutObjectError, _> =
            SdkError::construction_failure(io::Error::new(io::ErrorKind::TimedOut, "timeout"));
        assert!(matches!(Error::from(sdk_error), Error::StorageBackend(_)));
    }
}
