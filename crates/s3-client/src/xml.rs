//! The S3 XML bodies angos reads and writes, as serde shapes.

use chrono::{DateTime, Utc};
use quick_xml::de::from_reader;
use quick_xml::escape::escape;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::ops::UploadedPart;

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct S3ErrorBody {
    pub code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Output {
    pub contents: Vec<String>,
    pub common_prefixes: Vec<String>,
    /// Where the next page starts, `Some` exactly when the listing is
    /// truncated. A caller that stops on `None` therefore cannot mistake a
    /// truncated page for a complete one.
    pub next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(default)]
    contents: Vec<Keyed>,
    #[serde(default)]
    common_prefixes: Vec<Prefixed>,
    #[serde(default)]
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Keyed {
    key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Prefixed {
    prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BatchDeleteError {
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DeleteObjectsOutput {
    #[serde(default, rename = "Error")]
    pub errors: Vec<BatchDeleteError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    upload_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CopyPartResult {
    e_tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MultipartUploadOutput {
    pub key: String,
    pub upload_id: String,
    #[serde(rename = "Initiated")]
    pub initiated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMultipartUploadsOutput {
    pub uploads: Vec<MultipartUploadOutput>,
    /// Where the next page starts, `Some` exactly when the listing is
    /// truncated. `next_upload_id_marker` only refines it, so it is `None`
    /// whenever this is.
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListMultipartUploadsResult {
    #[serde(default, rename = "Upload")]
    uploads: Vec<MultipartUploadOutput>,
    #[serde(default)]
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartsOutput {
    pub parts: Vec<UploadedPart>,
    /// Where the next page starts, `Some` exactly when the listing is
    /// truncated, so the paging loop terminates on `None` rather than
    /// re-requesting the first page forever.
    pub next_part_number_marker: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListPartsResult {
    #[serde(default, rename = "Part")]
    parts: Vec<UploadedPart>,
    #[serde(default)]
    is_truncated: bool,
    next_part_number_marker: Option<u32>,
}

fn parse<T: DeserializeOwned>(body: &[u8]) -> Result<T, String> {
    from_reader(body).map_err(|e| e.to_string())
}

/// The marker a page resumes from: required on a truncated page, and dropped
/// on a complete one so a caller stopping on `None` is never sent round again.
fn resume_marker<T>(
    is_truncated: bool,
    marker: Option<T>,
    listing: &str,
) -> Result<Option<T>, String> {
    match (is_truncated, marker) {
        (true, None) => Err(format!("truncated {listing} listing carries no marker")),
        (true, marker) => Ok(marker),
        (false, _) => Ok(None),
    }
}

pub fn parse_error(body: &[u8]) -> S3ErrorBody {
    parse(body).unwrap_or_default()
}

pub fn parse_list_objects_v2(body: &[u8]) -> Result<ListObjectsV2Output, String> {
    let page: ListBucketResult = parse(body)?;
    Ok(ListObjectsV2Output {
        contents: page.contents.into_iter().map(|c| c.key).collect(),
        common_prefixes: page.common_prefixes.into_iter().map(|p| p.prefix).collect(),
        next_continuation_token: resume_marker(
            page.is_truncated,
            page.next_continuation_token,
            "object",
        )?,
    })
}

pub fn parse_delete_objects(body: &[u8]) -> Result<DeleteObjectsOutput, String> {
    parse(body)
}

pub fn parse_create_multipart_upload(body: &[u8]) -> Result<String, String> {
    parse(body).map(|result: InitiateMultipartUploadResult| result.upload_id)
}

pub fn parse_upload_part_copy(body: &[u8]) -> Result<String, String> {
    parse(body).map(|result: CopyPartResult| result.e_tag)
}

pub fn parse_list_multipart_uploads(body: &[u8]) -> Result<ListMultipartUploadsOutput, String> {
    let page: ListMultipartUploadsResult = parse(body)?;
    Ok(ListMultipartUploadsOutput {
        uploads: page.uploads,
        next_key_marker: resume_marker(page.is_truncated, page.next_key_marker, "multipart")?,
        next_upload_id_marker: page.next_upload_id_marker.filter(|_| page.is_truncated),
    })
}

pub fn parse_list_parts(body: &[u8]) -> Result<ListPartsOutput, String> {
    let page: ListPartsResult = parse(body)?;
    Ok(ListPartsOutput {
        parts: page.parts,
        next_part_number_marker: resume_marker(
            page.is_truncated,
            page.next_part_number_marker,
            "part",
        )?,
    })
}

pub fn delete_objects_xml(keys: &[String]) -> String {
    let objects = keys
        .iter()
        .map(|key| format!("<Object><Key>{}</Key></Object>", escape(key)))
        .collect::<Vec<_>>()
        .concat();
    format!(r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{objects}</Delete>"#)
}

pub fn complete_multipart_upload_xml(parts: &[UploadedPart]) -> String {
    let parts = parts
        .iter()
        .map(|part| {
            format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                part.part_number,
                escape(&part.e_tag)
            )
        })
        .collect::<Vec<_>>()
        .concat();
    format!(
        r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{parts}</CompleteMultipartUpload>"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_list_objects_v2() {
        let xml = b"<ListBucketResult>
            <IsTruncated>true</IsTruncated>
            <NextContinuationToken>next</NextContinuationToken>
            <Contents><Key>prefix/a</Key></Contents>
            <CommonPrefixes><Prefix>prefix/b/</Prefix></CommonPrefixes>
        </ListBucketResult>";

        let parsed = parse_list_objects_v2(xml).unwrap();
        assert_eq!(parsed.next_continuation_token.as_deref(), Some("next"));
        assert_eq!(parsed.contents, vec!["prefix/a"]);
        assert_eq!(parsed.common_prefixes, vec!["prefix/b/"]);
    }

    /// A page that says it is truncated but names nowhere to resume from is
    /// unusable: believing it would hand a caller a partial listing as a
    /// complete one, and anything deriving a delete from "not in the listing"
    /// would then delete live data.
    #[test]
    fn a_truncated_listing_without_a_marker_is_rejected() {
        let objects = parse_list_objects_v2(
            b"<ListBucketResult>
                <IsTruncated>true</IsTruncated>
                <Contents><Key>a</Key></Contents>
            </ListBucketResult>",
        );
        assert!(objects.is_err(), "got: {objects:?}");

        let uploads = parse_list_multipart_uploads(
            b"<ListMultipartUploadsResult>
                <IsTruncated>true</IsTruncated>
            </ListMultipartUploadsResult>",
        );
        assert!(uploads.is_err(), "got: {uploads:?}");

        let parts = parse_list_parts(
            b"<ListPartsResult>
                <IsTruncated>true</IsTruncated>
            </ListPartsResult>",
        );
        assert!(parts.is_err(), "got: {parts:?}");
    }

    /// The paging loop resumes from this marker, so a value it cannot read must
    /// not degrade to "no marker": that reads as a complete listing, and for
    /// parts it re-requests the first page forever.
    #[test]
    fn an_unreadable_part_number_marker_is_rejected() {
        let parsed = parse_list_parts(
            b"<ListPartsResult>
                <IsTruncated>true</IsTruncated>
                <NextPartNumberMarker>not-a-number</NextPartNumberMarker>
            </ListPartsResult>",
        );
        assert!(parsed.is_err(), "got: {parsed:?}");
    }

    /// A marker on a complete page is dropped rather than believed, so a caller
    /// stopping on `None` cannot be sent round again.
    #[test]
    fn a_complete_listing_carries_no_marker() {
        let parsed = parse_list_objects_v2(
            b"<ListBucketResult>
                <IsTruncated>false</IsTruncated>
                <NextContinuationToken>stale</NextContinuationToken>
            </ListBucketResult>",
        )
        .unwrap();
        assert_eq!(parsed.next_continuation_token, None);
    }

    #[test]
    fn parse_error_only_treats_top_level_error_as_s3_error() {
        let top_level =
            parse_error(b"<Error><Code>AccessDenied</Code><Message>denied</Message></Error>");
        assert_eq!(top_level.code.as_deref(), Some("AccessDenied"));
        assert_eq!(top_level.message.as_deref(), Some("denied"));

        let nested = parse_error(
            b"<DeleteResult><Error><Code>AccessDenied</Code><Message>denied</Message></Error></DeleteResult>",
        );
        assert_eq!(nested.code, None);
        assert_eq!(nested.message, None);
    }

    #[test]
    fn builds_complete_multipart_upload_xml() {
        let xml = complete_multipart_upload_xml(&[UploadedPart {
            part_number: 1,
            e_tag: r#""etag&1""#.to_string(),
            size: 5,
        }]);
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("&quot;etag&amp;1&quot;"));
    }
}
