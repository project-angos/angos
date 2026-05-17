use chrono::{DateTime, Utc};
use quick_xml::{Reader, events::Event};

use super::ops::UploadedPart;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ErrorBody {
    pub code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Output {
    pub contents: Vec<String>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchDeleteError {
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectsOutput {
    pub errors: Vec<BatchDeleteError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadOutput {
    pub key: String,
    pub upload_id: String,
    pub initiated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMultipartUploadsOutput {
    pub uploads: Vec<MultipartUploadOutput>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartsOutput {
    pub parts: Vec<UploadedPart>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
}

enum XmlEvent {
    Start(Tag),
    Text(TextKind, String),
    End(Tag),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TextKind {
    BatchDeleteErrorMessage,
    CopyPartETag,
    CreateMultipartUploadId,
    IsTruncated,
    ListCommonPrefix,
    ListObjectKey,
    MultipartUploadId,
    MultipartUploadInitiated,
    MultipartUploadKey,
    NextContinuationToken,
    NextKeyMarker,
    NextPartNumberMarker,
    NextUploadIdMarker,
    PartETag,
    PartNumber,
    PartSize,
    TopLevelErrorCode,
    TopLevelErrorMessage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::enum_variant_names,
    reason = "ETag is canonical S3 protocol naming"
)]
enum Tag {
    Code,
    CommonPrefixes,
    Contents,
    CopyPartResult,
    Error,
    ETag,
    Initiated,
    IsTruncated,
    Key,
    Message,
    NextContinuationToken,
    NextKeyMarker,
    NextPartNumberMarker,
    NextUploadIdMarker,
    Part,
    PartNumber,
    Prefix,
    Size,
    Upload,
    UploadId,
    Other,
}

fn local_tag(name: &[u8]) -> Tag {
    match name {
        b"Code" => Tag::Code,
        b"CommonPrefixes" => Tag::CommonPrefixes,
        b"Contents" => Tag::Contents,
        b"CopyPartResult" => Tag::CopyPartResult,
        b"Error" => Tag::Error,
        b"ETag" => Tag::ETag,
        b"Initiated" => Tag::Initiated,
        b"IsTruncated" => Tag::IsTruncated,
        b"Key" => Tag::Key,
        b"Message" => Tag::Message,
        b"NextContinuationToken" => Tag::NextContinuationToken,
        b"NextKeyMarker" => Tag::NextKeyMarker,
        b"NextPartNumberMarker" => Tag::NextPartNumberMarker,
        b"NextUploadIdMarker" => Tag::NextUploadIdMarker,
        b"Part" => Tag::Part,
        b"PartNumber" => Tag::PartNumber,
        b"Prefix" => Tag::Prefix,
        b"Size" => Tag::Size,
        b"Upload" => Tag::Upload,
        b"UploadId" => Tag::UploadId,
        _ => Tag::Other,
    }
}

fn text_value(text: &quick_xml::events::BytesText<'_>) -> Result<String, String> {
    let decoded = text.decode().map_err(|e| e.to_string())?;
    quick_xml::escape::unescape(&decoded)
        .map(std::borrow::Cow::into_owned)
        .map_err(|e| e.to_string())
}

fn cdata_value(cdata: &quick_xml::events::BytesCData<'_>) -> Result<String, String> {
    cdata
        .decode()
        .map(std::borrow::Cow::into_owned)
        .map_err(|e| e.to_string())
}

fn text_kind(stack: &[Tag]) -> Option<TextKind> {
    match stack {
        [Tag::Error, Tag::Code] => Some(TextKind::TopLevelErrorCode),
        [Tag::Error, Tag::Message] => Some(TextKind::TopLevelErrorMessage),
        [_, Tag::Error, Tag::Message] => Some(TextKind::BatchDeleteErrorMessage),
        [_, Tag::Contents, Tag::Key] => Some(TextKind::ListObjectKey),
        [_, Tag::CommonPrefixes, Tag::Prefix] => Some(TextKind::ListCommonPrefix),
        [_, Tag::IsTruncated] => Some(TextKind::IsTruncated),
        [_, Tag::NextContinuationToken] => Some(TextKind::NextContinuationToken),
        [_, Tag::UploadId] => Some(TextKind::CreateMultipartUploadId),
        [Tag::CopyPartResult, Tag::ETag] => Some(TextKind::CopyPartETag),
        [_, Tag::Upload, Tag::Key] => Some(TextKind::MultipartUploadKey),
        [_, Tag::Upload, Tag::UploadId] => Some(TextKind::MultipartUploadId),
        [_, Tag::Upload, Tag::Initiated] => Some(TextKind::MultipartUploadInitiated),
        [_, Tag::NextKeyMarker] => Some(TextKind::NextKeyMarker),
        [_, Tag::NextUploadIdMarker] => Some(TextKind::NextUploadIdMarker),
        [_, Tag::Part, Tag::PartNumber] => Some(TextKind::PartNumber),
        [_, Tag::Part, Tag::ETag] => Some(TextKind::PartETag),
        [_, Tag::Part, Tag::Size] => Some(TextKind::PartSize),
        [_, Tag::NextPartNumberMarker] => Some(TextKind::NextPartNumberMarker),
        _ => None,
    }
}

fn read_xml_events(
    body: &[u8],
    mut on_event: impl FnMut(XmlEvent) -> Result<(), String>,
) -> Result<(), String> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut stack = Vec::new();
    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|e| e.to_string())?
        {
            Event::Start(e) => {
                let tag = local_tag(e.local_name().as_ref());
                stack.push(tag);
                on_event(XmlEvent::Start(tag))?;
            }
            Event::Empty(e) => {
                let tag = local_tag(e.local_name().as_ref());
                stack.push(tag);
                on_event(XmlEvent::Start(tag))?;
                on_event(XmlEvent::End(tag))?;
                stack.pop();
            }
            Event::Text(e) => {
                if let Some(kind) = text_kind(&stack) {
                    on_event(XmlEvent::Text(kind, text_value(&e)?))?;
                }
            }
            Event::CData(e) => {
                if let Some(kind) = text_kind(&stack) {
                    on_event(XmlEvent::Text(kind, cdata_value(&e)?))?;
                }
            }
            Event::End(e) => {
                let tag = local_tag(e.local_name().as_ref());
                on_event(XmlEvent::End(tag))?;
                stack.pop();
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}

pub fn parse_error(body: &[u8]) -> S3ErrorBody {
    let mut parsed = S3ErrorBody {
        code: None,
        message: None,
    };
    let _ = read_xml_events(body, |event| {
        if let XmlEvent::Text(kind, text) = event {
            match kind {
                TextKind::TopLevelErrorCode => parsed.code = Some(text),
                TextKind::TopLevelErrorMessage => parsed.message = Some(text),
                _ => {}
            }
        }
        Ok(())
    });
    parsed
}

pub fn parse_list_objects_v2(body: &[u8]) -> Result<ListObjectsV2Output, String> {
    let mut output = ListObjectsV2Output {
        contents: Vec::new(),
        common_prefixes: Vec::new(),
        is_truncated: false,
        next_continuation_token: None,
    };
    let mut current_key: Option<String> = None;
    let mut current_prefix: Option<String> = None;

    read_xml_events(body, |event| {
        match event {
            XmlEvent::Start(name) => match name {
                Tag::Contents => current_key = None,
                Tag::CommonPrefixes => current_prefix = None,
                _ => {}
            },
            XmlEvent::Text(kind, text) => match kind {
                TextKind::ListObjectKey => current_key = Some(text),
                TextKind::ListCommonPrefix => current_prefix = Some(text),
                TextKind::IsTruncated => output.is_truncated = text == "true",
                TextKind::NextContinuationToken => output.next_continuation_token = Some(text),
                _ => {}
            },
            XmlEvent::End(name) => match name {
                Tag::Contents => {
                    if let Some(key) = current_key.take() {
                        output.contents.push(key);
                    }
                }
                Tag::CommonPrefixes => {
                    if let Some(prefix) = current_prefix.take() {
                        output.common_prefixes.push(prefix);
                    }
                }
                _ => {}
            },
        }
        Ok(())
    })?;

    Ok(output)
}

pub fn parse_delete_objects(body: &[u8]) -> Result<DeleteObjectsOutput, String> {
    let mut output = DeleteObjectsOutput { errors: Vec::new() };
    let mut current_message: Option<String> = None;

    read_xml_events(body, |event| {
        match event {
            XmlEvent::Start(Tag::Error) => current_message = None,
            XmlEvent::Text(TextKind::BatchDeleteErrorMessage, text) => {
                current_message = Some(text);
            }
            XmlEvent::End(Tag::Error) => {
                output.errors.push(BatchDeleteError {
                    message: current_message.take(),
                });
            }
            _ => {}
        }
        Ok(())
    })?;

    Ok(output)
}

pub fn parse_create_multipart_upload(body: &[u8]) -> Result<String, String> {
    let mut upload_id = None;
    read_xml_events(body, |event| {
        if let XmlEvent::Text(TextKind::CreateMultipartUploadId, text) = event {
            upload_id = Some(text);
        }
        Ok(())
    })?;
    upload_id.ok_or_else(|| "upload_id not found in response".to_string())
}

pub fn parse_upload_part_copy(body: &[u8]) -> Result<String, String> {
    let mut etag = None;
    read_xml_events(body, |event| {
        if let XmlEvent::Text(TextKind::CopyPartETag, text) = event {
            etag = Some(text);
        }
        Ok(())
    })?;
    etag.ok_or_else(|| "e_tag not found in copy result".to_string())
}

pub fn parse_list_multipart_uploads(body: &[u8]) -> Result<ListMultipartUploadsOutput, String> {
    let mut output = ListMultipartUploadsOutput {
        uploads: Vec::new(),
        is_truncated: false,
        next_key_marker: None,
        next_upload_id_marker: None,
    };
    let mut key: Option<String> = None;
    let mut upload_id: Option<String> = None;
    let mut initiated: Option<DateTime<Utc>> = None;

    read_xml_events(body, |event| {
        match event {
            XmlEvent::Start(Tag::Upload) => {
                key = None;
                upload_id = None;
                initiated = None;
            }
            XmlEvent::Text(kind, text) => match kind {
                TextKind::MultipartUploadKey => key = Some(text),
                TextKind::MultipartUploadId => upload_id = Some(text),
                TextKind::MultipartUploadInitiated => {
                    initiated = DateTime::parse_from_rfc3339(&text)
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc));
                }
                TextKind::IsTruncated => output.is_truncated = text == "true",
                TextKind::NextKeyMarker => output.next_key_marker = Some(text),
                TextKind::NextUploadIdMarker => output.next_upload_id_marker = Some(text),
                _ => {}
            },
            XmlEvent::End(Tag::Upload) => {
                if let (Some(key), Some(upload_id), Some(initiated_at)) =
                    (key.take(), upload_id.take(), initiated.take())
                {
                    output.uploads.push(MultipartUploadOutput {
                        key,
                        upload_id,
                        initiated_at,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    })?;

    Ok(output)
}

pub fn parse_list_parts(body: &[u8]) -> Result<ListPartsOutput, String> {
    let mut output = ListPartsOutput {
        parts: Vec::new(),
        is_truncated: false,
        next_part_number_marker: None,
    };
    let mut part_number: Option<u32> = None;
    let mut e_tag: Option<String> = None;
    let mut size: Option<u64> = None;

    read_xml_events(body, |event| {
        match event {
            XmlEvent::Start(Tag::Part) => {
                part_number = None;
                e_tag = None;
                size = None;
            }
            XmlEvent::Text(kind, text) => match kind {
                TextKind::PartNumber => part_number = text.parse::<u32>().ok(),
                TextKind::PartETag => e_tag = Some(text),
                TextKind::PartSize => size = text.parse::<u64>().ok(),
                TextKind::IsTruncated => output.is_truncated = text == "true",
                TextKind::NextPartNumberMarker => {
                    output.next_part_number_marker = text.parse::<u32>().ok();
                }
                _ => {}
            },
            XmlEvent::End(Tag::Part) => {
                if let (Some(part_number), Some(e_tag), Some(size)) =
                    (part_number.take(), e_tag.take(), size.take())
                {
                    output.parts.push(UploadedPart {
                        part_number,
                        e_tag,
                        size,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    })?;

    Ok(output)
}

fn push_escaped_xml(output: &mut String, input: &str) {
    for ch in input.chars() {
        match ch {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(ch),
        }
    }
}

pub fn delete_objects_xml(keys: &[String]) -> String {
    let keys_len = keys.iter().map(String::len).sum::<usize>();
    let mut xml = String::with_capacity(64 + keys_len + keys.len() * 28);
    xml.push_str(r#"<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
    for key in keys {
        xml.push_str("<Object><Key>");
        push_escaped_xml(&mut xml, key);
        xml.push_str("</Key></Object>");
    }
    xml.push_str("</Delete>");
    xml
}

pub fn complete_multipart_upload_xml(parts: &[UploadedPart]) -> String {
    let etag_len = parts.iter().map(|part| part.e_tag.len()).sum::<usize>();
    let mut xml = String::with_capacity(90 + etag_len + parts.len() * 64);
    xml.push_str(r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
    for part in parts {
        xml.push_str("<Part><PartNumber>");
        xml.push_str(&part.part_number.to_string());
        xml.push_str("</PartNumber><ETag>");
        push_escaped_xml(&mut xml, &part.e_tag);
        xml.push_str("</ETag></Part>");
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
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
        assert!(parsed.is_truncated);
        assert_eq!(parsed.next_continuation_token.as_deref(), Some("next"));
        assert_eq!(parsed.contents, vec!["prefix/a"]);
        assert_eq!(parsed.common_prefixes, vec!["prefix/b/"]);
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
