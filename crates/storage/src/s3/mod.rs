//! S3 [`ObjectStore`] and [`PresignedStore`]. An upload is an S3 multipart
//! upload whose state (upload id, committed parts) is recovered from S3 on
//! every call. Bytes short of a part are staged at `staged/<offset>` beside
//! the upload key, `offset` being the bytes committed to parts.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use angos_s3_client::{Backend as S3Backend, Error as S3Error, UploadedPart};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::task::AbortOnDropHandle;

use crate::{
    BoxedReader, ByteStream, Children, ChildrenPage, Error, KeyStream, MultipartUploadPage,
    ObjectMeta, ObjectStore, Page, PendingMultipartUpload, PresignedStore,
};

mod range_scan;

pub const DEFAULT_RANGE_CONCURRENCY: usize = 16;

const FRAME_SIZE: usize = 1024 * 1024;
const FRAME_BUFFER_CAPACITY: usize = 8;
/// S3's floor for a non-final part.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
const MAX_PARTS: usize = 10_000;

pub struct Builder {
    client: Arc<S3Backend>,
    part_size: u64,
    uniform_parts: bool,
    range_concurrency: usize,
}

impl Builder {
    /// Concurrent range chains a truncated whole-directory or whole-store scan
    /// fans out to.
    #[must_use]
    pub fn range_concurrency(mut self, concurrency: usize) -> Self {
        self.range_concurrency = concurrency.max(1);
        self
    }

    /// Part size (uniform mode) or flush threshold (non-uniform mode), raised to
    /// the S3 floor: a smaller part is only rejected at
    /// `CompleteMultipartUpload`, long after the bytes were written.
    #[must_use]
    pub fn part_size(mut self, size: u64) -> Self {
        self.part_size = size.max(MIN_PART_SIZE);
        self
    }

    /// Uniform mode emits parts of exactly `part_size`; non-uniform mode emits
    /// one part of everything available once it reaches `part_size`. Only a
    /// known-length write tells them apart.
    #[must_use]
    pub fn uniform_parts(mut self, on: bool) -> Self {
        self.uniform_parts = on;
        self
    }

    #[must_use]
    pub fn build(self) -> Backend {
        Backend {
            client: self.client,
            part_size: self.part_size,
            uniform_parts: self.uniform_parts,
            range_concurrency: self.range_concurrency,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Backend {
    client: Arc<S3Backend>,
    part_size: u64,
    uniform_parts: bool,
    range_concurrency: usize,
}

impl Backend {
    #[must_use]
    pub fn builder(client: Arc<S3Backend>) -> Builder {
        Builder {
            client,
            part_size: MIN_PART_SIZE,
            uniform_parts: false,
            range_concurrency: DEFAULT_RANGE_CONCURRENCY,
        }
    }

    /// The open multipart upload id, if any, and its committed parts.
    async fn recover_upload(
        &self,
        key: &str,
    ) -> Result<(Option<String>, Vec<UploadedPart>), Error> {
        let upload_id = self.search_upload_id(key).await?;
        let parts = match &upload_id {
            Some(id) => self.client.list_parts(key, id).await?,
            None => Vec::new(),
        };
        Ok((upload_id, parts))
    }

    async fn search_upload_id(&self, key: &str) -> Result<Option<String>, Error> {
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        loop {
            let listing = self
                .client
                .list_multipart_uploads(key, key_marker.as_deref(), upload_id_marker.as_deref())
                .await?;
            for upload in listing.uploads {
                if upload.key == key {
                    return Ok(Some(upload.upload_id));
                }
            }
            if listing.next_key_marker.is_none() {
                return Ok(None);
            }
            key_marker = listing.next_key_marker;
            upload_id_marker = listing.next_upload_id_marker;
        }
    }

    /// The size of the remainder staged at `offset`, 0 when there is none.
    async fn staged_size(&self, key: &str, offset: u64) -> Result<u64, Error> {
        match self.client.head_object(&staged_key(key, offset)).await {
            Ok(head) => Ok(head.size),
            Err(S3Error::NotFound(_)) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// Streams the parts of a known-length append of `available` bytes from
    /// `reader`, returning the remainder to restage.
    async fn emit_known_length_parts<R>(
        &self,
        key: &str,
        upload_id: &mut Option<String>,
        parts: &mut Vec<UploadedPart>,
        reader: &mut R,
        available: u64,
    ) -> Result<Vec<u8>, Error>
    where
        R: AsyncRead + Unpin + Send,
    {
        let PartPlan {
            count,
            emit_size,
            restaged,
        } = plan_known_length_parts(self.uniform_parts, self.part_size, available);

        for _ in 0..count {
            let part_number = next_part_number(parts)?;
            let id = ensure_upload_id(&self.client, upload_id, key).await?;
            let etag = stream_part(&self.client, key, &id, part_number, emit_size, reader).await?;
            parts.push(UploadedPart {
                part_number,
                e_tag: etag,
                size: emit_size,
            });
        }

        let mut remainder = Vec::with_capacity(
            usize::try_from(restaged).map_err(|e| Error::Backend(e.to_string()))?,
        );
        (&mut *reader)
            .take(restaged)
            .read_to_end(&mut remainder)
            .await?;
        let actual = remainder.len() as u64;
        if actual != restaged {
            return Err(Error::Backend(format!(
                "short read while restaging: expected {restaged}, got {actual}",
            )));
        }
        Ok(remainder)
    }

    /// Drains `reader` to EOF, flushing a `part_size` part each time one
    /// accumulates, so a mixed-mode session stays uniform. Returns the short
    /// tail as the remainder.
    async fn drain_chunked_parts<R>(
        &self,
        key: &str,
        upload_id: &mut Option<String>,
        parts: &mut Vec<UploadedPart>,
        reader: &mut R,
    ) -> Result<Vec<u8>, Error>
    where
        R: AsyncRead + Unpin + Send,
    {
        let cap = usize::try_from(self.part_size).map_err(|e| Error::Backend(e.to_string()))?;
        loop {
            let mut buf = Vec::with_capacity(cap);
            (&mut *reader)
                .take(self.part_size)
                .read_to_end(&mut buf)
                .await?;
            if (buf.len() as u64) < self.part_size {
                return Ok(buf);
            }
            let id = ensure_upload_id(&self.client, upload_id, key).await?;
            self.upload_buffered_part(key, &id, parts, buf).await?;
        }
    }

    /// Uploads `data` as the next part.
    async fn upload_buffered_part(
        &self,
        key: &str,
        upload_id: &str,
        parts: &mut Vec<UploadedPart>,
        data: Vec<u8>,
    ) -> Result<(), Error> {
        let part_number = next_part_number(parts)?;
        let size = data.len() as u64;
        let body: ByteStream = Box::pin(stream::once(async move { Ok(Bytes::from(data)) }));
        let e_tag = self
            .client
            .upload_part_streaming(key, upload_id, part_number, size, body)
            .await?;
        parts.push(UploadedPart {
            part_number,
            e_tag,
            size,
        });
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        Ok(self.client.read(key).await?)
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let result = self.client.get_object(key, offset).await?;
        // S3 reports the ranged length; the contract wants the total.
        let total = result.content_length + offset.unwrap_or(0);
        Ok((result.body, total))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
        Ok(self.client.put_object(key, data).await?)
    }

    async fn create_if_absent(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        match self.client.put_object_if_not_exists(key, data).await {
            Ok(()) => Ok(true),
            // A 412 means the key was already taken: the create lost, not an error.
            Err(S3Error::PreconditionFailed) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        Ok(self.client.delete_object(key).await?)
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        if prefix.is_empty() {
            return Ok(());
        }
        // The raw delete is a string prefix: `tags/v1` would take `tags/v1-rc`.
        let directory = format!("{}/", prefix.strip_suffix('/').unwrap_or(prefix));
        Ok(self.client.delete_prefix(&directory).await?)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error> {
        let head = self.client.head_object(key).await?;
        Ok(ObjectMeta {
            size: head.size,
            last_modified: head.last_modified,
        })
    }

    async fn list_after(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<Page<String>, Error> {
        // S3 rejects StartAfter alongside a continuation token; the token
        // already encodes the position, so it wins.
        let start_after = if token.is_some() { None } else { start_after };
        let listing = self
            .client
            .list_objects(prefix, n, token, start_after)
            .await?;
        Ok(Page {
            items: listing.keys,
            next_token: listing.next_token,
        })
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        // S3's start-after is an exclusive bound on raw keys, which cannot
        // express "after this child name": a directory child's own keys sort
        // after the bare name, and a sibling like `v1.2` sorts before `v1/`.
        // So bound on the bare name and drop what the contract excludes.
        let mut listing = self
            .client
            .list_prefixes(prefix, n, token, start_after.clone())
            .await?;
        if let Some(after) = start_after.as_deref() {
            listing.prefixes.retain(|name| name.as_str() > after);
            listing.objects.retain(|name| name.as_str() > after);
        }
        Ok(ChildrenPage {
            sub_prefixes: listing.prefixes,
            objects: listing.objects,
            next_token: listing.next_token,
        })
    }

    async fn list_all_children(&self, prefix: &str) -> Result<Children, Error> {
        range_scan::scan_all_children(self, prefix, self.range_concurrency).await
    }

    fn list_all<'a>(&'a self, prefix: &'a str) -> KeyStream<'a> {
        range_scan::scan_all_keys(self, prefix, self.range_concurrency)
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<u64, Error> {
        Ok(self.client.copy_object(source, destination).await?)
    }

    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, Error> {
        // Concurrent writes to one upload are unsupported: both would recover
        // the same offset and race the staged remainder.
        let (mut upload_id, mut parts) = self.recover_upload(key).await?;
        let committed_size = parts.iter().map(|p| p.size).sum::<u64>();

        if len == Some(0) {
            let staged_len = self.staged_size(key, committed_size).await?;
            return committed_size
                .checked_add(staged_len)
                .ok_or_else(|| Error::Backend("upload size overflow".to_string()));
        }

        let read_key = staged_key(key, committed_size);
        let (staged, staged_len) = staged_stream(&self.client, &read_key).await?;
        let mut reader = StreamReader::new(staged.chain(body));

        let remainder = match len {
            Some(len) => {
                let remainder = self
                    .emit_known_length_parts(
                        key,
                        &mut upload_id,
                        &mut parts,
                        &mut reader,
                        staged_len + len,
                    )
                    .await?;
                // A body longer than declared is refused, not truncated.
                let mut beyond = [0u8; 1];
                if reader.read(&mut beyond).await? > 0 {
                    return Err(Error::Backend(format!(
                        "s3 upload long body: expected {len} bytes, stream had more",
                    )));
                }
                remainder
            }
            None => {
                self.drain_chunked_parts(key, &mut upload_id, &mut parts, &mut reader)
                    .await?
            }
        };

        // At most one staged object survives per upload.
        let new_committed = parts.iter().map(|p| p.size).sum::<u64>();
        let restaged = remainder.len() as u64;
        if restaged > 0 {
            let write_key = staged_key(key, new_committed);
            self.client
                .put_object(&write_key, Bytes::from(remainder))
                .await?;
            if staged_len > 0 && write_key != read_key {
                let _ = self.client.delete_object(&read_key).await;
            }
        } else if staged_len > 0 {
            let _ = self.client.delete_object(&read_key).await;
        }

        new_committed
            .checked_add(restaged)
            .ok_or_else(|| Error::Backend("upload size overflow".to_string()))
    }

    async fn complete_upload(&self, key: &str) -> Result<(), Error> {
        let (upload_id, mut parts) = self.recover_upload(key).await?;
        let committed_size = parts.iter().map(|p| p.size).sum::<u64>();

        let read_key = staged_key(key, committed_size);
        let staged_len = self.staged_size(key, committed_size).await?;

        match (upload_id, staged_len) {
            (None, 0) => match self.head(key).await {
                // A re-run of a completed upload must not empty its object.
                Ok(_) => {}
                Err(Error::NotFound) => self.client.put_object(key, Bytes::new()).await?,
                Err(e) => return Err(e),
            },
            (None, _) => {
                // A `PUT`, as a server-side copy can report success over an
                // empty object.
                let data = self.client.read(&read_key).await?;
                self.client.put_object(key, Bytes::from(data)).await?;
                let _ = self.client.delete_object(&read_key).await;
            }
            (Some(upload_id), staged) => {
                if staged > 0 {
                    let data = self.client.read(&read_key).await?;
                    self.upload_buffered_part(key, &upload_id, &mut parts, data)
                        .await?;
                }

                self.client
                    .complete_multipart_upload(key, &upload_id, &parts)
                    .await?;
                let _ = self.client.delete_object(&read_key).await;
            }
        }

        Ok(())
    }

    async fn abort_upload(&self, key: &str) -> Result<(), Error> {
        // Relist until nothing new turns up, which bounds the sweep under an
        // eventually-consistent listing.
        let mut aborted: HashSet<String> = HashSet::new();
        loop {
            let listing = self.client.list_multipart_uploads(key, None, None).await?;
            let pending: Vec<_> = listing
                .uploads
                .into_iter()
                .filter(|u| u.key == key && !aborted.contains(&u.upload_id))
                .collect();
            if pending.is_empty() {
                break;
            }
            for u in pending {
                // A peer may have aborted it since the listing.
                match self
                    .client
                    .abort_multipart_upload(&u.key, &u.upload_id)
                    .await
                {
                    Ok(()) | Err(S3Error::NotFound(_)) => {}
                    Err(e) => return Err(Error::from(e)),
                }
                aborted.insert(u.upload_id);
            }
        }
        let _ = self.client.delete_prefix(&staged_container(key)).await;
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<MultipartUploadPage, Error> {
        let listing = self
            .client
            .list_multipart_uploads("", key_marker, upload_id_marker)
            .await?;
        Ok(MultipartUploadPage {
            uploads: listing
                .uploads
                .into_iter()
                .map(|u| PendingMultipartUpload {
                    key: u.key,
                    upload_id: u.upload_id,
                    initiated_at: u.initiated_at,
                })
                .collect(),
            next_key_marker: listing.next_key_marker,
            next_upload_id_marker: listing.next_upload_id_marker,
        })
    }
}

#[async_trait]
impl PresignedStore for Backend {
    async fn presign_get(
        &self,
        key: &str,
        ttl: Duration,
        content_type: Option<&str>,
    ) -> Result<String, Error> {
        Ok(self.client.generate_presigned_url(key, ttl, content_type)?)
    }
}

/// `count` parts of `emit_size` to flush now, `restaged` bytes to stage.
struct PartPlan {
    count: u64,
    emit_size: u64,
    restaged: u64,
}

/// Every emitted size stays within the S3 part bounds.
fn plan_known_length_parts(uniform: bool, part_size: u64, available: u64) -> PartPlan {
    if uniform {
        let count = available / part_size;
        return PartPlan {
            count,
            emit_size: part_size,
            restaged: available - count * part_size,
        };
    }
    if available < part_size {
        return PartPlan {
            count: 0,
            emit_size: 0,
            restaged: available,
        };
    }
    // One part per call, unless that would breach the per-part ceiling: then
    // the fewest equal parts that fit under it, which leaves at most `count`
    // bytes to restage rather than a second huge part to hold in memory.
    let count = available.div_ceil(MAX_PART_SIZE);
    let emit_size = available / count;
    PartPlan {
        count,
        emit_size,
        restaged: available - count * emit_size,
    }
}

fn next_part_number(parts: &[UploadedPart]) -> Result<u32, Error> {
    if parts.len() >= MAX_PARTS {
        return Err(Error::Backend(format!(
            "multipart upload would exceed the S3 {MAX_PARTS}-part limit"
        )));
    }
    u32::try_from(parts.len() + 1).map_err(|e| Error::Backend(e.to_string()))
}

/// `key` with its last segment replaced by `staged`: `.../<uuid>/data` stages
/// under `.../<uuid>/staged`.
fn staged_container(key: &str) -> String {
    match key.rfind('/') {
        Some(idx) => format!("{}/staged", &key[..idx]),
        None => "staged".to_string(),
    }
}

fn staged_key(key: &str, offset: u64) -> String {
    format!("{}/{offset}", staged_container(key))
}

/// Streamed rather than loaded, as the chunked path already buffers a whole
/// part. The length is zero when nothing is staged.
async fn staged_stream(client: &S3Backend, staging: &str) -> Result<(ByteStream, u64), Error> {
    match client.get_object(staging, None).await {
        Ok(staged) => Ok((
            Box::pin(ReaderStream::new(staged.body)),
            staged.content_length,
        )),
        Err(S3Error::NotFound(_)) => Ok((Box::pin(stream::empty()), 0)),
        Err(e) => Err(e.into()),
    }
}

async fn ensure_upload_id(
    client: &S3Backend,
    upload_id: &mut Option<String>,
    key: &str,
) -> Result<String, Error> {
    if let Some(id) = upload_id {
        return Ok(id.clone());
    }
    let new_id = client.create_multipart_upload(key).await?;
    *upload_id = Some(new_id.clone());
    Ok(new_id)
}

async fn stream_part<R>(
    client: &S3Backend,
    key: &str,
    upload_id: &str,
    part_number: u32,
    size: u64,
    source: &mut R,
) -> Result<String, Error>
where
    R: AsyncRead + Unpin + Send,
{
    let (tx, rx) = mpsc::channel::<Bytes>(FRAME_BUFFER_CAPACITY);
    let body: ByteStream = Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (Ok(chunk), rx))
    }));

    let upload_handle = AbortOnDropHandle::new(spawn({
        let client = client.clone();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        async move {
            client
                .upload_part_streaming(&key, &upload_id, part_number, size, body)
                .await
        }
    }));

    let mut sent: u64 = 0;
    let mut buf = BytesMut::with_capacity(FRAME_SIZE);
    while sent < size {
        let want = (size - sent).min(FRAME_SIZE as u64);
        buf.clear();
        let n = {
            let mut limited = source.take(want);
            limited.read_buf(&mut buf).await?
        };
        if n == 0 {
            return Err(Error::Backend(format!(
                "short read for part {part_number}: expected {size}, got {sent}",
            )));
        }
        sent += n as u64;
        if tx.send(buf.split().freeze()).await.is_err() {
            return match upload_handle.await {
                Ok(Ok(_)) => Err(Error::Backend(
                    "upload task stopped before receiving full part body".to_string(),
                )),
                Ok(Err(e)) => Err(e.into()),
                Err(e) => Err(Error::Backend(e.to_string())),
            };
        }
    }
    drop(tx);
    match upload_handle.await {
        Ok(Ok(etag)) => Ok(etag),
        Ok(Err(e)) => Err(e.into()),
        Err(e) => Err(Error::Backend(e.to_string())),
    }
}

#[cfg(test)]
mod tests;
