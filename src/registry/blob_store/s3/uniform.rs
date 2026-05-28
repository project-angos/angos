use std::io::{self, Cursor, ErrorKind};

use bytes::{Bytes, BytesMut};
use sha2::Sha256;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument;

use crate::{
    oci::Digest,
    registry::{
        blob_store::{
            Error,
            hashing_reader::HashingReader,
            s3::{
                Backend, FRAME_BUFFER_CAPACITY, FRAME_SIZE,
                multipart_helpers::{
                    CompletionPlan, UploadMode, next_part_number,
                    uploaded_size as total_uploaded_size,
                },
            },
            sha256_ext::Sha256Ext,
        },
        path_builder,
    },
};
use angos_storage::{
    Error as StorageError, MultipartStore, ObjectStore, Part, UploadId, channel_stream,
};

struct ChunkContext<'a> {
    upload_path: &'a str,
    upload_id: &'a str,
    part_number: u32,
    size: u64,
}

impl Backend {
    #[instrument(skip(self, chunk))]
    pub async fn store_staged_chunk(
        &self,
        namespace: &str,
        upload_id: &str,
        chunk: Bytes,
        offset: u64,
    ) -> Result<(), Error> {
        let key = path_builder::upload_staged_container_path(namespace, upload_id, offset);
        self.store.put(&key, chunk).await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn load_staged_chunk(
        &self,
        namespace: &str,
        upload_id: &str,
        offset: u64,
    ) -> Result<Vec<u8>, Error> {
        let key = path_builder::upload_staged_container_path(namespace, upload_id, offset);
        match self.store.get(&key).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn save_hasher(
        &self,
        name: &str,
        uuid: &str,
        offset: u64,
        state: Vec<u8>,
    ) -> Result<(), Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        self.store.put(&hash_state_path, Bytes::from(state)).await?;
        Ok(())
    }

    pub async fn load_hasher(&self, name: &str, uuid: &str, offset: u64) -> Result<Sha256, Error> {
        let hash_state_path = path_builder::upload_hash_context_path(name, uuid, "sha256", offset);
        let state = self.store.get(&hash_state_path).await?;
        Sha256::from_state(&state)
    }

    async fn resolve_uniform_state(
        &self,
        name: &str,
        uuid: &str,
        upload_path: &str,
        append: bool,
    ) -> Result<(Option<String>, Vec<Part>), Error> {
        if append {
            match self.read_session(name, uuid).await {
                Ok(record) => Ok((record.multipart_upload_id, record.parts)),
                Err(Error::UploadNotFound) => self.discover_multipart_upload(upload_path).await,
                Err(e) => Err(e),
            }
        } else {
            self.store.abort_pending_uploads(upload_path).await?;
            Ok((None, Vec::new()))
        }
    }

    async fn stream_part(
        &self,
        name: &str,
        uuid: &str,
        ctx: ChunkContext<'_>,
        reader: &mut HashingReader<impl AsyncRead + Unpin + Send + Sync>,
    ) -> Result<(), Error> {
        let (tx, rx) = mpsc::channel::<Bytes>(FRAME_BUFFER_CAPACITY);

        let store = self.store.clone();
        let upload_path = ctx.upload_path.to_string();
        let upload_id_str = ctx.upload_id.to_string();
        let upload_handle = AbortOnDropHandle::new(tokio::spawn(async move {
            let id = UploadId::new(&upload_id_str);
            store
                .upload_part(
                    &upload_path,
                    &id,
                    ctx.part_number,
                    ctx.size,
                    channel_stream(rx),
                )
                .await
        }));

        let mut part_reader = reader.take(ctx.size);
        let mut sent = 0;
        let mut buf = BytesMut::with_capacity(FRAME_SIZE);
        loop {
            buf.clear();
            let n = part_reader.read_buf(&mut buf).await?;
            if n == 0 {
                break;
            }
            sent += u64::try_from(n)?;
            if tx.send(buf.split().freeze()).await.is_err() {
                return match upload_handle.await {
                    Ok(Ok(_etag)) => Err(Error::StorageBackend(
                        "upload task stopped before receiving complete body".to_string(),
                    )),
                    Ok(Err(error)) => Err(error.into()),
                    Err(join_error) => Err(self
                        .handle_upload_task_failure(
                            UploadMode::Uniform,
                            name,
                            uuid,
                            ctx.upload_path,
                            ctx.upload_id,
                            join_error,
                        )
                        .await),
                };
            }
        }

        if sent != ctx.size {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!("expected {} bytes for multipart part, got {sent}", ctx.size),
            )
            .into());
        }

        drop(tx);
        match upload_handle.await {
            Ok(Ok(_etag)) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(join_error) => Err(self
                .handle_upload_task_failure(
                    UploadMode::Uniform,
                    name,
                    uuid,
                    ctx.upload_path,
                    ctx.upload_id,
                    join_error,
                )
                .await),
        }
    }

    #[instrument(skip(self, stream))]
    pub async fn write_upload_uniform(
        &self,
        name: &str,
        uuid: &str,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        content_length: u64,
        append: bool,
    ) -> Result<(Digest, u64), Error> {
        if append && content_length == 0 {
            let size = self.get_upload_size_uniform(name, uuid).await?;
            let digest = self.load_hasher(name, uuid, size).await?.digest();
            return Ok((digest, size));
        }

        let upload_path = path_builder::upload_path(name, uuid);

        let (mut upload_id, part_list) = self
            .resolve_uniform_state(name, uuid, &upload_path, append)
            .await?;

        let mut uploaded_size = total_uploaded_size(&part_list);
        let mut uploaded_parts = next_part_number(part_list.len())?;

        let staged = self.load_staged_chunk(name, uuid, uploaded_size).await?;
        let staged_len = u64::try_from(staged.len())?;
        let hasher = self.load_hasher(name, uuid, uploaded_size).await?;

        let reader = Cursor::new(staged).chain(stream);
        let mut reader = HashingReader::with_hasher(reader, hasher);

        let mut total_size = uploaded_size;
        let available = staged_len + content_length;
        let full_parts = available / self.multipart_part_size;
        let remainder = available % self.multipart_part_size;

        for _ in 0..full_parts {
            let upload_id = if let Some(id) = &upload_id {
                id
            } else {
                let id = self
                    .store
                    .create_multipart(&upload_path)
                    .await?
                    .into_inner();
                upload_id.insert(id)
            };
            self.stream_part(
                name,
                uuid,
                ChunkContext {
                    upload_path: &upload_path,
                    upload_id,
                    part_number: uploaded_parts,
                    size: self.multipart_part_size,
                },
                &mut reader,
            )
            .await?;
            uploaded_parts += 1;
            uploaded_size += self.multipart_part_size;
            total_size += self.multipart_part_size;
            if append {
                self.save_hasher(name, uuid, total_size, reader.serialized_state())
                    .await?;
            }
        }

        if remainder > 0 {
            let mut staged = Vec::with_capacity(usize::try_from(remainder)?);
            let mut remainder_reader = (&mut reader).take(remainder);
            remainder_reader.read_to_end(&mut staged).await?;
            if u64::try_from(staged.len())? != remainder {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "expected {remainder} bytes for staged multipart remainder, got {}",
                        staged.len()
                    ),
                )
                .into());
            }
            self.store_staged_chunk(name, uuid, Bytes::from(staged), uploaded_size)
                .await?;
            total_size += remainder;
        }

        if !append || remainder > 0 {
            self.save_hasher(name, uuid, total_size, reader.serialized_state())
                .await?;
        }

        let digest = reader.digest();
        Ok((digest, total_size))
    }

    #[instrument(skip(self))]
    pub async fn get_upload_size_uniform(&self, name: &str, uuid: &str) -> Result<u64, Error> {
        let parts_size = match self.read_session(name, uuid).await {
            Ok(record) => total_uploaded_size(&record.parts),
            Err(Error::UploadNotFound) => {
                let key = path_builder::upload_path(name, uuid);
                let (_, parts) = self.discover_multipart_upload(&key).await?;
                total_uploaded_size(&parts)
            }
            Err(e) => return Err(e),
        };

        let mut size = parts_size;
        let staged_path = path_builder::upload_staged_container_path(name, uuid, size);
        if let Ok(meta) = self.store.head(&staged_path).await {
            size += meta.size;
        }
        Ok(size)
    }

    /// Prepare the uniform upload for the transactional commit step.
    ///
    /// Runs all S3-protocol work (flushing the staged remainder into the
    /// multipart upload, then completing the multipart) so that the assembled
    /// object lands at `upload_path`. Returns a [`CompletionPlan`] that tells
    /// `complete_via_session` which `Mutation` to use in the engine
    /// transaction (`Move` or `PutEmpty`).
    ///
    /// The upload-container cleanup is **not** performed here; it happens
    /// after the transaction commits in `complete_via_session`.
    #[instrument(skip(self))]
    pub async fn prepare_complete_upload_uniform(
        &self,
        name: &str,
        uuid: &str,
        digest: Option<&Digest>,
    ) -> Result<CompletionPlan, Error> {
        let key = path_builder::upload_path(name, uuid);

        let (upload_id, mut parts) = match self.read_session(name, uuid).await {
            Ok(record) if record.multipart_upload_id.is_some() => {
                (record.multipart_upload_id, record.parts)
            }
            Ok(_) | Err(Error::UploadNotFound) => self.discover_multipart_upload(&key).await?,
            Err(e) => return Err(e),
        };
        let mut size = total_uploaded_size(&parts);

        let source_key = path_builder::upload_staged_container_path(name, uuid, size);

        if upload_id.is_none() {
            match self.store.head(&source_key).await {
                Ok(meta) => {
                    let staged_size = meta.size;
                    size += staged_size;
                    let digest = self.resolve_upload_digest(name, uuid, size, digest).await?;
                    return Ok(CompletionPlan::Move {
                        src: source_key,
                        digest,
                    });
                }
                Err(StorageError::NotFound) if size == 0 => {
                    let digest = self.resolve_upload_digest(name, uuid, size, digest).await?;
                    return Ok(CompletionPlan::PutEmpty { digest });
                }
                Err(StorageError::NotFound) => return Err(Error::UploadNotFound),
                Err(e) => return Err(e.into()),
            }
        }

        let upload_id = upload_id.ok_or(Error::UploadNotFound)?;

        if let Ok(meta) = self.store.head(&source_key).await {
            let staged_size = meta.size;
            let part_number = next_part_number(parts.len())?;
            let id = UploadId::new(&upload_id);
            let etag = self
                .store
                .upload_part_copy(&source_key, &key, &id, part_number, None)
                .await?;
            parts.push(Part {
                part_number,
                etag,
                size: staged_size,
            });
            size += staged_size;
        }

        let digest = self.resolve_upload_digest(name, uuid, size, digest).await?;

        self.complete_multipart_upload(&key, &upload_id, &parts)
            .await?;

        Ok(CompletionPlan::Move { src: key, digest })
    }
}
