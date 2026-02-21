use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hyper::{Response, StatusCode, header::CONTENT_TYPE};
use serde::Serialize;
use tracing::instrument;

use crate::{
    command::server::response_body::ResponseBody,
    oci::{Digest, Manifest, Namespace, Platform as OciPlatform},
    registry::{Error, Registry, metadata_store::link_kind::LinkKind},
};

impl Registry {
    #[instrument(skip(self))]
    pub async fn handle_list_repositories(&self) -> Result<Response<ResponseBody>, Error> {
        #[derive(Serialize, Debug)]
        struct RepositoryInfo {
            name: String,
            namespace_count: usize,
            pull_through_cache: bool,
            immutable_tags: bool,
        }

        #[derive(Serialize, Debug)]
        struct RepositoriesResponse {
            repositories: Vec<RepositoryInfo>,
        }

        let mut repositories = Vec::with_capacity(self.repositories.len());

        for name in self.repositories.keys() {
            let namespaces = self.list_repository_namespaces(name).await?;
            let (is_pull_through, _, immutable_tags, _) = self.get_repository_config(name);
            repositories.push(RepositoryInfo {
                name: name.clone(),
                namespace_count: namespaces.len(),
                pull_through_cache: is_pull_through,
                immutable_tags,
            });
        }

        repositories.sort_by(|a, b| a.name.cmp(&b.name));

        let response = RepositoriesResponse { repositories };
        let body = serde_json::to_string(&response)?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(ResponseBody::fixed(body.into_bytes()))?)
    }

    #[instrument(skip(self))]
    pub async fn handle_list_namespaces(
        &self,
        repository: &str,
    ) -> Result<Response<ResponseBody>, Error> {
        #[derive(Serialize, Debug)]
        struct NamespaceInfo {
            name: String,
            manifest_count: usize,
            upload_count: usize,
        }

        #[derive(Serialize, Debug)]
        struct NamespacesResponse {
            repository: String,
            namespaces: Vec<NamespaceInfo>,
            pull_through_cache: bool,
            upstream_urls: Vec<String>,
            immutable_tags: bool,
            immutable_tags_exclusions: Vec<String>,
        }

        let namespace_names = self.list_repository_namespaces(repository).await?;
        let mut namespaces = Vec::with_capacity(namespace_names.len());

        for name_str in namespace_names {
            let name = Namespace::new(&name_str).map_err(|_| Error::NameInvalid)?;
            let manifest_count = self.count_manifests(&name).await?;
            let upload_count = self.count_uploads(&name).await?;
            namespaces.push(NamespaceInfo {
                name: name_str,
                manifest_count,
                upload_count,
            });
        }

        let (is_pull_through, upstream_urls, immutable_tags, immutable_tags_exclusions) =
            self.get_repository_config(repository);

        let response = NamespacesResponse {
            repository: repository.to_string(),
            namespaces,
            pull_through_cache: is_pull_through,
            upstream_urls,
            immutable_tags,
            immutable_tags_exclusions,
        };

        let body = serde_json::to_string(&response)?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(ResponseBody::fixed(body.into_bytes()))?)
    }

    #[instrument(skip(self))]
    #[allow(clippy::too_many_lines)]
    pub async fn handle_list_revisions(
        &self,
        namespace: &Namespace,
    ) -> Result<Response<ResponseBody>, Error> {
        #[derive(Serialize, Debug, Clone)]
        struct Platform {
            os: String,
            architecture: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            variant: Option<String>,
        }

        impl From<OciPlatform> for Platform {
            fn from(p: OciPlatform) -> Self {
                Platform {
                    os: p.os,
                    architecture: p.architecture,
                    variant: p.variant,
                }
            }
        }

        #[derive(Serialize, Debug, Clone)]
        struct ParentRef {
            digest: String,
            tags: Vec<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            platform: Option<Platform>,
        }

        #[derive(Serialize, Debug, Clone)]
        #[serde(rename_all = "camelCase")]
        struct ReferrerInfo {
            digest: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            artifact_type: Option<String>,
            #[serde(skip_serializing_if = "HashMap::is_empty")]
            annotations: HashMap<String, String>,
        }

        #[derive(Serialize, Debug)]
        struct ManifestEntry {
            digest: String,
            tags: Vec<String>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            parents: Vec<ParentRef>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            referrers: Vec<ReferrerInfo>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pushed_at: Option<DateTime<Utc>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            last_pulled_at: Option<DateTime<Utc>>,
        }

        #[derive(Serialize, Debug)]
        struct RevisionsResponse<'a> {
            name: &'a str,
            manifests: Vec<ManifestEntry>,
        }

        let all_revisions = self.collect_all_revisions(namespace).await?;
        let digest_to_tags = self.build_digest_to_tags_map(namespace).await?;

        let mut child_to_parents: HashMap<Digest, Vec<(Digest, Option<Platform>)>> = HashMap::new();
        let mut docker_referrers: HashMap<Digest, Vec<ReferrerInfo>> = HashMap::new();

        for digest in &all_revisions {
            if let Ok(blob_data) = self.blob_store.read_blob(digest).await
                && let Ok(manifest) = Manifest::from_slice(&blob_data)
            {
                for child_descriptor in &manifest.manifests {
                    if let Some(subject_digest_str) = child_descriptor
                        .annotations
                        .get("vnd.docker.reference.digest")
                    {
                        if let Ok(subject_digest) = subject_digest_str.parse::<Digest>() {
                            let mut annotations = child_descriptor.annotations.clone();
                            if let Ok(child_blob) =
                                self.blob_store.read_blob(&child_descriptor.digest).await
                                && let Ok(child_manifest) = Manifest::from_slice(&child_blob)
                            {
                                for layer in &child_manifest.layers {
                                    if let Some(predicate_type) =
                                        layer.annotations.get("in-toto.io/predicate-type")
                                    {
                                        annotations.insert(
                                            "in-toto.io/predicate-type".to_string(),
                                            predicate_type.clone(),
                                        );
                                        break;
                                    }
                                }
                            }
                            docker_referrers.entry(subject_digest).or_default().push(
                                ReferrerInfo {
                                    digest: child_descriptor.digest.to_string(),
                                    artifact_type: child_descriptor.artifact_type.clone(),
                                    annotations,
                                },
                            );
                        }
                    } else {
                        let platform = child_descriptor.platform.clone().map(Platform::from);
                        child_to_parents
                            .entry(child_descriptor.digest.clone())
                            .or_default()
                            .push((digest.clone(), platform));
                    }
                }
            }
        }

        let mut manifests: Vec<ManifestEntry> = Vec::with_capacity(all_revisions.len());
        for digest in all_revisions {
            let tags = digest_to_tags.get(&digest).cloned().unwrap_or_default();
            let parents = child_to_parents
                .get(&digest)
                .map(|parents| {
                    parents
                        .iter()
                        .map(|(parent_digest, platform)| ParentRef {
                            digest: parent_digest.to_string(),
                            tags: digest_to_tags
                                .get(parent_digest)
                                .cloned()
                                .unwrap_or_default(),
                            platform: platform.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut referrers: Vec<ReferrerInfo> =
                docker_referrers.remove(&digest).unwrap_or_default();

            if let Ok(oci_referrers) = self.get_referrers(namespace, &digest, None).await {
                for descriptor in oci_referrers {
                    referrers.push(ReferrerInfo {
                        digest: descriptor.digest.to_string(),
                        artifact_type: descriptor.artifact_type,
                        annotations: descriptor.annotations,
                    });
                }
            }

            let (pushed_at, last_pulled_at) = self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
                .await
                .map(|m| (m.created_at, m.accessed_at))
                .unwrap_or((None, None));

            manifests.push(ManifestEntry {
                digest: digest.to_string(),
                tags,
                parents,
                referrers,
                pushed_at,
                last_pulled_at,
            });
        }

        let response = RevisionsResponse {
            name: namespace,
            manifests,
        };

        let body = serde_json::to_string(&response)?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(ResponseBody::fixed(body.into_bytes()))?)
    }

    #[instrument(skip(self))]
    pub async fn handle_list_uploads(
        &self,
        namespace: &Namespace,
    ) -> Result<Response<ResponseBody>, Error> {
        #[derive(Serialize, Debug)]
        struct UploadEntry {
            uuid: String,
            size: u64,
            started_at: DateTime<Utc>,
        }

        #[derive(Serialize, Debug)]
        struct UploadsResponse<'a> {
            name: &'a str,
            uploads: Vec<UploadEntry>,
        }

        let mut all_uploads = Vec::new();
        let mut continuation_token = None;

        loop {
            let (uploads, next_token) = self
                .blob_store
                .list_uploads(namespace, 1000, continuation_token)
                .await?;

            for uuid in uploads {
                if let Ok((_, size, started_at)) =
                    self.blob_store.read_upload_summary(namespace, &uuid).await
                {
                    all_uploads.push(UploadEntry {
                        uuid,
                        size,
                        started_at,
                    });
                }
            }

            if next_token.is_none() {
                break;
            }
            continuation_token = next_token;
        }

        let response = UploadsResponse {
            name: namespace,
            uploads: all_uploads,
        };

        let body = serde_json::to_string(&response)?;

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(ResponseBody::fixed(body.into_bytes()))?)
    }

    fn get_repository_config(&self, name: &str) -> (bool, Vec<String>, bool, Vec<String>) {
        if let Some(repo) = self.repositories.get(name) {
            let upstream_urls: Vec<String> = repo.upstreams.iter().map(|u| u.url.clone()).collect();
            let is_pull_through = !upstream_urls.is_empty();
            let immutable_tags = repo.immutable_tags || self.global_immutable_tags;
            let exclusions = if repo.immutable_tags_exclusions.is_empty() {
                self.global_immutable_tags_exclusions.clone()
            } else {
                repo.immutable_tags_exclusions.clone()
            };
            (is_pull_through, upstream_urls, immutable_tags, exclusions)
        } else {
            (
                false,
                Vec::new(),
                self.global_immutable_tags,
                self.global_immutable_tags_exclusions.clone(),
            )
        }
    }

    async fn count_manifests(&self, namespace: &Namespace) -> Result<usize, Error> {
        Ok(self.metadata_store.count_manifests(namespace).await?)
    }

    async fn count_uploads(&self, namespace: &Namespace) -> Result<usize, Error> {
        let mut count = 0;
        let mut continuation_token = None;

        loop {
            let (uploads, next_token) = self
                .blob_store
                .list_uploads(namespace, 1000, continuation_token)
                .await?;

            count += uploads.len();

            if next_token.is_none() {
                break;
            }
            continuation_token = next_token;
        }

        Ok(count)
    }

    async fn collect_all_revisions(&self, namespace: &Namespace) -> Result<Vec<Digest>, Error> {
        let mut all_revisions = Vec::new();
        let mut continuation_token = None;

        loop {
            let (revisions, next_token) = self
                .metadata_store
                .list_revisions(namespace, 1000, continuation_token)
                .await?;

            all_revisions.extend(revisions);

            if next_token.is_none() {
                break;
            }
            continuation_token = next_token;
        }

        Ok(all_revisions)
    }

    async fn build_digest_to_tags_map(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<Digest, Vec<String>>, Error> {
        let mut all_tags = Vec::new();
        let mut last: Option<String> = None;

        loop {
            let (tags, next_last) = self.metadata_store.list_tags(namespace, 1000, last).await?;
            all_tags.extend(tags);
            if next_last.is_none() {
                break;
            }
            last = next_last;
        }

        let mut digest_to_tags: HashMap<Digest, Vec<String>> = HashMap::new();

        for tag in all_tags {
            let link = LinkKind::Tag(tag.clone());
            if let Ok(link_metadata) = self.metadata_store.read_link(namespace, &link, false).await
            {
                digest_to_tags
                    .entry(link_metadata.target)
                    .or_default()
                    .push(tag);
            }
        }

        Ok(digest_to_tags)
    }

    async fn list_repository_namespaces(&self, repository: &str) -> Result<Vec<String>, Error> {
        if !self.repositories.contains_key(repository) {
            return Err(Error::NameUnknown);
        }

        let mut matching_namespaces = Vec::new();
        let mut continuation_token = None;

        loop {
            let (namespaces, next_token) = self
                .metadata_store
                .list_namespaces(1000, continuation_token)
                .await?;

            for ns in namespaces {
                if ns == repository || ns.starts_with(&format!("{repository}/")) {
                    matching_namespaces.push(ns);
                }
            }

            if next_token.is_none() {
                break;
            }
            continuation_token = next_token;
        }

        matching_namespaces.sort_unstable();
        Ok(matching_namespaces)
    }
}
