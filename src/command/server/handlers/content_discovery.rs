use hyper::{Response, StatusCode, header::LINK};
use serde::Serialize;

use crate::{
    command::server::{
        ServerContext,
        error::Error,
        handlers::build_response,
        response::{APPLICATION_JSON, HeaderMap, OCI_FILTERS_APPLIED, ResponseHeaders},
        response_body::ResponseBody,
    },
    oci::{Descriptor, Digest, Namespace, OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_SCHEMA_VERSION, Tag},
};

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ReferrerList {
    schema_version: i32,
    // The OCI index media type serializes as its string, so the constant is
    // carried directly rather than re-parsed through the fallible `MediaType`.
    media_type: &'static str,
    manifests: Vec<Descriptor>,
}

impl Default for ReferrerList {
    fn default() -> Self {
        ReferrerList {
            schema_version: OCI_MANIFEST_SCHEMA_VERSION,
            media_type: OCI_INDEX_MEDIA_TYPE,
            manifests: Vec::new(),
        }
    }
}

#[derive(Serialize)]
struct CatalogBody {
    repositories: Vec<String>,
}

#[derive(Serialize)]
struct TagsBody<'a> {
    name: &'a str,
    tags: Vec<Tag>,
}

fn referrers_headers(artifact_type_filtered: bool) -> HeaderMap {
    let headers = ResponseHeaders::new().content_type(OCI_INDEX_MEDIA_TYPE);
    if artifact_type_filtered {
        headers
            .with(OCI_FILTERS_APPLIED, "artifactType")
            .into_inner()
    } else {
        headers.into_inner()
    }
}

fn paginated_json_headers(link: Option<&str>) -> HeaderMap {
    let headers = ResponseHeaders::new().content_type(APPLICATION_JSON);
    match link {
        Some(link) => headers
            .with(LINK.as_str(), format!("<{link}>; rel=\"next\""))
            .into_inner(),
        None => headers.into_inner(),
    }
}

pub async fn handle_get_referrers(
    context: &ServerContext,
    namespace: &Namespace,
    digest: &Digest,
    artifact_type: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let (manifests, filtered) = context
        .registry
        .get_referrers(namespace, digest, artifact_type)
        .await?;

    let referrer_list = ReferrerList {
        manifests,
        ..ReferrerList::default()
    };

    build_response(
        StatusCode::OK,
        referrers_headers(filtered),
        ResponseBody::fixed(serde_json::to_vec(&referrer_list)?),
    )
}

pub async fn handle_list_catalog(
    context: &ServerContext,
    n: Option<u16>,
    last: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let (repositories, link) = context.registry.list_catalog_entries(n, last).await?;

    build_response(
        StatusCode::OK,
        paginated_json_headers(link.as_deref()),
        ResponseBody::fixed(serde_json::to_vec(&CatalogBody { repositories })?),
    )
}

pub async fn handle_list_tags(
    context: &ServerContext,
    namespace: &Namespace,
    n: Option<u16>,
    last: Option<String>,
) -> Result<Response<ResponseBody>, Error> {
    let (tags, link) = context
        .registry
        .list_tag_entries(namespace, n, last)
        .await?;

    build_response(
        StatusCode::OK,
        paginated_json_headers(link.as_deref()),
        ResponseBody::fixed(serde_json::to_vec(&TagsBody {
            name: namespace.as_ref(),
            tags,
        })?),
    )
}
