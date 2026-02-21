use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode, header::CONTENT_TYPE};
use rust_embed::Embed;

use super::{error::Error, response_body::ResponseBody};

#[derive(Embed)]
#[folder = "ui/build"]
struct UiAssets;

pub fn serve_asset(path: &str) -> Result<Response<ResponseBody>, Error> {
    let asset_path = path.trim_start_matches('/');
    let asset_path = if asset_path.is_empty() {
        "index.html"
    } else {
        asset_path
    };

    if let Some(content) = UiAssets::get(asset_path) {
        let mime = mime_guess::from_path(asset_path).first_or_octet_stream();
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, mime.as_ref())
            .body(ResponseBody::Fixed(Full::new(Bytes::from(
                content.data.into_owned(),
            ))))
            .map_err(|e| Error::Internal(e.to_string()));
    }

    if let Some(content) = UiAssets::get("index.html") {
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/html; charset=utf-8")
            .body(ResponseBody::Fixed(Full::new(Bytes::from(
                content.data.into_owned(),
            ))))
            .map_err(|e| Error::Internal(e.to_string()));
    }

    Err(Error::NotFound("UI asset not found".to_string()))
}
