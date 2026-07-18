use http_body_util::Full;
use hyper::{Response, StatusCode, body::Bytes, header::CONTENT_TYPE};
use serde::Serialize;

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::json_response, response_body::ResponseBody,
    },
    metrics_provider::metrics_provider,
};

pub fn handle_ui_config(context: &ServerContext) -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct UiConfigResponse<'a> {
        name: &'a str,
    }
    json_response(
        StatusCode::OK,
        &UiConfigResponse {
            name: &context.ui_name,
        },
    )
}

pub fn handle_healthz() -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct HealthResponse {
        status: &'static str,
    }
    json_response(StatusCode::OK, &HealthResponse { status: "ok" })
}

pub async fn handle_readyz(context: &ServerContext) -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct ReadyResponse {
        status: &'static str,
    }

    #[derive(Serialize)]
    struct NotReadyResponse {
        status: &'static str,
        error: String,
    }

    match context.registry.check_ready().await {
        Ok(()) => json_response(StatusCode::OK, &ReadyResponse { status: "ready" }),
        Err(e) => {
            let payload = NotReadyResponse {
                status: "not_ready",
                error: e.to_string(),
            };
            json_response(StatusCode::SERVICE_UNAVAILABLE, &payload)
        }
    }
}

pub fn handle_metrics() -> Result<Response<ResponseBody>, Error> {
    let (content_type, metrics) = metrics_provider().gather()?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .body(ResponseBody::Fixed(Full::new(Bytes::from(metrics))))?)
}
