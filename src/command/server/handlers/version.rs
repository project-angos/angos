use hyper::{Response, StatusCode};

use crate::{
    command::server::{error::Error, handlers::build_response, response_body::ResponseBody},
    registry::version::ApiVersionResponse,
};

pub fn handle_get_api_version() -> Result<Response<ResponseBody>, Error> {
    let response = ApiVersionResponse::default();
    build_response(StatusCode::OK, response.headers, ResponseBody::empty())
}
