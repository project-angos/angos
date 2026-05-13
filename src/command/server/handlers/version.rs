use hyper::{Response, StatusCode};

use crate::{
    command::server::{error::Error, handlers::build_response, response_body::ResponseBody},
    registry::version::api_version_response,
};

pub fn handle_get_api_version() -> Result<Response<ResponseBody>, Error> {
    build_response(
        StatusCode::OK,
        api_version_response(),
        ResponseBody::empty(),
    )
}
