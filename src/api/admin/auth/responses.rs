use super::{AuthFailure, LoginRequest};
use crate::api::http::{Body, Response};
use http_body_util::BodyExt;
use hyper::header::SET_COOKIE;
use hyper::StatusCode;

pub async fn parse_login_request<B>(req: hyper::Request<B>) -> Result<LoginRequest, AuthFailure>
where
    B: hyper::body::Body,
{
    let body = req
        .into_body()
        .collect()
        .await
        .map_err(|_| AuthFailure::InvalidCredentials)?
        .to_bytes();
    serde_json::from_slice::<LoginRequest>(&body).map_err(|_| AuthFailure::InvalidCredentials)
}

pub fn session_created_response(set_cookie: &str) -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, set_cookie)
        .body(Body::default())
        .unwrap()
}

pub fn session_deleted_response(clear_cookie_header: &str) -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, clear_cookie_header)
        .body(Body::default())
        .unwrap()
}
