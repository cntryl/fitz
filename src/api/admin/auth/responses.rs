use super::{AuthFailure, LoginRequest};
use crate::api::http::{Body, Response};
use http_body_util::BodyExt;
use hyper::header::SET_COOKIE;
use hyper::StatusCode;

/// Parses an admin login request body as JSON.
///
/// # Errors
///
/// Returns [`AuthFailure::InvalidCredentials`] when the request body cannot be
/// read or decoded as a valid [`LoginRequest`].
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

/// Builds the `204 No Content` response for a newly created admin session.
///
/// # Panics
///
/// Panics if the response builder rejects the provided `Set-Cookie` header.
pub fn session_created_response(set_cookie: &str) -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, set_cookie)
        .body(Body::default())
        .unwrap()
}

/// Builds the `204 No Content` response for an admin logout.
///
/// # Panics
///
/// Panics if the response builder rejects the provided `Set-Cookie` header.
pub fn session_deleted_response(clear_cookie_header: &str) -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, clear_cookie_header)
        .body(Body::default())
        .unwrap()
}
