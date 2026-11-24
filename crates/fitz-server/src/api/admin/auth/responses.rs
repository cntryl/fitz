use super::{AuthFailure, LoginRequest};
use crate::api::http::{Body, Response};
use bytes::{Buf, BytesMut};
use http_body_util::BodyExt;
use hyper::header::SET_COOKIE;
use hyper::StatusCode;
use std::time::Duration;

const MAX_LOGIN_BODY_BYTES: usize = 8 * 1024;
const LOGIN_BODY_READ_TIMEOUT: Duration = Duration::from_secs(5);

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
    let mut body = std::pin::pin!(req.into_body());
    let collect = async {
        let mut bytes = BytesMut::with_capacity(256);
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| AuthFailure::InvalidCredentials)?;
            let Ok(mut data) = frame.into_data() else {
                continue;
            };
            if bytes.len().saturating_add(data.remaining()) > MAX_LOGIN_BODY_BYTES {
                return Err(AuthFailure::InvalidCredentials);
            }
            while data.has_remaining() {
                let chunk = data.chunk();
                bytes.extend_from_slice(chunk);
                let chunk_len = chunk.len();
                data.advance(chunk_len);
            }
        }
        Ok::<_, AuthFailure>(bytes)
    };
    let body = tokio::time::timeout(LOGIN_BODY_READ_TIMEOUT, collect)
        .await
        .map_err(|_| AuthFailure::InvalidCredentials)??;
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
