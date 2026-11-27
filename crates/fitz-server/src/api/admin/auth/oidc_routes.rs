//! Public browser redirects for the admin OIDC flow.

use super::{cookies::extract_cookie_value, oidc, AdminAuth};
use crate::api::http::{Body, Response};
use hyper::{
    header::{CACHE_CONTROL, LOCATION, REFERRER_POLICY, SET_COOKIE},
    Request, StatusCode,
};
use url::form_urlencoded;

const FAILURE: &str = "/login?oidc_error=1";

pub(crate) async fn oidc_start<B>(request: Request<B>, admin: &AdminAuth) -> Response {
    let Some(oidc) = &admin.oidc else {
        return crate::api::admin::not_found();
    };
    let next = query_value(request.uri().query(), "next").unwrap_or_else(|| "/".to_owned());
    drop(request);
    let Ok(login) = oidc.start(&next).await else {
        return redirect(FAILURE);
    };
    let mut response = redirect(login.location.as_str());
    append_cookie(
        &mut response,
        &cookie(oidc::BROWSER_COOKIE, &login.browser, oidc::CHALLENGE_TTL),
    );
    response
}

pub(crate) async fn oidc_callback<B>(request: Request<B>, admin: &AdminAuth) -> Response {
    let Some(oidc) = &admin.oidc else {
        return crate::api::admin::not_found();
    };
    let query = request.uri().query();
    let (Some(state), Some(code), Some(browser)) = (
        query_value(query, "state"),
        query_value(query, "code"),
        extract_cookie_value(&request, oidc::BROWSER_COOKIE),
    ) else {
        return redirect(FAILURE);
    };
    if query_value(query, "error").is_some() {
        return redirect(FAILURE);
    }
    let previous = extract_cookie_value(&request, oidc::SESSION_COOKIE);
    drop(request);
    let Ok(login) = oidc.complete(admin, &state, &browser, &code).await else {
        return redirect(FAILURE);
    };

    if let Some(previous) = previous {
        oidc.revoke(&previous);
    }
    let mut response = redirect(&login.location);
    append_cookie(&mut response, &cookie(oidc::BROWSER_COOKIE, "", 0));
    append_cookie(&mut response, &admin.clear_session_cookie());
    append_cookie(
        &mut response,
        &cookie(oidc::SESSION_COOKIE, &login.token, login.max_age),
    );
    response
}

pub(super) fn clear_admin_oidc_cookie(
    previous: Option<&str>,
    admin: &AdminAuth,
    response: &mut Response,
) {
    if let Some(oidc) = &admin.oidc {
        if let Some(token) = previous {
            oidc.revoke(token);
        }
        append_cookie(response, &cookie(oidc::SESSION_COOKIE, "", 0));
        append_cookie(response, &cookie(oidc::BROWSER_COOKIE, "", 0));
    }
}

fn query_value(query: Option<&str>, name: &str) -> Option<String> {
    let query = query.filter(|query| query.len() <= 16 * 1024)?;
    let mut matches = form_urlencoded::parse(query.as_bytes()).filter(|(key, _)| key == name);
    let (_, value) = matches.next()?;
    matches.next().is_none().then(|| value.into_owned())
}

fn cookie(name: &str, value: &str, max_age: u64) -> String {
    format!("{name}={value}; HttpOnly; Secure; Path=/; SameSite=Lax; Max-Age={max_age}")
}

fn append_cookie(response: &mut Response, cookie: &str) {
    response.headers_mut().append(
        SET_COOKIE,
        cookie.parse().expect("safe admin session cookie"),
    );
}

fn redirect(location: &str) -> Response {
    hyper::Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(LOCATION, location)
        .header(CACHE_CONTROL, "no-store")
        .header(REFERRER_POLICY, "no-referrer")
        .body(Body::default())
        .expect("safe OIDC redirect")
}
