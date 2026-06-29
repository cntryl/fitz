use super::ADMIN_SESSION_COOKIE;
use hyper::header::COOKIE;

pub(super) fn extract_cookie_value<B>(
    req: &hyper::Request<B>,
    cookie_name: &str,
) -> Option<String> {
    let cookie_header = req.headers().get(COOKIE)?.to_str().ok()?;
    cookie_header.split(';').find_map(|cookie| {
        let trimmed = cookie.trim();
        let (name, value) = trimmed.split_once('=')?;
        if name == cookie_name {
            Some(value.to_string())
        } else {
            None
        }
    })
}

pub(super) fn build_cookie(token: &str, secure: bool, max_age: i64) -> String {
    let mut cookie = format!(
        "{ADMIN_SESSION_COOKIE}={token}; HttpOnly; Path=/; SameSite=Strict; Max-Age={max_age}"
    );

    if secure {
        cookie.push_str("; Secure");
    }

    cookie
}

pub(super) fn clear_cookie(secure: bool) -> String {
    let mut cookie =
        format!("{ADMIN_SESSION_COOKIE}=; HttpOnly; Path=/; SameSite=Strict; Max-Age=0");
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}
