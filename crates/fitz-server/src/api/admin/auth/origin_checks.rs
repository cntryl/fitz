use super::{AdminAuthSettings, AuthFailure};

fn request_header<'a, B>(req: &'a hyper::Request<B>, name: &str) -> Option<&'a str> {
    req.headers().get(name)?.to_str().ok()
}

fn single_header_value<'a, B>(
    req: &'a hyper::Request<B>,
    name: &str,
) -> Result<Option<&'a str>, AuthFailure> {
    let mut values = req.headers().get_all(name).iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthFailure::Csrf);
    }
    first.to_str().map(Some).map_err(|_| AuthFailure::Csrf)
}

pub(super) fn request_origin<B>(
    req: &hyper::Request<B>,
) -> Result<Option<crate::api::origin::ExactOrigin>, AuthFailure> {
    let origin = single_header_value(req, "origin")?;
    let referer = single_header_value(req, "referer")?;
    let parsed_referer = referer
        .map(crate::api::origin::parse_url_origin)
        .transpose()
        .map_err(|_| AuthFailure::Csrf)?;

    let Some(origin) = origin else {
        return Ok(parsed_referer);
    };

    let parsed_origin =
        crate::api::origin::parse_exact_origin(origin).map_err(|_| AuthFailure::Csrf)?;

    if parsed_referer
        .as_ref()
        .is_some_and(|referer| !parsed_origin.same_origin(referer))
    {
        return Err(AuthFailure::Csrf);
    }

    Ok(Some(parsed_origin))
}

pub(super) fn expected_admin_origin<B>(
    req: &hyper::Request<B>,
    settings: &AdminAuthSettings,
) -> Result<crate::api::origin::ExactOrigin, AuthFailure> {
    if let Some(origin) = &settings.public_origin {
        return crate::api::origin::parse_exact_origin(origin).map_err(|_| AuthFailure::Csrf);
    }

    let host = request_header(req, "host").ok_or(AuthFailure::Csrf)?;
    let proto = request_header(req, "x-forwarded-proto")
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("http");
    crate::api::origin::parse_exact_origin(&format!("{proto}://{host}"))
        .map_err(|_| AuthFailure::Csrf)
}
