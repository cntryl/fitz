//! Shared guards around domain-owned authorization route canonicalization.

use std::borrow::Cow;

pub(crate) fn canonicalize<'a>(
    scheme: &str,
    route: &'a str,
    canonicalize: impl FnOnce(&'a str) -> Result<Cow<'a, str>, String>,
) -> Result<Cow<'a, str>, String> {
    crate::utils::route_shape::validate_route_shape(route)?;
    if let Some((actual_scheme, _)) = route.split_once("://") {
        if actual_scheme != scheme {
            return Err(format!(
                "{scheme} message route must use {scheme}://, not {actual_scheme}://"
            ));
        }
    }

    let canonical = canonicalize(route)?;
    crate::utils::route_shape::validate_route_shape(canonical.as_ref())?;
    Ok(canonical)
}

#[cfg(test)]
mod tests;
