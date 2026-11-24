//! Browser Origin parsing and comparison helpers.
//!
//! Fitz treats browser origins as exact `(scheme, host, effective_port)` values.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactOrigin {
    serialized: String,
    scheme: String,
    host: String,
    port: u16,
}

impl ExactOrigin {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.serialized
    }

    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    #[must_use]
    pub fn same_origin(&self, other: &Self) -> bool {
        self.scheme == other.scheme
            && self.host.eq_ignore_ascii_case(&other.host)
            && self.port == other.port
    }

    #[must_use]
    pub fn is_loopback(&self) -> bool {
        self.host.eq_ignore_ascii_case("localhost")
            || self
                .host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|addr| addr.is_loopback())
    }
}

/// Parse an exact browser origin.
///
/// # Errors
///
/// Returns an error when the value is empty, malformed, or not an exact http/https origin.
pub fn parse_exact_origin(value: &str) -> Result<ExactOrigin, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("origin must not be empty".to_string());
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return Err("origin must be an exact http or https origin".to_string());
    }
    if origin_has_explicit_suffix(trimmed) {
        return Err("origin must not include a path, query, or fragment".to_string());
    }

    let url = url::Url::parse(trimmed).map_err(|_| "origin must be a valid URL".to_string())?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err("origin must not include query or fragment".to_string());
    }
    if url.path() != "/" {
        return Err("origin must not include a path".to_string());
    }

    origin_from_url(&url)
}

fn origin_has_explicit_suffix(value: &str) -> bool {
    value
        .split_once("://")
        .is_some_and(|(_, authority_and_suffix)| {
            authority_and_suffix.contains('/')
                || authority_and_suffix.contains('?')
                || authority_and_suffix.contains('#')
        })
}

/// Parse an origin from a full URL.
///
/// # Errors
///
/// Returns an error when the URL is empty, malformed, or not an http/https origin.
pub fn parse_url_origin(value: &str) -> Result<ExactOrigin, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("origin URL must not be empty".to_string());
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return Err("origin URL must be http or https".to_string());
    }

    let url = url::Url::parse(trimmed).map_err(|_| "origin URL must be valid".to_string())?;
    origin_from_url(&url)
}

fn origin_from_url(url: &url::Url) -> Result<ExactOrigin, String> {
    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err("origin scheme must be http or https".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("origin must not include credentials".to_string());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "origin must include a host".to_string())?
        .to_ascii_lowercase();
    if host.contains('*') {
        return Err("origin must be an exact http or https origin".to_string());
    }
    let port = url
        .port_or_known_default()
        .ok_or_else(|| "origin must have a known port".to_string())?;

    let host_for_display = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.clone()
    };
    let serialized = match url.port() {
        Some(explicit_port) => format!("{scheme}://{host_for_display}:{explicit_port}"),
        None => format!("{scheme}://{host_for_display}"),
    };

    Ok(ExactOrigin {
        serialized,
        scheme: scheme.to_string(),
        host,
        port,
    })
}

/// Parse a comma-separated list of exact browser origins.
///
/// # Errors
///
/// Returns an error when any entry is empty or invalid.
pub fn parse_exact_origin_list(value: &str) -> Result<Vec<ExactOrigin>, String> {
    let mut origins = Vec::new();
    for entry in value.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            return Err("origin list must not contain empty entries".to_string());
        }
        origins.push(parse_exact_origin(trimmed)?);
    }
    Ok(origins)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_exact_browser_origin() {
        // Arrange
        let raw = "https://app.example.com";

        // Act
        let origin = parse_exact_origin(raw).expect("parse origin");

        // Assert
        assert_eq!(origin.as_str(), "https://app.example.com");
    }

    #[test]
    fn should_reject_origin_with_path() {
        // Arrange
        let raw = "https://app.example.com/path";

        // Act
        let result = parse_exact_origin(raw);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_origin_with_explicit_trailing_slash() {
        // Arrange
        let raw = "https://app.example.com/";

        // Act
        let result = parse_exact_origin(raw);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_non_exact_browser_origin_values() {
        // Arrange
        let raw_values = [
            "https://app.example.com?next=/admin",
            "https://app.example.com#admin",
            "https://*.example.com",
            "null",
        ];

        // Act
        let results = raw_values.map(parse_exact_origin);

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_parse_origin_from_full_url() {
        // Arrange
        let raw = "https://app.example.com/settings?tab=security";

        // Act
        let origin = parse_url_origin(raw).expect("parse url origin");

        // Assert
        assert_eq!(origin.as_str(), "https://app.example.com");
    }
}
