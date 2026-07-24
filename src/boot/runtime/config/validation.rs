use super::env::env_non_empty;
use super::{BootResult, LocalListenerExposure, DEFAULT_LOCAL_WS_ALLOWED_ORIGIN_VALUES};

pub(super) fn configured_ws_allowed_origins(
    configured: &[crate::api::origin::ExactOrigin],
    configured_error: Option<&String>,
) -> BootResult<Vec<crate::api::origin::ExactOrigin>> {
    if let Some(error) = configured_error {
        return Err(error.clone().into());
    }
    match env_non_empty("FITZ_WS_ALLOWED_ORIGINS") {
        Some(value) => crate::api::origin::parse_exact_origin_list(&value)
            .map_err(|error| format!("FITZ_WS_ALLOWED_ORIGINS {error}").into()),
        None => Ok(configured.to_vec()),
    }
}

pub(super) fn parse_ws_allowed_origins_from_env(
) -> Option<(Vec<crate::api::origin::ExactOrigin>, Option<String>)> {
    env_non_empty("FITZ_WS_ALLOWED_ORIGINS").map(|value| {
        crate::api::origin::parse_exact_origin_list(&value).map_or_else(
            |error| (Vec::new(), Some(format!("FITZ_WS_ALLOWED_ORIGINS {error}"))),
            |origins| (origins, None),
        )
    })
}

pub(super) fn default_local_ws_allowed_origins(
) -> (Vec<crate::api::origin::ExactOrigin>, Option<String>) {
    let origins = DEFAULT_LOCAL_WS_ALLOWED_ORIGIN_VALUES
        .iter()
        .map(|origin| {
            crate::api::origin::parse_exact_origin(origin)
                .expect("default local WebSocket origin must be valid")
        })
        .collect();
    (origins, None)
}

pub(super) fn validate_public_origin_security(
    env_key: &str,
    origins: &[crate::api::origin::ExactOrigin],
) -> BootResult<()> {
    for origin in origins {
        if origin.scheme() == "http" && !origin.is_loopback() {
            return Err(format!(
                "{env_key} entries must use https unless they are loopback origins"
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn validate_ingress_security_boundary(
    assume_external_tls: bool,
    local_listener_exposure: LocalListenerExposure,
    bind_addr: &str,
    authenticated_listener: bool,
) -> BootResult<bool> {
    if assume_external_tls && local_listener_exposure.is_host_loopback_edge() {
        return Err(
            "FITZ_ASSUME_EXTERNAL_TLS and FITZ_ASSUME_LOCAL_LOOPBACK_EDGE are mutually exclusive"
                .into(),
        );
    }

    let public_bind =
        !bind_addr_is_loopback(bind_addr) && !local_listener_exposure.is_host_loopback_edge();
    if authenticated_listener && public_bind && !assume_external_tls {
        return Err(
            "FITZ_ASSUME_EXTERNAL_TLS=true is required when authenticated listeners bind to a non-loopback address"
                .into(),
        );
    }

    Ok(public_bind)
}

pub(super) fn validate_local_loopback_edge_origins(
    local_listener_exposure: LocalListenerExposure,
    origins: &[crate::api::origin::ExactOrigin],
) -> BootResult<()> {
    if local_listener_exposure.is_host_loopback_edge()
        && origins.iter().any(|origin| !origin.is_loopback())
    {
        return Err(
            "FITZ_ASSUME_LOCAL_LOOPBACK_EDGE requires loopback FITZ_WS_ALLOWED_ORIGINS entries"
                .into(),
        );
    }
    Ok(())
}

pub(super) fn validate_admin_browser_security(
    protected_admin_configured: bool,
    public_bind: bool,
    assume_local_loopback_edge: bool,
) -> BootResult<()> {
    if let Some(value) = env_non_empty("FITZ_ADMIN_COOKIE_SECURE") {
        let cookie_secure = value.parse::<bool>().map_err(|_| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "FITZ_ADMIN_COOKIE_SECURE must be true or false",
            )) as Box<dyn std::error::Error>
        })?;
        if protected_admin_configured && public_bind && !cookie_secure {
            return Err(
                "FITZ_ADMIN_COOKIE_SECURE=false is only allowed on loopback admin listeners".into(),
            );
        }
    }

    let Some(public_origin) = env_non_empty("FITZ_ADMIN_PUBLIC_ORIGIN") else {
        if protected_admin_configured && public_bind {
            return Err(
                "FITZ_ADMIN_PUBLIC_ORIGIN is required when protected admin binds to a non-loopback address"
                    .into(),
            );
        }
        return Ok(());
    };

    let origin = crate::api::origin::parse_exact_origin(&public_origin)
        .map_err(|error| format!("FITZ_ADMIN_PUBLIC_ORIGIN {error}"))?;
    if assume_local_loopback_edge && !origin.is_loopback() {
        return Err(
            "FITZ_ASSUME_LOCAL_LOOPBACK_EDGE requires a loopback FITZ_ADMIN_PUBLIC_ORIGIN".into(),
        );
    }
    if protected_admin_configured && public_bind && origin.scheme() != "https" {
        return Err(
            "FITZ_ADMIN_PUBLIC_ORIGIN must use https on non-loopback admin listeners".into(),
        );
    }
    validate_public_origin_security("FITZ_ADMIN_PUBLIC_ORIGIN", std::slice::from_ref(&origin))?;

    Ok(())
}

pub(super) fn bind_addr_is_loopback(bind_addr: &str) -> bool {
    let host = bind_addr
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']');

    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }

    host.parse::<std::net::IpAddr>()
        .is_ok_and(|addr| addr.is_loopback())
}
