//! Centralized configuration and environment variable parsing
//!
//! Environment variables:
//! - FITZ_WS_PORT: u16 (default 8080)
//! - FITZ_TCP_PORT: u16 (default 7070)
//! - FITZ_TLS_CERTS: path to PEM-encoded certificate chain (optional)
//! - FITZ_TLS_KEY: path to PEM-encoded private key (optional, PKCS#8 preferred; RSA supported)
//! - CONTROL_ROUTE: control plane route (default "self")
//! - CONTROL_CLIENT_ID: optional client id
//! - CONTROL_CLIENT_SECRET: optional client secret
//! - BROKER_ACK_WINDOW: usize (default 128)
//! - BROKER_TEST_ACK_DELAY_MS: u64 (default 0)

use std::env;
pub mod vars;

#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub cert_chain: Vec<rustls::Certificate>,
    pub priv_key: rustls::PrivateKey,
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub ws_port: u16,
    pub tcp_port: u16,
    pub tls: Option<TlsConfig>,
}

#[derive(Clone, Debug)]
pub struct ControlConfig {
    pub route: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub transport: TransportConfig,
    pub control: ControlConfig,
    pub auth: AuthConfig,
    pub broker: BrokerConfig,
}

#[derive(Clone, Debug)]
pub struct BrokerConfig {
    pub ack_window: usize,
    pub test_ack_delay_ms: u64,
    pub enforce_authz: bool,
}

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub no_auth: bool,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub client_permissions: Option<Vec<String>>,
    pub oidc_jwks: Option<String>,
}

pub fn load() -> AppConfig {
    AppConfig {
        transport: TransportConfig {
            ws_port: env::var(vars::FITZ_WS_PORT)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8080),
            tcp_port: env::var(vars::FITZ_TCP_PORT)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(7070),
            tls: load_tls_from_env(),
        },
        control: ControlConfig {
            route: env::var(vars::CONTROL_ROUTE).unwrap_or_else(|_| "self".to_string()),
            client_id: env::var(vars::CONTROL_CLIENT_ID).ok(),
            client_secret: env::var(vars::CONTROL_CLIENT_SECRET).ok(),
        },
        auth: AuthConfig {
            no_auth: env::var(vars::FITZ_NO_AUTH)
                .ok()
                .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            client_id: env::var(vars::FITZ_CLIENT_ID).ok(),
            client_secret: env::var(vars::FITZ_CLIENT_SECRET).ok(),
            client_permissions: env::var(vars::FITZ_CLIENT_PERMISSIONS)
                .ok()
                .map(|s| s.split(',').map(|p| p.trim().to_string()).collect()),
            oidc_jwks: env::var(vars::FITZ_OIDC_JWKS).ok(),
        },
        broker: BrokerConfig {
            ack_window: env::var("BROKER_ACK_WINDOW")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(128usize),
            test_ack_delay_ms: env::var("BROKER_TEST_ACK_DELAY_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0),
            enforce_authz: env::var("BROKER_ENFORCE_AUTHZ")
                .ok()
                .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        },
    }
}

pub fn load_tls_from_env() -> Option<TlsConfig> {
    let certs_path = env::var(vars::FITZ_TLS_CERTS).ok()?;
    let key_path = env::var(vars::FITZ_TLS_KEY).ok()?;
    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(certs_path).ok()?);
    let mut key_reader = std::io::BufReader::new(std::fs::File::open(key_path).ok()?);
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .ok()?
        .into_iter()
        .map(rustls::Certificate)
        .collect::<Vec<_>>();
    // try PKCS8 first, then RSA
    let pkcs8_keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader).ok()?;
    let key = if let Some(k) = pkcs8_keys.into_iter().next() {
        rustls::PrivateKey(k)
    } else {
        let key_file = std::fs::File::open(env::var(vars::FITZ_TLS_KEY).ok()?).ok()?;
        let mut kr = std::io::BufReader::new(key_file);
        let rsa_keys = rustls_pemfile::rsa_private_keys(&mut kr).ok()?;
        rsa_keys.into_iter().next().map(rustls::PrivateKey)?
    };
    Some(TlsConfig {
        cert_chain: certs,
        priv_key: key,
    })
}
