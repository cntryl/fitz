//! Mint a token for the repository's local Compose authentication fixtures.
use jsonwebtoken::{encode, EncodingKey, Header};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let jwks = match args.as_slice() {
        [] => false,
        [flag] if flag == "--jwks" => true,
        _ => return Err("Usage: cargo run --locked --example local_token [-- --jwks]".into()),
    };
    let secret = if jwks {
        "fitz-jwks-dev-secret"
    } else {
        "dev-test-secret"
    };
    let mut header = Header::default();
    let mut payload = json!({
        "sub":"local-dev", "aud":"fitz", "exp":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + 3600,
        "tid":"dev", "permissions":["notice://dev/**#read"],
    });
    if jwks {
        header.kid = Some("dev-hs256".into());
        payload["iss"] = json!("https://fitz.mock/");
    }
    println!(
        "{}",
        encode(
            &header,
            &payload,
            &EncodingKey::from_secret(secret.as_bytes())
        )?
    );
    Ok(())
}
