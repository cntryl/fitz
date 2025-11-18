use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub aud: Option<String>,
    pub exp: Option<u64>,
    pub perms: Option<Vec<String>>,
    pub scope: Option<String>,  // Space-separated scopes
    pub roles: Option<Vec<String>>,  // Array of role strings
}

/// A tiny mock validator that accepts any token of the form "mock:<subject>[:<aud>]"
/// Returns parsed claims when valid, otherwise None.
pub fn validate_mock_token(token: &str) -> Option<Claims> {
    if !token.starts_with("mock:") {
        return None;
    }
    let rest = &token[5..];
    // Support optional permission list after '|': mock:sub[:aud]|perm1,perm2
    let (head, perms_opt) = match rest.split_once('|') {
        Some((h, p)) => (h, Some(p)),
        None => (rest, None),
    };
    let parts: Vec<&str> = head.split(':').collect();
    let sub = parts.first()?.to_string();
    let aud = parts.get(1).map(|s| s.to_string());
    let perms = perms_opt.as_ref().map(|p| {
        p.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
    });
    
    // Build scope string from perms
    let scope = perms_opt.map(|p| p.replace(',', " "));
    
    // For mock tokens, roles same as perms
    let roles = perms.clone();
    
    Some(Claims {
        sub,
        aud,
        exp: None,
        perms,
        scope,
        roles,
    })
}
