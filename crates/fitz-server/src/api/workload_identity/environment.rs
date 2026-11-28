use super::{AccountFile, AzureFile, AzureVersion, GoogleFile, RoleFile, TrustFile};

fn required<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str, String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("FITZ_WORKLOAD_{name} is required"))
}

pub(super) fn read(
    mut get: impl FnMut(&str) -> Result<Option<String>, String>,
) -> Result<Option<TrustFile>, String> {
    const SETTINGS: [&str; 10] = [
        "PROVIDER",
        "AUDIENCE",
        "ROUTE_FAMILY",
        "PERMISSIONS",
        "SUBJECT",
        "AWS_ACCOUNT_ID",
        "AWS_ISSUER",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_TOKEN_VERSION",
    ];
    let values = SETTINGS
        .map(|key| get(&format!("FITZ_WORKLOAD_{key}")))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    let [provider, audience, route, permissions, subject, account, issuer, tenant, client, version] =
        values.as_slice()
    else {
        unreachable!()
    };
    let provider = required(provider.as_deref(), "PROVIDER")?;
    let audience = required(audience.as_deref(), "AUDIENCE")?.to_owned();
    let binding = RoleFile {
        subject: required(subject.as_deref(), "SUBJECT")?.to_owned(),
        client_id: client.clone(),
        route_family: route
            .as_deref()
            .unwrap_or("1")
            .parse()
            .map_err(|_| "FITZ_WORKLOAD_ROUTE_FAMILY must be a number")?,
        realm: None,
        permissions: required(permissions.as_deref(), "PERMISSIONS")?
            .split(',')
            .map(str::trim)
            .map(str::to_owned)
            .collect(),
    };
    let mut document = TrustFile {
        audience: None,
        accounts: vec![],
        azure: vec![],
        google: None,
    };
    match provider {
        "aws" if tenant.is_none() && client.is_none() && version.is_none() => {
            document.audience = Some(audience);
            document.accounts.push(AccountFile {
                account_id: required(account.as_deref(), "AWS_ACCOUNT_ID")?.to_owned(),
                issuer: required(issuer.as_deref(), "AWS_ISSUER")?.to_owned(),
                algorithms: super::default_aws_algorithms(),
                bindings: vec![binding],
            });
        }
        "azure" if account.is_none() && issuer.is_none() && (binding.subject != "*" || client.is_none()) => {
            let token_version = match version.as_deref().unwrap_or("2.0") {
                "1.0" => AzureVersion::V1,
                "2.0" => AzureVersion::V2,
                _ => return Err("FITZ_WORKLOAD_AZURE_TOKEN_VERSION must be 1.0 or 2.0".into()),
            };
            document.azure.push(AzureFile {
                tenant_id: required(tenant.as_deref(), "AZURE_TENANT_ID")?.to_owned(),
                token_version,
                audience,
                bindings: vec![binding],
            });
        }
        "google" if [account, issuer, tenant, client, version].iter().all(|value| value.is_none()) => {
            document.google = Some(GoogleFile { audience, bindings: vec![binding] });
        }
        _ => return Err("FITZ_WORKLOAD_PROVIDER must be aws, azure, or google, with only matching provider settings".into()),
    }
    Ok(Some(document))
}
