use rmcp::model::{Prompt, PromptArgument, Resource, ResourceTemplate};

const STATIC_DOCUMENTS: [(&str, &str, &str, &str); 3] = [
    (
        "fitz://docs/domain-guarantees",
        "domain-guarantees",
        "Fitz domain guarantees",
        include_str!("../../../../../docs/admin/mcp/domain-guarantees.md"),
    ),
    (
        "fitz://docs/operational-fields",
        "operational-fields",
        "MCP operational fields",
        include_str!("../../../../../docs/admin/mcp/operational-fields.md"),
    ),
    (
        "fitz://docs/troubleshooting",
        "troubleshooting",
        "Guided troubleshooting",
        include_str!("../../../../../docs/admin/mcp/troubleshooting.md"),
    ),
];

pub(super) fn prompts() -> Vec<Prompt> {
    let mut prompts = vec![
        Prompt::new(
            "diagnose_broker",
            Some("Summarize broker health and current global operational signals."),
            None,
        ),
        resource_prompt(),
        timeline_prompt(),
    ];
    prompts.extend(domain_prompts());
    prompts
}

fn resource_prompt() -> Prompt {
    Prompt::new(
        super::READ_PROMPT,
        Some("Inspect one Fitz resource using explicit route family and realm scope."),
        Some(scope_arguments(true, None)),
    )
}

fn timeline_prompt() -> Prompt {
    Prompt::new(
        super::TIMELINE_PROMPT,
        Some("Review bounded recent events for one Fitz resource using explicit scope."),
        Some(scope_arguments(
            true,
            Some("Maximum recent timeline entries, from 1 through 50"),
        )),
    )
}

fn scope_arguments(
    include_scheme: bool,
    limit_description: Option<&'static str>,
) -> Vec<PromptArgument> {
    let names: &[&str] = if include_scheme {
        &["scheme", "route_family", "realm", "area", "resource"]
    } else {
        &["route_family", "realm", "area", "resource"]
    };
    let mut arguments = names
        .iter()
        .map(|name| {
            PromptArgument::new(*name)
                .with_required(true)
                .with_description(if *name == "route_family" {
                    "Explicit broker route family; independent of realm"
                } else {
                    "Scope value for the selected Fitz resource"
                })
        })
        .collect::<Vec<_>>();
    if limit_description.is_some() || !include_scheme {
        let argument = PromptArgument::new("limit").with_required(false);
        arguments.push(match limit_description {
            Some(description) => argument.with_description(description),
            None => argument,
        });
    }
    arguments
}

fn domain_prompts() -> Vec<Prompt> {
    [
        ("diagnose_queue", "queue", "Queue"),
        ("diagnose_stream", "stream", "Stream"),
        ("diagnose_kv", "kv", "KV"),
        ("diagnose_lease", "lease", "Lease"),
        ("diagnose_schedule", "schedule", "Schedule"),
        ("diagnose_notice", "notice", "Notice"),
        ("diagnose_rpc", "rpc", "RPC"),
    ]
    .into_iter()
    .map(|(name, domain, display)| {
        Prompt::new(
            name,
            Some(format!(
                "Diagnose one {display} resource and its recent transitions. Domain {domain} is fixed by this prompt."
            )),
            Some(scope_arguments(false, Some("Maximum recent timeline entries, from 1 through 50"))),
        )
    })
    .collect()
}

pub(super) fn documentation_resources() -> Vec<Resource> {
    STATIC_DOCUMENTS
        .into_iter()
        .map(|(uri, name, title, contents)| {
            Resource::new(uri, name)
                .with_title(title)
                .with_description(
                    "Versioned Fitz MCP documentation; contains no broker-specific state",
                )
                .with_mime_type("text/markdown")
                .with_size(contents.len() as u64)
        })
        .collect()
}

pub(super) fn documentation(uri: &str) -> Option<&'static str> {
    STATIC_DOCUMENTS
        .iter()
        .find(|(resource_uri, _, _, _)| *resource_uri == uri)
        .map(|(_, _, _, contents)| *contents)
}

pub(super) fn resource_templates() -> Vec<ResourceTemplate> {
    crate::runtime::DomainKind::ALL
        .into_iter()
        .map(|domain| {
            ResourceTemplate::new(
                format!(
                    "fitz://resource/{}/{{route_family}}/{{realm}}/{{area}}/{{resource}}",
                    domain.as_str()
                ),
                format!("{}_resource_detail", domain.as_str()),
            )
            .with_description(format!(
                "Current bounded {} resource detail. route_family is independent of realm.",
                domain.as_str()
            ))
            .with_mime_type("application/json")
        })
        .collect()
}
