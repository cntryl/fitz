use rmcp::model::{Prompt, PromptArgument};

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
