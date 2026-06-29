mod model_and_sessions;
mod queue_schedule_lease_notice;
mod rpc_matching_and_filters;

use crate::api::admin::list;
use crate::boot::Runtime;
use crate::runtime::routing::{route_quad, route_triplet};
use std::collections::{BTreeMap, HashMap};

use model_and_sessions::{AdminSearchResult, Candidate, SearchOptions};
use queue_schedule_lease_notice::{
    collect_lease_candidates, collect_notice_candidates, collect_queue_candidates,
    collect_schedule_candidates,
};
use rpc_matching_and_filters::{
    candidate, collect_rpc_candidates, domain_href, match_candidate, matches_optional_filter,
    normalize_route_family_filter, parse_query_params, push_resource_candidate, resource_href,
    scope_filter_matches,
};

pub(crate) use model_and_sessions::handle_search;

#[cfg(test)]
use model_and_sessions::search_runtime;

#[cfg(test)]
mod tests;
