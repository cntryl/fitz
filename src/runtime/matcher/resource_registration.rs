//! Select registration coverage of a concrete resource without enumerating operations.
use super::{Pattern, PatternSegment};

pub(crate) fn matches_resource_registration(
    route: &str,
    realm: &str,
    area: &str,
    resource: &str,
) -> bool {
    matches_registration_prefix(route, &[realm, area, resource])
}

pub(crate) fn matches_registration_prefix(route: &str, components: &[&str]) -> bool {
    let Some((scheme, _)) = route.split_once("://") else {
        return false;
    };
    let pattern = Pattern::new(route);
    let mut states = vec![false; pattern.segments.len() + 1];
    states[0] = true;
    advance_empty_segments(&pattern.segments, &mut states);
    for component in components {
        let mut next = vec![false; states.len()];
        for (index, segment) in pattern.segments.iter().enumerate() {
            if !states[index] {
                continue;
            }
            match segment {
                PatternSegment::Literal(literal) if literal == component => next[index + 1] = true,
                PatternSegment::Star => next[index + 1] = true,
                PatternSegment::DoubleStar => next[index] = true,
                PatternSegment::Literal(_) => {}
            }
        }
        advance_empty_segments(&pattern.segments, &mut next);
        states = next;
    }
    // Group flexible routes by the requested prefix components. A reachable state
    // can finish with a finite literal/star suffix; ** needs no extra segments.
    let Some(max_suffix_depth) =
        crate::utils::route_shape::MAX_ROUTE_SEGMENTS.checked_sub(components.len())
    else {
        return false;
    };
    let prefix_bytes = scheme.len()
        + 3
        + components.iter().map(|part| part.len()).sum::<usize>()
        + components.len().saturating_sub(1);
    let Some(max_suffix_bytes) =
        crate::utils::route_shape::MAX_ROUTE_BYTES.checked_sub(prefix_bytes)
    else {
        return false;
    };
    let mut suffix_depth = 0;
    let mut suffix_bytes = 0;
    for index in (0..states.len()).rev() {
        if index < pattern.segments.len()
            && !matches!(pattern.segments[index], PatternSegment::DoubleStar)
        {
            suffix_depth += 1;
            suffix_bytes += 1 + match &pattern.segments[index] {
                PatternSegment::Literal(literal) => literal.len(),
                PatternSegment::Star => 1,
                PatternSegment::DoubleStar => unreachable!(),
            };
        }
        if states[index] && suffix_depth <= max_suffix_depth && suffix_bytes <= max_suffix_bytes {
            return true;
        }
    }
    false
}

fn advance_empty_segments(segments: &[PatternSegment], states: &mut [bool]) {
    for (index, segment) in segments.iter().enumerate() {
        if states[index] && matches!(segment, PatternSegment::DoubleStar) {
            states[index + 1] = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_membership_requiring_more_than_the_route_byte_limit() {
        // Arrange
        let route = format!(
            "rpc://**/{}",
            "x".repeat(crate::utils::route_shape::MAX_ROUTE_BYTES - 9)
        );

        // Act
        let matched = matches_resource_registration(&route, "acme", "jobs", "alpha");

        // Assert
        assert!(!matched);
    }

    #[test]
    fn should_reject_membership_requiring_more_than_the_route_depth_limit() {
        // Arrange
        let route = format!("rpc://**/{}", vec!["x"; 63].join("/"));

        // Act
        let matched = matches_resource_registration(&route, "acme", "jobs", "alpha");

        // Assert
        assert!(!matched);
    }

    #[test]
    fn should_agree_with_concrete_matching_over_a_finite_pattern_alphabet() {
        // Arrange
        let alphabet = ["a", "b", "*", "**"];
        let suffixes = (0..=4)
            .flat_map(|depth| {
                (0..(1usize << depth)).map(move |bits| {
                    (0..depth)
                        .map(|index| if bits & (1 << index) == 0 { "a" } else { "b" })
                        .collect::<Vec<_>>()
                        .join("/")
                })
            })
            .collect::<Vec<_>>();
        for first in alphabet {
            for second in alphabet {
                for third in alphabet {
                    for fourth in alphabet {
                        let route = format!("rpc://{first}/{second}/{third}/{fourth}");
                        let pattern = Pattern::new(&route);

                        // Act
                        let expected = suffixes.iter().any(|suffix| {
                            pattern.matches_str(&if suffix.is_empty() {
                                "rpc://a/b/a".into()
                            } else {
                                format!("rpc://a/b/a/{suffix}")
                            })
                        });
                        let matched = matches_resource_registration(&route, "a", "b", "a");

                        // Assert
                        assert_eq!(matched, expected, "{route}");
                    }
                }
            }
        }
    }

    #[test]
    fn should_match_nonterminal_wildcards_and_zero_segment_suffixes() {
        // Arrange
        let registrations = [
            "rpc://acme/**/run",
            "rpc://acme/**/run/**",
            "notice://acme/jobs/*/created",
            "rpc://acme/**/run/done",
        ];

        // Act
        let matched = registrations
            .map(|route| matches_resource_registration(route, "acme", "jobs", "alpha"));

        // Assert
        assert_eq!(matched, [true; 4]);
    }

    #[test]
    fn should_exclude_registrations_that_cannot_match_the_resource() {
        // Arrange
        let registrations = [
            "rpc://other/**/run",
            "rpc://acme/secret/*/run",
            "rpc://acme/jobs/beta/*",
        ];

        // Act
        let matched = registrations
            .map(|route| matches_resource_registration(route, "acme", "jobs", "alpha"));

        // Assert
        assert_eq!(matched, [false; 3]);
    }
}
