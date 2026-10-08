use super::*;

fn words(alphabet: &[&str], max_depth: usize) -> Vec<String> {
    let mut all = vec![String::new()];
    let mut layer = vec![String::new()];
    for _ in 0..max_depth {
        layer = layer
            .iter()
            .flat_map(|prefix| {
                alphabet.iter().map(move |segment| {
                    if prefix.is_empty() {
                        (*segment).to_owned()
                    } else {
                        format!("{prefix}/{segment}")
                    }
                })
            })
            .collect();
        all.extend(layer.iter().cloned());
    }
    all
}

#[test]
fn should_agree_with_direct_matching_for_exhaustive_legal_small_suffixes() {
    // Arrange
    let patterns = words(&["a", "b", "*", "**"], 4);
    let routes = words(&["a", "b"], 5)
        .into_iter()
        .map(|word| route(&format!("notice://realm/{word}")))
        .collect::<Vec<_>>();
    let mut mismatches = Vec::new();
    let mut comparisons = 0;

    // Act
    for word in patterns {
        let pattern_route = route(&format!("notice://realm/{word}"));
        let Ok(pattern) = crate::runtime::matcher::compile_registration_pattern(
            pattern_route.as_str(),
            "notice",
            crate::runtime::matcher::PatternDepth::Flexible,
        ) else {
            continue;
        };
        let mut index = SubscriptionIndex::new();
        index.insert(family(1), &pattern_route, sub_id(1));
        for published in &routes {
            comparisons += 1;
            let expected = usize::from(pattern.matches(published));
            let matches = index.match_all(family(1), published);
            if matches.len() != expected {
                mismatches.push((pattern_route.clone(), published.clone(), matches));
            }
        }
    }

    // Assert
    assert!(comparisons > 10_000, "legal suffix coverage was lost");
    assert!(
        mismatches.is_empty(),
        "matcher disagreement: {mismatches:?}"
    );
}

#[test]
fn should_match_suffixes_across_inline_and_spilled_frontier_sizes() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    index.insert(family(1), &route("notice://realm/**/end"), sub_id(1));
    let routes = [8, 9, 63, 64, 130].map(|depth| {
        let middle = vec!["a"; depth - 2].join("/");
        (
            route(&format!("notice://realm/{middle}/end")),
            route(&format!("notice://realm/{middle}/missing")),
        )
    });

    // Act
    let results = routes.map(|(matching, missing)| {
        (
            index.match_all(family(1), &matching),
            index.match_all(family(1), &missing),
        )
    });

    // Assert
    for (matching, missing) in results {
        assert_eq!(matching.as_slice(), &[sub_id(1)]);
        assert!(missing.is_empty());
    }
}

#[test]
fn should_preserve_subscriber_ids_when_equal_suffixes_are_interleaved_after_removal() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let patterns = ["a", "a", "b", "b", "a", "*/a", "*/b"];
    for (id, suffix) in (1..).zip(patterns) {
        index.insert(
            family(1),
            &route(&format!("notice://realm/**/{suffix}")),
            sub_id(id),
        );
    }
    index.remove(family(1), &route("notice://realm/**/a"), sub_id(1));

    // Act
    let mut matches = index.match_all(family(1), &route("notice://realm/area/a"));
    matches.sort_unstable_by_key(|id| id.0);

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(2), sub_id(5), sub_id(6)]);
}
