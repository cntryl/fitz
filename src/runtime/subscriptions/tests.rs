use super::segments_cache::SegmentsCache;
use super::*;

fn route(s: &str) -> Route {
    Route::new(s)
}

fn family(id: u64) -> RouteFamily {
    RouteFamily::try_from(id).expect("test family must fit in u32")
}

fn sub_id(n: u64) -> SubscriptionId {
    SubscriptionId(n)
}

#[test]
fn should_match_exact_pattern() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    let pattern = route("notify://realm/orders/create");

    // Act
    index.insert(f, &pattern, sub_id(1));
    let matches = index.match_all(f, &pattern);

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_not_match_different_route() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/create"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/update"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_match_single_star_wildcard() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

    // Act
    let matches_create = index.match_all(f, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(matches_create.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_not_cross_star_boundary() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/items/create"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_match_double_star_zero_segments() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_double_star_multiple_segments() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_not_duplicate_empty_suffix_double_star_matches() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_double_star_with_suffix() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/**/created"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/created"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_not_cross_realm_with_double_star() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://acme/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://other/orders"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_isolate_by_route_family() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f1 = family(1);
    let f2 = family(2);
    index.insert(f1, &route("notify://realm/orders/*"), sub_id(1));
    index.insert(f2, &route("notify://realm/orders/*"), sub_id(2));

    // Act
    let f1_matches = index.match_all(f1, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(f1_matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_isolate_families_independently() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f1 = family(1);
    let f2 = family(2);
    index.insert(f1, &route("notify://realm/orders/*"), sub_id(1));
    index.insert(f2, &route("notify://realm/orders/*"), sub_id(2));

    // Act
    let f2_matches = index.match_all(f2, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(f2_matches.as_slice(), &[sub_id(2)]);
}

#[test]
fn should_handle_multiple_subscribers_to_same_pattern() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    let pattern = route("notify://realm/orders/*");
    index.insert(f, &pattern, sub_id(1));
    index.insert(f, &pattern, sub_id(2));

    // Act
    let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
    matches.sort_by_key(|id| id.0);

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2)]);
}

#[test]
fn should_remove_subscription() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    let pattern = route("notify://realm/orders/*");
    index.insert(f, &pattern, sub_id(1));

    // Act
    index.remove(f, &pattern, sub_id(1));
    let matches = index.match_all(f, &route("notify://realm/orders/create"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_handle_mixed_patterns() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/create"), sub_id(1));
    index.insert(f, &route("notify://realm/orders/*"), sub_id(2));
    index.insert(f, &route("notify://realm/**"), sub_id(3));

    // Act
    let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
    matches.sort_by_key(|id| id.0);

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2), sub_id(3)]);
}

#[test]
fn should_return_unique_matches_for_overlapping_exact_star_doublestar_patterns() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/create"), sub_id(1));
    index.insert(f, &route("notify://realm/orders/*"), sub_id(2));
    index.insert(f, &route("notify://realm/**/create"), sub_id(3));
    index.insert(f, &route("notify://realm/**"), sub_id(4));

    // Act
    let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
    matches.sort_by_key(|id| id.0);

    // Assert
    assert_eq!(
        matches.as_slice(),
        &[sub_id(1), sub_id(2), sub_id(3), sub_id(4)]
    );
}

#[test]
fn should_deduplicate_subscription_id_across_overlapping_patterns() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/create"), sub_id(7));
    index.insert(f, &route("notify://realm/orders/*"), sub_id(7));
    index.insert(f, &route("notify://realm/**/create"), sub_id(7));
    index.insert(f, &route("notify://realm/**"), sub_id(7));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(7)]);
}

#[test]
fn should_match_sparse_10k_fanout_with_one_unique_id() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    for id in 0..10_000 {
        index.insert(
            f,
            &route(&format!("notify://realm/orders/item{id}/action")),
            sub_id(id),
        );
    }

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/item0/action"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(0)]);
}

#[test]
fn should_match_dense_10k_fanout_with_all_unique_ids() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    for id in 0..10_000 {
        index.insert(f, &route("notify://realm/**/action"), sub_id(id));
    }

    // Act
    let mut matches =
        index.match_all_with_capacity(f, &route("notify://realm/orders/items/action"), 10_000);
    matches.sort_by_key(|id| id.0);

    // Assert
    assert_eq!(matches.len(), 10_000);
    for (expected, actual) in matches.iter().enumerate() {
        assert_eq!(actual.0, expected as u64);
    }
}

#[test]
fn should_match_sparse_route_family_without_preallocating_gaps() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let sparse_family = family(1_000_000);
    let pattern = route("notify://realm/orders/*");

    // Act
    index.insert(sparse_family, &pattern, sub_id(7));
    let matches = index.match_all(sparse_family, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(7)]);
}

#[test]
fn should_bound_segments_cache_without_flushing_existing_entries() {
    // Arrange
    let mut cache = SegmentsCache::new();
    let pinned_route = "notify://realm/orders/pinned";

    // Act
    let original = cache.get_or_parse(pinned_route);
    for index in 0..4096 {
        let route = format!("notify://realm/orders/{index}");
        let _ = cache.get_or_parse(&route);
    }
    let reloaded = cache.get_or_parse(pinned_route);

    // Assert
    assert!(Arc::ptr_eq(&original, &reloaded));
    assert_eq!(cache.len(), 4096);
}

#[test]
fn should_match_double_star_suffix_with_many_segments() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/**/created"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/items/created"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_double_star_at_end() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_double_star_at_end_no_segments() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_double_star_at_end_many_segments() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/items/create"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_not_match_double_star_suffix_when_missing() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://acme/**/created"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://acme/orders/updated"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_remove_one_of_many_subscriptions() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    let pattern = route("notify://realm/orders/*");
    index.insert(f, &pattern, sub_id(1));
    index.insert(f, &pattern, sub_id(2));
    index.insert(f, &pattern, sub_id(3));

    // Act
    index.remove(f, &pattern, sub_id(2));
    let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
    matches.sort_by_key(|id| id.0);

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(3)]);
}

#[test]
fn should_match_single_star_for_create_operation() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_single_star_for_update_operation() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/update"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_single_star_for_delete_operation() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/delete"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_match_star_with_multiple_wildcards() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/*/*/created"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/create/created"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_not_match_insufficient_segments_for_star_chain() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/*/*/created"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/orders/created"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_match_route_after_batch_insert() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    let batch: Vec<(Route, SubscriptionId)> = vec![
        (route("notify://realm/orders/create"), sub_id(1)),
        (route("notify://realm/orders/*"), sub_id(2)),
        (route("notify://realm/**"), sub_id(3)),
    ];

    // Act
    index.insert_batch(f, &batch);
    let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
    matches.sort_by_key(|id| id.0);

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2), sub_id(3)]);
}

#[test]
fn should_count_subscriptions_across_patterns() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/create"), sub_id(1));
    index.insert(f, &route("notify://realm/orders/*"), sub_id(2));
    index.insert(f, &route("notify://realm/**"), sub_id(3));

    // Act
    let count = index.count_subscriptions(f);

    // Assert
    assert_eq!(count, 3);
}

#[test]
fn should_count_zero_for_unknown_family() {
    // Arrange
    let index = SubscriptionIndex::new();

    // Act
    let count = index.count_subscriptions(family(999));

    // Assert
    assert_eq!(count, 0);
}

#[test]
fn should_match_empty_route_family() {
    // Arrange
    let index = SubscriptionIndex::new();

    // Act
    let matches = index.match_all(family(1), &route("notify://realm/orders/create"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_prevent_duplicate_terminal_subscribers() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    let pattern = route("notify://realm/orders/create");

    // Act
    index.insert(f, &pattern, sub_id(1));
    index.insert(f, &pattern, sub_id(1));
    let matches = index.match_all(f, &route("notify://realm/orders/create"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_prevent_duplicate_double_star_subscribers() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    let pattern = route("notify://realm/**");

    // Act
    index.insert(f, &pattern, sub_id(1));
    index.insert(f, &pattern, sub_id(1));
    let matches = index.match_all(f, &route("notify://realm/orders"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_handle_remove_from_empty_index() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);

    // Act — should not panic
    index.remove(f, &route("notify://realm/orders/*"), sub_id(1));

    // Assert
    assert_eq!(index.count_subscriptions(f), 0);
}

#[test]
fn should_handle_remove_nonexistent_pattern() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/create"), sub_id(1));

    // Act — remove a different pattern, should not panic
    index.remove(f, &route("notify://realm/orders/update"), sub_id(2));

    // Assert — original subscription still there
    assert_eq!(index.count_subscriptions(f), 1);
}

#[test]
fn should_not_match_double_star_suffix_at_end() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://acme/**/created"), sub_id(1));

    // Act — route ends before suffix "created"
    let matches = index.match_all(f, &route("notify://acme/orders"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_handle_shallow_routes() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/**"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_handle_deep_routes_with_star() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/*/*/*/*"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/a/b/c/d"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_not_match_deep_route_with_shallow_star_pattern() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/*/*/*/*"), sub_id(1));

    // Act — only 3 segments after realm, pattern needs 4
    let matches = index.match_all(f, &route("notify://realm/a/b/c"));

    // Assert
    assert!(matches.is_empty());
}

#[test]
fn should_match_double_star_suffix_deep() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/**/action"), sub_id(1));

    // Act
    let matches = index.match_all(f, &route("notify://realm/a/b/c/action"));

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1)]);
}

#[test]
fn should_collect_results_with_capacity() {
    // Arrange
    let mut index = SubscriptionIndex::new();
    let f = family(1);
    index.insert(f, &route("notify://realm/orders/create"), sub_id(1));
    index.insert(f, &route("notify://realm/orders/*"), sub_id(2));
    index.insert(f, &route("notify://realm/**"), sub_id(3));

    // Act
    let mut matches = index.match_all_with_capacity(f, &route("notify://realm/orders/create"), 3);
    matches.sort_by_key(|id| id.0);

    // Assert
    assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2), sub_id(3)]);
}

// Property-based tests

proptest::proptest! {
    #[test]
    fn should_match_bruteforce_scan_for_generated_patterns(
        // Generate up to 8 patterns, each with up to 6 segments
        patterns in proptest::collection::vec(
            proptest::string::string_regex("[a-z]{1,5}").unwrap(),
            1..=8,
        ),
        route_str in proptest::string::string_regex("[a-z]{1,5}(/[a-z]{1,5}){0,5}").unwrap(),
    ) {
        use crate::runtime::matcher::Pattern;

        // Arrange
        let f = family(1);
        let mut index = SubscriptionIndex::new();

        // Insert each pattern as an exact route (no wildcards for simplicity)
        for (i, p) in patterns.iter().enumerate() {
            let full_pattern = format!("test://{p}");
            index.insert(f, &route(&full_pattern), sub_id(i as u64));
        }

        let full_route = format!("test://{route_str}");
        let r = route(&full_route);

        // Act
        // Get trie matches
        let mut trie_matches = index.match_all(f, &r);
        trie_matches.sort_by_key(|id| id.0);

        // Brute-force scan: check each pattern with Pattern::matches
        let mut bf_matches = SubscriptionMatches::new();
        for (i, p) in patterns.iter().enumerate() {
            let full_pattern = format!("test://{p}");
            let pattern = Pattern::new(&full_pattern);
            if pattern.matches(&r) {
                bf_matches.push(sub_id(i as u64));
            }
        }
        bf_matches.sort_by_key(|id| id.0);

        // Assert
        assert_eq!(trie_matches, bf_matches, "trie diverged from brute-force for patterns={patterns:?} route={full_route}");
    }

    #[test]
    fn should_preserve_roundtrip_after_insert_remove(
        pattern_str in proptest::string::string_regex("[a-z]{1,5}(/[a-z]{1,5}){0,3}").unwrap(),
        route_str in proptest::string::string_regex("[a-z]{1,5}(/[a-z]{1,5}){0,5}").unwrap(),
    ) {
        use crate::runtime::matcher::Pattern;

        // Arrange
        let f = family(1);
        let mut index = SubscriptionIndex::new();
        let full_pattern = format!("test://{pattern_str}");
        let full_route = format!("test://{route_str}");
        let p = route(&full_pattern);
        let r = route(&full_route);

        // Act
        index.insert(f, &p, sub_id(42));
        let matches_after_insert = index.match_all(f, &r);
        index.remove(f, &p, sub_id(42));
        let matches_after_remove = index.match_all(f, &r);

        // Assert
        // After remove, subscription 42 should never appear
        assert!(!matches_after_remove.contains(&sub_id(42)));

        // If it matched before, it should have contained 42
        let pattern = Pattern::new(&full_pattern);
        if pattern.matches(&r) {
            assert!(matches_after_insert.contains(&sub_id(42)));
        }
    }
}

#[test]
fn should_match_double_star_dense_pattern_in_bounded_time() {
    // A backtracking `**` implementation is exponential in the number of `**`
    // segments. Registrations accept non-adjacent `**` up to the shared segment
    // bound, so one legal Notice/RPC pattern could otherwise stall the domain
    // actor for minutes on a single publish.

    // Arrange
    let mut segments = Vec::new();
    for _ in 0..12 {
        segments.push("**");
        segments.push("a");
    }
    segments.push("zzz");
    let pattern = route(&format!("notice://{}", segments.join("/")));
    let published = route(&format!("notice://{}", vec!["a"; 40].join("/")));
    let f = family(1);
    let mut index = SubscriptionIndex::new();
    index.insert(f, &pattern, sub_id(1));

    // Act
    let started_at = std::time::Instant::now();
    let matches = index.match_all(f, &published);
    let elapsed = started_at.elapsed();

    // Assert
    assert!(matches.is_empty());
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "dense ** matching took {elapsed:?}; matching must stay polynomial"
    );
}

#[test]
fn should_match_double_star_suffix_consistently_with_pattern_matcher() {
    // `SubscriptionIndex` and `Pattern` carry separate `**` implementations, so
    // they must agree on every shape or delivery diverges from authorization.

    // Arrange
    let cases = [
        ("notice://a/**/d", "notice://a/b/c/d", true),
        ("notice://a/**/d", "notice://a/d", true),
        ("notice://a/**/d", "notice://a/d/e", false),
        ("notice://**/d", "notice://a/b/d", true),
        ("notice://a/**/c/**/e", "notice://a/b/c/d/e", true),
        ("notice://a/**/c/**/e", "notice://a/b/c/d", false),
        ("notice://a/**", "notice://a", true),
        ("notice://a/*/**/z", "notice://a/b/z", true),
        ("notice://a/*/**/z", "notice://a/z", false),
    ];
    let f = family(1);

    // Act
    let results: Vec<(bool, bool)> = cases
        .iter()
        .map(|(pattern_str, route_str, _)| {
            let mut index = SubscriptionIndex::new();
            index.insert(f, &route(pattern_str), sub_id(1));
            let indexed = !index.match_all(f, &route(route_str)).is_empty();
            let direct =
                crate::runtime::matcher::Pattern::new(pattern_str).matches(&route(route_str));
            (indexed, direct)
        })
        .collect();

    // Assert
    for ((pattern_str, route_str, expected), (indexed, direct)) in cases.iter().zip(&results) {
        assert_eq!(indexed, expected, "index: {pattern_str} vs {route_str}");
        assert_eq!(direct, expected, "matcher: {pattern_str} vs {route_str}");
    }
}

#[test]
fn should_reclaim_trie_nodes_when_patterns_are_removed() {
    // The node pool only ever grew before: a session churning distinct wildcard
    // patterns retained one node per pattern segment for the process lifetime,
    // and the per-session wildcard cap bounds live registrations only.

    // Arrange
    let f = family(1);
    let mut index = SubscriptionIndex::new();
    let churn = |index: &mut SubscriptionIndex, generation: usize| {
        for pattern_index in 0..64_u64 {
            let pattern = route(&format!(
                "notice://realm/gen{generation}/area{pattern_index}/*"
            ));
            index.insert(f, &pattern, sub_id(pattern_index));
            index.remove(f, &pattern, sub_id(pattern_index));
        }
    };

    // Act
    churn(&mut index, 0);
    let pool_after_first_generation = index.node_pool_len();
    for generation in 1..10 {
        churn(&mut index, generation);
    }
    let pool_after_ten_generations = index.node_pool_len();

    // Assert
    assert_eq!(
        pool_after_first_generation, pool_after_ten_generations,
        "node pool must not grow across churned pattern generations"
    );
    assert_eq!(index.count_subscriptions(f), 0);
}

#[test]
fn should_keep_matching_correct_after_node_slots_are_reused() {
    // A reused slot must carry none of its previous occupant's state.

    // Arrange
    let f = family(1);
    let mut index = SubscriptionIndex::new();
    let retired = route("notice://realm/retired/*");
    index.insert(f, &retired, sub_id(1));
    index.remove(f, &retired, sub_id(1));

    // Act
    let replacement = route("notice://realm/live/*");
    index.insert(f, &replacement, sub_id(2));
    let retired_matches = index.match_all(f, &route("notice://realm/retired/thing"));
    let live_matches = index.match_all(f, &route("notice://realm/live/thing"));

    // Assert
    assert!(
        retired_matches.is_empty(),
        "removed pattern must not match through a recycled node"
    );
    assert_eq!(live_matches.as_slice(), [sub_id(2)]);
}
