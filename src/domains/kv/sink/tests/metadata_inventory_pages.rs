use super::*;
use crate::domains::kv::inventory::{encode_estimate, KvInventoryEstimate};
use crate::domains::kv::KvActor;

fn sink() -> KvDomain {
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    for family in [RouteFamily::new(1), RouteFamily::new(2)] {
        sink.run_on_family_for_tests(family, move |runtime| {
            let mut tx = runtime
                .core
                .store
                .begin(family.id(), crate::domains::kv::TxMode::ReadWrite)
                .unwrap();
            for (realm, area, resource) in [
                ("acme", "jobs", "alpha"),
                ("acme", "jobs", "beta"),
                ("acme", "jobs", "gamma"),
                ("acme", "other", "private"),
                ("sibling", "jobs", "private"),
            ] {
                let metadata = if family.id() == 1 {
                    encode_estimate(KvInventoryEstimate {
                        estimated_record_count: 7,
                        estimated_storage_bytes: 999,
                        estimate_complete: false,
                    })
                } else {
                    b"invalid sibling metadata".to_vec()
                };
                tx.put(
                    KvActor::inventory_metadata_key(realm, area, resource),
                    metadata,
                )
                .unwrap();
                let prefix = KvActor::realm_resource_prefix(realm, area, resource);
                tx.put(
                    KvActor::encode_scoped_key(&prefix, b"private-value"),
                    vec![3; 1024 * 1024],
                )
                .unwrap();
            }
            tx.commit(crate::domains::WritePolicy::Sync).unwrap();
        });
    }
    sink
}

#[test]
fn should_page_only_selected_kv_family_realm_and_area_metadata() {
    // Arrange
    let sink = sink();
    let family = RouteFamily::new(1);

    // Act
    let (first, more) = sink
        .admin_inventory_page(family, "acme", Some("jobs"), None, 2)
        .unwrap();
    let after = first.last().unwrap();
    let (last, last_more) = sink
        .admin_inventory_page(
            family,
            "acme",
            Some("jobs"),
            Some((&after.area, &after.resource)),
            2,
        )
        .unwrap();

    // Assert
    assert!(more);
    assert!(!last_more);
    assert_eq!(
        first
            .iter()
            .map(|entry| entry.resource.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    assert_eq!(last[0].resource, "gamma");
    assert!(first
        .iter()
        .chain(&last)
        .all(|entry| entry.route_family == 1 && entry.realm == "acme" && entry.area == "jobs"));
}

#[test]
fn should_leave_incomplete_estimates_unchanged_after_metadata_pagination() {
    // Arrange
    let sink = sink();
    let family = RouteFamily::new(1);

    // Act
    let (page, _) = sink
        .admin_inventory_page(family, "acme", Some("jobs"), None, 2)
        .unwrap();
    let persisted = sink
        .admin_inventory_metadata_resource(family, "acme", "jobs", "alpha")
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(page[0].estimated_record_count, 7);
    assert_eq!(page[0].estimated_storage_bytes, 999);
    assert!(!page[0].estimate_complete);
    assert_eq!(persisted.estimated_record_count, 7);
    assert_eq!(persisted.estimated_storage_bytes, 999);
    assert!(!persisted.estimate_complete);
}
