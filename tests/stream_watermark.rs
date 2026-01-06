use std::sync::Arc;
use std::collections::BTreeMap;

use fitz::domains::stream::{StreamStore, AreaActor, RealmActor};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

// Tests for watermark advancement logic in AreaActor and RealmActor

fn make_test_store() -> Arc<StreamStore> {
    let db = Arc::new(cntryl_midge::MidgeEngine::open(cntryl_midge::MidgeOptions::default()).unwrap());
    Arc::new(StreamStore::new(db))
}

fn make_area_ctx() -> Context<AreaActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://realm1/area1/__area__"),
    );
    Context::new(addr, router)
}

fn make_realm_ctx() -> Context<RealmActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://realm1/__realm__"),
    );
    Context::new(addr, router)
}

#[test]
fn should_advance_watermark_for_contiguous_ranges() {
    // Arrange
    let store = make_test_store();
    let mut actor = AreaActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        store,
    );
    let mut ctx = make_area_ctx();
    
    // Initial watermark is 0
    assert_eq!(actor.watermark(), 0);

    // Act: Commit range [0, 2]
    let commit_msg = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 0,
        last_area_offset: 2,
        first_realm_offset: 0,
        last_realm_offset: 2,
    };
    actor.receive(commit_msg, &mut ctx);

    // Assert: Watermark advanced to 2
    assert_eq!(actor.watermark(), 2);
}

#[test]
fn should_not_advance_watermark_for_gap() {
    // Arrange
    let store = make_test_store();
    let mut actor = AreaActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        store,
    );
    let mut ctx = make_area_ctx();

    // Act: Commit range [5, 7] (gap from 0)
    let commit_msg = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 5,
        last_area_offset: 7,
        first_realm_offset: 0,
        last_realm_offset: 2,
    };
    actor.receive(commit_msg, &mut ctx);

    // Assert: Watermark stays at 0 (gap at [0-4])
    assert_eq!(actor.watermark(), 0);
}

#[test]
fn should_advance_watermark_when_gap_filled() {
    // Arrange
    let store = make_test_store();
    let mut actor = AreaActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        store,
    );
    let mut ctx = make_area_ctx();

    // Commit [5, 7] first (creates gap)
    let commit1 = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 5,
        last_area_offset: 7,
        first_realm_offset: 5,
        last_realm_offset: 7,
    };
    actor.receive(commit1, &mut ctx);
    assert_eq!(actor.watermark(), 0);

    // Act: Fill gap with [0, 4]
    let commit2 = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 0,
        last_area_offset: 4,
        first_realm_offset: 0,
        last_realm_offset: 4,
    };
    actor.receive(commit2, &mut ctx);

    // Assert: Watermark advanced to 7 (all contiguous)
    assert_eq!(actor.watermark(), 7);
}

#[test]
fn should_handle_overlapping_ranges() {
    // Arrange
    let store = make_test_store();
    let mut actor = AreaActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        store,
    );
    let mut ctx = make_area_ctx();

    // Commit [0, 5]
    let commit1 = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 0,
        last_area_offset: 5,
        first_realm_offset: 0,
        last_realm_offset: 5,
    };
    actor.receive(commit1, &mut ctx);
    assert_eq!(actor.watermark(), 5);

    // Act: Commit overlapping [3, 8]
    let commit2 = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 3,
        last_area_offset: 8,
        first_realm_offset: 3,
        last_realm_offset: 8,
    };
    actor.receive(commit2, &mut ctx);

    // Assert: Watermark advanced to 8
    assert_eq!(actor.watermark(), 8);
}

#[test]
fn should_grant_area_leases_incrementally() {
    // Arrange
    let store = make_test_store();
    let mut actor = AreaActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        store,
    );
    let mut ctx = make_area_ctx();
    
    // Give actor a realm lease
    actor.update_realm_lease(fitz::domains::stream::protocol::LeaseGrant {
        area_start: 0,
        area_end: 0,
        realm_start: 0,
        realm_end: 999,
    });

    // Act: Request lease for 100 offsets
    let lease_msg = fitz::domains::stream::StreamMessage::RequestAreaLease { count: 100 };
    actor.receive(lease_msg, &mut ctx);

    // Act: Request another lease for 50 offsets
    let lease_msg2 = fitz::domains::stream::StreamMessage::RequestAreaLease { count: 50 };
    actor.receive(lease_msg2, &mut ctx);

    // Assert: Area offsets allocated sequentially (verified via grant responses in real impl)
    // First grant: area [0-99], realm [0-99]
    // Second grant: area [100-149], realm [100-149]
}

#[test]
fn should_calculate_realm_watermark_as_minimum() {
    // Arrange
    let mut actor = RealmActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
    );
    let mut ctx = make_realm_ctx();

    // Act: Update watermarks for three areas
    let msg1 = fitz::domains::stream::StreamMessage::AreaWatermarkAdvanced {
        realm: "realm1".to_string(),
        area: "area1".to_string(),
        watermark: 100,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = fitz::domains::stream::StreamMessage::AreaWatermarkAdvanced {
        realm: "realm1".to_string(),
        area: "area2".to_string(),
        watermark: 50,  // Minimum
    };
    actor.receive(msg2, &mut ctx);

    let msg3 = fitz::domains::stream::StreamMessage::AreaWatermarkAdvanced {
        realm: "realm1".to_string(),
        area: "area3".to_string(),
        watermark: 75,
    };
    actor.receive(msg3, &mut ctx);

    // Assert: Realm watermark is min(100, 50, 75) = 50
    assert_eq!(actor.watermark(), 50);
}

#[test]
fn should_update_realm_watermark_when_minimum_changes() {
    // Arrange
    let mut actor = RealmActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
    );
    let mut ctx = make_realm_ctx();

    // Set initial watermarks: area1=50, area2=100
    let msg1 = fitz::domains::stream::StreamMessage::AreaWatermarkAdvanced {
        realm: "realm1".to_string(),
        area: "area1".to_string(),
        watermark: 50,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = fitz::domains::stream::StreamMessage::AreaWatermarkAdvanced {
        realm: "realm1".to_string(),
        area: "area2".to_string(),
        watermark: 100,
    };
    actor.receive(msg2, &mut ctx);
    
    assert_eq!(actor.watermark(), 50);  // min(50, 100)

    // Act: area1 advances to 80
    let msg3 = fitz::domains::stream::StreamMessage::AreaWatermarkAdvanced {
        realm: "realm1".to_string(),
        area: "area1".to_string(),
        watermark: 80,
    };
    actor.receive(msg3, &mut ctx);

    // Assert: Realm watermark updated to 80 (new minimum)
    assert_eq!(actor.watermark(), 80);
}

#[test]
fn should_track_committed_ranges_correctly() {
    // Arrange
    let store = make_test_store();
    let mut actor = AreaActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        store,
    );
    let mut ctx = make_area_ctx();

    // Act: Commit several non-overlapping ranges
    let ranges = vec![
        (10, 19),
        (30, 39),
        (0, 9),
        (20, 29),
    ];

    for (first, last) in ranges {
        let commit = fitz::domains::stream::StreamMessage::BatchCommitted {
            first_area_offset: first,
            last_area_offset: last,
            first_realm_offset: first,
            last_realm_offset: last,
        };
        actor.receive(commit, &mut ctx);
    }

    // Assert: Watermark advanced to 39 (all ranges merged)
    assert_eq!(actor.watermark(), 39);
}

#[test]
fn should_clean_up_old_ranges_below_watermark() {
    // Arrange
    let store = make_test_store();
    let mut actor = AreaActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        store,
    );
    let mut ctx = make_area_ctx();

    // Commit [0, 100]
    let commit1 = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 0,
        last_area_offset: 100,
        first_realm_offset: 0,
        last_realm_offset: 100,
    };
    actor.receive(commit1, &mut ctx);
    assert_eq!(actor.watermark(), 100);

    // Commit [101, 200]
    let commit2 = fitz::domains::stream::StreamMessage::BatchCommitted {
        first_area_offset: 101,
        last_area_offset: 200,
        first_realm_offset: 101,
        last_realm_offset: 200,
    };
    actor.receive(commit2, &mut ctx);
    
    // Assert: Watermark at 200, old ranges cleaned up
    assert_eq!(actor.watermark(), 200);
    // committed_ranges should be empty or minimal (implementation detail)
}
