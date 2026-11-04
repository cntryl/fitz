//! Standalone DROP frames retain family and session-lifecycle validation.

use super::*;
use crate::domains::kv::{
    KvActor, KvClientFrame, KvClientRequest, KvDropRequest, KvDropScope, KvMessage, KvResponse,
    TxMode,
};
use crate::domains::CommitPersistence;

const ROUTE: &str = "kv://acme/app/rows";

struct Fixture {
    sink: KvDomain,
    mailbox: Arc<Mailbox>,
    source: RouteAddress,
}

impl Fixture {
    fn new() -> Self {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let mailbox = Arc::new(Mailbox::new(8));
        let source = RouteAddress::new(family, Route::new("inbox://session/7"));
        router.register(source.clone(), mailbox.clone());
        let sink = KvDomain::new(
            crate::testkit::midge::create_in_memory_test_engine_with_cfs(&[1, 2]),
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
        );
        Self {
            sink,
            mailbox,
            source,
        }
    }

    fn seed(&self, family: RouteFamily) {
        let scope = KvResourceScope::new(family, "acme", "app", "rows");
        let mut actor = KvActor::new(self.sink.config.store.clone());
        let KvResponse::BeginOk { tx_id } = actor.handle(KvMessage::Begin {
            scope: scope.clone(),
            mode: TxMode::ReadWrite,
        }) else {
            panic!("seed transaction must begin")
        };
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            }),
            KvResponse::PutOk
        ));
        assert!(matches!(
            actor.handle(KvMessage::Commit {
                tx_id,
                scope,
                persistence: CommitPersistence::Sync
            }),
            KvResponse::CommitOk
        ));
    }

    fn drop_frame(&self, decoded_family: RouteFamily) -> Envelope {
        let family = RouteFamily::new(1);
        let request = KvClientRequest::new(
            crate::runtime::ClientFrameMeta::new(
                7,
                crate::runtime::ClientChannel::Pub,
                crate::dispatch::protocol::kv::msg_type::DROP,
                family,
            ),
            Ok(KvClientFrame::Drop(KvDropRequest {
                scope: KvDropScope::parse(decoded_family, ROUTE).unwrap(),
                persistence: CommitPersistence::Sync,
            })),
        );
        Envelope::from_route(
            self.source.clone(),
            RouteAddress::new(family, Route::new("kv://inbound")),
            request,
        )
    }

    fn value(&self, family: RouteFamily) -> Option<Vec<u8>> {
        self.sink
            .admin_get_committed_value(family, "acme", "app", "rows", b"key")
            .unwrap()
    }
}

#[test]
fn should_reject_standalone_drop_when_its_decoded_family_differs_from_the_request() {
    // Arrange
    let fixture = Fixture::new();
    let other_family = RouteFamily::new(2);
    fixture.seed(other_family);
    let request = fixture.drop_frame(other_family);

    // Act
    fixture.sink.deliver(request).unwrap();
    let response = receive_frame(&fixture.mailbox, "DROP family mismatch");

    // Assert
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_INVALID_ROUTE
    );
    assert_eq!(fixture.value(other_family), Some(b"value".to_vec()));
    assert!(fixture.sink.actors_are_empty_for_tests());
}

#[test]
fn should_reject_a_queued_standalone_drop_after_session_cleanup() {
    // Arrange
    let fixture = Fixture::new();
    let family = RouteFamily::new(1);
    fixture.seed(family);
    let queued = fixture.drop_frame(family);
    fixture.sink.cleanup_session(7).unwrap();

    // Act
    fixture.sink.deliver(queued).unwrap();
    let response = receive_frame(&fixture.mailbox, "DROP after cleanup");

    // Assert
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_INVALID_ROUTE
    );
    assert_eq!(fixture.value(family), Some(b"value".to_vec()));
    assert!(fixture.sink.actors_are_empty_for_tests());
}

#[test]
fn should_complete_standalone_drop_without_creating_a_session_transaction_actor() {
    // Arrange
    let fixture = Fixture::new();
    let family = RouteFamily::new(1);
    fixture.seed(family);

    // Act
    fixture.sink.deliver(fixture.drop_frame(family)).unwrap();
    let response = receive_frame(&fixture.mailbox, "standalone DROP success");

    // Assert
    assert_eq!(response.payload.as_ref(), &[0]);
    assert!(fixture.value(family).is_none());
    assert!(fixture.sink.actors_are_empty_for_tests());
    assert_eq!(fixture.sink.active_transaction_count(), 0);
}
