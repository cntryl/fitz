use fitz::auth::Permission;
use fitz::domains::kv::{KvMessage, TxMode, SessionActor};
use fitz::domains::kv::actor::KvActor;
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::SessionId;
use fitz::testkit::create_test_engine_with_cfs;
use fitz::runtime::routing::RouteFamily;

#[test]
fn should_reject_read_only_session_begin_read_write() {
    // Arrange
    let p = Permission::parse("kv://acme#read").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p]);
    let actor = SessionActor::new(SessionId(1), perms);
    let mut kv = KvActor::new(create_test_engine_with_cfs(vec![1]));
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    // Act
    let res = actor.begin(msg, &mut kv);

    // Assert
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("unauthorized"));
}

#[test]
fn should_allow_read_only_session_begin_read_only() {
    // Arrange
    let p = Permission::parse("kv://acme#read").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p]);
    let actor = SessionActor::new(SessionId(1), perms);
    let mut kv = KvActor::new(create_test_engine_with_cfs(vec![1]));
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    // Act
    let res = actor.begin(msg, &mut kv);

    // Assert
    assert!(res.is_ok());
}

#[test]
fn should_allow_write_session_begin_read_write() {
    // Arrange
    let p = Permission::parse("kv://acme#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p]);
    let actor = SessionActor::new(SessionId(1), perms);
    let mut kv = KvActor::new(create_test_engine_with_cfs(vec![1]));
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    // Act
    let res = actor.begin(msg, &mut kv);

    // Assert
    assert!(res.is_ok());
}

#[test]
fn should_allow_write_session_begin_read_only() {
    // Arrange
    let p = Permission::parse("kv://acme#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p]);
    let actor = SessionActor::new(SessionId(1), perms);
    let mut kv = KvActor::new(create_test_engine_with_cfs(vec![1]));
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "table1".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    // Act
    let res = actor.begin(msg, &mut kv);

    // Assert
    assert!(res.is_ok());
}
