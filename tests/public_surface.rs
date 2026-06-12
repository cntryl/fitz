use std::fs;
use std::path::Path;

fn read_source(path: &str) -> String {
    fs::read_to_string(path).expect("failed to read source file")
}

#[test]
fn should_keep_server_prelude_narrow() {
    let source = read_source("src/prelude/mod.rs");
    assert!(source.contains("pub use crate::runtime::actor::Actor;"));
    assert!(!source.contains("runtime::*"));
}

#[test]
fn should_expose_internal_server_modules() {
    // Arrange
    let source = read_source("src/lib.rs");
    let required_modules = [
        "pub mod control;",
        "pub mod protocol;",
        "pub mod runtime;",
        "pub mod session;",
        "pub mod utils;",
        "pub mod testkit;",
        "pub mod benchkit;",
    ];

    // Act
    for required in required_modules {
        assert!(
            source.contains(required),
            "missing feature-gated module fragment: {required}"
        );
    }

    // Assert
    assert!(!source.contains("feature = \"internal-api\""));
    assert!(!source.contains("pub use crate::runtime::*"));
}

#[test]
fn should_not_expose_ownerless_queue_or_stream_session_modules() {
    // Arrange
    let queue_mod = read_source("src/domains/queue/mod.rs");
    let stream_mod = read_source("src/domains/stream/mod.rs");

    // Act
    let queue_session_exists = Path::new("src/domains/queue/session.rs").exists();
    let stream_session_exists = Path::new("src/domains/stream/session.rs").exists();

    // Assert
    assert!(!queue_session_exists);
    assert!(!stream_session_exists);
    assert!(!queue_mod.contains("pub mod session;"));
    assert!(!queue_mod.contains("pub use session::SessionActor;"));
    assert!(!stream_mod.contains("pub mod session;"));
    assert!(!stream_mod.contains("pub use session::SessionActor;"));
}
