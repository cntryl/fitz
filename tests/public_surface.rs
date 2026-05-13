use std::fs;

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
    let source = read_source("src/lib.rs");

    for required in [
        "pub mod control;",
        "pub mod protocol;",
        "pub mod runtime;",
        "pub mod session;",
        "pub mod utils;",
        "pub mod testkit;",
        "pub mod benchkit;",
    ] {
        assert!(
            source.contains(required),
            "missing feature-gated module fragment: {required}"
        );
    }

    assert!(!source.contains("feature = \"internal-api\""));
    assert!(!source.contains("pub use crate::runtime::*"));
}
