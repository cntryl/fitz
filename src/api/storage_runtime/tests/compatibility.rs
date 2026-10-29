use super::*;

const LEGACY_FILES: &[(&str, &[u8])] = &[
    (
        "FORMAT",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/midge-0.4.1/FORMAT"
        )),
    ),
    (
        "manifest.snapshot.json",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/midge-0.4.1/manifest.snapshot.json"
        )),
    ),
    (
        "manifest.journal",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/midge-0.4.1/manifest.journal"
        )),
    ),
    (
        "wal/LEDGER",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/midge-0.4.1/wal/LEDGER"
        )),
    ),
    (
        "wal/00000000000000000001.wal",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/midge-0.4.1/wal/00000000000000000001.wal"
        )),
    ),
];

#[tokio::test]
async fn should_reject_legacy_store_without_changing_persisted_data() {
    // Arrange
    let directory = TempDir::new().expect("temporary storage directory");
    for (relative_path, contents) in LEGACY_FILES {
        let path = directory.path().join(relative_path);
        std::fs::create_dir_all(path.parent().expect("fixture file parent"))
            .expect("create fixture directory");
        std::fs::write(path, contents).expect("write legacy fixture");
    }
    let config = BootConfig::with_memory_storage().with_storage_mode(StorageMode::LocalDisk {
        db_path: directory.path().to_string_lossy().into_owned(),
    });

    // Act
    let error = match init(&config).await {
        Ok(_) => panic!("legacy store must not open with the new engine"),
        Err(error) => error.to_string(),
    };

    // Assert
    assert!(
        error.contains("unsupported on-disk format version 8"),
        "{error}"
    );
    for (relative_path, contents) in LEGACY_FILES {
        let actual = std::fs::read(directory.path().join(relative_path)).expect("read fixture");
        assert_eq!(actual, *contents, "persisted file changed: {relative_path}");
    }
}
