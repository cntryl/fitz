use cntryl_midge::WriteOptions;
use fitz::domains::WritePolicy;

#[test]
fn should_preserve_each_write_guarantee_at_the_engine_boundary() {
    // Arrange
    let cases = [
        (WritePolicy::Sync, WriteOptions::sync()),
        (WritePolicy::Buffered, WriteOptions::buffered()),
        (WritePolicy::BestEffort, WriteOptions::best_effort()),
        (WritePolicy::CloudAsync, WriteOptions::cloud_async()),
        (WritePolicy::CloudStrict, WriteOptions::cloud_strict()),
    ];

    // Act
    let converted = cases.map(|(policy, _)| WriteOptions::from(policy));

    // Assert
    assert_eq!(converted, cases.map(|(_, options)| options));
}
