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

#[test]
fn should_accept_existing_engine_options_as_fitz_write_policies() {
    // Arrange
    let cases = [
        (WriteOptions::sync(), WritePolicy::Sync),
        (WriteOptions::buffered(), WritePolicy::Buffered),
        (WriteOptions::best_effort(), WritePolicy::BestEffort),
        (WriteOptions::cloud_async(), WritePolicy::CloudAsync),
        (WriteOptions::cloud_strict(), WritePolicy::CloudStrict),
    ];

    // Act
    let converted = cases.map(|(options, _)| WritePolicy::from(options));

    // Assert
    assert_eq!(converted, cases.map(|(_, policy)| policy));
}
