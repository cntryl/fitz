//! The pinned Midge rejection below is emitted before WAL submission.

pub(crate) fn is_l0_admission_rejection(error: &cntryl_midge::MidgeError, family: u32) -> bool {
    matches!(error, cntryl_midge::MidgeError::WriteStall(detail)
        if detail.starts_with(&format!("column family {family} has no free L0 slot (")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_only_the_pinned_family_specific_pre_wal_rejection() {
        // Arrange
        let rejected = cntryl_midge::MidgeError::WriteStall(
            "column family 1 has no free L0 slot (15/14)".to_string(),
        );

        // Act
        let matched = is_l0_admission_rejection(&rejected, 1);
        let other_family = is_l0_admission_rejection(&rejected, 2);

        // Assert
        assert!(matched);
        assert!(!other_family);
    }

    #[test]
    fn should_reject_other_storage_errors_as_automatic_retry_authority() {
        // Arrange
        let errors = [
            cntryl_midge::MidgeError::Timeout(
                "column family 1 has no free L0 slot (15/14)".to_string(),
            ),
            cntryl_midge::MidgeError::WriteStall("cloud upload stalled".to_string()),
            cntryl_midge::MidgeError::WriteConflict("conflicting write".to_string()),
        ];

        // Act
        let retryable = errors
            .iter()
            .any(|error| is_l0_admission_rejection(error, 1));

        // Assert
        assert!(!retryable);
    }
}
