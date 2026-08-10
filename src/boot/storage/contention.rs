#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ContentionKind {
    Retryable,
    Permanent,
}

pub(super) fn is_retryable(error: &cntryl_midge::MidgeError) -> bool {
    kind(error) == ContentionKind::Retryable
}

fn kind(error: &cntryl_midge::MidgeError) -> ContentionKind {
    if matches!(error, cntryl_midge::MidgeError::LeaseHeld(_)) {
        ContentionKind::Retryable
    } else {
        ContentionKind::Permanent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_retry_typed_lease_contention() {
        // Arrange
        let error = cntryl_midge::MidgeError::LeaseHeld("active writer".to_string());

        // Act
        let retryable = is_retryable(&error);

        // Assert
        assert!(retryable);
    }

    #[test]
    fn should_not_retry_untyped_internal_failure() {
        // Arrange
        let error = cntryl_midge::MidgeError::Internal("lease text".to_string());

        // Act
        let retryable = is_retryable(&error);

        // Assert
        assert!(!retryable);
    }
}
