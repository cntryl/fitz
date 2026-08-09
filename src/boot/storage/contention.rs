pub(super) const LEASE_ACQUISITION_PREFIX: &str =
    "FATAL: another Midge instance is already running against this storage. Only one writable instance is allowed at a time. Error: ";

#[derive(Clone, Copy)]
pub(super) enum ProviderShape {
    S3Compatible,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ContentionKind {
    Retryable,
    Permanent,
}

pub(super) trait StorageOpenError {
    fn lease_acquisition_detail(&self) -> Option<&str>;
}

impl StorageOpenError for cntryl_midge::MidgeError {
    fn lease_acquisition_detail(&self) -> Option<&str> {
        let Self::Internal(message) = self else {
            return None;
        };
        message
            .strip_prefix(LEASE_ACQUISITION_PREFIX)
            .map(|detail| {
                detail
                    .strip_prefix("lease acquisition failed: ")
                    .unwrap_or(detail)
            })
    }
}

pub(super) fn local_is_retryable(error: &impl StorageOpenError) -> bool {
    local_kind(error) == ContentionKind::Retryable
}

pub(super) fn local_kind(error: &impl StorageOpenError) -> ContentionKind {
    if error.lease_acquisition_detail().is_some_and(|detail| {
        detail
            .starts_with("another Midge instance is already running against this storage (holder:")
            || detail.starts_with("lost CAS race: expected holder=")
            || detail == "leader acquisition lock remained unavailable after stale-lock cleanup"
            || detail
                .strip_prefix("another acquire is in progress: ")
                .is_some_and(is_already_exists_error)
    }) {
        ContentionKind::Retryable
    } else {
        ContentionKind::Permanent
    }
}

pub(super) fn cloud_is_retryable(
    error: &impl StorageOpenError,
    provider_shape: ProviderShape,
) -> bool {
    cloud_kind(error, provider_shape) == ContentionKind::Retryable
}

pub(super) fn cloud_kind(
    error: &impl StorageOpenError,
    provider_shape: ProviderShape,
) -> ContentionKind {
    if error.lease_acquisition_detail().is_some_and(|detail| {
        detail.starts_with("another instance holds the lease (holder:")
            || cloud_precondition_conflict(detail, provider_shape)
    }) {
        ContentionKind::Retryable
    } else {
        ContentionKind::Permanent
    }
}

fn is_already_exists_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("already exists")
        || normalized.contains("file exists")
        || normalized.contains("os error 17")
        || normalized.contains("os error 183")
}

fn cloud_precondition_conflict(detail: &str, provider_shape: ProviderShape) -> bool {
    let Some(error) = detail.strip_prefix("cloud lease conditional write failed: ") else {
        return false;
    };
    let normalized = error.to_ascii_lowercase();
    normalized.contains("precondition failed")
        || normalized.contains("status 412")
        || (normalized.contains("status 409")
            && matches!(provider_shape, ProviderShape::S3Compatible))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeOpenError(&'static str);

    impl StorageOpenError for FakeOpenError {
        fn lease_acquisition_detail(&self) -> Option<&str> {
            Some(self.0)
        }
    }

    #[test]
    fn should_classify_fake_cloud_contention_without_opening_storage() {
        // Arrange
        let error = FakeOpenError("cloud lease conditional write failed: HTTP status 409");

        // Act
        let kind = cloud_kind(&error, ProviderShape::S3Compatible);

        // Assert
        assert_eq!(kind, ContentionKind::Retryable);
    }
}
