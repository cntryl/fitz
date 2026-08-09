use super::model::{Duration, Instant, LeaseDomainRuntime};

impl LeaseDomainRuntime<'_> {
    pub(super) fn lease_expiry(
        now: Instant,
        ttl_secs: u64,
    ) -> Result<Instant, crate::domains::lease::protocol::LeaseResponse> {
        if ttl_secs == 0 {
            return Err(crate::domains::lease::protocol::LeaseResponse::Error(
                "ttl_secs must be greater than zero".to_string(),
            ));
        }

        now.checked_add(Duration::from_secs(ttl_secs))
            .ok_or_else(|| {
                crate::domains::lease::protocol::LeaseResponse::Error(
                    "ttl_secs exceeds the supported lease duration".to_string(),
                )
            })
    }
}
