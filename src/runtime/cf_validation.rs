//! Column Family validation for Fitz → Midge persistence
//!
//! # Purpose
//!
//! Enforces the critical architectural invariant:
//! **All persisted Fitz domains MUST map RouteFamily → ColumnFamily explicitly.**
//! **The default column family MUST NEVER be used.**
//!
//! # Rules
//!
//! 1. **RouteFamily mapping**: Every persisted write MUST resolve to an explicit ColumnFamily
//! 2. **Default CF prohibition**: The Midge default column family is FORBIDDEN
//! 3. **Domain responsibility**: Domains define and register their ColumnFamily at startup
//! 4. **API invariants**: Writer APIs MUST require a RouteFamily (or resolved handle)
//! 5. **Validation**: On startup, validate all persisted domains have registered CFs
//!
//! # Design
//!
//! RouteFamily → ColumnFamily mapping is 1:1 by value:
//! - RouteFamily with id=1 maps to ColumnFamilyId(1)
//! - RouteFamily with id=2 maps to ColumnFamilyId(2)
//! - etc.
//!
//! This ensures:
//! - Data isolation per route family
//! - No silent data mixing
//! - Auditable persistence layout
//! - Zero-overhead mapping (simple cast)

/// Validate that a ColumnFamilyId is not the default CF (0)
///
/// # Panics
///
/// Panics if `cf_id` is 0 (the default column family)
///
/// # Example
///
/// ```no_run
/// # use fitz::runtime::cf_validation::validate_cf_not_default;
/// use cntryl_midge::ColumnFamilyId;
///
/// // ✅ Valid - explicit CF
/// validate_cf_not_default(ColumnFamilyId(1));
///
/// // ❌ Panic - default CF
/// // validate_cf_not_default(ColumnFamilyId(0));
/// ```
pub fn validate_cf_not_default(cf_id: cntryl_midge::ColumnFamilyId) {
    if cf_id.0 == 0 {
        panic!(
            "CRITICAL: Attempted to use default column family (CF=0). \
             All Fitz domains MUST use explicit RouteFamily → ColumnFamily mapping. \
             This is a critical architectural violation."
        );
    }
}

/// Validate that a RouteFamily maps to a valid (non-default) ColumnFamilyId
///
/// # Panics
///
/// Panics if `family.id()` is 0 (would map to default CF)
///
/// # Example
///
/// ```no_run
/// # use fitz::runtime::cf_validation::validate_route_family;
/// # use fitz::runtime::routing::RouteFamily;
///
/// // ✅ Valid - explicit family
/// validate_route_family(RouteFamily::new(1));
///
/// // ❌ Panic - would map to default CF
/// // validate_route_family(RouteFamily::new(0));
/// ```
pub fn validate_route_family(family: crate::runtime::routing::RouteFamily) {
    if family.id() == 0 {
        panic!(
            "CRITICAL: RouteFamily with id=0 would map to default column family. \
             RouteFamily IDs MUST be non-zero. This is a critical architectural violation."
        );
    }
}

/// Convert RouteFamily to ColumnFamilyId with validation
///
/// This is the canonical way to convert RouteFamily → ColumnFamilyId.
/// It ensures the mapping is explicit and validates that the default CF is not used.
///
/// # Panics
///
/// Panics if `family.id()` is 0
///
/// # Example
///
/// ```no_run
/// # use fitz::runtime::cf_validation::route_family_to_cf;
/// # use fitz::runtime::routing::RouteFamily;
///
/// let family = RouteFamily::new(1);
/// let cf_id = route_family_to_cf(family);
/// assert_eq!(cf_id.0, 1);
/// ```
pub fn route_family_to_cf(family: crate::runtime::routing::RouteFamily) -> cntryl_midge::ColumnFamilyId {
    validate_route_family(family);
    cntryl_midge::ColumnFamilyId(family.id() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_valid_cf_id() {
        // Arrange & Act & Assert
        validate_cf_not_default(cntryl_midge::ColumnFamilyId(1));
        validate_cf_not_default(cntryl_midge::ColumnFamilyId(999));
    }

    #[test]
    #[should_panic(expected = "default column family")]
    fn should_panic_on_default_cf() {
        // Arrange & Act & Assert
        validate_cf_not_default(cntryl_midge::ColumnFamilyId(0));
    }

    #[test]
    fn should_accept_valid_route_family() {
        // Arrange & Act & Assert
        validate_route_family(crate::runtime::routing::RouteFamily::new(1));
        validate_route_family(crate::runtime::routing::RouteFamily::new(999));
    }

    #[test]
    #[should_panic(expected = "default column family")]
    fn should_panic_on_zero_route_family() {
        // Arrange & Act & Assert
        validate_route_family(crate::runtime::routing::RouteFamily::new(0));
    }

    #[test]
    fn should_convert_route_family_to_cf() {
        // Arrange
        let family = crate::runtime::routing::RouteFamily::new(42);

        // Act
        let cf_id = route_family_to_cf(family);

        // Assert
        assert_eq!(cf_id.0, 42);
    }

    #[test]
    #[should_panic(expected = "default column family")]
    fn should_panic_converting_zero_family_to_cf() {
        // Arrange
        let family = crate::runtime::routing::RouteFamily::new(0);

        // Act & Assert
        let _cf_id = route_family_to_cf(family);
    }
}
