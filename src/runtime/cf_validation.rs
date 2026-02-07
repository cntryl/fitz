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
/// # Errors
///
/// Returns an error if `cf_id` is 0 (the default column family)
pub fn validate_cf_not_default(cf_id: cntryl_midge::ColumnFamilyId) -> Result<(), String> {
    if cf_id.0 == 0 {
        Err("Attempted to use default column family (CF=0). \
             All Fitz domains MUST use explicit RouteFamily → ColumnFamily mapping."
            .to_string())
    } else {
        Ok(())
    }
}

/// Validate that a RouteFamily maps to a valid (non-default) ColumnFamilyId
///
/// # Errors
///
/// Returns an error if `family.id()` is 0 (would map to default CF)
pub fn validate_route_family(family: crate::runtime::routing::RouteFamily) -> Result<(), String> {
    if family.id() == 0 {
        Err("RouteFamily with id=0 would map to default column family. \
             RouteFamily IDs MUST be non-zero."
            .to_string())
    } else {
        Ok(())
    }
}

/// Convert RouteFamily to ColumnFamilyId with validation
///
/// This is the canonical way to convert RouteFamily → ColumnFamilyId.
/// It ensures the mapping is explicit and validates that the default CF is not used.
///
/// # Errors
///
/// Returns an error if `family.id()` is 0
pub fn route_family_to_cf(
    family: crate::runtime::routing::RouteFamily,
) -> Result<cntryl_midge::ColumnFamilyId, String> {
    validate_route_family(family)?;
    Ok(cntryl_midge::ColumnFamilyId(family.id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_valid_cf_id() {
        // Arrange & Act & Assert
        assert!(validate_cf_not_default(cntryl_midge::ColumnFamilyId(1)).is_ok());
        assert!(validate_cf_not_default(cntryl_midge::ColumnFamilyId(999)).is_ok());
    }

    #[test]
    fn should_reject_default_cf() {
        // Arrange & Act & Assert
        assert!(validate_cf_not_default(cntryl_midge::ColumnFamilyId(0)).is_err());
    }

    #[test]
    fn should_accept_valid_route_family() {
        // Arrange & Act & Assert
        assert!(validate_route_family(crate::runtime::routing::RouteFamily::new(1)).is_ok());
        assert!(validate_route_family(crate::runtime::routing::RouteFamily::new(999)).is_ok());
    }

    #[test]
    fn should_reject_zero_route_family() {
        // Arrange & Act & Assert
        assert!(validate_route_family(crate::runtime::routing::RouteFamily::new(0)).is_err());
    }

    #[test]
    fn should_convert_route_family_to_cf() {
        // Arrange
        let family = crate::runtime::routing::RouteFamily::new(42);

        // Act
        let cf_id = route_family_to_cf(family);

        // Assert
        assert!(cf_id.is_ok());
        assert_eq!(cf_id.unwrap().0, 42);
    }

    #[test]
    fn should_reject_converting_zero_family_to_cf() {
        // Arrange
        let family = crate::runtime::routing::RouteFamily::new(0);

        // Act
        let result = route_family_to_cf(family);

        // Assert
        assert!(result.is_err());
    }
}
