//! One wire-policy inventory shared by BEGIN decoding and broker remapping.

use crate::domains::WritePolicy;

const WIRE_POLICIES: [WritePolicy; 2] = [WritePolicy::Buffered, WritePolicy::Sync];

pub(crate) fn decode_wire_policy(value: u8) -> Result<WritePolicy, String> {
    WIRE_POLICIES
        .get(usize::from(value))
        .copied()
        .ok_or_else(|| format!("Invalid durability mode: {value}"))
}

pub(super) fn resolve_policy(
    requested: WritePolicy,
    buffered: WritePolicy,
    sync: WritePolicy,
) -> WritePolicy {
    WIRE_POLICIES
        .iter()
        .zip([buffered, sync])
        .find_map(|(&wire, configured)| (wire == requested).then_some(configured))
        .unwrap_or(requested)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_resolve_every_wire_policy_to_its_configured_guarantee() {
        // Arrange
        let configured = [WritePolicy::CloudAsync, WritePolicy::CloudStrict];

        // Act
        let resolved = [0, 1].map(|wire| {
            resolve_policy(
                decode_wire_policy(wire).expect("known wire policy"),
                configured[0],
                configured[1],
            )
        });

        // Assert
        assert_eq!(resolved, configured);
    }

    #[test]
    fn should_preserve_explicit_policies_outside_the_wire_inventory() {
        // Arrange
        let explicit = [
            WritePolicy::BestEffort,
            WritePolicy::CloudAsync,
            WritePolicy::CloudStrict,
        ];

        // Act
        let resolved =
            explicit.map(|policy| resolve_policy(policy, WritePolicy::Buffered, WritePolicy::Sync));

        // Assert
        assert_eq!(resolved, explicit);
    }

    #[test]
    fn should_reject_values_outside_the_wire_policy_inventory() {
        // Arrange
        let invalid = 2..=u8::MAX;

        // Act
        let rejected = invalid
            .map(decode_wire_policy)
            .all(|result| result.is_err());

        // Assert
        assert!(rejected);
    }
}
