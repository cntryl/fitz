//! The error code catalog: every code wire format 2 can emit, with its class
//! and typical effect. `docs/clients/spec/error-codes.md` is the human mirror.

use super::envelope::{Effect, ErrorClass};

/// One catalog row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodeEntry {
    pub code: u16,
    pub name: &'static str,
    pub class: ErrorClass,
    pub effect: Effect,
}

/// Domain blocks, as thousands.
pub mod block {
    pub const PROTOCOL: u16 = 0;
    pub const KV: u16 = 1000;
    pub const STREAM: u16 = 2000;
    pub const NOTICE: u16 = 3000;
    pub const QUEUE: u16 = 4000;
    pub const LEASE: u16 = 5000;
    pub const RPC: u16 = 6000;
    pub const SCHEDULE: u16 = 7000;
    pub const ALL: [u16; 8] = [PROTOCOL, KV, STREAM, NOTICE, QUEUE, LEASE, RPC, SCHEDULE];
}

/// Highest shared slot; domain-specific codes begin at `DOMAIN_FIRST`.
pub const SHARED_LAST: u16 = 10;
/// First domain-specific slot in every block.
pub const DOMAIN_FIRST: u16 = 50;

/// The shared code for `class` in `block`, or `None` for [`ErrorClass::Domain`].
#[must_use]
pub const fn shared(block: u16, class: ErrorClass) -> Option<u16> {
    match class.shared_slot() {
        Some(slot) => Some(block + slot),
        None => None,
    }
}

const fn row(code: u16, name: &'static str, class: ErrorClass, effect: Effect) -> CodeEntry {
    CodeEntry {
        code,
        name,
        class,
        effect,
    }
}

const fn shared_rows(block: u16) -> [CodeEntry; 10] {
    use Effect::{Indeterminate as Ind, NotApplied as No};
    use ErrorClass as C;
    [
        row(block + 1, "UNAUTHORIZED", C::Unauthorized, No),
        row(block + 2, "INVALID_ROUTE", C::InvalidRoute, No),
        row(block + 3, "INVALID_PATTERN", C::InvalidPattern, No),
        row(block + 4, "REGISTRATION_LIMIT", C::RegistrationLimit, No),
        row(block + 5, "BUSY", C::Busy, No),
        row(block + 6, "BACKEND", C::Backend, Ind),
        row(block + 7, "TIMEOUT", C::Timeout, Ind),
        row(block + 8, "BAD_REQUEST", C::BadRequest, No),
        row(block + 9, "NOT_FOUND", C::NotFound, No),
        row(block + 10, "UNSUPPORTED", C::Unsupported, No),
    ]
}

/// Protocol-block codes beyond the shared slots, by name.
pub const VERSION_MISMATCH: u16 = 50;
pub const FAMILY_NOT_PROVISIONED: u16 = 51;
pub const CONNECT_TIMEOUT: u16 = 52;
pub const MESSAGE_TOO_LARGE: u16 = 53;
pub const DRAINING: u16 = 54;
pub const FORCED_CLOSE: u16 = 55;
pub const LIMIT_CHANGED: u16 = 56;

/// Protocol-block codes beyond the shared slots.
pub const PROTOCOL_ROWS: [CodeEntry; 7] = {
    use Effect::{Indeterminate as Ind, NotApplied as No};
    use ErrorClass as C;
    [
        row(VERSION_MISMATCH, "VERSION_MISMATCH", C::Unsupported, No),
        row(
            FAMILY_NOT_PROVISIONED,
            "FAMILY_NOT_PROVISIONED",
            C::InvalidRoute,
            No,
        ),
        row(CONNECT_TIMEOUT, "CONNECT_TIMEOUT", C::BadRequest, No),
        row(MESSAGE_TOO_LARGE, "MESSAGE_TOO_LARGE", C::BadRequest, No),
        row(DRAINING, "DRAINING", C::Busy, No),
        row(FORCED_CLOSE, "FORCED_CLOSE", C::Backend, Ind),
        row(LIMIT_CHANGED, "LIMIT_CHANGED", C::Unsupported, No),
    ]
};

/// Domain-specific codes, by block.
pub const DOMAIN_ROWS: [CodeEntry; 38] = {
    use Effect::NotApplied as No;
    use ErrorClass as C;
    [
        row(1050, "TRANSACTION_NOT_FOUND", C::NotFound, No),
        row(1051, "INVALID_MODE", C::BadRequest, No),
        row(1052, "KEY_NOT_FOUND", C::NotFound, No),
        row(1053, "ISOLATION_CONFLICT", C::Domain, No),
        row(1054, "WRITE_IN_READONLY", C::BadRequest, No),
        row(1055, "KEY_EXISTS", C::Domain, No),
        row(1056, "REALM_MISMATCH", C::InvalidRoute, No),
        row(1057, "TRANSACTION_ABORTED", C::Domain, No),
        row(2050, "CONCURRENCY_CONFLICT", C::Domain, No),
        row(2051, "SESSION_ALREADY_ACTIVE", C::Domain, No),
        row(2052, "SESSION_NOT_FOUND", C::NotFound, No),
        row(2053, "INVALID_READ_BOUND", C::BadRequest, No),
        row(2054, "RESOURCE_NOT_FOUND", C::NotFound, No),
        row(2055, "FILTER_UNSUPPORTED_VERSION", C::Unsupported, No),
        row(2056, "FILTER_INVALID_PAYLOAD", C::BadRequest, No),
        row(2057, "READ_RESPONSE_TOO_LARGE", C::BadRequest, No),
        row(3050, "TRANSPORT_CLOSED", C::Domain, No),
        row(4050, "INVALID_TOKEN", C::BadRequest, No),
        row(4051, "INFLIGHT_EXPIRED", C::Domain, No),
        row(4052, "MESSAGE_NOT_FOUND", C::NotFound, No),
        row(4053, "QUEUE_NOT_FOUND", C::NotFound, No),
        row(5050, "HELD", C::Domain, No),
        row(5051, "INVALID_FENCE", C::BadRequest, No),
        row(5052, "EXPIRED", C::Domain, No),
        row(5053, "LEASE_NOT_FOUND", C::NotFound, No),
        row(5054, "INVALID_TOKEN", C::BadRequest, No),
        row(5055, "ACQUIRE_WAIT_EXPIRED", C::Domain, No),
        row(5056, "WAITERS_LIMIT", C::Busy, No),
        row(5057, "INVALID_LIST_CURSOR", C::BadRequest, No),
        row(6050, "WORKER_NOT_FOUND", C::NotFound, No),
        row(6051, "ROUTE_NOT_REGISTERED", C::NotFound, No),
        row(6052, "CORRELATION_NOT_FOUND", C::NotFound, No),
        row(6053, "INVALID_SEQUENCE", C::BadRequest, No),
        row(6054, "DUPLICATE_CORRELATION", C::BadRequest, No),
        row(6055, "WRONG_WORKER", C::Unauthorized, No),
        row(7050, "SCHEDULE_NOT_FOUND", C::NotFound, No),
        row(7051, "INVALID_CRON", C::BadRequest, No),
        row(7052, "DEFINITION_LIMIT", C::RegistrationLimit, No),
    ]
};

/// The last two Schedule rows do not fit the array above's size budget
/// cleanly; they live here so the arrays stay readable.
pub const SCHEDULE_TAIL_ROWS: [CodeEntry; 2] = [
    row(
        7053,
        "INVALID_TARGET",
        ErrorClass::BadRequest,
        Effect::NotApplied,
    ),
    row(
        7054,
        "INVALID_DELIVERY_MODE",
        ErrorClass::BadRequest,
        Effect::NotApplied,
    ),
];

/// Every code in the catalog, shared slots first, in block order.
#[must_use]
pub fn all_entries() -> Vec<CodeEntry> {
    let mut entries = Vec::with_capacity(block::ALL.len() * 10 + 47);
    for block in block::ALL {
        entries.extend_from_slice(&shared_rows(block));
    }
    entries.extend_from_slice(&PROTOCOL_ROWS);
    entries.extend_from_slice(&DOMAIN_ROWS);
    entries.extend_from_slice(&SCHEDULE_TAIL_ROWS);
    entries
}

/// Look a code up.
#[must_use]
pub fn lookup(code: u16) -> Option<CodeEntry> {
    all_entries().into_iter().find(|entry| entry.code == code)
}

/// Wire format 1 code to wire format 2 code, for migration notes and tests.
/// Two old codes split by meaning map to the row that carried the primary
/// meaning; the spec page names the secondary mapping.
pub const WIRE1_TO_WIRE2: &[(u16, u16)] = &[
    (1001, 1050),
    (1002, 1051),
    (1003, 1052),
    (1004, 1053),
    (1005, 1054),
    (1006, 1055),
    (1007, 1002),
    (1008, 1056),
    (1009, 1006),
    (1010, 1057),
    (1011, 1001),
    (1012, 1003),
    (1013, 1004),
    (1014, 1005),
    (2001, 2050),
    (2002, 2051),
    (2003, 2052),
    (2004, 2053),
    (2005, 2054),
    (2006, 2055),
    (2007, 2056),
    (2009, 2001),
    (2010, 2003),
    (2011, 2004),
    (2012, 2006),
    (2013, 2057),
    (2014, 2005),
    (3001, 3002),
    (3002, 3003),
    (3003, 3004),
    (3004, 3050),
    (3005, 3006),
    (3006, 3005),
    (3009, 3001),
    (4001, 4050),
    (4002, 4051),
    (4003, 4052),
    (4004, 4053),
    (4005, 4005),
    (4006, 4008),
    (4007, 4006),
    (4009, 4001),
    (4010, 4003),
    (4011, 4004),
    (5001, 5050),
    (5002, 5051),
    (5003, 5052),
    (5004, 5053),
    (5005, 5054),
    (5006, 5055),
    (5007, 5056),
    (5008, 5008),
    (5009, 5001),
    (5010, 5003),
    (5011, 5057),
    (5012, 5003),
    (6001, 6007),
    (6002, 6050),
    (6003, 6005),
    (6004, 6051),
    (6005, 6052),
    (6006, 6053),
    (6007, 6054),
    (6008, 6055),
    (6009, 6001),
    (6010, 6006),
    (6011, 6002),
    (6012, 6003),
    (6013, 6004),
    (7001, 7050),
    (7002, 7051),
    (7003, 7052),
    (7004, 7008),
    (7005, 7053),
    (7006, 7003),
    (7007, 7004),
    (7008, 7054),
    (7009, 7001),
    (7010, 7005),
    (7011, 7007),
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn should_keep_every_code_unique_with_its_slot_class() {
        // Arrange
        let entries = all_entries();

        // Act
        let codes = entries
            .iter()
            .map(|entry| entry.code)
            .collect::<HashSet<_>>();
        let slot_mismatches = entries
            .iter()
            .filter(|entry| {
                let slot = entry.code % 1000;
                (1..=SHARED_LAST).contains(&slot) && entry.class.shared_slot() != Some(slot)
            })
            .count();
        let gap_violations = entries
            .iter()
            .filter(|entry| {
                let slot = entry.code % 1000;
                slot == 0 || (slot > SHARED_LAST && slot < DOMAIN_FIRST)
            })
            .count();

        // Assert
        assert_eq!(codes.len(), entries.len(), "duplicate code");
        assert_eq!(slot_mismatches, 0);
        assert_eq!(gap_violations, 0);
    }

    #[test]
    fn should_map_every_wire1_code_onto_a_catalog_row() {
        // Arrange
        let mapped_from = WIRE1_TO_WIRE2
            .iter()
            .map(|(from, _)| *from)
            .collect::<Vec<_>>();

        // Act
        let dangling = WIRE1_TO_WIRE2
            .iter()
            .filter(|(_, new)| lookup(*new).is_none())
            .map(|(_, new)| *new)
            .collect::<Vec<_>>();
        let distinct_sources = mapped_from
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();

        // Assert
        assert_eq!(
            dangling,
            Vec::<u16>::new(),
            "mappings to codes the catalog lacks"
        );
        assert_eq!(distinct_sources, mapped_from.len());
        assert_eq!(mapped_from.len(), 80, "every wire 1 code is mapped");
    }

    #[test]
    fn should_give_every_block_the_same_ten_shared_slots() {
        // Arrange
        let entries = all_entries();

        // Act
        let per_block = block::ALL.map(|block| {
            (1..=SHARED_LAST)
                .filter(|slot| entries.iter().any(|entry| entry.code == block + slot))
                .count()
        });

        // Assert
        assert_eq!(per_block, [10; 8]);
        assert_eq!(shared(block::SCHEDULE, ErrorClass::Busy), Some(7005));
        assert_eq!(shared(block::KV, ErrorClass::Domain), None);
    }
}
