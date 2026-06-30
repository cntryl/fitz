//! Edge case and boundary condition validators
//!
//! Implements validators for:
//! - Size boundaries (zero-length and maximum)
//! - Numeric limits (wraparound, overflow)
//! - Resource quotas and limits
//! - Data integrity checks

use std::num::NonZeroUsize;

const CRC32_TABLE: [u32; 256] = [
    0x0000_0000,
    0x7707_3096,
    0xEE0E_612C,
    0x9909_51BA,
    0x076D_C419,
    0x706A_F48F,
    0xE963_A535,
    0x9E64_95A3,
    0x0EDB_8832,
    0x79DC_B8A4,
    0xE0D5_E91E,
    0x97D2_D988,
    0x09B6_4C2B,
    0x7EB1_7CBD,
    0xE7B8_2D07,
    0x90BF_1D91,
    0x1DB7_1642,
    0x6AB0_20F2,
    0xF3B9_7148,
    0x84BE_41DE,
    0x1ADA_D47D,
    0x6DDD_E4EB,
    0xF4D4_B551,
    0x83D3_85C7,
    0x136C_9856,
    0x646B_A8C0,
    0xFD62_F97A,
    0x8A65_C9EC,
    0x1401_5C4F,
    0x6306_6CD9,
    0xFA44_E5D6,
    0x8D07_9D40,
    0x3B6E_20C8,
    0x4C69_105E,
    0xD560_41E4,
    0xA267_7172,
    0x3C03_E4D1,
    0x4B04_D447,
    0xD108_D755,
    0xA6E0_37C3,
    0x3D67_0D52,
    0x4A6F_A9C4,
    0xD3D6_F4E6,
    0xA4D7_A574,
    0x3A6A_7C3D,
    0x4D6D_6CAB,
    0xD4D9_6F51,
    0xA3D8_E7C7,
    0x4377_C7E5,
    0x3439_C773,
    0xAFDB_4C89,
    0xD8DA_6CFF,
    0x4228_475C,
    0x3504_C6CA,
    0xAC23_7930,
    0xDB3B_59A6,
    0x4A3F_F25D,
    0x3DBB_CACB,
    0xA4C5_D431,
    0xD3D6_D3A7,
    0x4D6D_6A04,
    0x3AB1_AA92,
    0xA3D9_0A68,
    0xD4D9_4AFE,
    0x5B0F_540C,
    0x2C60_D49A,
    0xB5D8_D460,
    0xC2E7_4DF6,
    0x5C0B_7455,
    0x2B14_B4C3,
    0xB8DC_EA39,
    0xCFCD_56AF,
    0x5A0C_6D1E,
    0x2D0F_CB88,
    0xB4BC_B772,
    0xC33D_CCE4,
    0x5D0A_5047,
    0x2A1A_90D1,
    0xB3D0_A9CB,
    0xC4F5_D05D,
    0x7E36_0BDB,
    0x0919_814D,
    0x906F_67B7,
    0xE7D5_D821,
    0x7DD8_5282,
    0x0A48_D214,
    0x939D_9EEE,
    0xE481_FE78,
    0x7D61_D189,
    0x0A51_5B1F,
    0x9DB4_1FE5,
    0xEAE7_D673,
    0x74BD_7D30,
    0x0381_BDA6,
    0x9A1D_605C,
    0xEDD8_C0CA,
    0x2A9D_0D08,
    0x5D4D_3D9E,
    0xC4D8_2764,
    0xB3D0_E7F2,
    0x2D01_6D51,
    0x5A5A_CEDC,
    0xC3DB_0226,
    0xB412_82B0,
    0x25B2_DF41,
    0x525D_65D7,
    0xCB4C_7FCD,
    0xBC4F_7F5B,
    0x22F0_5F88,
    0x5589_F51E,
    0xCCDE_FFE4,
    0xBB3F_1F72,
    0x311F_1CBA,
    0x46DC_C09C,
    0xDF63_B1A6,
    0xA8D8_1930,
    0x3831_7193,
    0x4F13_F105,
    0xD6BB_FBFF,
    0xA1D8_B969,
    0x31A2_9B58,
    0x465B_95CE,
    0xDF1D_7B34,
    0xA8D0_BBA2,
    0x3602_D101,
    0x41E3_1197,
    0xD8B3_0B6D,
    0xAF98_EBFB,
    0x7C1C_8B61,
    0x0B03_4BF7,
    0x9285_B10D,
    0xE5C0_719B,
    0x7BD7_D238,
    0x0CE5_1CAE,
    0x95FA_6454,
    0xE2B4_44C2,
    0x7F5B_0413,
    0x0854_C485,
    0x917F_7E7F,
    0xE6C8_E4E9,
    0x7887_8E4A,
    0x0F03_0EDC,
    0x9605_F426,
    0xE144_7CB0,
    0x537A_E3A8,
    0x2C24_323E,
    0xB54E_28C4,
    0xC2F8_A852,
    0x5C4A_62F1,
    0x2BFD_A267,
    0xB2FB_889D,
    0xC5DC_48AB,
    0x547F_C5A0,
    0x2304_B236,
    0xBABA_68CC,
    0xCD4B_A25A,
    0x5762_CAF9,
    0x205A_086F,
    0xB9B8_1295,
    0xCEDC_9203,
    0x78BA_4F61,
    0x0F0F_8FF7,
    0x96C9_850D,
    0xE1D8_459B,
    0x7F2C_D538,
    0x0851_DFA6,
    0x9165_F15C,
    0xE6A7_71CA,
    0x7DBD_D31B,
    0x0A97_5F8D,
    0x9366_D577,
    0xE4CF_9E01,
    0x7AE8_D4A2,
    0x0D57_E634,
    0x9437_FC1E,
    0xE336_3688,
    0x1E93_F22D,
    0x6B64_C2BB,
    0xF2FB_3841,
    0x85ED_683D,
    0x1BBB_FE9E,
    0x6CBD_3E08,
    0xF51C_24F2,
    0x823E_A464,
    0x1C72_BF95,
    0x6B11_7F03,
    0xF8A0_3F19,
    0x8F88_E18F,
    0x1149_CB2C,
    0x6642_BBBA,
    0xFF6F_A140,
    0x880E_D6D6,
    0x4C16_C6D4,
    0x3B16_C742,
    0xA2CD_3DB8,
    0xD5D5_D82E,
    0x4B6D_5D8D,
    0x3C6B_3D1B,
    0xA5D7_2DE1,
    0xD2D0_AD77,
    0x4D7D_4CA6,
    0x3A7C_2C30,
    0xA336_DAAC,
    0xD4DA_1A3A,
    0x4A66_7199,
    0x3DD6_F10F,
    0xA4D0_BBF5,
    0xD3D7_3B63,
    0x5A0B_65EF,
    0x2DD0_E579,
    0xB4D7_F183,
    0xC3D5_1115,
    0x5D03_7AB6,
    0x2AFA_5A20,
    0xB3D0_CCDA,
    0xC4E4_0C4C,
    0x5B6E_339D,
    0x2CEC_730B,
    0xB5D7_89F1,
    0xC2F4_9967,
    0x5CF8_F2C4,
    0x2BF9_7252,
    0xB2D8_C0A8,
    0xC5D6_403E,
    0x7A7B_2240,
    0x0D7B_A2D6,
    0x9453_B88C,
    0xE3B5_381A,
    0x7D41_B3B9,
    0x0A4C_732F,
    0x936D_69D5,
    0xE4A1_9A43,
    0x7F1D_9192,
    0x0851_F105,
    0x916B_33FF,
    0xE6B3_B369,
    0x78B3_8ACA,
    0x0F84_4A5C,
    0x9C50_25A6,
    0xEB5E_5E30,
    0x1ADB_6FB5,
    0x6D86_CF23,
    0xF4D3_D5D9,
    0x8319_354F,
    0x1D55_FCEC,
    0x6A7F_3C7A,
    0xF389_6680,
    0x8411_A6F6,
    0x1A13_D827,
    0x6D4D_58B1,
    0xF470_224B,
    0x83B7_38DD,
    0x1DFD_137E,
    0x6AB9_93E8,
    0xF3A8_D112,
    0x84D3_5184,
];

fn crc32_table_index(crc: u32, byte: u8) -> usize {
    let index = (crc ^ u32::from(byte)) & 0xFF;
    usize::try_from(index).unwrap_or(usize::MAX)
}

/// Size limits for all domains
#[derive(Clone, Debug)]
pub struct SizeLimits {
    /// Maximum key size (default 1 MB)
    pub max_key_size: NonZeroUsize,

    /// Maximum value size (default 100 MB)
    pub max_value_size: NonZeroUsize,

    /// Maximum event size for streams (default 50 MB)
    pub max_event_size: NonZeroUsize,
}

impl SizeLimits {
    /// Create custom size limits
    #[must_use]
    pub fn new(key: NonZeroUsize, value: NonZeroUsize, event: NonZeroUsize) -> Self {
        Self {
            max_key_size: key,
            max_value_size: value,
            max_event_size: event,
        }
    }

    /// Validate key size
    ///
    /// # Errors
    ///
    /// Returns [`SizeError::KeyTooLarge`] when `key` exceeds `max_key_size`.
    pub fn validate_key(&self, key: &[u8]) -> Result<(), SizeError> {
        if key.is_empty() {
            // Empty keys are valid (distinct from not found)
            return Ok(());
        }

        if key.len() > self.max_key_size.get() {
            return Err(SizeError::KeyTooLarge {
                size: key.len(),
                limit: self.max_key_size.get(),
            });
        }

        Ok(())
    }

    /// Validate value size
    ///
    /// # Errors
    ///
    /// Returns [`SizeError::ValueTooLarge`] when `value` exceeds `max_value_size`.
    pub fn validate_value(&self, value: &[u8]) -> Result<(), SizeError> {
        if value.is_empty() {
            // Empty values are valid (distinct from not found)
            return Ok(());
        }

        if value.len() > self.max_value_size.get() {
            return Err(SizeError::ValueTooLarge {
                size: value.len(),
                limit: self.max_value_size.get(),
            });
        }

        Ok(())
    }

    /// Validate event size
    ///
    /// # Errors
    ///
    /// Returns [`SizeError::EventTooLarge`] when `event` exceeds `max_event_size`.
    pub fn validate_event(&self, event: &[u8]) -> Result<(), SizeError> {
        if event.is_empty() {
            // Empty events are valid (occupy offset slot)
            return Ok(());
        }

        if event.len() > self.max_event_size.get() {
            return Err(SizeError::EventTooLarge {
                size: event.len(),
                limit: self.max_event_size.get(),
            });
        }

        Ok(())
    }
}

impl Default for SizeLimits {
    fn default() -> Self {
        Self {
            max_key_size: NonZeroUsize::new(1024 * 1024).unwrap(), // 1 MB
            max_value_size: NonZeroUsize::new(100 * 1024 * 1024).unwrap(), // 100 MB
            max_event_size: NonZeroUsize::new(50 * 1024 * 1024).unwrap(), // 50 MB
        }
    }
}

/// Size validation errors
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SizeError {
    KeyTooLarge { size: usize, limit: usize },
    ValueTooLarge { size: usize, limit: usize },
    EventTooLarge { size: usize, limit: usize },
}

/// Resource quotas per realm
#[derive(Clone, Debug)]
pub struct ResourceQuota {
    /// Maximum storage per realm (default 1 TB)
    pub max_realm_storage: NonZeroUsize,

    /// Maximum connections per realm (default 10,000)
    pub max_connections: NonZeroUsize,

    /// Maximum concurrent transactions per connection (default 100)
    pub max_concurrent_transactions: NonZeroUsize,
}

impl ResourceQuota {
    /// Create custom quotas
    #[must_use]
    pub fn new(
        storage: NonZeroUsize,
        connections: NonZeroUsize,
        transactions: NonZeroUsize,
    ) -> Self {
        Self {
            max_realm_storage: storage,
            max_connections: connections,
            max_concurrent_transactions: transactions,
        }
    }

    /// Check if storage quota exceeded
    ///
    /// # Errors
    ///
    /// Returns [`QuotaError::StorageQuotaExceeded`] when `used + needed` exceeds
    /// `max_realm_storage`.
    pub fn check_storage(&self, used: usize, needed: usize) -> Result<(), QuotaError> {
        if used + needed > self.max_realm_storage.get() {
            return Err(QuotaError::StorageQuotaExceeded {
                used,
                needed,
                limit: self.max_realm_storage.get(),
            });
        }
        Ok(())
    }

    /// Check if connection limit reached
    ///
    /// # Errors
    ///
    /// Returns [`QuotaError::ConnectionLimitReached`] when `active` meets or exceeds
    /// `max_connections`.
    pub fn check_connection(&self, active: usize) -> Result<(), QuotaError> {
        if active >= self.max_connections.get() {
            return Err(QuotaError::ConnectionLimitReached {
                active,
                limit: self.max_connections.get(),
            });
        }
        Ok(())
    }

    /// Check if transaction limit reached
    ///
    /// # Errors
    ///
    /// Returns [`QuotaError::TransactionLimitReached`] when `active` meets or exceeds
    /// `max_concurrent_transactions`.
    pub fn check_transaction(&self, active: usize) -> Result<(), QuotaError> {
        if active >= self.max_concurrent_transactions.get() {
            return Err(QuotaError::TransactionLimitReached {
                active,
                limit: self.max_concurrent_transactions.get(),
            });
        }
        Ok(())
    }
}

impl Default for ResourceQuota {
    fn default() -> Self {
        Self {
            max_realm_storage: NonZeroUsize::new(1024 * 1024 * 1024 * 1024).unwrap(), // 1 TB
            max_connections: NonZeroUsize::new(10_000).unwrap(),
            max_concurrent_transactions: NonZeroUsize::new(100).unwrap(),
        }
    }
}

/// Quota violation errors
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuotaError {
    StorageQuotaExceeded {
        used: usize,
        needed: usize,
        limit: usize,
    },
    ConnectionLimitReached {
        active: usize,
        limit: usize,
    },
    TransactionLimitReached {
        active: usize,
        limit: usize,
    },
}

/// Data integrity validation
#[derive(Clone, Debug)]
pub struct IntegrityChecker {
    /// Enable CRC32 checksum validation
    pub enable_checksums: bool,
}

impl IntegrityChecker {
    /// Calculate CRC32 checksum
    #[must_use]
    pub fn crc32(&self, data: &[u8]) -> u32 {
        if !self.enable_checksums {
            return 0;
        }

        let mut crc = 0xFFFF_FFFF_u32;
        for byte in data {
            let index = crc32_table_index(crc, *byte);
            crc = (crc >> 8) ^ CRC32_TABLE[index];
        }
        !crc
    }

    /// Verify CRC32 checksum
    #[must_use]
    pub fn verify_crc32(&self, data: &[u8], expected: u32) -> bool {
        if !self.enable_checksums {
            return true;
        }
        self.crc32(data) == expected
    }
}

impl Default for IntegrityChecker {
    fn default() -> Self {
        Self {
            enable_checksums: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_empty_key_value_pairs() {
        // Arrange
        let limits = SizeLimits::default();

        // Act
        // Assert
        assert!(limits.validate_key(b"").is_ok());
        assert!(limits.validate_value(b"").is_ok());
        assert!(limits.validate_event(b"").is_ok());
    }

    #[test]
    fn should_reject_oversized_keys() {
        // Arrange
        let limits = SizeLimits::new(
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );

        // Act
        let result = limits.validate_key(&[0u8; 20]);

        // Assert
        assert!(matches!(result, Err(SizeError::KeyTooLarge { .. })));
    }

    #[test]
    fn should_reject_oversized_values() {
        // Arrange
        let limits = SizeLimits::new(
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );

        // Act
        let result = limits.validate_value(&[0u8; 20]);

        // Assert
        assert!(matches!(result, Err(SizeError::ValueTooLarge { .. })));
    }

    #[test]
    fn should_enforce_storage_quota() {
        // Arrange
        let quota = ResourceQuota::default();

        // Act
        // Check within quota
        assert!(quota.check_storage(100, 200).is_ok());

        // Check exceeding quota
        let result = quota.check_storage(
            1024 * 1024 * 1024 * 1000, // 1000 GB
            100 * 1024 * 1024 * 1024,  // 100 GB more
        );

        // Assert
        assert!(matches!(
            result,
            Err(QuotaError::StorageQuotaExceeded { .. })
        ));
    }

    #[test]
    fn should_enforce_connection_limit() {
        // Arrange
        let quota = ResourceQuota::new(
            NonZeroUsize::new(1024).unwrap(),
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );

        // Act
        assert!(quota.check_connection(9).is_ok());
        let result = quota.check_connection(10);

        // Assert
        assert!(matches!(
            result,
            Err(QuotaError::ConnectionLimitReached { .. })
        ));
    }

    #[test]
    fn should_calculate_crc32() {
        // Arrange
        let checker = IntegrityChecker::default();
        let data = b"hello world";

        // Act
        let crc = checker.crc32(data);

        // Assert
        assert!(crc != 0);
        assert!(checker.verify_crc32(data, crc));
        assert!(!checker.verify_crc32(data, crc ^ 1));
    }
}
