//! Edge case and boundary condition validators
//!
//! Implements validators for:
//! - Size boundaries (zero-length and maximum)
//! - Numeric limits (wraparound, overflow)
//! - Resource quotas and limits
//! - Data integrity checks

use std::num::NonZeroUsize;

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
    pub fn new(key: NonZeroUsize, value: NonZeroUsize, event: NonZeroUsize) -> Self {
        Self {
            max_key_size: key,
            max_value_size: value,
            max_event_size: event,
        }
    }

    /// Validate key size
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
            max_key_size: NonZeroUsize::new(1024 * 1024).unwrap(),           // 1 MB
            max_value_size: NonZeroUsize::new(100 * 1024 * 1024).unwrap(),  // 100 MB
            max_event_size: NonZeroUsize::new(50 * 1024 * 1024).unwrap(),   // 50 MB
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
    pub fn crc32(&self, data: &[u8]) -> u32 {
        if !self.enable_checksums {
            return 0;
        }

        const CRC32_TABLE: [u32; 256] = [
            0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
            0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,
            0x1DB71642, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
            0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA44E5D6, 0x8D079D40,
            0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD108D755, 0xA6E037C3,
            0x3D670D52, 0x4A6FA9C4, 0xD3D6F4E6, 0xA4D7A574, 0x3A6A7C3D, 0x4D6D6CAB, 0xD4D96F51, 0xA3D8E7C7,
            0x4377C7E5, 0x3439C773, 0xAFDB4C89, 0xD8DA6CFF, 0x4228475C, 0x3504C6CA, 0xAC237930, 0xDB3B59A6,
            0x4A3FF25D, 0x3DBBCACB, 0xA4C5D431, 0xD3D6D3A7, 0x4D6D6A04, 0x3AB1AA92, 0xA3D90A68, 0xD4D94AFE,
            0x5B0F540C, 0x2C60D49A, 0xB5D8D460, 0xC2E74DF6, 0x5C0B7455, 0x2B14B4C3, 0xB8DCEA39, 0xCFCD56AF,
            0x5A0C6D1E, 0x2D0FCB88, 0xB4BCB772, 0xC33DCCE4, 0x5D0A5047, 0x2A1A90D1, 0xB3D0A9CB, 0xC4F5D05D,
            0x7E360BDB, 0x0919814D, 0x906F67B7, 0xE7D5D821, 0x7DD85282, 0x0A48D214, 0x939D9EEE, 0xE481FE78,
            0x7D61D189, 0x0A515B1F, 0x9DB41FE5, 0xEAE7D673, 0x74BD7D30, 0x0381BDA6, 0x9A1D605C, 0xEDD8C0CA,
            0x2A9D0D08, 0x5D4D3D9E, 0xC4D82764, 0xB3D0E7F2, 0x2D016D51, 0x5A5ACEDC, 0xC3DB0226, 0xB41282B0,
            0x25B2DF41, 0x525D65D7, 0xCB4C7FCD, 0xBC4F7F5B, 0x22F05F88, 0x5589F51E, 0xCCDEFFE4, 0xBB3F1F72,
            0x311F1CBA, 0x46DCC09C, 0xDF63B1A6, 0xA8D81930, 0x38317193, 0x4F13F105, 0xD6BBFBFF, 0xA1D8B969,
            0x31A29B58, 0x465B95CE, 0xDF1D7B34, 0xA8D0BBA2, 0x3602D101, 0x41E31197, 0xD8B30B6D, 0xAF98EBFB,
            0x7C1C8B61, 0x0B034BF7, 0x9285B10D, 0xE5C0719B, 0x7BD7D238, 0x0CE51CAE, 0x95FA6454, 0xE2B444C2,
            0x7F5B0413, 0x0854C485, 0x917F7E7F, 0xE6C8E4E9, 0x78878E4A, 0x0F030EDC, 0x9605F426, 0xE1447CB0,
            0x537AE3A8, 0x2C24323E, 0xB54E28C4, 0xC2F8A852, 0x5C4A62F1, 0x2BFDA267, 0xB2FB889D, 0xC5DC48AB,
            0x547FC5A0, 0x2304B236, 0xBABA68CC, 0xCD4BA25A, 0x5762CAF9, 0x205A086F, 0xB9B81295, 0xCEDC9203,
            0x78BA4F61, 0x0F0F8FF7, 0x96C9850D, 0xE1D8459B, 0x7F2CD538, 0x0851DFA6, 0x9165F15C, 0xE6A771CA,
            0x7DBDD31B, 0x0A975F8D, 0x9366D577, 0xE4CF9E01, 0x7AE8D4A2, 0x0D57E634, 0x9437FC1E, 0xE3363688,
            0x1E93F22D, 0x6B64C2BB, 0xF2FB3841, 0x85ED683D, 0x1BBBFE9E, 0x6CBD3E08, 0xF51C24F2, 0x823EA464,
            0x1C72BF95, 0x6B117F03, 0xF8A03F19, 0x8F88E18F, 0x1149CB2C, 0x6642BBBA, 0xFF6FA140, 0x880ED6D6,
            0x4C16C6D4, 0x3B16C742, 0xA2CD3DB8, 0xD5D5D82E, 0x4B6D5D8D, 0x3C6B3D1B, 0xA5D72DE1, 0xD2D0AD77,
            0x4D7D4CA6, 0x3A7C2C30, 0xA336DAAC, 0xD4DA1A3A, 0x4A667199, 0x3DD6F10F, 0xA4D0BBF5, 0xD3D73B63,
            0x5A0B65EF, 0x2DD0E579, 0xB4D7F183, 0xC3D51115, 0x5D037AB6, 0x2AFA5A20, 0xB3D0CCDA, 0xC4E40C4C,
            0x5B6E339D, 0x2CEC730B, 0xB5D789F1, 0xC2F49967, 0x5CF8F2C4, 0x2BF97252, 0xB2D8C0A8, 0xC5D6403E,
            0x7A7B2240, 0x0D7BA2D6, 0x9453B88C, 0xE3B5381A, 0x7D41B3B9, 0x0A4C732F, 0x936D69D5, 0xE4A19A43,
            0x7F1D9192, 0x0851F105, 0x916B33FF, 0xE6B3B369, 0x78B38ACA, 0x0F844A5C, 0x9C5025A6, 0xEB5E5E30,
            0x1ADB6FB5, 0x6D86CF23, 0xF4D3D5D9, 0x8319354F, 0x1D55FCEC, 0x6A7F3C7A, 0xF3896680, 0x8411A6F6,
            0x1A13D827, 0x6D4D58B1, 0xF470224B, 0x83B738DD, 0x1DFD137E, 0x6AB993E8, 0xF3A8D112, 0x84D35184,
        ];
        
        let mut crc = 0xFFFFFFFF_u32;
        for byte in data {
            let index = ((crc ^ (*byte as u32)) & 0xFF) as usize;
            crc = (crc >> 8) ^ CRC32_TABLE[index];
        }
        !crc
    }

    /// Verify CRC32 checksum
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
    fn should_accept_empty_keys_and_values() {
        let limits = SizeLimits::default();
        assert!(limits.validate_key(b"").is_ok());
        assert!(limits.validate_value(b"").is_ok());
        assert!(limits.validate_event(b"").is_ok());
    }

    #[test]
    fn should_reject_oversized_keys() {
        let limits = SizeLimits::new(
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );

        let result = limits.validate_key(&[0u8; 20]);
        assert!(matches!(result, Err(SizeError::KeyTooLarge { .. })));
    }

    #[test]
    fn should_reject_oversized_values() {
        let limits = SizeLimits::new(
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );

        let result = limits.validate_value(&[0u8; 20]);
        assert!(matches!(result, Err(SizeError::ValueTooLarge { .. })));
    }

    #[test]
    fn should_enforce_storage_quota() {
        let quota = ResourceQuota::default();
        
        // Check within quota
        assert!(quota.check_storage(100, 200).is_ok());
        
        // Check exceeding quota
        let result = quota.check_storage(
            1024 * 1024 * 1024 * 1000, // 1000 GB
            100 * 1024 * 1024 * 1024,  // 100 GB more
        );
        assert!(matches!(result, Err(QuotaError::StorageQuotaExceeded { .. })));
    }

    #[test]
    fn should_enforce_connection_limit() {
        let quota = ResourceQuota::new(
            NonZeroUsize::new(1024).unwrap(),
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(100).unwrap(),
        );

        assert!(quota.check_connection(9).is_ok());
        let result = quota.check_connection(10);
        assert!(matches!(result, Err(QuotaError::ConnectionLimitReached { .. })));
    }

    #[test]
    fn should_calculate_crc32() {
        let checker = IntegrityChecker::default();
        let data = b"hello world";
        
        let crc = checker.crc32(data);
        assert!(crc != 0);
        assert!(checker.verify_crc32(data, crc));
        assert!(!checker.verify_crc32(data, crc ^ 1));
    }
}
