use crate::protocol::tlv::TlvDecoder;
use crate::runtime::routing::Route;
use bytes::Bytes;

/// TLV payload for schedules. MUST be TLV encoded.
///
/// The scheduler is a clock: it matches a cron expression against wall-clock time
/// and emits a notice at the target route.
///
/// Required fields:
/// - cron: string (standard 5-field cron expression)
/// - target_resource: string (resource name to emit notice for)
/// - target_operation: string (operation name to emit notice for)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulePayload {
    pub cron: String,
    pub target_resource: String,
    pub target_operation: String,
}

impl SchedulePayload {
    pub fn decode(input: &[u8]) -> Result<Self, String> {
        // Use TlvDecoder to decode records and extract string fields
        let decoder = TlvDecoder::new();
        let records = decoder
            .decode_all(input)
            .map_err(|e| format!("tlv decode error: {}", e))?;

        let mut cron: Option<String> = None;
        let mut target_resource: Option<String> = None;
        let mut target_operation: Option<String> = None;

        for rec in records {
            // Type numbers chosen arbitrarily and documented internally
            match rec.msg_type.0 {
                1 => cron = Some(String::from_utf8(rec.value.to_vec()).unwrap_or_default()),
                2 => {
                    target_resource =
                        Some(String::from_utf8(rec.value.to_vec()).unwrap_or_default())
                }
                3 => {
                    target_operation =
                        Some(String::from_utf8(rec.value.to_vec()).unwrap_or_default())
                }
                _ => (),
            }
        }

        let cron = cron.ok_or_else(|| "missing field: cron".to_string())?;
        let target_resource =
            target_resource.ok_or_else(|| "missing field: target_resource".to_string())?;
        let target_operation =
            target_operation.ok_or_else(|| "missing field: target_operation".to_string())?;

        Ok(Self {
            cron,
            target_resource,
            target_operation,
        })
    }

    pub fn encode(&self) -> Bytes {
        // Encode as TLV records with types 1,2,3 using TlvEncoder
        use crate::protocol::tlv::{MessageType, TlvEncoder};
        let mut enc = TlvEncoder::new();
        enc.encode(MessageType(1), self.cron.as_bytes());
        enc.encode(MessageType(2), self.target_resource.as_bytes());
        enc.encode(MessageType(3), self.target_operation.as_bytes());
        enc.finish()
    }
}

/// Parse a schedule route into (realm, area, resource, operation).
///
/// Expected format: `{scheme}://{realm}/{area}/{resource}/{operation}`
/// or `/{realm}/{area}/{resource}/{operation}`
pub fn parse_schedule_route(route: &Route) -> Result<(String, String, String, String), String> {
    let path = route.as_str();

    let path_without_scheme = if let Some(pos) = path.find("://") {
        &path[pos + 3..]
    } else {
        path
    };

    let parts: Vec<&str> = path_without_scheme
        .trim_start_matches('/')
        .split('/')
        .collect();

    if parts.len() != 4 {
        return Err("Schedule routes require exactly 4 segments".to_string());
    }

    Ok((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
        parts[3].to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::tlv::{MessageType, TlvEncoder};

    #[test]
    fn should_roundtrip_schedule_tlv() {
        // Arrange
        let payload = SchedulePayload {
            cron: "* * * * *".to_string(),
            target_resource: "res1".to_string(),
            target_operation: "op".to_string(),
        };

        // Act
        let enc = payload.encode();
        let dec = SchedulePayload::decode(&enc).unwrap();

        // Assert
        assert_eq!(dec, payload);
    }

    #[test]
    fn should_encode_all_fields_correctly() {
        // Arrange
        let payload = SchedulePayload {
            cron: "0 */6 * * *".to_string(),
            target_resource: "notifications".to_string(),
            target_operation: "send".to_string(),
        };

        // Act
        let enc = payload.encode();
        let dec = SchedulePayload::decode(&enc).unwrap();

        // Assert
        assert_eq!(dec.cron, "0 */6 * * *");
        assert_eq!(dec.target_resource, "notifications");
        assert_eq!(dec.target_operation, "send");
    }

    #[test]
    fn should_reject_missing_fields() {
        // Arrange - Only cron field (type 1)
        let mut enc = TlvEncoder::new();
        enc.encode(MessageType(1), b"* * * * *");
        let data = enc.finish();

        // Act
        let res = SchedulePayload::decode(&data);

        // Assert
        assert!(res.is_err());
    }

    #[test]
    fn should_handle_complex_cron_expressions() {
        // Arrange - Every weekday at noon
        let payload = SchedulePayload {
            cron: "0 12 * * 1-5".to_string(),
            target_resource: "reports".to_string(),
            target_operation: "generate".to_string(),
        };

        // Act
        let enc = payload.encode();
        let dec = SchedulePayload::decode(&enc).unwrap();

        // Assert
        assert_eq!(dec.cron, "0 12 * * 1-5");
    }

    #[test]
    fn should_parse_schedule_route_with_operation() {
        // Arrange
        let route = Route::new("schedule://acme/jobs/backup/create");

        // Act
        let result = parse_schedule_route(&route).unwrap();

        // Assert
        assert_eq!(result.0, "acme");
        assert_eq!(result.1, "jobs");
        assert_eq!(result.2, "backup");
        assert_eq!(result.3, "create");
    }

    #[test]
    fn should_reject_schedule_route_missing_operation() {
        // Arrange
        let route = Route::new("schedule://acme/jobs/backup");

        // Act
        let result = parse_schedule_route(&route);

        // Assert
        assert!(result.is_err());
    }
}

/// Schedule errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    /// Invalid realm format (3040)
    InvalidRealm,

    /// Realm mismatch - operation targets different realm than existing schedule (3041)
    RealmMismatch,
}

impl ScheduleError {
    pub fn code(&self) -> u16 {
        match self {
            ScheduleError::InvalidRealm => 3040,
            ScheduleError::RealmMismatch => 3041,
        }
    }
}
