//! KV domain types
//!
//! KV operations use TLV encoding with route-based key namespacing.

/// KV operation types determined by route resource or operation segment
#[derive(Debug, Clone)]
pub enum KvOperation {
    /// Put - store key-value pair (TAG_ID = key, TAG_BODY = value)
    Put,
    /// Get - retrieve value by key (TAG_ID = key)
    Get,
    /// Delete - remove key (TAG_ID = key)
    Delete,
    /// DeleteRange - remove keys in range (TAG_BODY = "start_key\nend_key")
    DeleteRange,
    /// Scan - list keys from start (TAG_BODY = "start_key\nend_key" - end_key optional)
    Scan,
    /// Batch - atomic multi-operation transaction (TAG_BODY = operations)
    Batch,
    /// GetMany - retrieve multiple keys (TAG_BODY = newline-separated keys)
    GetMany,
    /// BeginTransaction - start a new transaction
    BeginTransaction,
    /// CommitTransaction - commit the current transaction
    CommitTransaction,
    /// RollbackTransaction - rollback the current transaction
    RollbackTransaction,
}

impl KvOperation {
    /// Determine operation from route
    /// Route patterns:
    /// - kv://realm/area/*/begin_transaction -> BeginTransaction
    /// - kv://realm/area/*/commit_transaction -> CommitTransaction
    /// - kv://realm/area/*/rollback_transaction -> RollbackTransaction
    /// - kv://realm/area/resource/put -> Put
    /// - kv://realm/area/resource/get -> Get
    /// - kv://realm/area/resource/delete -> Delete
    /// - kv://realm/area/*/delete -> DeleteRange (resource wildcard)
    /// - kv://realm/area/*/scan -> Scan
    /// - kv://realm/area/*/batch -> Batch
    /// - kv://realm/area/*/get-many -> GetMany
    ///
    /// Parameter passing:
    /// - Single key operations (Put/Get/Delete): TAG_ID = key, TAG_BODY = value
    /// - Range operations (Scan/DeleteRange): TAG_BODY = "start_key\nend_key"
    /// - Batch operations (Batch/GetMany): TAG_BODY = operation data
    pub fn from_route(route: &crate::protocol::route::Route) -> Result<Self, String> {
        // Validate that area is present and not a wildcard
        if route.area.as_deref() != Some("*") && route.area.is_none() {
            return Err("KV routes must specify an area".to_string());
        }
        if route.area.as_deref() == Some("*") {
            return Err("KV routes cannot use wildcards in area position".to_string());
        }

        // Handle transaction operations
        if let Some(op) = route.operation.as_deref() {
            match op {
                "begin_transaction" => return Ok(KvOperation::BeginTransaction),
                "commit_transaction" => return Ok(KvOperation::CommitTransaction),
                "rollback_transaction" => return Ok(KvOperation::RollbackTransaction),
                _ => {}
            }
        }

        // Check for wildcard in resource (indicates range/batch operation)
        let is_wildcard_resource = route.resource.as_deref() == Some("*");

        // Determine operation from route
        match route.operation.as_deref() {
            Some("get") => Ok(KvOperation::Get),
            Some("put") => Ok(KvOperation::Put),
            Some("delete") => {
                if is_wildcard_resource {
                    Ok(KvOperation::DeleteRange)
                } else {
                    Ok(KvOperation::Delete)
                }
            }
            Some("scan") => Ok(KvOperation::Scan),
            Some("batch") => {
                if is_wildcard_resource {
                    Ok(KvOperation::Batch)
                } else {
                    Err("batch operation requires wildcard (*) in resource position".to_string())
                }
            }
            Some("get-many") => {
                if is_wildcard_resource {
                    Ok(KvOperation::GetMany)
                } else {
                    Err("get-many operation requires wildcard (*) in resource position".to_string())
                }
            }
            None => {
                // Default operation based on presence of body
                // If body present, assume Put; otherwise Get
                Ok(KvOperation::Get)
            }
            Some(op) => Err(format!("Unknown KV operation: {}", op)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::route::{Route, Scheme};

    #[test]
    fn should_parse_put_operation() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("key1".to_string()),
            operation: Some("put".to_string()),
            raw: "kv://test/config/key1/put".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(matches!(result, Ok(KvOperation::Put)));
    }

    #[test]
    fn should_parse_get_operation() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("key1".to_string()),
            operation: Some("get".to_string()),
            raw: "kv://test/config/key1/get".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(matches!(result, Ok(KvOperation::Get)));
    }

    #[test]
    fn should_parse_delete_operation() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("key1".to_string()),
            operation: Some("delete".to_string()),
            raw: "kv://test/config/key1/delete".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(matches!(result, Ok(KvOperation::Delete)));
    }

    #[test]
    fn should_parse_delete_range_operation_with_wildcard() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("*".to_string()),
            operation: Some("delete".to_string()),
            raw: "kv://test/config/*/delete".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(matches!(result, Ok(KvOperation::DeleteRange)));
    }

    #[test]
    fn should_parse_scan_operation() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("*".to_string()),
            operation: Some("scan".to_string()),
            raw: "kv://test/config/*/scan".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(matches!(result, Ok(KvOperation::Scan)));
    }

    #[test]
    fn should_parse_batch_operation_with_wildcard() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("*".to_string()),
            operation: Some("batch".to_string()),
            raw: "kv://test/config/*/batch".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(matches!(result, Ok(KvOperation::Batch)));
    }

    #[test]
    fn should_reject_batch_operation_without_wildcard() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("key1".to_string()),
            operation: Some("batch".to_string()),
            raw: "kv://test/config/key1/batch".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_parse_get_many_operation_with_wildcard() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("*".to_string()),
            operation: Some("get-many".to_string()),
            raw: "kv://test/config/*/get-many".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(matches!(result, Ok(KvOperation::GetMany)));
    }

    #[test]
    fn should_reject_get_many_operation_without_wildcard() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("key1".to_string()),
            operation: Some("get-many".to_string()),
            raw: "kv://test/config/key1/get-many".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_parse_transaction_operations() {
        // Arrange
        let test_cases = vec![
            ("begin_transaction", KvOperation::BeginTransaction),
            ("commit_transaction", KvOperation::CommitTransaction),
            ("rollback_transaction", KvOperation::RollbackTransaction),
        ];

        // Act
        for (op_name, expected_op) in test_cases {
            let route = Route {
                scheme: Scheme::Kv,
                realm: Some("test".to_string()),
                area: Some("config".to_string()),
                resource: Some("*".to_string()),
                operation: Some(op_name.to_string()),
                raw: format!("kv://test/config/*/{op_name}"),
            };
            let result = KvOperation::from_route(&route);

            // Assert
            assert!(result.is_ok(), "Failed to parse operation: {}", op_name);
            match result.unwrap() {
                KvOperation::BeginTransaction if matches!(expected_op, KvOperation::BeginTransaction) => {},
                KvOperation::CommitTransaction if matches!(expected_op, KvOperation::CommitTransaction) => {},
                KvOperation::RollbackTransaction if matches!(expected_op, KvOperation::RollbackTransaction) => {},
                actual => panic!("Operation {} parsed as {:?}, expected {:?}", op_name, actual, expected_op),
            }
        }
    }

    #[test]
    fn should_reject_route_without_area() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: None,
            resource: Some("key1".to_string()),
            operation: Some("get".to_string()),
            raw: "kv://test//key1/get".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_route_with_wildcard_area() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("*".to_string()),
            resource: Some("key1".to_string()),
            operation: Some("get".to_string()),
            raw: "kv://test/*/key1/get".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_unknown_operation() {
        // Arrange
        let route = Route {
            scheme: Scheme::Kv,
            realm: Some("test".to_string()),
            area: Some("config".to_string()),
            resource: Some("key1".to_string()),
            operation: Some("unknown".to_string()),
            raw: "kv://test/config/key1/unknown".to_string(),
        };

        // Act
        let result = KvOperation::from_route(&route);

        // Assert
        assert!(result.is_err());
    }
}
