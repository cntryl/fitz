use crate::auth::{Access, Permission};

#[test]
fn should_parse_permission_with_access_fragment() {
    // Arrange
    let perm_str = "notice://prod/orders/**#read";

    // Act
    let perm = Permission::parse(perm_str).unwrap();

    // Assert
    assert_eq!(perm.raw, perm_str);
    assert!(matches!(perm.access, Access::Read));
}

#[test]
fn should_parse_permission_without_access_defaults_to_all() {
    // Arrange
    let perm_str = "notice://prod/orders/**";

    // Act
    let perm = Permission::parse(perm_str).unwrap();

    // Assert
    assert_eq!(perm.raw, perm_str);
    assert!(matches!(perm.access, Access::All));
}

#[test]
fn should_reject_invalid_access_level() {
    // Arrange
    let perm_str = "notice://prod/orders/**#invalid";

    // Act
    let result = Permission::parse(perm_str);

    // Assert
    assert!(result.is_err());
}
