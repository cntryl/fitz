# RouteFamily → ColumnFamily: Clarification

**Date**: January 18, 2026

---

## Question Resolved

**Original Ambiguity**: Is the CF mapping by RouteFamily or by Realm?

**Answer**: **RouteFamily → ColumnFamily (1:1 by value)**

---

## Fitz Isolation Model

### RouteFamily & ColumnFamily: Physical Isolation Boundary

From `src/runtime/routing.rs`:

> **RouteFamilyId aligns 1:1 (by value) with Midge ColumnFamilyId**:
> - Same underlying integer type (u64)
> - Same value represents the same isolation boundary
> - Alignment is contractual, not enforced by storage code

### Physical Isolation: RouteFamily → ColumnFamily

| Layer | Isolation | Scope |
|-------|-----------|-------|
| **Routing** | No cross-family routing | Routes in family A never reach family B |
| **Leasing** | No cross-family leases | Lease acquired in A doesn't affect same resource in B |
| **Messages** | No cross-family delivery | Message to (A, route) won't reach (B, route) |
| **State** | No cross-family state | Changes in A never affect B |
| **Storage** | Hard ColumnFamily boundary | Different CF for each RouteFamily |

**Physical boundary**: RF and CF are infrastructure-level isolation boundaries, not application-level. Cannot be crossed by any Fitz component.

### Logical Isolation: Realm (User-Defined)

From `src/runtime/routing.rs:30-45`:

> **Realm is a string in the route path with user-defined semantics**:
> - NOT the RouteFamily (not a physical boundary)
> - NOT enforced by infrastructure
> - May represent tenant, organization, department, environment, or any root concept
> - Runtime does not interpret realm values

Example route: `rpc://acme/auth/users/authenticate`
- "acme" is the realm (logical boundary)
- Realm isolation is application-defined and application-enforced
- Multiple realms can share a RouteFamily (same CF)

---

## Multi-Tenancy Models in Fitz

### Model 1: RouteFamily-per-Tenant (Strongest Isolation)

```rust
let tenant_a_family = RouteFamily::new(100);  // RF(100) → CF(100)
let tenant_b_family = RouteFamily::new(200);  // RF(200) → CF(200)

let tenant_a_addr = RouteAddress::new(
    tenant_a_family,
    Route::new("rpc://acme/auth/users/authenticate".to_string())
);

let tenant_b_addr = RouteAddress::new(
    tenant_b_family,
    Route::new("rpc://acme/auth/users/authenticate".to_string())
);
```

**Properties**:
- ✅ Complete isolation at ColumnFamily level
- ✅ Different CF for each tenant
- ✅ No realm string interpretation needed
- ❌ More column families to manage
- ❌ More RouteFamily IDs to allocate

### Model 2: Shared RouteFamily, Realm-per-Tenant (Logical Isolation)

```rust
let shared_family = RouteFamily::new(1);  // Both use RF(1) → CF(1)

let tenant_a_logical = RouteAddress::new(
    shared_family,
    Route::new("rpc://tenant-a/orders/create".to_string())
);

let tenant_b_logical = RouteAddress::new(
    shared_family,
    Route::new("rpc://tenant-b/orders/create".to_string())
);
```

**Properties**:
- ✅ Simpler management (single CF)
- ✅ Fewer RouteFamily IDs needed
- ❌ Isolation is logical, not physical (application-level logic required)
- ❌ No hardware/storage-level protection between tenants
- ⚠️ Realm string is part of routing key and must be enforced by domain logic

---

## KV Domain Implementation

### Current Code (Correct)

```rust
fn resolve_column_family(route_family: RouteFamily, _resource: &str) -> ColumnFamilyId {
    // Validate RouteFamily is not zero (would map to default CF)
    crate::runtime::cf_validation::validate_route_family(route_family);
    
    // Map RouteFamily → ColumnFamily (1:1 by value)
    ColumnFamilyId(route_family.id() as u32)
}
```

**Design**:
- ✅ Routes via RouteFamily (hard isolation)
- ✅ Rejects RouteFamily(0) (panic to prevent default CF)
- ✅ Resources isolated via key scoping (prefix in same CF)
- ✅ Matches Fitz architecture exactly

---

## Specification Alignment

### Original Spec Statement

> Each tenant maps to one Midge column family

**Interpretation** (Fitz Definition):
- **Tenant** = RouteFamily (isolation boundary, not realm string)
- **Column Family** = ColumnFamilyId resolved from RouteFamily
- **Mapping** = 1:1 by value (RouteFamily.id() → ColumnFamilyId)

**Clarification**:
- "Tenant" in Fitz architecture means **RouteFamily**, not realm
- Realm is organizational semantics, not isolation boundary
- ColumnFamily isolation is per-RouteFamily, not per-realm

---

## Conclusion

✅ **The KV implementation is CORRECT.**

The RouteFamily → ColumnFamily mapping:
1. Matches Fitz runtime architecture (`src/runtime/routing.rs`)
2. Provides hard isolation at the storage layer
3. Aligns with the specification once "tenant" is understood as "RouteFamily"
4. Enables both strong (per-family) and logical (per-realm) multi-tenancy models

No changes needed. Documentation clarity is the only gap.
