# Phase 1.3: StandardEncoder - DRY Improvements

**Status**: ✅ Prototype Complete  
**Date**: 2024

## Overview

Created a StandardEncoder abstraction layer that dramatically reduces boilerplate in domain encoding functions while maintaining the same buffer pool efficiency.

## Implementation

### New Component: `internal/core/encoding/encoder.go`

Provides high-level encoding primitives:

```go
// Primary pattern wrapper
func EncodeWithBuffer(fn func(*bytes.Buffer)) []byte

// Encoding helpers
func WriteU64(buf *bytes.Buffer, v uint64)
func WriteU32(buf *bytes.Buffer, v uint32)
func WriteString(buf *bytes.Buffer, s string)        // [u32 len][bytes]
func WriteBytes(buf *bytes.Buffer, data []byte)      // [u32 len][bytes]
func WriteRoute(buf *bytes.Buffer, route string)     // Semantic alias
func WriteBytesRaw(buf *bytes.Buffer, data []byte)   // No length prefix
```

### Test Coverage

✅ 9 tests covering all encoding primitives - **All Passing**

## Code Improvement Metrics

### Example 1: EncodeCommit

**Before** (19 lines):
```go
func EncodeCommit(txID uint64, route string) ([]byte, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	routeBytes := []byte(route)

	// [u64 BE] tx_id
	connection.WriteU64BE(buf, txID)

	// [u32 BE] route_len + [bytes] route
	connection.WriteU32BE(buf, uint32(len(routeBytes)))
	buf.Write(routeBytes)

	// Return copy
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}
```

**After** (5 lines):
```go
func EncodeCommit(txID uint64, route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
	}), nil
}
```

**Improvement**: **74% reduction** (19 → 5 lines)

### Example 2: EncodeGet (with validation)

**Before** (29 lines):
```go
func EncodeGet(txID uint64, route string, key []byte) ([]byte, error) {
	// Validate key size
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	routeBytes := []byte(route)

	// [u64 BE] tx_id
	connection.WriteU64BE(buf, txID)

	// [u32 BE] route_len + [bytes] route
	connection.WriteU32BE(buf, uint32(len(routeBytes)))
	buf.Write(routeBytes)

	// [u32 BE] key_len + [bytes] key
	connection.WriteU32BE(buf, uint32(len(key)))
	buf.Write(key)

	// Return copy
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}
```

**After** (13 lines):
```go
func EncodeGet(txID uint64, route string, key []byte) ([]byte, error) {
	// Validate key size
	if err := ValidateKeySize(key); err != nil {
		return nil, err
	}

	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, txID)
		encoding.WriteRoute(buf, route)
		encoding.WriteBytes(buf, key)
	}), nil
}
```

**Improvement**: **55% reduction** (29 → 13 lines)

## Benefits

### 1. Readability
- **Self-documenting**: `WriteRoute(buf, route)` vs `WriteU32BE(buf, uint32(len([]byte(route))))`
- **Intent-revealing**: Function names describe what's being encoded, not how
- **Less noise**: No manual buffer lifecycle management visible

### 2. Maintainability
- **Single source of truth**: Buffer lifecycle logic in one place
- **Easier to update**: Change encoding pattern once, affects all callers
- **Fewer bugs**: Can't forget to return buffer to pool (handled by EncodeWithBuffer)

### 3. Consistency
- **Uniform patterns**: All domains will use same encoding style
- **Semantic clarity**: `WriteRoute` vs generic `WriteString` shows intent
- **Easier onboarding**: New developers see clear encoding patterns

### 4. Performance
- **Same efficiency**: Still uses buffer pools underneath
- **No overhead**: Direct delegation to connection.Write* functions
- **Zero allocations added**: EncodeWithBuffer uses same pattern as before

## Refactored Functions (Validated)

✅ **EncodeCommit** - 74% code reduction, all tests passing
✅ **EncodeRollback** - 74% code reduction, all tests passing
✅ **EncodeGet** - 55% code reduction, all tests passing

All 17 KV domain tests **still passing** after refactoring.

## Potential Coverage

If applied to all KV and Notice encoding functions:

### KV Domain (8 functions)
- EncodeBegin
- EncodePut
- EncodeGet ✅ Done
- EncodeDelete
- EncodeDeleteRange
- EncodeScan
- EncodeCommit ✅ Done
- EncodeRollback ✅ Done

**Expected**: ~150 lines → ~60 lines (60% reduction)

### Notice Domain (2 functions)
- encodePublish
- encodeSubscribe

**Expected**: ~40 lines → ~16 lines (60% reduction)

### Total Impact
- **~190 lines → ~76 lines** (60% average reduction)
- **Same test coverage**: 22 existing tests preserved
- **Same performance**: Buffer pools still used
- **Better DRY**: Single encoding pattern

## Comparison: Before vs After Patterns

### Pattern: Route Encoding

**Before**:
```go
routeBytes := []byte(route)
connection.WriteU32BE(buf, uint32(len(routeBytes)))
buf.Write(routeBytes)
```

**After**:
```go
encoding.WriteRoute(buf, route)
```

### Pattern: Key/Value Encoding

**Before**:
```go
connection.WriteU32BE(buf, uint32(len(key)))
buf.Write(key)
```

**After**:
```go
encoding.WriteBytes(buf, key)
```

### Pattern: Full Function

**Before**:
```go
func EncodeXxx(...) ([]byte, error) {
    buf := connection.GetBuffer()
    defer connection.PutBuffer(buf)
    
    // encode fields...
    
    result := make([]byte, buf.Len())
    copy(result, buf.Bytes())
    return result, nil
}
```

**After**:
```go
func EncodeXxx(...) ([]byte, error) {
    return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
        encoding.WriteU64(buf, txID)
        encoding.WriteRoute(buf, route)
        encoding.WriteBytes(buf, key)
    }), nil
}
```

## Decision Point

**Question**: Should we apply StandardEncoder refactoring to all encoding functions?

### Option A: Proceed with Full Refactoring (Recommended)
- Refactor all 10 remaining KV/Notice functions
- Expected: 60% code reduction (~114 lines eliminated)
- Risk: Low (3 functions already validated)
- Time: ~15 minutes
- Benefit: Codebase-wide consistency

### Option B: Keep Hybrid Approach
- Leave 7 functions as-is (manual buffer pool)
- Keep 3 functions with StandardEncoder
- Benefit: Less immediate work
- Downside: Inconsistent patterns, harder to maintain

### Option C: Revert StandardEncoder
- Remove encoding package
- Restore 3 functions to manual style
- Benefit: One less abstraction layer
- Downside: Lose readability gains

## Recommendation

**Proceed with Option A**: Full refactoring

**Rationale**:
1. **Validated approach**: 3 functions refactored, all tests passing
2. **High ROI**: 60% code reduction with zero performance cost
3. **Maintainability**: Single pattern easier to understand and modify
4. **Low risk**: Test coverage ensures correctness

## Next Steps if Approved

1. Refactor remaining KV functions (5 functions)
2. Refactor Notice domain functions (2 functions)
3. Run full test suite
4. Update buffer pool audit
5. Document StandardEncoder in architecture docs
6. Proceed to Phase 1.4 (Frame encoding optimization)
