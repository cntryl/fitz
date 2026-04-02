---
name: write-unit-test
description: 'Write unit tests for Rust code in the Fitz project. Use when asked to write, add, create, or generate tests, unit tests, or test cases. Enforces should_* naming, AAA structure, single-behavior principle, and meta-test compliance.'
argument-hint: 'Describe the function or behavior to test'
---

# Write Unit Test (Fitz)

## When to Use
- Writing new tests for any Rust module
- Adding test coverage to a function or method
- Reviewing whether existing tests follow project conventions
- Generating test stubs before implementation

## Checklist (Run Before Output)
- [ ] Name starts with `should_`
- [ ] If >5 lines → has `// Arrange`, `// Act`, `// Assert` (exact, no suffixes)
- [ ] Only ONE `// Act` section per test
- [ ] Each test exercises ONE specific behavior
- [ ] Multiple asserts only if all verify facets of the same single operation

---

## Procedure

### Step 1 — Identify the behavior to test

Ask: *What is the single observable outcome I am verifying?*

If the answer covers more than one distinct input/output pair or more than one state transition, **split into multiple tests**.

### Step 2 — Choose the test structure

| Test size       | AAA required? |
|-----------------|---------------|
| ≤ 5 lines total | No            |
| > 5 lines total | Yes — all three comments, exact format |

### Step 3 — Name the test

Pattern: `should_{result}_{when/given_condition}`

```
should_return_none_when_key_does_not_exist
should_reject_expired_lease
should_fanout_to_all_subscribers_given_wildcard_route
```

**Never use** `test_*` prefix — it fails the meta-test.

### Step 4 — Write the test body

```rust
#[test]
fn should_{result}_{condition}() {
    // Arrange
    let subject = create_thing();

    // Act
    let result = subject.do_thing(input);

    // Assert
    assert_eq!(result, expected);
}
```

**Exact comment rules:**
- ✅ `// Arrange` — never `// Arrange: ...`, never `// Setup`
- ✅ `// Act` — never `// Act: ...`, never combined
- ✅ `// Assert` — never `// Assert: ...`, never combined
- ❌ Never `// Arrange & Act` or `// Act & Assert`

### Step 5 — Apply the single-behavior rule

Each `assert_eq!` that covers a **different input** = a different test.

```rust
// ❌ Three inputs → three tests needed
assert_eq!(svc.get(0).len(), 1);
assert_eq!(svc.get(1).len(), 2);
assert_eq!(svc.get(2).len(), 0);

// ✅ Three assertions for ONE operation → fine
assert_eq!(response.id,   original.id);
assert_eq!(response.name, original.name);
assert_eq!(response.ttl,  original.ttl);
```

### Step 6 — Handle multi-step operations

If verifying two sequential actions (e.g. upload then download), split into two tests.
The second test's **Arrange** section performs setup that would otherwise be a second Act.

### Step 7 — Validate

Run the meta-test to confirm compliance:

```powershell
cargo test test_guidelines_compliance
```

Or validate all tests at once:

```powershell
cargo fitz-tools validate-tests --summary
```

---

## Common Patterns

### Happy / Sad path pair

```rust
#[test]
fn should_return_value_when_key_exists() {
    // Arrange
    let mut db = store();
    db.put(b"k", b"v");

    // Act
    let result = db.get(b"k");

    // Assert
    assert_eq!(result, Some(b"v".as_ref()));
}

#[test]
fn should_return_none_when_key_does_not_exist() {
    // Arrange
    let db = store();

    // Act
    let result = db.get(b"missing");

    // Assert
    assert_eq!(result, None);
}
```

### Serialize / Deserialize (always split)

```rust
#[test]
fn should_serialize_to_valid_json() {
    // Arrange
    let manifest = Manifest::default();

    // Act
    let result = serde_json::to_string(&manifest);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_deserialize_to_equal_struct() {
    // Arrange
    let original = Manifest::default();
    let json = serde_json::to_string(&original).unwrap();

    // Act
    let restored: Manifest = serde_json::from_str(&json).unwrap();

    // Assert
    assert_eq!(restored, original);
}
```

### Table-driven (same operation, varying inputs)

```rust
#[test]
fn should_validate_range_bounds_for_all_cases() {
    // Arrange
    let cases = vec![
        (0, 10, true),
        (10, 0, false),
        (5, 5, false),
    ];

    // Act & Assert
    for (start, end, expected) in cases {
        assert_eq!(is_valid_range(start, end), expected, "({start}, {end})");
    }
}
```

> Table-driven is the **only** acceptable `// Act & Assert` combination — it is a single logical operation over parameterized inputs.

### Domain actor test

```rust
#[test]
fn should_return_begin_ok_given_valid_realm() {
    // Arrange
    let store = create_test_store();
    let mut actor = KvActor::new(store);

    // Act
    let response = actor.handle(KvMessage::Begin {
        realm: "acme".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
    });

    // Assert
    assert!(matches!(response, KvResponse::BeginOk { .. }));
}
```

---

## Reference Examples in Codebase

- Single-behavior tests: [src/index/range_tombstone.rs](../../src/index/range_tombstone.rs)
- Clean AAA structure: [src/manifest.rs](../../src/manifest.rs)
- Upload/download split pattern: [src/cloud/mock.rs](../../src/cloud/mock.rs)
