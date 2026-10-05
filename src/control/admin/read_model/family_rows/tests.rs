use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Row {
    id: usize,
    family: u64,
    wildcard: bool,
    identity_reads: Arc<AtomicUsize>,
}

impl FamilyRow for Row {
    fn family(&self) -> u64 {
        self.family
    }

    fn inventory_identity(&self) -> Option<ResourceIdentity> {
        self.identity_reads.fetch_add(1, Ordering::Relaxed);
        Some((
            self.family,
            "acme".into(),
            "jobs".into(),
            if self.wildcard { "*" } else { "orders" }.into(),
        ))
    }
}

fn rows(identity_reads: &Arc<AtomicUsize>) -> FamilyRows<Row> {
    (0..256)
        .map(|id| Row {
            id,
            family: if id < 128 { 1 } else { 2 },
            wildcard: !id.is_multiple_of(2),
            identity_reads: Arc::clone(identity_reads),
        })
        .collect::<Vec<_>>()
        .into()
}

#[test]
fn should_visit_each_secondary_index_row_once_during_bulk_family_removal() {
    // Arrange
    let identity_reads = Arc::new(AtomicUsize::new(0));
    let mut rows = rows(&identity_reads);

    // Act
    rows.retain(|row| row.family != 1);

    // Assert
    assert_eq!(rows.retention_index_visits, 256);
    assert_eq!(rows.len(), 128);
}

#[test]
fn should_remove_mixed_exact_and_wildcard_rows_from_all_indices() {
    // Arrange
    let identity_reads = Arc::new(AtomicUsize::new(0));
    let mut rows = rows(&identity_reads);
    let surviving_allocations = rows
        .families
        .get(&2)
        .unwrap()
        .iter()
        .map(Arc::as_ptr)
        .collect::<Vec<_>>();

    // Act
    rows.retain(|row| row.family != 1);

    // Assert
    assert_eq!(rows.families.len(), 1);
    assert_eq!(rows.resources.len(), 1);
    assert_eq!(rows.patterns.len(), 1);
    assert_eq!(rows.patterns.get(&2).unwrap().len(), 64);
    let exact = rows.resources.values().next().unwrap();
    assert_eq!(exact.len(), 64);
    assert!(exact.iter().all(|row| row.family == 2 && !row.wildcard));
    assert!(rows
        .patterns
        .get(&2)
        .unwrap()
        .iter()
        .all(|row| row.family == 2 && row.wildcard));
    assert_eq!(
        rows.families
            .get(&2)
            .unwrap()
            .iter()
            .map(Arc::as_ptr)
            .collect::<Vec<_>>(),
        surviving_allocations
    );
}

#[test]
fn should_remove_bulk_rows_without_rebuilding_inventory_identities() {
    // Arrange
    let identity_reads = Arc::new(AtomicUsize::new(0));
    let mut rows = rows(&identity_reads);
    let reads_before_removal = identity_reads.load(Ordering::Relaxed);

    // Act
    rows.retain(|row| row.id.is_multiple_of(3));

    // Assert
    assert_eq!(identity_reads.load(Ordering::Relaxed), reads_before_removal);
    assert!(rows.iter().all(|row| row.id.is_multiple_of(3)));
}

#[test]
fn should_remove_empty_secondary_indices_after_all_rows_are_removed() {
    // Arrange
    let identity_reads = Arc::new(AtomicUsize::new(0));
    let mut rows = rows(&identity_reads);

    // Act
    rows.retain(|_| false);

    // Assert
    assert_eq!(rows.families.len(), 0);
    assert_eq!(rows.resources.len(), 0);
    assert_eq!(rows.patterns.len(), 0);
}

#[test]
fn should_skip_secondary_index_cleanup_when_no_rows_are_removed() {
    // Arrange
    let identity_reads = Arc::new(AtomicUsize::new(0));
    let mut rows = rows(&identity_reads);

    // Act
    rows.retain(|_| true);

    // Assert
    assert_eq!(rows.retention_index_visits, 0);
    assert_eq!(rows.len(), 256);
}
