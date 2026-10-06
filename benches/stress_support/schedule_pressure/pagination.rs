pub fn advances(cursor: Option<&str>, next: Option<&str>, page_entries: usize) -> bool {
    page_entries > 0 && next.is_some_and(|value| !value.is_empty()) && next != cursor
}
