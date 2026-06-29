use super::INDEX_PATH;

pub(super) fn normalize_request_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() || trimmed == "/" {
        return Some(INDEX_PATH.to_string());
    }

    let mut normalized = Vec::new();
    for component in trimmed.split('/') {
        if component == "." || component == ".." || component.contains('\\') {
            return None;
        }
        if component.is_empty() {
            continue;
        }
        normalized.push(component);
    }

    if normalized.is_empty() {
        Some(INDEX_PATH.to_string())
    } else {
        Some(normalized.join("/"))
    }
}
