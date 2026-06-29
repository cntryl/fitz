use crate::api::http::Response;
use hyper::header::{self, HeaderMap};
use hyper::StatusCode;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::model::{
    AssetEntry, AssetFingerprint, AssetResolution, CachedAssetEntry, ResolvedAsset,
};
use super::paths::normalize_request_path;
use super::response::{if_none_match_matches, response_for_asset};
use super::INDEX_PATH;

pub(super) struct AssetServer {
    canonical_root: Option<PathBuf>,
    cache: RwLock<HashMap<String, CachedAssetEntry>>,
    #[cfg(test)]
    entry_builds: std::sync::atomic::AtomicUsize,
}

impl AssetServer {
    pub(super) fn new(root: &Path) -> Self {
        let canonical_root = fs::canonicalize(root).ok().filter(|path| path.is_dir());

        Self {
            canonical_root,
            cache: RwLock::new(HashMap::new()),
            #[cfg(test)]
            entry_builds: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(super) fn serve(&self, path: &str, headers: &HeaderMap) -> Response {
        let Some(resolved_asset) = self.resolve_path(path) else {
            return super::super::not_found();
        };

        let Some(asset) = self.asset_entry(&resolved_asset) else {
            return super::super::not_found();
        };

        let representation = asset.select_representation(headers);
        if if_none_match_matches(headers.get(header::IF_NONE_MATCH), &representation.etag) {
            return response_for_asset(StatusCode::NOT_MODIFIED, &asset, representation, true);
        }

        response_for_asset(StatusCode::OK, &asset, representation, false)
    }

    fn resolve_path(&self, path: &str) -> Option<ResolvedAsset> {
        let normalized_path = normalize_request_path(path)?;
        match self.resolve_file(normalized_path.as_str()) {
            AssetResolution::Found(resolved_asset) => return Some(resolved_asset),
            AssetResolution::Unsafe => return None,
            AssetResolution::Missing => {}
        }

        match self.resolve_file(INDEX_PATH) {
            AssetResolution::Found(resolved_asset) => Some(resolved_asset),
            AssetResolution::Missing | AssetResolution::Unsafe => None,
        }
    }

    fn resolve_file(&self, relative_path: &str) -> AssetResolution {
        let Some(canonical_root) = self.canonical_root.as_ref() else {
            return AssetResolution::Missing;
        };
        let absolute_path = match fs::canonicalize(canonical_root.join(relative_path)) {
            Ok(path) => path,
            Err(_) => return AssetResolution::Missing,
        };

        if !absolute_path.starts_with(canonical_root) {
            return AssetResolution::Unsafe;
        }

        let metadata = match fs::metadata(&absolute_path) {
            Ok(metadata) => metadata,
            Err(_) => return AssetResolution::Missing,
        };
        if !metadata.is_file() {
            return AssetResolution::Missing;
        }

        AssetResolution::Found(ResolvedAsset {
            relative_path: relative_path.to_string(),
            absolute_path,
            fingerprint: AssetFingerprint::from_metadata(&metadata),
        })
    }

    fn asset_entry(&self, resolved_asset: &ResolvedAsset) -> Option<Arc<AssetEntry>> {
        if let Some(cached_asset) = self.cache.read().get(resolved_asset.relative_path.as_str()) {
            if cached_asset.fingerprint == resolved_asset.fingerprint {
                return Some(Arc::clone(&cached_asset.entry));
            }
        }

        let bytes = fs::read(&resolved_asset.absolute_path).ok()?;
        let metadata = fs::metadata(&resolved_asset.absolute_path).ok()?;
        if !metadata.is_file() {
            return None;
        }
        let fingerprint = AssetFingerprint::from_metadata(&metadata);

        #[cfg(test)]
        self.entry_builds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let entry = Arc::new(AssetEntry::new(&resolved_asset.relative_path, bytes));
        self.cache.write().insert(
            resolved_asset.relative_path.clone(),
            CachedAssetEntry {
                fingerprint,
                entry: Arc::clone(&entry),
            },
        );

        Some(entry)
    }

    #[cfg(test)]
    pub(super) fn reset_entry_build_count(&self) {
        self.entry_builds
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn entry_build_count(&self) -> usize {
        self.entry_builds.load(std::sync::atomic::Ordering::Relaxed)
    }
}
