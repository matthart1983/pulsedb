//! Segment metadata cache for fast query planning.
//!
//! Tracks which segments exist, their time ranges, and series keys
//! so the query planner can prune segments without reading them from disk.

use std::path::{Path, PathBuf};

/// Metadata for a single segment file.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub path: PathBuf,
    pub series_key: String,
    pub min_time: i64,
    pub max_time: i64,
    pub point_count: u64,
}

/// In-memory cache of segment metadata.
pub struct SegmentCache {
    segments: Vec<SegmentMeta>,
}

impl SegmentCache {
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Register a segment's metadata.
    pub fn add(&mut self, meta: SegmentMeta) {
        self.segments.push(meta);
    }

    /// Find segments that overlap the given time range for a specific series.
    pub fn segments_for_range(
        &self,
        series_key: &str,
        min_time: i64,
        max_time: i64,
    ) -> Vec<&SegmentMeta> {
        self.segments
            .iter()
            .filter(|s| {
                s.series_key == series_key && s.min_time <= max_time && s.max_time >= min_time
            })
            .collect()
    }

    /// Find all segments for a specific series key.
    pub fn segments_for_series(&self, series_key: &str) -> Vec<&SegmentMeta> {
        self.segments
            .iter()
            .filter(|s| s.series_key == series_key)
            .collect()
    }

    /// Total number of tracked segments.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Return all unique series keys that start with the given measurement prefix.
    pub fn series_keys_for_measurement(&self, measurement: &str) -> Vec<String> {
        let prefix = format!("{},", measurement);
        let mut keys: Vec<String> = self
            .segments
            .iter()
            .filter(|s| s.series_key == measurement || s.series_key.starts_with(&prefix))
            .map(|s| s.series_key.clone())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    /// Return all segment metadata entries.
    pub fn all_metas(&self) -> &[SegmentMeta] {
        &self.segments
    }

    /// Remove metadata for a segment by path.
    pub fn remove(&mut self, path: &Path) {
        self.segments.retain(|s| s.path != path);
    }
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(key: &str, min: i64, max: i64) -> SegmentMeta {
        SegmentMeta {
            path: PathBuf::from(format!("{}_{}_{}.seg", key, min, max)),
            series_key: key.to_string(),
            min_time: min,
            max_time: max,
            point_count: 100,
        }
    }

    #[test]
    fn add_and_query() {
        let mut cache = SegmentCache::new();
        cache.add(meta("cpu,host=a", 100, 200));
        cache.add(meta("cpu,host=a", 200, 300));
        cache.add(meta("cpu,host=b", 100, 200));

        assert_eq!(cache.len(), 3);

        let results = cache.segments_for_range("cpu,host=a", 150, 250);
        assert_eq!(results.len(), 2);

        let results = cache.segments_for_range("cpu,host=a", 250, 350);
        assert_eq!(results.len(), 1);

        let results = cache.segments_for_range("cpu,host=a", 400, 500);
        assert!(results.is_empty());
    }

    #[test]
    fn filter_by_series() {
        let mut cache = SegmentCache::new();
        cache.add(meta("cpu,host=a", 100, 200));
        cache.add(meta("cpu,host=b", 100, 200));

        let results = cache.segments_for_series("cpu,host=a");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].series_key, "cpu,host=a");
    }

    #[test]
    fn remove_segment() {
        let mut cache = SegmentCache::new();
        cache.add(meta("cpu,host=a", 100, 200));
        cache.add(meta("cpu,host=a", 200, 300));
        assert_eq!(cache.len(), 2);

        cache.remove(Path::new("cpu,host=a_100_200.seg"));
        assert_eq!(cache.len(), 1);
    }
}
