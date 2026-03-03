//! Series key → numeric ID mapping.
//!
//! Every unique combination of measurement + tags gets assigned a compact
//! `SeriesId` for efficient internal referencing.

use std::collections::HashMap;

use crate::model::SeriesId;

/// Maps series key strings to compact numeric IDs.
pub struct SeriesIndex {
    map: HashMap<String, SeriesId>,
    next_id: u64,
}

impl SeriesIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            next_id: 1,
        }
    }

    /// Get the ID for a series key, creating a new one if it doesn't exist.
    pub fn get_or_create(&mut self, key: &str) -> SeriesId {
        if let Some(&id) = self.map.get(key) {
            return id;
        }
        let id = SeriesId(self.next_id);
        self.next_id += 1;
        self.map.insert(key.to_string(), id);
        id
    }

    /// Look up the ID for a series key without creating it.
    pub fn get(&self, key: &str) -> Option<SeriesId> {
        self.map.get(key).copied()
    }

    /// Total number of registered series.
    pub fn series_count(&self) -> usize {
        self.map.len()
    }
}

impl Default for SeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_or_create_new() {
        let mut idx = SeriesIndex::new();
        let id = idx.get_or_create("cpu,host=a");
        assert_eq!(id, SeriesId(1));
        assert_eq!(idx.series_count(), 1);
    }

    #[test]
    fn get_or_create_existing() {
        let mut idx = SeriesIndex::new();
        let id1 = idx.get_or_create("cpu,host=a");
        let id2 = idx.get_or_create("cpu,host=a");
        assert_eq!(id1, id2);
        assert_eq!(idx.series_count(), 1);
    }

    #[test]
    fn different_keys_get_different_ids() {
        let mut idx = SeriesIndex::new();
        let id1 = idx.get_or_create("cpu,host=a");
        let id2 = idx.get_or_create("cpu,host=b");
        assert_ne!(id1, id2);
        assert_eq!(idx.series_count(), 2);
    }

    #[test]
    fn get_missing_returns_none() {
        let idx = SeriesIndex::new();
        assert_eq!(idx.get("cpu,host=a"), None);
    }

    #[test]
    fn get_after_create() {
        let mut idx = SeriesIndex::new();
        let id = idx.get_or_create("cpu,host=a");
        assert_eq!(idx.get("cpu,host=a"), Some(id));
    }

    #[test]
    fn ids_are_sequential() {
        let mut idx = SeriesIndex::new();
        let id1 = idx.get_or_create("a");
        let id2 = idx.get_or_create("b");
        let id3 = idx.get_or_create("c");
        assert_eq!(id1, SeriesId(1));
        assert_eq!(id2, SeriesId(2));
        assert_eq!(id3, SeriesId(3));
    }
}
