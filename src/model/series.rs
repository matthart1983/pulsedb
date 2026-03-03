use serde::{Deserialize, Serialize};

use super::point::Tags;

/// Compact numeric identifier for a unique series.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SeriesId(pub u64);

/// Uniquely identifies a series by its measurement name and tag set.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesKey {
    pub measurement: String,
    pub tags: Tags,
}

impl SeriesKey {
    /// Returns the canonical key string in the form `measurement,tag1=val1,tag2=val2`.
    pub fn to_key_string(&self) -> String {
        let mut key = self.measurement.clone();
        for (k, v) in &self.tags {
            key.push(',');
            key.push_str(k);
            key.push('=');
            key.push_str(v);
        }
        key
    }
}
