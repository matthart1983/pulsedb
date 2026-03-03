use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Type alias for tag sets. BTreeMap ensures tags are always sorted by key.
pub type Tags = BTreeMap<String, String>;

/// Represents a single field value within a data point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    UInteger(u64),
    Boolean(bool),
    String(String),
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Float(v) => write!(f, "{v}"),
            FieldValue::Integer(v) => write!(f, "{v}i"),
            FieldValue::UInteger(v) => write!(f, "{v}u"),
            FieldValue::Boolean(v) => write!(f, "{v}"),
            FieldValue::String(v) => write!(f, "\"{v}\""),
        }
    }
}

/// A single time-series data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    /// The measurement name (analogous to a table name).
    pub measurement: String,
    /// Tag set for indexing and grouping.
    pub tags: Tags,
    /// Field set containing the actual values.
    pub fields: BTreeMap<String, FieldValue>,
    /// Timestamp in nanoseconds since the Unix epoch.
    pub timestamp: i64,
}

impl DataPoint {
    /// Returns the series key in the form `measurement,tag1=val1,tag2=val2`.
    ///
    /// Tags are naturally sorted because `Tags` is a `BTreeMap`.
    pub fn series_key(&self) -> String {
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
