use std::collections::BTreeMap;

use crate::model::{DataPoint, FieldValue};

/// In-memory buffer for recently written data points.
///
/// Data is organised as:
///   series_key → timestamp → field_name → FieldValue
pub struct MemTable {
    data: BTreeMap<String, BTreeMap<i64, BTreeMap<String, FieldValue>>>,
    size_bytes: usize,
    point_count: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
            point_count: 0,
        }
    }

    /// Insert a single data point into the memtable.
    pub fn insert(&mut self, point: DataPoint) {
        let key = point.series_key();

        // Rough size estimate: series key + timestamp + field names + values.
        let mut entry_size = key.len() + std::mem::size_of::<i64>();
        for (name, value) in &point.fields {
            entry_size += name.len() + Self::field_value_size(value);
        }

        let ts_map = self.data.entry(key).or_default();
        let field_map = ts_map.entry(point.timestamp).or_default();

        for (name, value) in point.fields {
            field_map.insert(name, value);
        }

        self.size_bytes += entry_size;
        self.point_count += 1;
    }

    /// Approximate size of all data held in this memtable.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Number of data points inserted.
    pub fn point_count(&self) -> usize {
        self.point_count
    }

    pub fn is_empty(&self) -> bool {
        self.point_count == 0
    }

    /// Iterate over each series and its timestamp→fields map.
    pub fn iter_series(
        &self,
    ) -> impl Iterator<Item = (&String, &BTreeMap<i64, BTreeMap<String, FieldValue>>)> {
        self.data.iter()
    }

    /// Consume this memtable and produce an immutable snapshot.
    pub fn freeze(self) -> FrozenMemTable {
        FrozenMemTable {
            data: self.data,
            size_bytes: self.size_bytes,
            point_count: self.point_count,
        }
    }

    fn field_value_size(v: &FieldValue) -> usize {
        match v {
            FieldValue::Float(_) => std::mem::size_of::<f64>(),
            FieldValue::Integer(_) => std::mem::size_of::<i64>(),
            FieldValue::UInteger(_) => std::mem::size_of::<u64>(),
            FieldValue::Boolean(_) => std::mem::size_of::<bool>(),
            FieldValue::String(s) => s.len(),
        }
    }
}

/// An immutable snapshot of a [`MemTable`] pending flush to persistent storage.
pub struct FrozenMemTable {
    data: BTreeMap<String, BTreeMap<i64, BTreeMap<String, FieldValue>>>,
    size_bytes: usize,
    point_count: usize,
}

impl FrozenMemTable {
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn point_count(&self) -> usize {
        self.point_count
    }

    pub fn is_empty(&self) -> bool {
        self.point_count == 0
    }

    pub fn iter_series(
        &self,
    ) -> impl Iterator<Item = (&String, &BTreeMap<i64, BTreeMap<String, FieldValue>>)> {
        self.data.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_point(measurement: &str, host: &str, ts: i64, value: f64) -> DataPoint {
        DataPoint {
            measurement: measurement.into(),
            tags: BTreeMap::from([("host".into(), host.into())]),
            fields: BTreeMap::from([("usage".into(), FieldValue::Float(value))]),
            timestamp: ts,
        }
    }

    #[test]
    fn insert_and_count() {
        let mut mt = MemTable::new();
        assert!(mt.is_empty());

        mt.insert(make_point("cpu", "a", 1, 10.0));
        mt.insert(make_point("cpu", "a", 2, 20.0));
        mt.insert(make_point("cpu", "b", 1, 30.0));

        assert_eq!(mt.point_count(), 3);
        assert!(!mt.is_empty());
        assert!(mt.size_bytes() > 0);
    }

    #[test]
    fn iter_series_groups_by_key() {
        let mut mt = MemTable::new();
        mt.insert(make_point("cpu", "a", 1, 10.0));
        mt.insert(make_point("cpu", "a", 2, 20.0));
        mt.insert(make_point("mem", "a", 1, 50.0));

        let keys: Vec<&String> = mt.iter_series().map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn freeze_preserves_data() {
        let mut mt = MemTable::new();
        mt.insert(make_point("cpu", "a", 1, 10.0));
        mt.insert(make_point("cpu", "a", 2, 20.0));

        let size = mt.size_bytes();
        let count = mt.point_count();

        let frozen = mt.freeze();
        assert_eq!(frozen.point_count(), count);
        assert_eq!(frozen.size_bytes(), size);
        assert!(!frozen.is_empty());

        let series_count = frozen.iter_series().count();
        assert_eq!(series_count, 1);
    }

    #[test]
    fn overwrite_same_timestamp() {
        let mut mt = MemTable::new();
        mt.insert(make_point("cpu", "a", 1, 10.0));
        mt.insert(make_point("cpu", "a", 1, 99.0));

        // Both insertions count as separate points.
        assert_eq!(mt.point_count(), 2);

        // But the field value should be the latest one.
        let (_, ts_map) = mt.iter_series().next().unwrap();
        let fields = ts_map.get(&1).unwrap();
        assert_eq!(fields.get("usage"), Some(&FieldValue::Float(99.0)));
    }
}
