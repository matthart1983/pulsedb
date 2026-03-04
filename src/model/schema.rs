use std::collections::BTreeMap;

use anyhow::{bail, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::model::{DataPoint, FieldValue};

/// The data type of a measurement field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Float,
    Integer,
    UInteger,
    Boolean,
    String,
}

impl FieldType {
    pub fn from_field_value(v: &FieldValue) -> Self {
        match v {
            FieldValue::Float(_) => FieldType::Float,
            FieldValue::Integer(_) => FieldType::Integer,
            FieldValue::UInteger(_) => FieldType::UInteger,
            FieldValue::Boolean(_) => FieldType::Boolean,
            FieldValue::String(_) => FieldType::String,
        }
    }
}

/// Schema describing the expected field types for a measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementSchema {
    pub name: String,
    pub field_types: BTreeMap<String, FieldType>,
}

/// Registry that tracks field types per measurement and rejects type mismatches.
pub struct SchemaRegistry {
    schemas: RwLock<BTreeMap<String, BTreeMap<String, FieldType>>>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(BTreeMap::new()),
        }
    }

    /// Validate a data point's fields against the schema.
    /// On first write, registers the field types.
    /// On subsequent writes, rejects type mismatches.
    pub fn validate(&self, point: &DataPoint) -> Result<()> {
        let mut schemas = self.schemas.write();
        let schema = schemas
            .entry(point.measurement.clone())
            .or_insert_with(BTreeMap::new);

        for (field_name, field_value) in &point.fields {
            let actual_type = FieldType::from_field_value(field_value);
            if let Some(&expected_type) = schema.get(field_name) {
                if expected_type != actual_type {
                    bail!(
                        "schema conflict: field '{}' in measurement '{}' has type {:?} but got {:?}",
                        field_name,
                        point.measurement,
                        expected_type,
                        actual_type,
                    );
                }
            } else {
                schema.insert(field_name.clone(), actual_type);
            }
        }

        Ok(())
    }

    pub fn field_names(&self, measurement: &str) -> Vec<String> {
        let schemas = self.schemas.read();
        match schemas.get(measurement) {
            Some(fields) => fields.keys().cloned().collect(),
            None => Vec::new(),
        }
    }

    pub fn measurement_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::model::{DataPoint, FieldValue};

    fn make_point(measurement: &str, fields: Vec<(&str, FieldValue)>) -> DataPoint {
        DataPoint {
            measurement: measurement.into(),
            tags: BTreeMap::new(),
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            timestamp: 1_000_000_000,
        }
    }

    #[test]
    fn first_write_registers_schema() {
        let registry = SchemaRegistry::new();
        let point = make_point("cpu", vec![("usage", FieldValue::Float(42.0))]);
        assert!(registry.validate(&point).is_ok());

        let schemas = registry.schemas.read();
        assert_eq!(schemas["cpu"]["usage"], FieldType::Float);
    }

    #[test]
    fn same_types_succeed() {
        let registry = SchemaRegistry::new();
        let p1 = make_point("cpu", vec![("usage", FieldValue::Float(42.0))]);
        let p2 = make_point("cpu", vec![("usage", FieldValue::Float(99.0))]);
        assert!(registry.validate(&p1).is_ok());
        assert!(registry.validate(&p2).is_ok());
    }

    #[test]
    fn mismatched_type_is_rejected() {
        let registry = SchemaRegistry::new();
        let p1 = make_point("cpu", vec![("usage", FieldValue::Float(42.0))]);
        let p2 = make_point("cpu", vec![("usage", FieldValue::Integer(42))]);
        assert!(registry.validate(&p1).is_ok());
        let err = registry.validate(&p2).unwrap_err();
        assert!(err.to_string().contains("schema conflict"));
        assert!(err.to_string().contains("usage"));
    }

    #[test]
    fn different_measurements_have_independent_schemas() {
        let registry = SchemaRegistry::new();
        let p1 = make_point("cpu", vec![("value", FieldValue::Float(1.0))]);
        let p2 = make_point("mem", vec![("value", FieldValue::Integer(1024))]);
        assert!(registry.validate(&p1).is_ok());
        assert!(registry.validate(&p2).is_ok());
    }
}
