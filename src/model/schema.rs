use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The data type of a measurement field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Float,
    Integer,
    UInteger,
    Boolean,
    String,
}

/// Schema describing the expected field types for a measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementSchema {
    pub name: String,
    pub field_types: BTreeMap<String, FieldType>,
}
