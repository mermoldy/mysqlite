//! # Row Management
use super::column::{ColumnType, ColumnValue};
use super::schema::TableSchema;
use crate::errors::Error;
use bincode::{Decode, Encode};
use std::collections::HashMap;

/// Represents a database row with flexible column storage.
///
/// The `Row` struct uses a `HashMap` to store column values, allowing
/// for dynamic and schema-based row representation.
#[derive(Encode, Decode, Debug)]
pub struct Row {
    /// Internal storage of column values
    pub inner: HashMap<String, ColumnValue>,
}

impl Row {
    /// Retrieves a column value as a string representation.
    ///
    /// # Arguments
    /// * `column` - The name of the column to retrieve
    ///
    /// # Returns
    /// An `Option` containing the string representation of the column value
    pub fn get_column(&self, column: &str) -> Option<String> {
        Some(self.inner.get(column)?.to_string())
    }

    /// Extracts the primary key value from the row based on the table schema.
    ///
    /// # Arguments
    /// * `schema` - Reference to the table schema
    ///
    /// # Returns
    /// The primary key as a `u32`, or an error if:
    /// - No primary key column is defined
    /// - Primary key column is missing from the row
    /// - Primary key is not of integer type
    pub fn get_id(&self, schema: &TableSchema) -> Result<u32, Error> {
        schema
            .columns
            .iter()
            .find(|col_schema| col_schema.is_primary)
            .ok_or_else(|| Error::Schema("No primary key column defined".into()))
            .and_then(|primary_col| {
                self.inner
                    .get(&primary_col.name)
                    .ok_or_else(|| Error::Schema("Primary key column missing in the row".into()))
                    .and_then(|value| match value {
                        ColumnValue::Int(v) => Ok(*v as u32),
                        _ => Err(Error::Schema("Invalid primary key type".into())),
                    })
            })
    }

    /// Creates a new row from a set of columns and values.
    ///
    /// # Arguments
    /// * `schema` - Reference to the table schema
    /// * `columns` - List of column names
    /// * `values` - Corresponding list of column values
    ///
    /// # Returns
    /// A new `Row` instance, or an error if validation fails
    pub fn from_columns(
        schema: &TableSchema,
        columns: &[String],
        values: &[String],
    ) -> Result<Self, Error> {
        build_row(schema, columns, values)
    }

    /// Validates the row against the given schema.
    ///
    /// # Arguments
    /// * `schema` - Reference to the table schema
    ///
    /// # Returns
    /// `true` if the row is valid according to the schema, `false` otherwise
    pub fn validate(&self, schema: &TableSchema) -> bool {
        schema.columns.iter().all(|col_schema| {
            self.inner.get(&col_schema.name).map_or_else(
                || col_schema.default.is_some(), // Column missing but has default
                |value| {
                    // Check if value matches column type
                    match (&col_schema.type_, value) {
                        (ColumnType::INT, ColumnValue::Int(_)) => true,
                        (ColumnType::SMALLINT, ColumnValue::SmallInt(_)) => true,
                        (ColumnType::TINYINT, ColumnValue::TinyInt(_)) => true,
                        (ColumnType::BIGINT, ColumnValue::BigInt(_)) => true,
                        (ColumnType::FLOAT, ColumnValue::Float(_)) => true,
                        (ColumnType::DOUBLE, ColumnValue::Double(_)) => true,
                        (ColumnType::VARCHAR(_), ColumnValue::VarChar(_)) => true,
                        (ColumnType::TEXT, ColumnValue::Text(_)) => true,
                        (ColumnType::DATETIME, ColumnValue::DateTime(_)) => true,
                        (ColumnType::TIMESTAMP, ColumnValue::Timestamp(_)) => true,
                        (ColumnType::BOOLEAN, ColumnValue::Boolean(_)) => true,
                        _ => false,
                    }
                },
            )
        })
    }
}

/// Builds a row from given schema, columns, and values.
///
/// # Arguments
/// * `schema` - Reference to the table schema
/// * `columns` - List of column names
/// * `values` - Corresponding list of column values
///
/// # Returns
/// A new `Row` instance, or an error if:
/// - Column and value lists have different lengths
/// - Any column is missing a value
/// - Any value cannot be parsed according to column type
pub fn build_row(
    schema: &TableSchema,
    columns: &[String],
    values: &[String],
) -> Result<Row, Error> {
    // Validate input lengths
    if columns.len() != values.len() {
        return Err(Error::Schema("Columns and values length mismatch".into()));
    }

    let mut row = Row {
        inner: HashMap::new(),
    };

    for col_schema in &schema.columns {
        // Find value for column, prioritizing provided values over defaults
        let value = columns
            .iter()
            .position(|c| c == &col_schema.name)
            .map(|idx| &values[idx])
            .or_else(|| col_schema.default.as_ref())
            .ok_or_else(|| {
                Error::Schema(format!("Missing value for column: {}", col_schema.name))
            })?;

        // Parse and validate column value
        let parsed_value = match &col_schema.type_ {
            ColumnType::INT => ColumnValue::Int(
                value
                    .parse()
                    .map_err(|_| Error::Schema(format!("Invalid INT: {value}")))?,
            ),
            ColumnType::SMALLINT => ColumnValue::SmallInt(
                value
                    .parse()
                    .map_err(|_| Error::Schema(format!("Invalid SMALLINT: {value}")))?,
            ),
            ColumnType::TINYINT => ColumnValue::TinyInt(
                value
                    .parse()
                    .map_err(|_| Error::Schema(format!("Invalid TINYINT: {value}")))?,
            ),
            ColumnType::BIGINT => ColumnValue::BigInt(
                value
                    .parse()
                    .map_err(|_| Error::Schema(format!("Invalid BIGINT: {value}")))?,
            ),
            ColumnType::FLOAT => ColumnValue::Float(
                value
                    .parse()
                    .map_err(|_| Error::Schema(format!("Invalid FLOAT: {value}")))?,
            ),
            ColumnType::DOUBLE => ColumnValue::Double(
                value
                    .parse()
                    .map_err(|_| Error::Schema(format!("Invalid DOUBLE: {value}")))?,
            ),
            ColumnType::VARCHAR(len) => {
                let mut v = vec![0u8; *len as usize];
                let bytes = value.as_bytes();
                v[..bytes.len().min(*len as usize)]
                    .copy_from_slice(&bytes[..bytes.len().min(*len as usize)]);
                ColumnValue::VarChar(v)
            }
            ColumnType::TEXT => ColumnValue::Text(value.clone().into_bytes()),
            ColumnType::DATETIME => ColumnValue::DateTime(value.clone().into_bytes()),
            ColumnType::TIMESTAMP => ColumnValue::Timestamp(value.clone().into_bytes()),
            ColumnType::BOOLEAN => ColumnValue::Boolean(
                value
                    .parse()
                    .map_err(|_| Error::Schema(format!("Invalid BOOLEAN: {value}")))?,
            ),
        };

        row.inner.insert(col_schema.name.clone(), parsed_value);
    }

    Ok(row)
}

// Optional: Implement additional traits for better usability
impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let formatted_columns: Vec<String> = self
            .inner
            .iter()
            .map(|(name, value)| format!("{}: {}", name, value.to_string()))
            .collect();

        write!(f, "Row({})", formatted_columns.join(", "))
    }
}
