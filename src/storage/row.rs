use super::column::{ColumnType, ColumnValue};
use super::schema::TableSchema;
use crate::errors;
use bincode::{Decode, Encode};
use std;
use std::collections::HashMap;

#[derive(Encode, Decode, Debug)]
pub struct Row {
    pub inner: HashMap<String, ColumnValue>,
}

impl Row {
    pub fn get_column(&self, column: &String) -> Option<String> {
        Some(self.inner.get(column)?.to_string())
    }
}

pub fn build_row(
    schema: &TableSchema,
    columns: &[String],
    values: &[String],
) -> Result<Row, errors::Error> {
    if columns.len() != values.len() {
        return Err(errors::Error::Schema(
            "Columns and values length mismatch".into(),
        ));
    }

    let mut row = Row {
        inner: HashMap::new(),
    };

    for col_schema in &schema.columns {
        let value = columns
            .iter()
            .position(|c| c == &col_schema.name)
            .map(|idx| &values[idx])
            .or_else(|| col_schema.default.as_ref())
            .ok_or_else(|| {
                errors::Error::Schema(format!("Missing value for column: {}", col_schema.name))
            })?;

        let parsed_value = match &col_schema.type_ {
            ColumnType::INT => ColumnValue::Int(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid INT: {value}")))?,
            ),
            ColumnType::SMALLINT => ColumnValue::SmallInt(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid SMALLINT: {value}")))?,
            ),
            ColumnType::TINYINT => ColumnValue::TinyInt(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid TINYINT: {value}")))?,
            ),
            ColumnType::BIGINT => ColumnValue::BigInt(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid BIGINT: {value}")))?,
            ),
            ColumnType::FLOAT => ColumnValue::Float(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid FLOAT: {value}")))?,
            ),
            ColumnType::DOUBLE => ColumnValue::Double(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid DOUBLE: {value}")))?,
            ),
            ColumnType::VARCHAR(len) => {
                let mut v = vec![0u8; *len as usize];
                let bytes = value.as_bytes();
                v[..bytes.len().min(*len as usize)]
                    .copy_from_slice(&bytes[..bytes.len().min(*len as usize)]);
                ColumnValue::VarChar(v)
            }
            ColumnType::TEXT => ColumnValue::Text(value.clone().as_bytes().to_vec()),
            ColumnType::DATETIME => ColumnValue::DateTime(value.clone().as_bytes().to_vec()),
            ColumnType::TIMESTAMP => ColumnValue::Timestamp(value.clone().as_bytes().to_vec()),
            ColumnType::BOOLEAN => ColumnValue::Boolean(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid BOOLEAN: {value}")))?,
            ),
        };

        row.inner.insert(col_schema.name.clone(), parsed_value);
    }

    Ok(row)
}
