use crate::errors;
use bincode;
use bincode::{config, Decode, Encode};
use std::collections::HashMap;
use std::usize;

#[derive(Debug, Clone)]
pub enum DataType {
    INT,          // i64, equivalent to SQL's BIGINT
    SMALLINT,     // i16
    TINYINT,      // i8
    BIGINT,       // i128
    FLOAT,        // 32-bit floating point
    DOUBLE,       // 64-bit floating point
    VARCHAR(u16), // Variable-length string with max length
    TEXT,         // Unbounded string
    DATETIME,     // Date and time combined
    TIMESTAMP,    // Date and time with timezone information
    BOOLEAN,      // True/False value
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub default: Option<String>,
    pub primary: bool,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
    pub version: u32,
    pub row_size: u32,
}

#[derive(Encode, Decode, Debug)]
pub enum Value {
    Int(i64),
    SmallInt(i16),
    TinyInt(i8),
    BigInt(i128),
    Float(f32),
    Double(f64),
    Str(Vec<u8>),
    Text(String),
    DateTime(String),
    Timestamp(String),
    Boolean(bool),
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Int(v) => v.to_string(),
            Value::SmallInt(v) => v.to_string(),
            Value::TinyInt(v) => v.to_string(),
            Value::BigInt(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::Str(v) => {
                let trimmed = v.split(|&b| b == 0).next().unwrap_or(&[]);
                String::from_utf8_lossy(trimmed).to_string()
            }
            Value::Text(v) => v.clone(),
            Value::DateTime(v) => v.clone(),
            Value::Timestamp(v) => v.clone(),
            Value::Boolean(v) => v.to_string(),
        }
    }
}

#[derive(Encode, Decode, Debug)]
pub struct Row {
    inner: HashMap<String, Value>,
}

impl Row {
    pub fn get_column(&self, column: &String) -> Option<String> {
        Some(self.inner.get(column)?.to_string())
    }
}

pub fn serialize_row(schema: &TableSchema, row: Row) -> Result<Vec<u8>, errors::Error> {
    let schema: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let config = config::standard();

    let values: Vec<&Value> = schema.into_iter().map(|col| &row.inner[&col]).collect();
    match bincode::encode_to_vec(&values, config) {
        Ok(r) => Ok(r),
        Err(e) => Err(errors::Error::Schema(format!(
            "Failed to serialize row. {}",
            e
        ))),
    }
}

pub fn deserialize_row(schema: &TableSchema, bytes: &[u8]) -> Result<Row, errors::Error> {
    let schema: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let config = config::standard();

    let (decoded, _): (Vec<Value>, usize) = bincode::decode_from_slice(&bytes, config)
        .map_err(|e| errors::Error::Schema(format!("Failed to deserialize row: {}", e)))?;

    Ok(Row {
        inner: schema.iter().cloned().zip(decoded).collect(),
    })
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

    let schema_map: HashMap<_, _> = schema.columns.iter().map(|col| (&col.name, col)).collect();
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

        let parsed_value = match &col_schema.data_type {
            DataType::INT => Value::Int(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid INT: {value}")))?,
            ),
            DataType::SMALLINT => Value::SmallInt(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid SMALLINT: {value}")))?,
            ),
            DataType::TINYINT => Value::TinyInt(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid TINYINT: {value}")))?,
            ),
            DataType::BIGINT => Value::BigInt(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid BIGINT: {value}")))?,
            ),
            DataType::FLOAT => Value::Float(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid FLOAT: {value}")))?,
            ),
            DataType::DOUBLE => Value::Double(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid DOUBLE: {value}")))?,
            ),
            DataType::VARCHAR(len) => {
                let mut v = vec![0u8; *len as usize];
                let bytes = value.as_bytes();
                v[..bytes.len().min(*len as usize)]
                    .copy_from_slice(&bytes[..bytes.len().min(*len as usize)]);
                Value::Str(v)
            }
            DataType::TEXT => Value::Text(value.clone()),
            DataType::DATETIME => Value::DateTime(value.clone()),
            DataType::TIMESTAMP => Value::Timestamp(value.clone()),
            DataType::BOOLEAN => Value::Boolean(
                value
                    .parse()
                    .map_err(|_| errors::Error::Schema(format!("Invalid BOOLEAN: {value}")))?,
            ),
        };

        row.inner.insert(col_schema.name.clone(), parsed_value);
    }

    Ok(row)
}

pub fn slice_to_array<const N: usize>(slice: &[u8]) -> [u8; N] {
    let mut arr = [0u8; N];
    let len = slice.len().min(N);
    arr[..len].copy_from_slice(&slice[..len]);
    arr
}
