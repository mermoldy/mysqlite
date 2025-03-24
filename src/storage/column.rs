use bincode::{Decode, Encode};
use std::fmt;

#[derive(Debug, Clone)]
pub enum ColumnType {
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

#[derive(Encode, Decode, Debug)]
pub enum ColumnValue {
    Int(i64),
    SmallInt(i16),
    TinyInt(i8),
    BigInt(i128),
    Float(f32),
    Double(f64),
    VarChar(Vec<u8>),
    Text(Vec<u8>),
    DateTime(Vec<u8>),
    Timestamp(Vec<u8>),
    Boolean(bool),
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnType::INT => write!(f, "INT"),
            ColumnType::SMALLINT => write!(f, "SMALLINT"),
            ColumnType::TINYINT => write!(f, "TINYINT"),
            ColumnType::BIGINT => write!(f, "BIGINT"),
            ColumnType::FLOAT => write!(f, "FLOAT"),
            ColumnType::DOUBLE => write!(f, "DOUBLE"),
            ColumnType::VARCHAR(max_length) => write!(f, "VARCHAR({})", max_length),
            ColumnType::TEXT => write!(f, "TEXT"),
            ColumnType::DATETIME => write!(f, "DATETIME"),
            ColumnType::TIMESTAMP => write!(f, "TIMESTAMP"),
            ColumnType::BOOLEAN => write!(f, "BOOLEAN"),
        }
    }
}

impl ToString for ColumnValue {
    fn to_string(&self) -> String {
        match self {
            ColumnValue::Int(v) => v.to_string(),
            ColumnValue::SmallInt(v) => v.to_string(),
            ColumnValue::TinyInt(v) => v.to_string(),
            ColumnValue::BigInt(v) => v.to_string(),
            ColumnValue::Float(v) => v.to_string(),
            ColumnValue::Double(v) => v.to_string(),
            ColumnValue::VarChar(v) => {
                let trimmed = v.split(|&b| b == 0).next().unwrap_or(&[]);
                String::from_utf8_lossy(trimmed).to_string()
            }
            ColumnValue::Text(v) => {
                let trimmed = v.split(|&b| b == 0).next().unwrap_or(&[]);
                String::from_utf8_lossy(trimmed).to_string()
            }
            ColumnValue::DateTime(v) => {
                let trimmed = v.split(|&b| b == 0).next().unwrap_or(&[]);
                String::from_utf8_lossy(trimmed).to_string()
            }
            ColumnValue::Timestamp(v) => {
                let trimmed = v.split(|&b| b == 0).next().unwrap_or(&[]);
                String::from_utf8_lossy(trimmed).to_string()
            }
            ColumnValue::Boolean(v) => v.to_string(),
        }
    }
}
