use super::column::{ColumnType, ColumnValue};
use super::row::Row;
use super::schema::TableSchema;
use crate::errors;
use bincode::{config, decode_from_slice, encode_into_slice};
use std;
use std::collections::HashMap;

const TEXT_SIZE: usize = 65_535;
const DATETIME_SIZE: usize = 8;
const TIMESTAMP_SIZE: usize = 8;
const VARCHAR_MAXSIZE: usize = 2048;

impl ColumnType {
    pub fn fixed_size(&self) -> usize {
        match self {
            ColumnType::INT => 8,                              // i64: 8 bytes
            ColumnType::SMALLINT => 2,                         // i16: 2 bytes
            ColumnType::TINYINT => 1,                          // i8: 1 byte
            ColumnType::BIGINT => 16,                          // i128: 16 bytes
            ColumnType::FLOAT => 4,                            // f32: 4 bytes
            ColumnType::DOUBLE => 8,                           // f64: 8 bytes
            ColumnType::TEXT => TEXT_SIZE, // Fixed size for TEXT (e.g., 32 bytes)
            ColumnType::DATETIME => DATETIME_SIZE, // Fixed size for DATETIME (e.g., 12 bytes)
            ColumnType::TIMESTAMP => TIMESTAMP_SIZE, // Fixed size for TIMESTAMP (e.g., 16 bytes)
            ColumnType::VARCHAR(max_len) => *max_len as usize, // Max length specified
            ColumnType::BOOLEAN => 1,      // bool: 1 byte
        }
    }

    pub fn from_fixed_bytes(&self, buffer: &[u8]) -> Result<ColumnValue, errors::Error> {
        let c = config::standard();

        match self {
            ColumnType::INT => {
                let (val, _) = decode_from_slice::<i64, _>(buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to decode INT. {}", e)))?;
                Ok(ColumnValue::Int(val))
            }
            ColumnType::SMALLINT => {
                let (val, _) = decode_from_slice::<i16, _>(buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to decode SMALLINT. {}", e)))?;
                Ok(ColumnValue::SmallInt(val))
            }
            ColumnType::TINYINT => {
                let (val, _) = decode_from_slice::<i8, _>(buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to decode TINYINT. {}", e)))?;
                Ok(ColumnValue::TinyInt(val))
            }
            ColumnType::BIGINT => {
                let (val, _) = decode_from_slice::<i128, _>(buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to decode BIGINT. {}", e)))?;
                Ok(ColumnValue::BigInt(val))
            }
            ColumnType::FLOAT => {
                let (val, _) = decode_from_slice::<f32, _>(buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to decode FLOAT. {}", e)))?;
                Ok(ColumnValue::Float(val))
            }
            ColumnType::DOUBLE => {
                let (val, _) = decode_from_slice::<f64, _>(buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to decode DOUBLE. {}", e)))?;
                Ok(ColumnValue::Double(val))
            }
            ColumnType::TEXT => {
                let text = String::from_utf8_lossy(&buffer)
                    .trim_end_matches('\0')
                    .to_string()
                    .as_bytes()
                    .to_vec();
                Ok(ColumnValue::Text(text))
            }
            ColumnType::DATETIME => Ok(ColumnValue::DateTime(buffer.to_vec())),
            ColumnType::TIMESTAMP => Ok(ColumnValue::Timestamp(buffer.to_vec())),
            ColumnType::VARCHAR(_) => {
                let text = String::from_utf8_lossy(&buffer)
                    .trim_end_matches('\0')
                    .to_string()
                    .as_bytes()
                    .to_vec();
                Ok(ColumnValue::VarChar(text))
            }
            ColumnType::BOOLEAN => {
                let (val, _) = decode_from_slice::<bool, _>(buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to decode BOOLEAN. {}", e)))?;
                Ok(ColumnValue::Boolean(val))
            }
        }
    }
}

impl ColumnValue {
    pub fn to_fixed_bytes(&self, max_size: usize) -> Result<Vec<u8>, errors::Error> {
        let mut buffer = vec![0u8; max_size];
        let c = config::standard();

        match self {
            ColumnValue::Int(v) => encode_into_slice(v, &mut buffer, c)
                .map_err(|e| err!(Encoding, format!("Failed to encode INT. {}", e)))?,
            ColumnValue::SmallInt(v) => encode_into_slice(v, &mut buffer, c)
                .map_err(|e| err!(Encoding, format!("Failed to encode SMALLINT. {}", e)))?,
            ColumnValue::TinyInt(v) => encode_into_slice(v, &mut buffer, c)
                .map_err(|e| err!(Encoding, format!("Failed to encode TINYINT. {}", e)))?,
            ColumnValue::BigInt(v) => encode_into_slice(v, &mut buffer, c)
                .map_err(|e| err!(Encoding, format!("Failed to encode BIGINT. {}", e)))?,
            ColumnValue::Float(v) => encode_into_slice(v, &mut buffer, c)
                .map_err(|e| err!(Encoding, format!("Failed to encode FLOAT. {}", e)))?,
            ColumnValue::Double(v) => encode_into_slice(v, &mut buffer, c)
                .map_err(|e| err!(Encoding, format!("Failed to encode DOUBLE. {}", e)))?,
            ColumnValue::Text(s) => {
                if s.len() > TEXT_SIZE {
                    return Err(errors::Error::Schema(format!(
                        "Text exceeds max length: {} > {}",
                        s.len(),
                        TEXT_SIZE
                    )));
                }
                buffer[..TEXT_SIZE].copy_from_slice(s);
                TEXT_SIZE
            }
            ColumnValue::DateTime(bytes) => {
                buffer.copy_from_slice(bytes);
                bytes.len() as usize
            }
            ColumnValue::Timestamp(bytes) => {
                buffer.copy_from_slice(bytes);
                bytes.len() as usize
            }
            ColumnValue::VarChar(s) => {
                if s.len() > VARCHAR_MAXSIZE {
                    return Err(errors::Error::Schema(format!(
                        "Varchar exceeds max length: {} > {}",
                        s.len(),
                        VARCHAR_MAXSIZE
                    )));
                }
                buffer[..s.len()].copy_from_slice(s);
                s.len() as usize
            }
            ColumnValue::Boolean(v) => {
                encode_into_slice(v, &mut buffer, c)
                    .map_err(|e| err!(Encoding, format!("Failed to encode BOOLEAN. {}", e)))?;
                1
            }
        };
        Ok(buffer)
    }
}

// Encode a row from bytes based on the schema
pub fn encode_row(schema: &TableSchema, row: Row) -> Result<Vec<u8>, errors::Error> {
    let row_size = schema.get_row_size();
    let mut result = Vec::with_capacity(row_size);

    for column in &schema.columns {
        let value = row
            .inner
            .get(&column.name)
            .ok_or_else(|| errors::Error::Schema(format!("Missing column: {}", column.name)))?;
        let fixed_bytes = value.to_fixed_bytes(column.type_.fixed_size())?;
        result.extend_from_slice(&fixed_bytes);
    }

    debug_assert_eq!(
        result.len(),
        row_size,
        "Encoded row size doesn't match expected size"
    );

    Ok(result)
}

// Decode a row from bytes based on the schema
pub fn decode_row(schema: &TableSchema, encoded: Vec<u8>) -> Result<Row, errors::Error> {
    let mut row = Row {
        inner: HashMap::new(),
    };
    let mut offset = 0;

    let row_size = schema.get_row_size();
    if encoded.len() != row_size {
        return Err(errors::Error::Schema(format!(
            "Encoded row size mismatch: expected {}, got {}",
            row_size,
            encoded.len()
        )));
    }

    for column in &schema.columns {
        let size = column.type_.fixed_size();
        if offset + size > encoded.len() {
            return Err(errors::Error::Schema(format!(
                "Not enough data for column '{}': need {} bytes at offset {}",
                column.name, size, offset
            )));
        }

        let slice = &encoded[offset..offset + size];
        row.inner
            .insert(column.name.clone(), column.type_.from_fixed_bytes(slice)?);
        offset += size;
    }

    Ok(row)
}
