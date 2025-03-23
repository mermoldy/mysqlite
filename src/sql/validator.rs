use crate::errors;
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref COLUMN_REGEX: Regex = Regex::new(r#"[A-Za-z_][A-Za-z0-9_]*"#).unwrap();
    static ref TYPE_REGEX: Regex = Regex::new(r#"(?i)(INT|SMALLINT|TINYINT|BIGINT|FLOAT|DOUBLE|VARCHAR\(\d+\)|TEXT|DATETIME|TIMESTAMP|BOOLEAN)"#).unwrap();
}

/// Validates a column name against the regex.
///
/// # Arguments
/// * `name` - The column name to validate.
///
/// # Returns
/// A `Result` containing the validated name or an `errors::Error`.
pub fn validate_column_name(name: &str) -> Result<String, errors::Error> {
    if name.is_empty() {
        return Err(errors::Error::Syntax(
            "Column name cannot be empty.".to_owned(),
        ));
    }
    if COLUMN_REGEX.is_match(name) {
        Ok(name.to_string())
    } else {
        Err(errors::Error::Syntax(format!(
            "Column name ({}) must match regex {}.",
            name,
            COLUMN_REGEX.as_str()
        )))
    }
}

/// Validates a column type against the regex.
///
/// # Arguments
/// * `type_str` - The column type to validate.
///
/// # Returns
/// An`errors::Error` if type is invalid.
pub fn validate_column_type(type_str: &str) -> Result<(), errors::Error> {
    if !TYPE_REGEX.is_match(type_str) {
        return Err(errors::Error::Syntax(format!(
            "Invalid column type: {}.",
            type_str
        )));
    }
    Ok(())
}

/// Validates a value, ensuring text is quoted if it contains spaces.
///
/// # Arguments
/// * `value` - The value to validate.
///
/// # Returns
/// A `Result` containing the validated value or an `errors::Error`.
pub fn validate_value(value: &str) -> Result<String, errors::Error> {
    if value.is_empty() {
        return Err(errors::Error::Syntax("Value cannot be empty.".to_owned()));
    }
    let trimmed = value.trim();
    if trimmed.contains(' ') {
        if trimmed.starts_with("'") && trimmed.ends_with("'") {
            Ok(trimmed[1..trimmed.len() - 1].to_string())
        } else {
            Err(errors::Error::Syntax(
                "Text values with spaces must be enclosed in single quotes.".to_owned(),
            ))
        }
    } else {
        Ok(trimmed.to_string())
    }
}
