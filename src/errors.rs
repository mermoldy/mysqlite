use std::fmt;

/// Custom error type for database operations, including specific error codes.
///
/// Each variant represents a distinct error condition with a unique error code for easier debugging
/// and error handling in applications.
#[derive(Debug)]
pub enum Error {
    /// I/O-related error (e.g., file operations).
    /// Error code: 1000
    Io(std::io::Error),
    /// Database-specific error (e.g., connection failure).
    /// Error code: 2000
    Db(String),
    /// SQL syntax error.
    /// Error code: 3000
    Syntax(String),
    /// Error acquiring a table lock.
    /// Error code: 4000
    LockTable(String),
    /// Schema-related error (e.g., invalid column).
    /// Error code: 5000
    Schema(String),
    /// Row serialization/deserialization error.
    /// Error code: 6000
    Serialization(String),
    /// Invalid operation (e.g., dropping current database).
    /// Error code: 7000
    InvalidOperation(String),
    /// Miscellaneous uncategorized error.
    /// Error code: 9000
    Other(String),
}

impl Error {
    /// Returns the error code associated with this error variant.
    ///
    /// # Examples
    /// ```
    /// let err = Error::Syntax("Missing semicolon".to_string());
    /// assert_eq!(err.code(), 3000);
    /// ```
    pub fn code(&self) -> u32 {
        match self {
            Error::Io(_) => 1000,
            Error::Db(_) => 2000,
            Error::Syntax(_) => 3000,
            Error::LockTable(_) => 4000,
            Error::Schema(_) => 5000,
            Error::Serialization(_) => 6000,
            Error::InvalidOperation(_) => 7000,
            Error::Other(_) => 9000,
        }
    }

    /// Returns a human-readable error category for this error variant.
    ///
    /// # Examples
    /// ```
    /// let err = Error::LockTable("Failed to lock table".to_string());
    /// assert_eq!(err.category(), "Table Lock");
    /// ```
    pub fn category(&self) -> &'static str {
        match self {
            Error::Io(_) => "I/O",
            Error::Db(_) => "Database",
            Error::Syntax(_) => "Syntax",
            Error::LockTable(_) => "Table Lock",
            Error::Schema(_) => "Schema",
            Error::Serialization(_) => "Serialization",
            Error::InvalidOperation(_) => "Invalid Operation",
            Error::Other(_) => "Other",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "[{}] I/O Error: {}", self.code(), e),
            Error::Db(msg) => write!(f, "[{}] Database Error: {}", self.code(), msg),
            Error::Syntax(msg) => write!(f, "[{}] Syntax Error: {}", self.code(), msg),
            Error::LockTable(msg) => write!(f, "[{}] Lock Table Error: {}", self.code(), msg),
            Error::Schema(msg) => write!(f, "[{}] Schema Error: {}", self.code(), msg),
            Error::Serialization(msg) => {
                write!(f, "[{}] Serialization Error: {}", self.code(), msg)
            }
            Error::InvalidOperation(msg) => {
                write!(f, "[{}] Invalid Operation: {}", self.code(), msg)
            }
            Error::Other(msg) => write!(f, "[{}] Unknown Error: {}", self.code(), msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Serialization(format!("UTF-8 conversion error: {}", err))
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::Serialization(format!("Integer parsing error: {}", err))
    }
}

/// Convenience macro to create an `Error` with a formatted message.
///
/// # Examples
/// ```
/// use crate::errors::{Error, err};
/// let err = err!(Syntax, "Missing WHERE clause");
/// assert_eq!(err.code(), 3000);
/// assert_eq!(err.to_string(), "[3000] Syntax Error: Missing WHERE clause");
///
/// let err = err!(LockTable, "Failed to lock table '{}'", "users");
/// assert_eq!(err.code(), 4000);
/// assert_eq!(err.to_string(), "[4000] Lock Table Error: Failed to lock table 'users'");
/// ```
#[macro_export]
macro_rules! err {
    ($variant:ident, $msg:expr) => {
        $crate::errors::Error::$variant($msg.to_string())
    };
    ($variant:ident, $fmt:expr, $($arg:tt)*) => {
        $crate::errors::Error::$variant(format!($fmt, $($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_and_display() {
        let err = Error::Syntax("Invalid token".to_string());
        assert_eq!(err.code(), 3000);
        assert_eq!(err.to_string(), "[3000] Syntax Error: Invalid token");
        assert_eq!(err.category(), "Syntax");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let err = Error::from(io_err);
        assert_eq!(err.code(), 1000);
        assert_eq!(err.to_string(), "[1000] I/O Error: File not found");
    }

    #[test]
    fn test_error_macro() {
        let err = err!(LockTable, "Deadlock detected");
        assert_eq!(err.code(), 4000);
        assert_eq!(
            err.to_string(),
            "[4000] Lock Table Error: Deadlock detected"
        );

        let err = err!(Db, "Connection failed: {}", "timeout");
        assert_eq!(err.code(), 2000);
        assert_eq!(
            err.to_string(),
            "[2000] Database Error: Connection failed: timeout"
        );
    }
}
