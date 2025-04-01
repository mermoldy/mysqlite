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
    /// SQL syntax error (e.g., missing semicolon, invalid token).
    /// Error code: 3000
    Syntax(String),
    /// Error acquiring a table lock (e.g., deadlock).
    /// Error code: 4000
    LockTable(String),
    /// Schema-related error (e.g., invalid column, table not found).
    /// Error code: 5000
    Schema(String),
    /// Row serialization/deserialization error (e.g., encoding/decoding issues).
    /// Error code: 6000
    Encoding(String),
    /// Invalid command error (e.g., dropping current database, unrecognized command).
    /// Error code: 7000
    Command(String),
    /// Storage-related error (e.g., disk full, page corruption).
    /// Error code: 8000
    Storage(String),
    /// Session-related error (e.g., session expired, invalid session ID).
    /// Error code: 8100
    Session(String),
    /// SQL semantic error (e.g., ambiguous column, invalid join).
    /// Error code: 3100
    Semantic(String),
    /// Transaction-related error (e.g., rollback failure, conflict).
    /// Error code: 8200
    Transaction(String),
    /// Authentication/authorization error (e.g., invalid credentials).
    /// Error code: 8300
    Auth(String),
    /// Resource limit exceeded (e.g., too many connections).
    /// Error code: 8400
    ResourceLimit(String),
    /// Miscellaneous uncategorized error.
    /// Error code: 9000
    Other(String),
}

impl Error {
    /// Returns the error code associated with this error variant.
    pub fn code(&self) -> u32 {
        match self {
            Error::Io(_) => 1000,
            Error::Syntax(_) => 3000,
            Error::LockTable(_) => 4000,
            Error::Schema(_) => 5000,
            Error::Encoding(_) => 6000,
            Error::Command(_) => 7000,
            Error::Storage(_) => 8000,
            Error::Session(_) => 8100,
            Error::Semantic(_) => 3100,
            Error::Transaction(_) => 8200,
            Error::Auth(_) => 8300,
            Error::ResourceLimit(_) => 8400,
            Error::Other(_) => 9000,
        }
    }

    /// Returns a human-readable error category for this error variant.
    pub fn category(&self) -> &'static str {
        match self {
            Error::Io(_) => "I/O",
            Error::Syntax(_) => "SQL Syntax",
            Error::LockTable(_) => "Table Lock",
            Error::Schema(_) => "Schema",
            Error::Encoding(_) => "Encoding",
            Error::Command(_) => "Command",
            Error::Storage(_) => "Storage",
            Error::Session(_) => "Session",
            Error::Semantic(_) => "SQL Semantic",
            Error::Transaction(_) => "Transaction",
            Error::Auth(_) => "Authentication",
            Error::ResourceLimit(_) => "Resource Limit",
            Error::Other(_) => "Other",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "[{}] I/O Error: {}", self.code(), e),
            Error::Syntax(msg) => write!(f, "[{}] Syntax Error: {}", self.code(), msg),
            Error::LockTable(msg) => write!(f, "[{}] Lock Table Error: {}", self.code(), msg),
            Error::Schema(msg) => write!(f, "[{}] Schema Error: {}", self.code(), msg),
            Error::Encoding(msg) => write!(f, "[{}] Encoding Error: {}", self.code(), msg),
            Error::Command(msg) => write!(f, "[{}] Command Error: {}", self.code(), msg),
            Error::Storage(msg) => write!(f, "[{}] Storage Error: {}", self.code(), msg),
            Error::Session(msg) => write!(f, "[{}] Session Error: {}", self.code(), msg),
            Error::Semantic(msg) => write!(f, "[{}] Semantic Error: {}", self.code(), msg),
            Error::Transaction(msg) => write!(f, "[{}] Transaction Error: {}", self.code(), msg),
            Error::Auth(msg) => write!(f, "[{}] Authentication Error: {}", self.code(), msg),
            Error::ResourceLimit(msg) => {
                write!(f, "[{}] Resource Limit Error: {}", self.code(), msg)
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
        Error::Encoding(format!("UTF-8 conversion error: {}", err))
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::Encoding(format!("Integer parsing error: {}", err))
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
/// let err = err!(Command, "Unrecognized command '{}'", "FOO");
/// assert_eq!(err.code(), 7000);
/// assert_eq!(err.to_string(), "[7000] Command Error: Unrecognized command 'FOO'");
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
