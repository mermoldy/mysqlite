use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Db(String),
    Syntax(String),
    LockTable(String),
    Schema(String),
    Serialization(String),
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error. {}", e),
            Error::Db(msg) => write!(f, "DB Error. {}", msg),
            Error::Syntax(msg) => write!(f, "You have an error in your SQL syntax. {}", msg),
            Error::LockTable(msg) => write!(f, "Table Lock error. {}", msg),
            Error::Schema(msg) => write!(f, "Table schema error. {}", msg),
            Error::Serialization(msg) => write!(f, "Table serialization error. {}", msg),
            Error::Other(msg) => write!(f, "Error. {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}
