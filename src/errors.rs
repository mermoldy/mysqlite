use std::error;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Db(String),
    Syntax(String),
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error. {}", e),
            Error::Db(msg) => write!(f, "DB Error. {}", msg),
            Error::Syntax(msg) => write!(f, "SQL Syntax Error. {}", msg),
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
