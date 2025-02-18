/// The command processor.
use crate::{echo, errors};

pub enum Statement {
    Select,
    Insert,
    Update,
    Delete,
}

/// Execute a statement.
pub fn execute(statement: Statement) {
    match statement {
        Statement::Select => {
            echo!("This is where we would do a select.");
        }
        Statement::Insert => echo!("This is where we would do an insert."),
        Statement::Update => echo!("This is where we would do an update."),
        Statement::Delete => echo!("This is where we would do a delete."),
    }
}

/// Parse a statement.
pub fn parse(statement: &str) -> Result<Statement, errors::Error> {
    let s = statement.to_lowercase();
    if s.starts_with("select") {
        Ok(Statement::Select)
    } else if s.starts_with("insert") {
        Ok(Statement::Insert)
    } else if s.starts_with("update") {
        Ok(Statement::Update)
    } else if s.starts_with("delete") {
        Ok(Statement::Delete)
    } else {
        return Err(errors::Error::Other("Unrecognized statement".to_string()));
    }
}
