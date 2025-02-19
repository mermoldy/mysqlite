/// The command processor.
use crate::{echo, errors};
use std::collections::VecDeque;

pub enum Statement {
    Select,
    Insert,
    Update,
    Delete,
}

pub enum Clause {
    Where,
}

/// SQL command consists of sequence of clauses.
pub struct SqlCommand {
    statement: Statement,
    command: VecDeque<String>,
}

// impl SqlCommand {
//     pub fn head(&self) -> &Statement {
//         return self.vec.get(0).unwrap();
//     }
// }

/// Execute a statement.
pub fn execute(c: SqlCommand) {
    match c.statement {
        Statement::Select => {
            echo!("This is where we would do a select.");
        }
        Statement::Insert => echo!("This is where we would do an insert."),
        Statement::Update => echo!("This is where we would do an update."),
        Statement::Delete => echo!("This is where we would do a delete."),
    }
}

/// Parse a statement.
pub fn parse(s: &str) -> Result<SqlCommand, errors::Error> {
    let without_suffix = s.strip_suffix(';').unwrap_or(&s);
    let upper = without_suffix.to_uppercase();
    let mut deque: VecDeque<String> = upper.split(' ').map(String::from).collect();

    let first = match deque.pop_front() {
        Some(f) => f,
        None => {
            return Err(errors::Error::Syntax(
                "Expected at least one element.".to_owned(),
            ))
        }
    };

    match first.as_str() {
        "SELECT" => Ok(SqlCommand {
            statement: Statement::Select,
            command: deque,
        }),
        "INSERT" => Ok(SqlCommand {
            statement: Statement::Insert,
            command: deque,
        }),
        "UPDATE" => Ok(SqlCommand {
            statement: Statement::Update,
            command: deque,
        }),
        "DELETE" => Ok(SqlCommand {
            statement: Statement::Delete,
            command: deque,
        }),
        _ => {
            return Err(errors::Error::Syntax("Unrecognized statement.".to_owned()));
        }
    }
}
