use crate::storage;
use crate::{echo, errors};
use clap::builder::Str;
use std::any;
use std::collections::VecDeque;
use tracing::{info, trace};

pub enum Statement {
    Select,
    Insert(InsertStatement),
    Update,
    Delete,
}

pub enum Clause {
    Where,
}

/// SQL command consists of sequence of clauses.
pub struct SqlCommand {
    statement: Statement,
    sql: String,
}

pub struct InsertStatement {
    table: String,
    columns: Vec<String>,
    values: Vec<String>,
}

pub struct SelectStatement {
    table: String,
    columns: Vec<String>,
}

pub struct DeleteStatement {
    table: String,
}

pub struct UpdateStatement {
    table: String,
    columns: Vec<String>,
    values: Vec<String>,
}

/// Execute a statement.
pub fn execute(c: SqlCommand) {
    let mut p = match storage::load("default".into()) {
        Ok(t) => t,
        Err(e) => {
            echo!("Failed to load table. {}\n", e);
            return;
        }
    };

    match c.statement {
        Statement::Select => match execute_select(&mut p) {
            Ok(rows) => {
                if rows.len() == 0 {
                    echo!("No rows found.\n");
                    return;
                }

                for r in rows {
                    echo!(
                        "({}, {}, {})\n",
                        r.id,
                        String::from_utf8(r.username.to_vec()).unwrap_or("*****".to_owned()),
                        String::from_utf8(r.email.to_vec()).unwrap_or("*****".to_owned()),
                    );
                }
            }
            Err(e) => echo!("Error: {}", e),
        },
        Statement::Insert(s) => {
            match execute_insert(
                &mut p,
                &storage::Row {
                    id: 1,
                    username: storage::str_to_fixed_bytes("user"),
                    email: storage::str_to_fixed_bytes(""),
                },
            ) {
                Ok(_) => {
                    echo!("Inserted row.");
                }
                Err(e) => echo!("Error: {}", e),
            }
        }
        Statement::Update => echo!("This is where we would do an update."),
        Statement::Delete => echo!("This is where we would do a delete."),
    }
}

/// Parse a statement.
pub fn parse(sql: &str) -> Result<SqlCommand, errors::Error> {
    let sql = sql.strip_suffix(';').unwrap_or(&sql);
    let upper = sql.to_uppercase();
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
            sql: sql.to_string(),
        }),
        "INSERT" => Ok(SqlCommand {
            statement: Statement::Insert(parse_insert(sql)?),
            sql: sql.to_string(),
        }),
        "UPDATE" => Ok(SqlCommand {
            statement: Statement::Update,
            sql: sql.to_string(),
        }),
        "DELETE" => Ok(SqlCommand {
            statement: Statement::Delete,
            sql: sql.to_string(),
        }),
        _ => {
            return Err(errors::Error::Syntax("Unrecognized statement.".to_owned()));
        }
    }
}

pub fn execute_insert(table: &mut storage::Table, row: &storage::Row) -> Result<(), errors::Error> {
    if table.num_rows >= storage::TABLE_MAX_ROWS {
        return Err(errors::Error::Db("Table is full.".to_owned()));
    }

    let bin_row = storage::serialize_row(row);
    storage::insert_row(table, &bin_row)?;

    return Ok(());
}

fn parse_insert(sql: &str) -> Result<InsertStatement, errors::Error> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 6 || parts[0].to_uppercase() != "INSERT" || parts[1].to_uppercase() != "INTO" {
        return Err(errors::Error::Syntax(
            "Invalid INSERT statement.".to_owned(),
        ));
    }

    let table = parts[2].trim_matches(|c| c == '(' || c == ')').to_string();
    let columns = parts[3]
        .trim_matches(|c| c == '(' || c == ')')
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();
    let values = parts[5]
        .trim_matches(|c| c == '(' || c == ')')
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();

    Ok(InsertStatement {
        table,
        columns,
        values,
    })
}

pub fn execute_select(table: &mut storage::Table) -> Result<Vec<storage::Row>, errors::Error> {
    let rows = storage::select_rows(table)?;
    return Ok(rows);
}
