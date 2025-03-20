use crate::{echo, errors};
use clap::builder::Str;
use lazy_static::lazy_static;
use regex::Regex;
use std::any;
use std::collections::VecDeque;
use tracing::{info, trace};

lazy_static! {
    static ref COLUMN_REGEX: Regex = Regex::new(r#"[A-Za-z_][A-Za-z0-9_]*"#).unwrap();
}

/// SQL command consists of sequence of clauses.
pub struct SqlCommand {
    pub statement: Statement,
    pub sql: String,
}

pub enum CreateStatement {
    CreateDatabaseStatement(CreateDatabaseStatement),
    CreateTableStatement(CreateTableStatement),
}

pub enum DropStatement {
    DropDatabasesStatement(String),
    DropTablesStatement(String),
}

pub enum ShowStatement {
    ShowDatabasesStatement,
    ShowTablesStatement,
}

pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update,
    Delete,
    Create(CreateStatement),
    Drop(DropStatement),
    Show(ShowStatement),
}

pub enum Clause {
    Join,
    Where,
}

pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

pub enum Columns {
    All,
    List(Vec<String>),
}

pub struct SelectStatement {
    pub table: String,
    pub columns: Columns,
}

// CREATE DATABASE my_database;
pub struct CreateDatabaseStatement {
    pub name: String,
}

// CREATE TABLE my_table (
//     id INT PRIMARY KEY,
//     name VARCHAR(255),
//     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
// );
pub struct CreateTableStatement {
    pub name: String,
    pub columns_schemas: Vec<String>,
}

enum ColumnType {
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

pub struct ColumnSchema {
    pub name: String,
    pub is_primary: bool,
    pub type_: ColumnType,
    pub default: Option<String>,
}

pub struct DeleteStatement {
    table: String,
}

pub struct UpdateStatement {
    table: String,
    columns: Vec<String>,
    values: Vec<String>,
}

// Split an SQL statement respecing spaces inside parentheses.
fn split_sql(sql: &str) -> Result<VecDeque<String>, errors::Error> {
    let mut result = VecDeque::new();
    let mut current = String::new();
    let mut inside_parens = false;
    let mut inside_text = false;

    for c in sql.chars() {
        match c {
            '\'' => {
                inside_text = !inside_text;
                current.push(c);
            }
            ')' => {
                if !inside_text {
                    if !inside_parens {
                        return Err(errors::Error::Syntax(
                            "Wrong enccolsing parentheses.".to_owned(),
                        ));
                    }
                    inside_parens = false;
                }
                current.push(c);
            }
            '(' => {
                if !inside_text {
                    if inside_parens {
                        return Err(errors::Error::Syntax(
                            "Wrong opening parentheses.".to_owned(),
                        ));
                    }
                    inside_parens = true;
                }
                current.push(c);
            }
            ' ' if !inside_parens => {
                if !current.is_empty() {
                    result.push_back(current);
                    current = String::new();
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        result.push_back(current);
    }
    if inside_parens {
        return Err(errors::Error::Syntax(
            "Missing enclosing parentheses.".to_owned(),
        ));
    }

    Ok(result)
}

pub fn parse_insert(sql: &mut VecDeque<String>) -> Result<InsertStatement, errors::Error> {
    if sql
        .pop_front()
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("INTO"))
        != Some(true)
    {
        return Err(errors::Error::Syntax(
            "'INSERT' must be followed by an INTO statement.".to_owned(),
        ));
    }

    let table = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'INSERT INTO' must be followed by a table name.".to_owned(),
            ))
        }
    };
    let columns_sql = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'INSERT INTO table' must be followed by column names enclosed in parentheses."
                    .to_owned(),
            ));
        }
    };
    if !(columns_sql.starts_with("(") & columns_sql.ends_with(")")) {
        return Err(errors::Error::Syntax(
            "Column names must be enclosed in parentheses.".to_owned(),
        ));
    }
    let columns = columns_sql[1..columns_sql.len() - 1]
        .split(',')
        .map(|s| {
            let trimmed = s.trim();
            if trimmed.len() == 0 {
                return Err(errors::Error::Syntax("Missing column name(s).".to_owned()));
            }
            if COLUMN_REGEX.is_match(trimmed) {
                Ok(trimmed.to_string())
            } else {
                return Err(errors::Error::Syntax(format!(
                    "Column name ({}) must match the regex {}.",
                    trimmed,
                    COLUMN_REGEX.as_str()
                )));
            }
        })
        .collect::<Result<Vec<_>, errors::Error>>()?;

    if sql
        .pop_front()
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("VALUES"))
        != Some(true)
    {
        return Err(errors::Error::Syntax(
            "'INSERT INTO table (...)' must be followed by an VALUES statement.".to_owned(),
        ));
    }
    let values_sql = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'INSERT INTO table (...) VALUES' must be followed by values enclosed in parentheses."
                    .to_owned(),
            ));
        }
    };
    if !(values_sql.starts_with("(") & values_sql.ends_with(")")) {
        return Err(errors::Error::Syntax(
            "Values must be enclosed in parentheses.".to_owned(),
        ));
    }

    let values = values_sql[1..values_sql.len() - 1]
        .split(',')
        .map(|s| {
            let mut trimmed = s.trim();
            if trimmed.len() == 0 {
                return Err(errors::Error::Syntax("Empty value.".to_owned()));
            }

            if trimmed.contains(' ') {
                if !(trimmed.starts_with("'") & trimmed.ends_with("'")) {
                    return Err(errors::Error::Syntax(
                        "Text values must be enclosed in quotes.".to_owned(),
                    ));
                }
                trimmed = &trimmed[1..trimmed.len() - 1];
            }

            Ok(trimmed.to_string())
        })
        .collect::<Result<Vec<_>, errors::Error>>()?;

    if columns.len() != values.len() {
        return Err(errors::Error::Syntax(format!(
            "Column count ({}) does not match value count ({}).",
            columns.len(),
            values.len()
        )));
    }

    Ok(InsertStatement {
        table,
        columns,
        values,
    })
}

pub fn parse_select(sql: &mut VecDeque<String>) -> Result<SelectStatement, errors::Error> {
    let mut columns_sql = String::new();

    // Read all until FROM statement.
    loop {
        let s = match sql.pop_front() {
            Some(t) => t,
            _ => {
                if columns_sql.len() > 0 {
                    return Err(errors::Error::Syntax(
                        "'SELECT' column names must be followed by a FROM statement.".into(),
                    ));
                }
                return Err(errors::Error::Syntax(
                    "'SELECT' must be followed by column names.".into(),
                ));
            }
        };
        if s.to_uppercase() == "FROM" {
            break;
        }
        columns_sql.push_str(s.as_str());
    }

    if (columns_sql.starts_with("(") & columns_sql.ends_with(")")) {
        columns_sql = columns_sql[1..columns_sql.len() - 1].to_string();
    }
    columns_sql = columns_sql.replace(" ", "");
    let columns = if columns_sql == "*" {
        Columns::All
    } else {
        Columns::List(
            columns_sql
                .split(',')
                .map(|s| {
                    let trimmed = s.trim();
                    if trimmed.len() == 0 {
                        return Err(errors::Error::Syntax("Missing column names".into()));
                    }
                    if COLUMN_REGEX.is_match(trimmed) {
                        Ok(trimmed.to_string())
                    } else {
                        return Err(errors::Error::Syntax(format!(
                            "Column name ({}) must match the regex {}.",
                            trimmed,
                            COLUMN_REGEX.as_str()
                        )));
                    }
                })
                .collect::<Result<Vec<_>, errors::Error>>()?,
        )
    };

    let table = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'SELECT ... FROM' must be followed by a table name.".to_owned(),
            ))
        }
    };

    Ok(SelectStatement { table, columns })
}

pub fn parse_create(sql: &mut VecDeque<String>) -> Result<CreateStatement, errors::Error> {
    let entity: String = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'CREATE' must be followed by type (DATABASE or TABLE).".to_owned(),
            ));
        }
    }
    .to_uppercase();

    match entity.as_str() {
        "DATABASE" => Ok(CreateStatement::CreateDatabaseStatement(
            parse_create_database(sql)?,
        )),
        "TABLE" => Ok(CreateStatement::CreateTableStatement(parse_create_table(
            sql,
        )?)),
        _ => {
            return Err(errors::Error::Syntax(format!(
                "Unknown entity to create {}.",
                entity
            )));
        }
    }
}

pub fn parse_show(sql: &mut VecDeque<String>) -> Result<ShowStatement, errors::Error> {
    let entity: String = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'SHOW' must be followed by type (DATABASES or TABLES).".to_owned(),
            ));
        }
    }
    .to_uppercase();

    match entity.to_uppercase().as_str() {
        "DATABASES" => Ok(ShowStatement::ShowDatabasesStatement),
        "TABLES" => Ok(ShowStatement::ShowTablesStatement),
        _ => {
            return Err(errors::Error::Syntax(format!(
                "Unknown entity to show {}.",
                entity
            )));
        }
    }
}

pub fn parse_drop(sql: &mut VecDeque<String>) -> Result<DropStatement, errors::Error> {
    let entity: String = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'DROP' must be followed by type (DATABASE or TABLE).".to_owned(),
            ));
        }
    }
    .to_uppercase();

    match entity.to_uppercase().as_str() {
        "DATABASE" => {
            let name: String = match sql.pop_front() {
                Some(t) => t,
                _ => {
                    return Err(errors::Error::Syntax(
                        "'DROP DATABASE' must be followed by name.".into(),
                    ));
                }
            };
            Ok(DropStatement::DropDatabasesStatement(name))
        }
        "TABLE" => {
            let name: String = match sql.pop_front() {
                Some(t) => t,
                _ => {
                    return Err(errors::Error::Syntax(
                        "'DROP TABLE' must be followed by name.".into(),
                    ));
                }
            };
            Ok(DropStatement::DropTablesStatement(name))
        }
        _ => {
            return Err(errors::Error::Syntax(format!(
                "Unknown entity to drop {}.",
                entity
            )));
        }
    }
}

pub fn parse_create_database(
    sql: &mut VecDeque<String>,
) -> Result<CreateDatabaseStatement, errors::Error> {
    let name: String = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'CREATE DATABASE' must be followed by database name.".to_owned(),
            ));
        }
    };

    Ok(CreateDatabaseStatement { name })
}

pub fn parse_create_table(
    sql: &mut VecDeque<String>,
) -> Result<CreateTableStatement, errors::Error> {
    let name: String = match sql.pop_front() {
        Some(t) => t,
        _ => {
            return Err(errors::Error::Syntax(
                "'CREATE TABLE' must be followed by table name.".to_owned(),
            ));
        }
    };
    let columns_schemas = Vec::new();

    Ok(CreateTableStatement {
        name,
        columns_schemas,
    })
}

/// Parse a statement.
pub fn parse(raw_sql: String) -> Result<SqlCommand, errors::Error> {
    let mut sql = split_sql(&raw_sql.strip_suffix(';').unwrap_or(&raw_sql))?;

    let first = match sql.pop_front() {
        Some(f) => f,
        None => {
            return Err(errors::Error::Syntax(
                "Expected at least one element.".to_owned(),
            ))
        }
    };

    match first.to_uppercase().as_str() {
        "SELECT" => Ok(SqlCommand {
            statement: Statement::Select(parse_select(&mut sql)?),
            sql: raw_sql,
        }),
        "INSERT" => Ok(SqlCommand {
            statement: Statement::Insert(parse_insert(&mut sql)?),
            sql: raw_sql,
        }),
        "UPDATE" => Ok(SqlCommand {
            statement: Statement::Update,
            sql: raw_sql,
        }),
        "DELETE" => Ok(SqlCommand {
            statement: Statement::Delete,
            sql: raw_sql,
        }),
        "CREATE" => Ok(SqlCommand {
            statement: Statement::Create(parse_create(&mut sql)?),
            sql: raw_sql,
        }),
        "DROP" => Ok(SqlCommand {
            statement: Statement::Drop(parse_drop(&mut sql)?),
            sql: raw_sql,
        }),
        "SHOW" => Ok(SqlCommand {
            statement: Statement::Show(parse_show(&mut sql)?),
            sql: raw_sql,
        }),
        _ => {
            return Err(errors::Error::Syntax("Unrecognized statement.".to_owned()));
        }
    }
}
