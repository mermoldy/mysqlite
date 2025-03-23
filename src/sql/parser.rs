use super::statement::*;
use super::tokenizer;
use super::validator;
use crate::errors;
use std::collections::VecDeque;

/// Parses an `INSERT` statement from tokenized SQL.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `InsertStatement` or an `errors::Error`.
fn parse_insert(sql: &mut VecDeque<String>) -> Result<InsertStatement, errors::Error> {
    expect_token(sql, "INTO", "'INSERT' must be followed by 'INTO'.")?;
    let table = pop_token(sql, "'INSERT INTO' must be followed by a table name.")?;
    let columns_sql = pop_token(
        sql,
        "'INSERT INTO table' must be followed by column names in parentheses.",
    )?;
    let (columns, values) = parse_columns_and_values(sql, columns_sql)?;
    Ok(InsertStatement {
        table,
        columns,
        values,
    })
}

/// Parses a `SELECT` statement from tokenized SQL.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `SelectStatement` or an `errors::Error`.
fn parse_select(sql: &mut VecDeque<String>) -> Result<SelectStatement, errors::Error> {
    let mut columns_sql = String::new();
    while let Some(token) = sql.front() {
        if token.to_uppercase() == "FROM" {
            sql.pop_front(); // Consume "FROM"
            break;
        }
        columns_sql.push_str(&sql.pop_front().unwrap());
    }
    if columns_sql.is_empty() {
        return Err(errors::Error::Syntax(
            "'SELECT' must specify columns.".to_owned(),
        ));
    }
    let columns = parse_columns(columns_sql)?;
    let table = pop_token(sql, "'SELECT ... FROM' must be followed by a table name.")?;
    Ok(SelectStatement { table, columns })
}

/// Parses a `CREATE` statement from tokenized SQL.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `CreateStatement` or an `errors::Error`.
fn parse_create(sql: &mut VecDeque<String>) -> Result<CreateStatement, errors::Error> {
    let entity = pop_token(sql, "'CREATE' must specify 'DATABASE' or 'TABLE'.")?.to_uppercase();
    match entity.as_str() {
        "DATABASE" => Ok(CreateStatement::CreateDatabaseStatement(
            parse_create_database(sql)?,
        )),
        "TABLE" => Ok(CreateStatement::CreateTableStatement(parse_create_table(
            sql,
        )?)),
        _ => Err(errors::Error::Syntax(format!(
            "Unknown entity to create: {}.",
            entity
        ))),
    }
}

/// Parses a `SHOW` statement from tokenized SQL.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `ShowStatement` or an `errors::Error`.
fn parse_show(tokens: &mut VecDeque<String>) -> Result<ShowStatement, errors::Error> {
    let entity = pop_token(tokens, "'SHOW' must specify 'DATABASES' or 'TABLES'.")?.to_uppercase();
    match entity.as_str() {
        "DATABASES" => Ok(ShowStatement::ShowDatabasesStatement),
        "TABLES" => Ok(ShowStatement::ShowTablesStatement),
        _ => Err(errors::Error::Syntax(format!(
            "Unknown entity to show: {}.",
            entity
        ))),
    }
}

/// Parses a `DROP` statement from tokenized SQL.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `DropStatement` or an `errors::Error`.
fn parse_drop(tokens: &mut VecDeque<String>) -> Result<DropStatement, errors::Error> {
    let entity = pop_token(tokens, "'DROP' must specify 'DATABASE' or 'TABLE'.")?.to_uppercase();
    let name = pop_token(
        tokens,
        &format!("'DROP {}' must be followed by a name.", entity),
    )?;
    match entity.as_str() {
        "DATABASE" => Ok(DropStatement::DropDatabasesStatement(name)),
        "TABLE" => Ok(DropStatement::DropTablesStatement(name)),
        _ => Err(errors::Error::Syntax(format!(
            "Unknown entity to drop: {}.",
            entity
        ))),
    }
}

/// Parses a `DELETE` statement from tokenized SQL.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `DeleteStatement` or an `errors::Error`.
fn parse_delete(tokens: &mut VecDeque<String>) -> Result<DeleteStatement, errors::Error> {
    expect_token(tokens, "FROM", "'DELETE' must be followed by 'FROM'.")?;
    let table = pop_token(tokens, "'DELETE FROM' must be followed by a table name.")?;
    let where_clause = parse_where_clause(tokens)?;
    Ok(DeleteStatement {
        table,
        where_clause,
    })
}

/// Parses an `UPDATE` statement from tokenized SQL.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `UpdateStatement` or an `errors::Error`.
fn parse_update(tokens: &mut VecDeque<String>) -> Result<UpdateStatement, errors::Error> {
    let table = pop_token(tokens, "'UPDATE' must be followed by a table name.")?;
    expect_token(tokens, "SET", "'UPDATE table' must be followed by 'SET'.")?;
    let mut sets = Vec::new();
    while let Some(token) = tokens.front() {
        if token.to_uppercase() == "WHERE" {
            break;
        }
        let column = pop_token(tokens, "Missing column in SET clause.")?;
        expect_token(tokens, "=", "Expected '=' after column in SET clause.")?;
        let value = pop_token(tokens, "Missing value after '=' in SET clause.")?;
        sets.push((column, value));
    }
    let where_clause = parse_where_clause(tokens)?;
    Ok(UpdateStatement {
        table,
        sets,
        where_clause,
    })
}

/// Parses a `CREATE DATABASE` statement.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `CreateDatabaseStatement` or an `errors::Error`.
fn parse_create_database(
    tokens: &mut VecDeque<String>,
) -> Result<CreateDatabaseStatement, errors::Error> {
    let name = pop_token(
        tokens,
        "'CREATE DATABASE' must be followed by a database name.",
    )?;
    Ok(CreateDatabaseStatement { name })
}

/// Parses a `CREATE TABLE` statement with column schemas.
///
/// # Arguments
/// * `sql` - A mutable `VecDeque<String>` of SQL tokens.
///
/// # Returns
/// A `Result` containing the parsed `CreateTableStatement` or an `errors::Error`.
fn parse_create_table(sql: &mut VecDeque<String>) -> Result<CreateTableStatement, errors::Error> {
    let name = pop_token(sql, "'CREATE TABLE' must be followed by a table name.")?;
    let columns_sql = pop_token(
        sql,
        "'CREATE TABLE name' must be followed by column definitions in parentheses.",
    )?;
    if !columns_sql.starts_with('(') || !columns_sql.ends_with(')') {
        return Err(errors::Error::Syntax(
            "Column definitions must be enclosed in parentheses.".to_owned(),
        ));
    }
    let columns_str = &columns_sql[1..columns_sql.len() - 1];
    let columns_schemas = parse_column_schemas(columns_str)?;
    Ok(CreateTableStatement {
        name,
        columns_schemas,
    })
}

/// Helper function to expect and consume a specific token.
///
/// # Arguments
/// * `tokens` - The token queue.
/// * `expected` - The token to expect (case-insensitive).
/// * `error_msg` - The error message if the token is not found.
///
/// # Returns
/// A `Result` indicating success or an `errors::Error`.
fn expect_token(
    tokens: &mut VecDeque<String>,
    expected: &str,
    error_msg: &str,
) -> Result<(), errors::Error> {
    match tokens.pop_front() {
        Some(token) if token.to_uppercase() == expected.to_uppercase() => Ok(()),
        _ => Err(errors::Error::Syntax(error_msg.to_owned())),
    }
}

/// Helper function to pop a token or return an error.
///
/// # Arguments
/// * `tokens` - The token queue.
/// * `error_msg` - The error message if no token is available.
///
/// # Returns
/// A `Result` containing the token or an `errors::Error`.
fn pop_token(tokens: &mut VecDeque<String>, error_msg: &str) -> Result<String, errors::Error> {
    tokens
        .pop_front()
        .ok_or_else(|| errors::Error::Syntax(error_msg.to_owned()))
}

/// Parses columns and values for `INSERT` statements.
///
/// # Arguments
/// * `tokens` - The token queue.
/// * `columns_sql` - The string containing column definitions.
///
/// # Returns
/// A `Result` containing a tuple of column and value vectors or an `errors::Error`.
fn parse_columns_and_values(
    tokens: &mut VecDeque<String>,
    columns_sql: String,
) -> Result<(Vec<String>, Vec<String>), errors::Error> {
    if !columns_sql.starts_with('(') || !columns_sql.ends_with(')') {
        return Err(errors::Error::Syntax(
            "Column names must be enclosed in parentheses.".to_owned(),
        ));
    }
    let columns = columns_sql[1..columns_sql.len() - 1]
        .split(',')
        .map(|s| validator::validate_column_name(s.trim()))
        .collect::<Result<Vec<_>, _>>()?;

    expect_token(
        tokens,
        "VALUES",
        "'INSERT INTO table (...)' must be followed by 'VALUES'.",
    )?;
    let values_sql = pop_token(
        tokens,
        "'VALUES' must be followed by values in parentheses.",
    )?;
    if !values_sql.starts_with('(') || !values_sql.ends_with(')') {
        return Err(errors::Error::Syntax(
            "Values must be enclosed in parentheses.".to_owned(),
        ));
    }
    let values = values_sql[1..values_sql.len() - 1]
        .split(',')
        .map(|s| validator::validate_value(s.trim()))
        .collect::<Result<Vec<_>, _>>()?;

    if columns.len() != values.len() {
        return Err(errors::Error::Syntax(format!(
            "Column count ({}) does not match value count ({}).",
            columns.len(),
            values.len()
        )));
    }
    Ok((columns, values))
}

/// Parses column names for `SELECT` statements.
///
/// # Arguments
/// * `columns_sql` - The string containing column definitions.
///
/// # Returns
/// A `Result` containing the `Columns` enum or an `errors::Error`.
fn parse_columns(columns_sql: String) -> Result<Columns, errors::Error> {
    let trimmed = columns_sql.replace(" ", "");
    if trimmed == "*" {
        Ok(Columns::All)
    } else {
        let columns = trimmed
            .split(',')
            .map(|s| validator::validate_column_name(s.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() {
            return Err(errors::Error::Syntax(
                "No columns specified in SELECT.".to_owned(),
            ));
        }
        Ok(Columns::List(columns))
    }
}

/// Parses column schemas for `CREATE TABLE`.
///
/// # Arguments
/// * `columns_str` - The string containing column definitions.
///
/// # Returns
/// A `Result` containing a vector of `ColumnSchema` or an `errors::Error`.
fn parse_column_schemas(columns_str: &str) -> Result<Vec<ColumnSchema>, errors::Error> {
    let mut schemas = Vec::new();
    for column_def in columns_str.split(',') {
        let parts: Vec<&str> = column_def.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue; // Skip empty definitions
        }
        let name = validator::validate_column_name(parts[0])?;
        let type_str = parts
            .get(1)
            .ok_or_else(|| errors::Error::Syntax(format!("Column '{}' missing type.", name)))?;
        let type_ = parse_column_type(type_str)?;
        let mut is_primary = false;
        let mut default = None;
        let mut i = 2;
        while i < parts.len() {
            match parts[i].to_uppercase().as_str() {
                "PRIMARY" => {
                    i += 1;
                    expect_keyword(&parts, i, "KEY", "PRIMARY must be followed by KEY.")?;
                    is_primary = true;
                    i += 1;
                }
                "DEFAULT" => {
                    i += 1;
                    default = Some(pop_value(
                        &parts,
                        i,
                        "DEFAULT must be followed by a value.",
                    )?);
                    i += 1;
                }
                _ => break,
            }
        }
        schemas.push(ColumnSchema {
            name,
            is_primary,
            type_,
            default,
        });
    }
    if schemas.is_empty() {
        return Err(errors::Error::Syntax(
            "No valid column definitions found.".to_owned(),
        ));
    }
    Ok(schemas)
}

/// Parses a column type from a string.
///
/// # Arguments
/// * `type_str` - The type string to parse.
///
/// # Returns
/// A `Result` containing the `ColumnType` or an `errors::Error`.
fn parse_column_type(type_str: &str) -> Result<ColumnType, errors::Error> {
    validator::validate_column_type(type_str)?;
    let upper = type_str.to_uppercase();

    match upper.as_str() {
        "INT" => Ok(ColumnType::Int),
        "SMALLINT" => Ok(ColumnType::SmallInt),
        "TINYINT" => Ok(ColumnType::TinyInt),
        "BIGINT" => Ok(ColumnType::BigInt),
        "FLOAT" => Ok(ColumnType::Float),
        "DOUBLE" => Ok(ColumnType::Double),
        "TEXT" => Ok(ColumnType::Text),
        "DATETIME" => Ok(ColumnType::DateTime),
        "TIMESTAMP" => Ok(ColumnType::Timestamp),
        "BOOLEAN" => Ok(ColumnType::Boolean),
        _ if upper.starts_with("VARCHAR(") && upper.ends_with(")") => {
            let len_str = &upper[8..upper.len() - 1];
            let len = len_str.parse::<u16>().map_err(|_| {
                errors::Error::Syntax(format!("Invalid VARCHAR length: {}.", len_str))
            })?;
            Ok(ColumnType::VarChar(len))
        }
        _ => Err(errors::Error::Syntax(format!(
            "Unsupported column type: {}.",
            type_str
        ))),
    }
}

/// Helper to expect a keyword in a parts array.
///
/// # Arguments
/// * `parts` - The array of parts.
/// * `index` - The index to check.
/// * `expected` - The expected keyword.
/// * `error_msg` - The error message if not found.
///
/// # Returns
/// A `Result` indicating success or an `errors::Error`.
fn expect_keyword(
    parts: &[&str],
    index: usize,
    expected: &str,
    error_msg: &str,
) -> Result<(), errors::Error> {
    if parts
        .get(index)
        .map_or(false, |&p| p.to_uppercase() == expected)
    {
        Ok(())
    } else {
        Err(errors::Error::Syntax(error_msg.to_owned()))
    }
}

/// Helper to pop a value from a parts array.
///
/// # Arguments
/// * `parts` - The array of parts.
/// * `index` - The index to pop from.
/// * `error_msg` - The error message if not found.
///
/// # Returns
/// A `Result` containing the value or an `errors::Error`.
fn pop_value(parts: &[&str], index: usize, error_msg: &str) -> Result<String, errors::Error> {
    parts
        .get(index)
        .map(|&s| s.to_string())
        .ok_or_else(|| errors::Error::Syntax(error_msg.to_owned()))
}

/// Parses an optional `WHERE` clause.
///
/// # Arguments
/// * `tokens` - The token queue.
///
/// # Returns
/// A `Result` containing an optional WHERE clause string or an `errors::Error`.
fn parse_where_clause(sql: &mut VecDeque<String>) -> Result<Option<String>, errors::Error> {
    if let Some(token) = sql.front() {
        if token.to_uppercase() == "WHERE" {
            sql.pop_front(); // Consume "WHERE"
            let clause = sql
                .into_iter()
                .map(|c| c.clone())
                .collect::<Vec<_>>()
                .join(" ");
            if clause.is_empty() {
                return Err(errors::Error::Syntax(
                    "WHERE clause cannot be empty.".to_owned(),
                ));
            }
            return Ok(Some(clause));
        }
    }
    Ok(None)
}

/// Parses a full SQL statement.
///
/// # Arguments
/// * `raw_sql` - The raw SQL string to parse.
///
/// # Returns
/// A `Result` containing the parsed `SqlCommand` or an `errors::Error`.
pub fn parse(raw_sql: String) -> Result<SqlCommand, errors::Error> {
    let mut tokens = tokenizer::tokenize_sql(raw_sql.strip_suffix(';').unwrap_or(&raw_sql))?;

    let first = pop_token(&mut tokens, "SQL statement cannot be empty.")?.to_uppercase();
    let statement = match first.as_str() {
        "SELECT" => Statement::Select(parse_select(&mut tokens)?),
        "INSERT" => Statement::Insert(parse_insert(&mut tokens)?),
        "UPDATE" => Statement::Update(parse_update(&mut tokens)?),
        "DELETE" => Statement::Delete(parse_delete(&mut tokens)?),
        "CREATE" => Statement::Create(parse_create(&mut tokens)?),
        "DROP" => Statement::Drop(parse_drop(&mut tokens)?),
        "SHOW" => Statement::Show(parse_show(&mut tokens)?),
        _ => {
            return Err(errors::Error::Syntax(format!(
                "Unrecognized statement: {}.",
                first
            )))
        }
    };
    if !tokens.is_empty() {
        return Err(errors::Error::Syntax(
            "Unexpected tokens after statement.".to_owned(),
        ));
    }
    Ok(SqlCommand {
        statement,
        sql: raw_sql,
    })
}
