use crate::{database, errors, session, sql, storage};
use std::sync::{Arc, Mutex};

/// Result of executing an SQL statement.
///
/// Represents either a success response with affected rows or a result set with columns and rows.
#[derive(Debug)]
pub enum SqlResult {
    /// Success response for `INSERT`, `UPDATE`, `DELETE`, or other commands with affected row count.
    Ok { affected_rows: u64 },
    /// Result set from `SELECT`-like queries, containing column names and rows of data.
    ResultSet {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

/// Executes an SQL command within a session.
///
/// # Arguments
/// * `session` - The mutable session context containing the database state.
/// * `command` - The parsed SQL command to execute.
///
/// # Returns
/// A `Result` containing the `SqlResult` or an `errors::Error` if execution fails.
///
/// # Examples
/// ```rust
/// let mut session = session::Session::new("mydb");
/// let command = sql::parse("SELECT * FROM users".to_string()).unwrap();
/// let result = execute(&mut session, command).unwrap();
/// match result {
///     SqlResult::ResultSet { columns, rows } => println!("Columns: {:?}", columns),
///     SqlResult::Ok { affected_rows } => println!("Affected rows: {}", affected_rows),
/// }
/// ```
pub fn execute(
    session: &mut session::Session,
    command: sql::SqlCommand,
) -> Result<SqlResult, errors::Error> {
    match command.statement {
        sql::Statement::Select(select_stmt) => execute_select_statement(session, select_stmt),
        sql::Statement::Insert(insert_stmt) => execute_insert_statement(session, insert_stmt),
        sql::Statement::Update(update_stmt) => execute_update_statement(session, update_stmt),
        sql::Statement::Delete(delete_stmt) => execute_delete_statement(session, delete_stmt),
        sql::Statement::Create(create_stmt) => execute_create_statement(session, create_stmt),
        sql::Statement::Show(show_stmt) => execute_show_statement(session, show_stmt),
        sql::Statement::Drop(drop_stmt) => execute_drop_statement(session, drop_stmt),
        sql::Statement::Describe(describe_stmt) => {
            execute_describe_statement(session, describe_stmt)
        }
    }
}

/// Executes a `SELECT` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `SelectStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::ResultSet` with query results or an `errors::Error`.
fn execute_select_statement(
    session: &mut session::Session,
    stmt: sql::SelectStatement,
) -> Result<SqlResult, errors::Error> {
    let table = session.database.find_table(&stmt.table)?;
    let rows = execute_select(table)?;

    if rows.is_empty() {
        return Ok(SqlResult::Ok { affected_rows: 0 });
    }

    let columns = match stmt.columns {
        sql::Columns::All => {
            let locked_table = table.lock().map_err(|_| {
                errors::Error::LockTable("Failed to lock table for schema access".to_string())
            })?;
            locked_table
                .schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect()
        }
        sql::Columns::List(cols) => cols,
    };

    let formatted_rows = rows
        .into_iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| row.get_column(col).unwrap_or_else(|| "-".to_string()))
                .collect()
        })
        .collect();

    Ok(SqlResult::ResultSet {
        columns,
        rows: formatted_rows,
    })
}

/// Executes a `DESCRIBE` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `DescribeStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::ResultSet` with query results or an `errors::Error`.
fn execute_describe_statement(
    session: &mut session::Session,
    stmt: sql::DescribeStatement,
) -> Result<SqlResult, errors::Error> {
    let _ = session.database.find_table(&stmt.name)?;

    let columns: Vec<String> = Vec::from([
        "Field".into(),
        "Type".into(),
        "Null".into(),
        "Key".into(),
        "Default".into(),
        "Extra".into(),
    ]);

    let rows: Vec<Vec<String>> = storage::SCHEMA
        .columns
        .clone()
        .into_iter()
        .map(|c| {
            Vec::from([
                c.name.clone(),
                c.type_.clone().to_string(),
                c.is_nullable.then(|| "YES").unwrap_or("NO").to_string(),
                c.is_primary.then(|| "PRI").unwrap_or("-").to_string(),
                c.default.clone().unwrap_or("NULL".to_string()),
                "".into(),
            ])
        })
        .collect();

    Ok(SqlResult::ResultSet { columns, rows })
}

/// Executes an `INSERT` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `InsertStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::Ok` with affected rows or an `errors::Error`.
fn execute_insert_statement(
    session: &mut session::Session,
    stmt: sql::InsertStatement,
) -> Result<SqlResult, errors::Error> {
    let table = session.database.find_table(&stmt.table)?;
    let row = storage::build_row(&storage::SCHEMA, &stmt.columns, &stmt.values)?;
    execute_insert(table, row)?;
    Ok(SqlResult::Ok { affected_rows: 1 })
}

/// Executes an `UPDATE` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `UpdateStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::Ok` with affected rows or an `errors::Error`.
fn execute_update_statement(
    session: &mut session::Session,
    stmt: sql::UpdateStatement,
) -> Result<SqlResult, errors::Error> {
    let table = session.database.find_table(&stmt.table)?;
    let mut _locked_table = table
        .lock()
        .map_err(|_| errors::Error::LockTable("Failed to lock table for update".to_string()))?;
    // let affected_rows = storage::update_rows(&mut locked_table, &stmt.sets, &stmt.where_clause)?;
    let affected_rows = 0;
    Ok(SqlResult::Ok { affected_rows })
}

/// Executes a `DELETE` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `DeleteStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::Ok` with affected rows or an `errors::Error`.
fn execute_delete_statement(
    session: &mut session::Session,
    stmt: sql::DeleteStatement,
) -> Result<SqlResult, errors::Error> {
    let table = session.database.find_table(&stmt.table)?;
    let mut _locked_table = table
        .lock()
        .map_err(|_| errors::Error::LockTable("Failed to lock table for delete".to_string()))?;
    // let affected_rows = storage::delete_rows(&mut locked_table, &stmt.where_clause)?;
    let affected_rows = 0;
    Ok(SqlResult::Ok { affected_rows })
}

/// Executes a `CREATE` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `CreateStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::Ok` or an `errors::Error`.
fn execute_create_statement(
    session: &mut session::Session,
    stmt: sql::CreateStatement,
) -> Result<SqlResult, errors::Error> {
    match stmt {
        sql::CreateStatement::CreateDatabaseStatement(db_stmt) => {
            database::Database::create(&db_stmt.name)?;
            Ok(SqlResult::Ok { affected_rows: 0 })
        }
        sql::CreateStatement::CreateTableStatement(table_stmt) => {
            session.database.create_table(&table_stmt.name)?;
            Ok(SqlResult::Ok { affected_rows: 0 })
        }
    }
}

/// Executes a `SHOW` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `ShowStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::ResultSet` or an `errors::Error`.
fn execute_show_statement(
    session: &mut session::Session,
    stmt: sql::ShowStatement,
) -> Result<SqlResult, errors::Error> {
    match stmt {
        sql::ShowStatement::ShowDatabasesStatement => {
            let columns = vec!["Database".to_string()];
            let rows = database::show_databases()?
                .into_iter()
                .map(|db| vec![db])
                .collect();
            Ok(SqlResult::ResultSet { columns, rows })
        }
        sql::ShowStatement::ShowTablesStatement => {
            let columns = vec![format!("Tables_in_{}", &session.database.name)];
            let rows = storage::table::show_tables(&session.database.name)?
                .into_iter()
                .map(|table| vec![table])
                .collect();
            Ok(SqlResult::ResultSet { columns, rows })
        }
    }
}

/// Executes a `DROP` statement.
///
/// # Arguments
/// * `session` - The session context.
/// * `stmt` - The `DropStatement` to execute.
///
/// # Returns
/// A `Result` containing a `SqlResult::Ok` or an `errors::Error`.
fn execute_drop_statement(
    session: &mut session::Session,
    stmt: sql::DropStatement,
) -> Result<SqlResult, errors::Error> {
    match stmt {
        sql::DropStatement::DropDatabasesStatement(name) => {
            if name == session.database.name {
                return Err(errors::Error::Command(
                    "Cannot drop the currently used database".to_string(),
                ));
            }
            database::drop_database(&name)?;
            Ok(SqlResult::Ok { affected_rows: 0 })
        }
        sql::DropStatement::DropTablesStatement(name) => {
            session.database.drop_table(&name)?;
            Ok(SqlResult::Ok { affected_rows: 0 })
        }
    }
}

/// Inserts a row into a table.
///
/// # Arguments
/// * `table` - The table to insert into, wrapped in an `Arc<Mutex<storage::Table>>`.
/// * `row` - The row to insert.
///
/// # Returns
/// A `Result` indicating success or an `errors::Error` if the operation fails.
pub fn execute_insert(
    table: &Arc<Mutex<storage::Table>>,
    row: storage::Row,
) -> Result<(), errors::Error> {
    let mut locked_table = table
        .lock()
        .map_err(|_| errors::Error::LockTable("Failed to lock table for insert".to_string()))?;

    storage::insert_row(&mut locked_table, &row)?;
    Ok(())
}

/// Selects rows from a table.
///
/// # Arguments
/// * `table` - The table to select from, wrapped in an `Arc<Mutex<storage::Table>>`.
///
/// # Returns
/// A `Result` containing a vector of `schema::Row`s or an `errors::Error`.
pub fn execute_select(
    table: &Arc<Mutex<storage::Table>>,
) -> Result<Vec<storage::Row>, errors::Error> {
    let mut locked_table = table
        .lock()
        .map_err(|_| errors::Error::LockTable("Failed to lock table for select".to_string()))?;
    storage::select_rows(&mut locked_table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl;
    use crate::sql;

    // Mock implementations for testing
    fn mock_session() -> session::Session {
        session::Session::open_test().expect("Failed to open testing session")
    }

    fn mock_sql_command(stmt: sql::Statement) -> sql::SqlCommand {
        sql::SqlCommand {
            statement: stmt,
            sql: String::new(),
        }
    }

    #[test]
    fn test_execute_insert() {
        let mut session = mock_session();

        let create_stmt = sql::CreateTableStatement {
            name: "users".to_string(),
            columns_schemas: Vec::new(),
        };
        let command = mock_sql_command(sql::Statement::Create(
            sql::CreateStatement::CreateTableStatement(create_stmt),
        ));
        let result = execute(&mut session, command);
        assert!(result.is_ok());

        let insert_stmt = sql::InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string()],
            values: vec![
                "1".to_string(),
                "John".to_string(),
                "john@mail.com".to_string(),
            ],
        };
        let command = mock_sql_command(sql::Statement::Insert(insert_stmt));
        let result = execute(&mut session, command);

        assert!(result.is_ok());
        if let Ok(SqlResult::Ok { affected_rows }) = result {
            assert_eq!(affected_rows, 1);
        }
    }

    #[test]
    fn test_execute_insert_multiple_pages() {
        let mut session = session::Session::open_test().expect("Failed to open testing session");

        assert!(execute(
            &mut session,
            sql::parser::parse("create table users (ID INT)".into())
                .expect("Failed to build SQL to create users table")
        )
        .is_ok());

        let commands = [
                "insert into users (id, username, email) values (18, 'user18', 'person18@example.com');",
                "insert into users (id, username, email) values (7, 'user7', 'person7@example.com');",
                "insert into users (id, username, email) values (10, 'user10', 'person10@example.com');",
                "insert into users (id, username, email) values (29, 'user29', 'person29@example.com');",
                "insert into users (id, username, email) values (23, 'user23', 'person23@example.com');",
                "insert into users (id, username, email) values (4, 'user4', 'person4@example.com');",
                "insert into users (id, username, email) values (14, 'user14', 'person14@example.com');",
                "insert into users (id, username, email) values (30, 'user30', 'person30@example.com');",
                "insert into users (id, username, email) values (15, 'user15', 'person15@example.com');",
                "insert into users (id, username, email) values (26, 'user26', 'person26@example.com');",
                "insert into users (id, username, email) values (22, 'user22', 'person22@example.com');",
                "insert into users (id, username, email) values (19, 'user19', 'person19@example.com');",
                "insert into users (id, username, email) values (2, 'user2', 'person2@example.com');",
                "insert into users (id, username, email) values (1, 'user1', 'person1@example.com');",
                "insert into users (id, username, email) values (21, 'user21', 'person21@example.com');",
                "insert into users (id, username, email) values (11, 'user11', 'person11@example.com');",
                "insert into users (id, username, email) values (6, 'user6', 'person6@example.com');",
                "insert into users (id, username, email) values (20, 'user20', 'person20@example.com');",
                "insert into users (id, username, email) values (5, 'user5', 'person5@example.com');",
                "insert into users (id, username, email) values (8, 'user8', 'person8@example.com');",
                "insert into users (id, username, email) values (9, 'user9', 'person9@example.com');",
                "insert into users (id, username, email) values (3, 'user3', 'person3@example.com');",
                "insert into users (id, username, email) values (12, 'user12', 'person12@example.com');",
                "insert into users (id, username, email) values (27, 'user27', 'person27@example.com');",
                "insert into users (id, username, email) values (17, 'user17', 'person17@example.com');",
                "insert into users (id, username, email) values (16, 'user16', 'person16@example.com');",
                "insert into users (id, username, email) values (13, 'user13', 'person13@example.com');",
                "insert into users (id, username, email) values (24, 'user24', 'person24@example.com');",
                "insert into users (id, username, email) values (25, 'user25', 'person25@example.com');",
                "insert into users (id, username, email) values (28, 'user28', 'person28@example.com');",
            ];

        for c in commands {
            let q = sql::parser::parse(c.into());
            assert!(q.is_ok(), "Failed to build '{}'", c);
            let r = execute(&mut session, q.unwrap());
            if let Err(err) = r {
                assert!(false, "Command '{}' execute failed with error: {}", c, err);
            } else {
                assert!(true, "Command execute was successful");
            }
        }

        let (total, colums, rows) = session
            .database
            .find_table(&"users".into())
            .unwrap()
            .try_lock()
            .unwrap()
            .build_btree()
            .unwrap();
        println!("{}", repl::console::build_table(&colums, &rows));
        println!("Total nodes: {}", total);
    }
}
