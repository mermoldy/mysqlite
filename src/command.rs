use crate::{console, database, echo, errors, schema, session, sql, storage};
use clap::builder::Str;
use lazy_static::lazy_static;
use regex::Regex;
use std::any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tracing::{info, trace};

/// Execute a statement.
pub fn execute(session: &mut session::Session, c: sql::SqlCommand) -> Result<(), errors::Error> {
    match c.statement {
        sql::Statement::Select(s) => {
            let mut t = session.database.find_table(&s.table)?;
            match execute_select(t) {
                Ok(rows) => {
                    if rows.len() == 0 {
                        echo!("Empty set (0.00 sec)");
                        return Ok(());
                    }

                    let columns: Vec<String> = match s.columns {
                        sql::Columns::All => {
                            let l = &t.lock().unwrap();
                            l.schema.columns.iter().map(|c| c.name.clone()).collect()
                        }
                        sql::Columns::List(c) => c,
                    };

                    let rows: Vec<Vec<String>> = rows
                        .into_iter()
                        .map(|r| {
                            columns
                                .iter()
                                .map(|c| r.get_column(c).unwrap_or("-".into()))
                                .collect::<Vec<String>>()
                        })
                        .collect();

                    console::echo_lines(format!("{}", console::echo_table(&columns, &rows)));
                    echo!("1 row in set (0.01 sec)")
                }
                Err(e) => echo!("Error: {}", e),
            }
        }
        sql::Statement::Insert(i) => {
            let mut t = session.database.find_table(&i.table)?;

            let row = schema::build_row(&storage::SCHEMA, &i.columns, &i.values)?;

            match execute_insert(t, row) {
                Ok(_) => {
                    echo!("Query OK, 1 row affected (0.01 sec)");
                }
                Err(e) => echo!("Error: {}", e),
            }
        }
        sql::Statement::Update => echo!("This is where we would do an update."),
        sql::Statement::Delete => echo!("This is where we would do a delete."),
        sql::Statement::Create(s) => match s {
            sql::CreateStatement::CreateDatabaseStatement(s) => {
                database::Database::create(&s.name)?;
                echo!("Created database '{}'", &s.name);
            }
            sql::CreateStatement::CreateTableStatement(s) => {
                session.database.create_table(&s.name)?;
                echo!("Query OK, 0 row affected (0.01 sec)");
            }
        },
        sql::Statement::Show(s) => match s {
            sql::ShowStatement::ShowDatabasesStatement => {
                let headers = Vec::from(["Database".into()]);
                let rows: Vec<Vec<String>> = database::show_databases()?
                    .into_iter()
                    .map(|x| Vec::from([x]))
                    .collect();
                console::echo_lines(format!("{}", console::echo_table(&headers, &rows)));
            }
            sql::ShowStatement::ShowTablesStatement => {
                let header = format!("Tables_in_{}", &session.database.name);
                let headers = Vec::from([header]);
                let rows: Vec<Vec<String>> = storage::show_tables(&session.database.name)?
                    .into_iter()
                    .map(|x| Vec::from([x]))
                    .collect();
                console::echo_lines(format!("{}", console::echo_table(&headers, &rows)));
                echo!("{} rows in set (0.00 sec)", rows.len())
            }
        },
        sql::Statement::Drop(d) => match d {
            sql::DropStatement::DropDatabasesStatement(name) => {
                if name == session.database.name {
                    echo!("Cannot drop the currently used database");
                } else {
                    database::drop_database(&name)?;
                    echo!("Dropped '{}' database", name);
                }
            }
            sql::DropStatement::DropTablesStatement(name) => {
                session.database.drop_table(&name)?;
                echo!("{} rows in set (0.00 sec)", 0);
            }
        },
    }

    Ok(())
}

pub fn execute_insert(
    table: &Arc<Mutex<storage::Table>>,
    row: schema::Row,
) -> Result<(), errors::Error> {
    match table.lock() {
        Ok(mut t) => {
            let bin_row = schema::serialize_row(&storage::SCHEMA, row)?;
            storage::insert_row(&mut t, &bin_row)?;
        }
        Err(e) => {
            return Err(errors::Error::LockTable(
                "Failed to acquire table lock".into(),
            ))
        }
    }
    return Ok(());
}

pub fn execute_select(
    table: &Arc<Mutex<storage::Table>>,
) -> Result<Vec<schema::Row>, errors::Error> {
    match table.lock() {
        Ok(t) => {
            let rows = storage::select_rows(&t)?;
            Ok(rows)
        }
        Err(e) => {
            return Err(errors::Error::LockTable(
                "Failed to acquire table lock".into(),
            ))
        }
    }
}
