use crate::{database, errors, schema, session, sql, storage};
use clap::builder::Str;
use lazy_static::lazy_static;
use regex::Regex;
use std::any;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tracing::{info, trace};

#[derive(Debug)]
pub enum SqlResult {
    /// OK response from INSERT/UPDATE/DELETE
    Ok { affected_rows: u64 },
    /// Result set from SELECT-like queries
    ResultSet {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

/// Execute a statement.
pub fn execute(
    session: &mut session::Session,
    c: sql::SqlCommand,
) -> Result<SqlResult, errors::Error> {
    match c.statement {
        sql::Statement::Select(s) => {
            let mut t = session.database.find_table(&s.table)?;
            match execute_select(t) {
                Ok(rows) => {
                    if rows.len() == 0 {
                        return Ok(SqlResult::Ok { affected_rows: 0 });
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

                    //  console::echo_lines(format!("{}", console::build_table(&columns, &rows)));
                    Ok(SqlResult::Ok { affected_rows: 0 })
                }
                Err(e) => Err(e),
            }
        }
        sql::Statement::Insert(i) => {
            let mut t = session.database.find_table(&i.table)?;

            let row = schema::build_row(&storage::SCHEMA, &i.columns, &i.values)?;

            execute_insert(t, row)?;
            Ok(SqlResult::Ok { affected_rows: 1 })
        }
        sql::Statement::Update => Ok(SqlResult::Ok { affected_rows: 0 }),
        sql::Statement::Delete => Ok(SqlResult::Ok { affected_rows: 0 }),
        sql::Statement::Create(s) => match s {
            sql::CreateStatement::CreateDatabaseStatement(s) => {
                database::Database::create(&s.name)?;
                Ok(SqlResult::Ok { affected_rows: 0 })
            }
            sql::CreateStatement::CreateTableStatement(s) => {
                session.database.create_table(&s.name)?;
                Ok(SqlResult::Ok { affected_rows: 0 })
            }
        },
        sql::Statement::Show(s) => match s {
            sql::ShowStatement::ShowDatabasesStatement => {
                let headers: Vec<String> = Vec::from(["Database".into()]);
                let rows: Vec<Vec<String>> = database::show_databases()?
                    .into_iter()
                    .map(|x| Vec::from([x]))
                    .collect();
                //  console::echo_lines(format!("{}", console::build_table(&headers, &rows)));
                Ok(SqlResult::Ok { affected_rows: 0 })
            }
            sql::ShowStatement::ShowTablesStatement => {
                let header = format!("Tables_in_{}", &session.database.name);
                let headers = Vec::from([header]);
                let rows: Vec<Vec<String>> = storage::show_tables(&session.database.name)?
                    .into_iter()
                    .map(|x| Vec::from([x]))
                    .collect();
                // console::echo_lines(format!("{}", console::build_table(&headers, &rows)));
                //  println!("{} rows in set (0.00 sec)", rows.len())
                Ok(SqlResult::Ok { affected_rows: 0 })
            }
        },
        sql::Statement::Drop(d) => match d {
            sql::DropStatement::DropDatabasesStatement(name) => {
                if name == session.database.name {
                    //   println!("Cannot drop the currently used database");
                } else {
                    database::drop_database(&name)?;
                    //    println!("Dropped '{}' database", name);
                }
                Ok(SqlResult::Ok { affected_rows: 0 })
            }
            sql::DropStatement::DropTablesStatement(name) => {
                session.database.drop_table(&name)?;
                Ok(SqlResult::Ok { affected_rows: 0 })
            }
        },
    }
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
