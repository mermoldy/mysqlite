#![allow(dead_code)]
#[macro_use]
mod errors;
mod command;
mod database;
mod repl;
mod session;
mod sql;
mod storage;
use clap::Parser;
use std::fs::OpenOptions;
use std::io;
use tracing_subscriber::EnvFilter;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(
    name = "mysqlite",
    version = VERSION,
    about = "Tiny SQL database."
)]

struct Cli {
    /// The server host address to bind to. Defaults to 0.0.0.0, allowing connections from any interface.
    #[arg(long, env = "MYSQLITE_HOST", default_value = "0.0.0.0")]
    host: Option<String>,
    /// The server port number to listen on. Defaults to 4012.
    #[arg(long, env = "MYSQLITE_PORT", default_value = "4012")]
    port: Option<u16>,
    /// Start the database server as a standalone process.
    #[arg(long, short, env = "MYSQLITE_SERVER", default_value = "false")]
    server: bool,
}

fn main() {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("mysqlite.log")
        .expect("Failed to open log file");

    tracing_subscriber::fmt()
        .with_writer(file)
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace")),
        )
        .init();

    let cli = Cli::parse();
    if cli.server {
        println!("Server mode is not supported yet.");
        return;
    }

    match repl::console::start() {
        Ok(_) => (),
        Err(errors::Error::Io(e)) if e.kind() == io::ErrorKind::Interrupted => (), // Silence Ctrl+C
        Err(e) => println!("\nError: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_insert_2() {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")),
            )
            .init();

        let mut session = session::Session::open_test().expect("Failed to open testing session");

        assert!(command::execute(
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
            let r = command::execute(&mut session, q.unwrap());
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
