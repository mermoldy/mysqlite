#![allow(warnings)]
#![allow(dead_code)]
mod command;
mod console;
mod database;
mod errors;
mod repl;
mod schema;
mod session;
mod sql;
mod storage;
use clap::Parser;
use std::fs::File;
use std::fs::OpenOptions;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(
    name = "marble",
    version = VERSION,
    about = "Tiny SQL database."
)]
struct Cli {
    /// Execute a command.
    #[arg(short, long)]
    command: Option<String>,
}

fn main() {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("marble.log")
        .expect("Failed to open log file");

    tracing_subscriber::fmt()
        .with_writer(file)
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace")),
        )
        .init();

    let cli = Cli::parse();
    if let Some(statement) = cli.command {
        match sql::parse(statement) {
            Ok(s) => {
                let mut session = session::Session::open().unwrap();
                command::execute(&mut session, s).unwrap();
                session.close().unwrap();
                echo!("\n");
            }
            Err(e) => {
                echo!("\n");
                error!("{}\n", e);
            }
        };
        return;
    }

    match repl::main() {
        Ok(_) => (),
        Err(e) => echo!("Error: {}", e),
    }
}
