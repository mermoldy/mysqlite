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
