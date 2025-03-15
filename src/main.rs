#![allow(warnings)]
#![allow(dead_code)]
mod command;
mod console;
mod database;
mod errors;
mod repl;
mod session;
mod storage;
use clap::Parser;
use tracing::info;
use tracing_subscriber;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(
    name = "mysqlite",
    version = VERSION,
    about = "Tiny SQL database."
)]
struct Cli {
    /// Execute a command.
    #[arg(short, long)]
    command: Option<String>,
}

fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    if let Some(statement) = cli.command {
        match command::parse(statement.as_str()) {
            Ok(s) => {
                command::execute(s);
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
