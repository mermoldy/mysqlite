#![allow(warnings)]
#![allow(dead_code)]
mod cmdproc;
mod console;
mod errors;
mod pager;
mod repl;

use clap::Parser;

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
    let cli = Cli::parse();

    if let Some(statement) = cli.command {
        match cmdproc::parse(statement.as_str()) {
            Ok(s) => {
                cmdproc::execute(s);
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
