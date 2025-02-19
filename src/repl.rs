/// The REPL (Read-Eval-Print-Loop) module.
use crate::cmdproc;
use crate::echo;
use crate::echo_lines;
use crate::error;

use crate::console::{print_continue_prompt, print_prompt};
use crate::errors;
use crossterm::{
    cursor,
    event::{self, KeyCode, KeyEvent, KeyModifiers},
    execute, style, terminal, ExecutableCommand,
};

use std::fmt::format;
use std::io::{self, Write};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

const BANNER: &str = r#"
Use "open FILENAME" to reopen on a persistent database.
Commands end with ; or \g. Type 'help;' or '\h' for help.
"#;

const HELP: &str = r#"List of all mySQLite commands:
Note that all text commands must be first on line and end with ';'

?         (\?) Synonym for `help'.
open      (\open) Open a persistent database.
quit      (\q) Quit mySQLite.
r"#;

pub fn repl_loop() -> Result<(), errors::Error> {
    echo!("Welcome to the mySQLiteite {} REPL.\n", VERSION);
    echo_lines!("{}\n", BANNER);

    let mut input = String::new();
    let mut continue_prompt: bool = false;
    let mut history: Vec<String> = vec![];
    let mut history_index = 0;

    loop {
        if !continue_prompt {
            input.clear();
            print_prompt()?;
        }
        continue_prompt = false;

        read_input(&mut input, &mut history, &mut history_index)?;

        match input.trim() {
            "exit" | "quit" | "\\q" => {
                execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                echo!("\nBye\n");
                break;
            }
            "open" | "\\o" => {}
            "version" | "\\v" => {
                execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                echo!("\nmySQLite version: {}\n", VERSION);
            }
            "help" | "\\h" | "\\?" | "?" => {
                execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                echo_lines!("\n{}\n", HELP);
            }
            (i) => {
                if i.starts_with("\\") {
                    echo!("\nUnrecognized command: {}", i);
                    echo!("{}", HELP);
                    continue;
                } else if i.ends_with(';') {
                    match cmdproc::parse(i) {
                        Ok(s) => {
                            execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                            echo!("\n");
                            cmdproc::execute(s);
                            echo!("\n");
                        }
                        Err(e) => {
                            execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                            echo!("\n");
                            error!("{}\n", e);
                        }
                    };
                    continue_prompt = false;
                } else {
                    print_continue_prompt();
                    continue_prompt = true;
                }
            }
        };
    }
    Ok(())
}

fn read_input(
    input: &mut String,
    history: &mut Vec<String>,
    history_index: &mut usize,
) -> io::Result<()> {
    loop {
        if let event::Event::Key(KeyEvent {
            code, modifiers, ..
        }) = event::read()?
        {
            match code {
                KeyCode::Enter => {
                    if !input.trim().is_empty() {
                        history.push(input.clone());
                        *history_index = history.len();
                    }
                    break;
                }
                KeyCode::Up => {
                    if *history_index > 0 {
                        *history_index -= 1;
                        input.clear();
                        input.push_str(history[*history_index].as_str());

                        execute!(std::io::stdout(), cursor::MoveToColumn(10));
                        execute!(
                            std::io::stdout(),
                            terminal::Clear(terminal::ClearType::UntilNewLine)
                        );
                        write!(io::stdout(), "{}", input);
                        io::stdout().flush()?;
                    }
                }
                KeyCode::Down => {
                    execute!(std::io::stdout(), cursor::MoveToColumn(10));
                    execute!(
                        std::io::stdout(),
                        terminal::Clear(terminal::ClearType::UntilNewLine)
                    );

                    if *history_index < history.len().saturating_sub(1) {
                        *history_index += 1;
                        input.clear();
                        input.push_str(history[*history_index].as_str());
                        write!(io::stdout(), "{}", input);
                        io::stdout().flush()?;
                    } else {
                        input.clear();
                        io::stdout().flush()?;
                    }
                }

                KeyCode::Backspace => {
                    if input.len() > 0 {
                        execute!(std::io::stdout(), cursor::MoveLeft(0));
                        execute!(
                            std::io::stdout(),
                            terminal::Clear(terminal::ClearType::UntilNewLine)
                        );
                        input.pop();
                        io::stdout().flush()?;
                    }
                }
                KeyCode::Esc | KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
                    execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                    echo!("\nBye\n");
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "Ctrl-C"));
                }
                KeyCode::Char('d') if modifiers.contains(KeyModifiers::CONTROL) => {
                    execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                    echo!("\nBye\n");
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "Ctrl-C"));
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    write!(io::stdout(), "{}", c);
                    io::stdout().flush()?;
                }
                _ => {}
            }
        }
    }

    Ok(())
}

pub fn main() -> Result<(), errors::Error> {
    terminal::enable_raw_mode()?;
    execute!(std::io::stdout(), cursor::EnableBlinking);
    io::stdout().execute(terminal::Clear(terminal::ClearType::FromCursorDown))?;
    match repl_loop() {
        Ok(_) => {
            terminal::disable_raw_mode()?;
            Ok(())
        }
        Err(e) => {
            terminal::disable_raw_mode()?;
            Err(e)
        }
    }
}
