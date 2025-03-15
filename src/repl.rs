/// The REPL (Read-Eval-Print-Loop) module.
use crate::command;
use crate::database;
use crate::echo;
use crate::echo_lines;
use crate::error;
use crate::session;

use crate::console::{print_continue_prompt, print_prompt};
use crate::errors;
use crossterm::{
    cursor,
    event::{self, KeyCode, KeyEvent, KeyModifiers},
    execute, style, terminal, ExecutableCommand,
};

use dirs;
use std::fmt::format;
use std::fs::File;
use std::io::BufWriter;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::path::PathBuf;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

const BANNER: &str = r#"
Use "use DBNAME" to open a database.
Commands end with ; or \g. Type 'help;' or '\h' for help.
"#;

const HELP: &str = r#"List of all mySQLite commands:
Note that all text commands must be first on line and end with ';'

?         (\h) Synonym for `help'.
use       (\u) Use another database. Takes database name as argument.
quit      (\q) Quit mySQLite.
r"#;

pub fn repl_loop(
    history: &mut Vec<String>,
    session: &mut session::Session,
) -> Result<(), errors::Error> {
    echo!("Welcome to the mySQLite {} REPL.\n", VERSION);
    echo_lines!("{}\n", BANNER);

    let mut input = String::new();
    let mut continue_prompt: bool = false;
    let mut history_index = history.len();
    let mut cpos = 0;

    loop {
        if !continue_prompt {
            input.clear();
            print_prompt()?;
        }
        continue_prompt = false;

        read_input(&mut input, history, &mut history_index, &mut cpos)?;

        match input.trim() {
            "exit" | "quit" | "\\q" => {
                execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                echo!("\nBye\n");
                break;
            }
            input if input.starts_with("use") | input.starts_with("\\u") => {
                let dbname = input.split_whitespace().nth(1);

                if let Some(n) = dbname {
                    let n = n.trim_end_matches(|c: char| c == ';');

                    echo!("\n");
                    match database::Database::open(n.to_string()) {
                        Ok(d) => {
                            match session.set_database(d) {
                                Ok(_) => {
                                    echo!("Use: {}\n", n);
                                }
                                Err(e) => {
                                    echo!("\n");
                                    error!("Failed to select database. {}\n", e);
                                }
                            };
                        }
                        Err(e) => {
                            echo!("\n");
                            error!("Failed to open database. {}\n", e);
                        }
                    };
                } else {
                    echo!("\n");
                    error!("USE must be followed by a database name\n");
                }
            }
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
                    match command::parse(i) {
                        Ok(s) => {
                            execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                            echo!("\n");
                            command::execute(s);
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
    cpos: &mut usize,
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
                    *cpos = 0;
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

                        *cpos = input.len();
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

                    *cpos = input.len();
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

                        if *cpos > 0 {
                            *cpos -= 1;
                        }
                    }
                }
                KeyCode::Left => {
                    if *cpos > 0 {
                        execute!(std::io::stdout(), cursor::MoveLeft(0));
                        *cpos -= 1;
                    }
                }
                KeyCode::Right => {
                    if *cpos < input.len() {
                        execute!(std::io::stdout(), cursor::MoveRight(0));
                        *cpos += 1;
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
                    if *cpos < input.len() {
                        input.insert(*cpos, c);
                        execute!(std::io::stdout(), cursor::MoveToColumn(10 + (*cpos as u16)));
                        write!(io::stdout(), "{}", &input[*cpos..input.len()]);
                        io::stdout().flush()?;
                        *cpos += 1;
                        execute!(std::io::stdout(), cursor::MoveToColumn(10 + (*cpos as u16)));
                    } else {
                        input.push(c);
                        write!(io::stdout(), "{}", c);
                        io::stdout().flush()?;
                        *cpos += 1;
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn get_home_file(filename: &str) -> PathBuf {
    let mut path = dirs::home_dir().expect("Failed to get home directory");
    path.push(filename);
    path
}

fn save_history(strings: Vec<String>, filename: &str) -> std::io::Result<()> {
    let path = get_home_file(filename);
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);
    for line in strings {
        writeln!(writer, "{}", line)?;
    }
    writer.flush()?;
    Ok(())
}

fn load_history(filename: &str) -> Vec<String> {
    let path = get_home_file(filename);
    match File::open(path) {
        Ok(f) => {
            let reader = io::BufReader::new(f);
            match reader.lines().collect::<Result<Vec<_>, _>>() {
                Ok(l) => l,
                Err(_) => {
                    vec![]
                }
            }
        }
        Err(_) => {
            vec![]
        }
    }
}

pub fn main() -> Result<(), errors::Error> {
    terminal::enable_raw_mode()?;
    execute!(std::io::stdout(), cursor::EnableBlinking);
    io::stdout().execute(terminal::Clear(terminal::ClearType::FromCursorDown))?;

    let mut history: Vec<String> = load_history(".mysqlite_history");
    let mut session = session::Session::open()?;

    let res = match repl_loop(&mut history, &mut session) {
        Ok(_) => {
            terminal::disable_raw_mode()?;
            Ok(())
        }
        Err(e) => {
            terminal::disable_raw_mode()?;
            Err(e)
        }
    };

    match save_history(history, ".mysqlite_history") {
        Ok(_) => {}
        Err(e) => {
            echo!("\n");
            echo!("Failed to save history. {}\n", e);
        }
    }
    match session.close() {
        Ok(_) => {}
        Err(e) => {
            echo!("\n");
            echo!("Failed to close session. {}\n", e);
        }
    }

    res
}
