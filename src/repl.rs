/// The REPL (Read-Eval-Print-Loop) module.
use crate::command;
use crate::console::{print_continue_prompt, print_prompt};
use crate::database;
use crate::echo;
use crate::echo_lines;
use crate::error;
use crate::errors;
use crate::session;
use crate::sql;
use clap::builder::Str;
use crossterm::{
    cursor,
    event::{self, KeyCode, KeyEvent, KeyModifiers},
    execute, style, terminal, ExecutableCommand,
};
use std::sync::Once;

use dirs;
use std::fmt::format;
use std::fs::File;
use std::io::BufWriter;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::path::PathBuf;
use tracing::info;

static INIT: Once = Once::new();

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

const BANNER: &str = r#"
Use "use DBNAME" to open a database.
Commands end with ; or \g. Type 'help;' or '\h' for help.
"#;

const HELP: &str = r#"List of all Marble commands:
Note that all text commands must be first on line and end with ';'

?         (\h) Synonym for `help'.
use       (\u) Use another database. Takes database name as argument.
quit      (\q) Quit Marble.
r"#;

pub fn repl_loop(
    history: &mut Vec<String>,
    session: &mut session::Session,
) -> Result<(), errors::Error> {
    echo!("Welcome to the Marble {} REPL.\n", VERSION);
    echo_lines!("{}\n", BANNER);

    let mut input = String::new();
    let mut continue_prompt: bool = false;
    let mut history_index = history.len();
    let mut cpos_y = 0;
    let mut cpos_x = 0;

    loop {
        if !continue_prompt {
            input.clear();
            print_prompt()?;

            let (_, y) = cursor::position().unwrap();
            cpos_y = y;
        }
        continue_prompt = false;

        let mut input_line = String::new();
        read_input_line(
            &mut input_line,
            history,
            &mut history_index,
            &mut cpos_x,
            &mut cpos_y,
        )?;

        match input_line.trim() {
            "exit" | "quit" | "\\q" => {
                execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                echo!("\nBye\n");
                break;
            }
            i if i.starts_with("use") | i.starts_with("\\u") => {
                history.push(i.to_string().clone());
                history_index = history.len();
                let dbname = i.split_whitespace().nth(1);

                if let Some(n) = dbname {
                    let n = n.trim_end_matches(|c: char| c == ';');
                    match database::Database::get(&n.to_string()) {
                        Ok(d) => {
                            match session.set_database(d) {
                                Ok(_) => {
                                    echo!("\n");
                                    echo!("Database changed\n");
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
                echo!("\nMarble version: {}\n", VERSION);
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
                }

                if input != "" {
                    input.push_str(" ");
                }
                input.push_str(input_line.as_str());

                if input.ends_with(';') {
                    if !input.trim().is_empty() {
                        history.push(input.clone());
                        history_index = history.len();
                    }

                    match sql::parse(input.clone()) {
                        Ok(s) => {
                            execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
                            echo!("\n");
                            match command::execute(session, s) {
                                Ok(_) => {
                                    echo!("\n");
                                }
                                Err(e) => {
                                    error!("{}\n", e);
                                }
                            }
                        }
                        Err(e) => {
                            echo!("\n");
                            execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
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

fn calculate_wraps(input: &str) -> io::Result<u16> {
    let (width, _) = terminal::size()?;
    let input_len = input.len() as u16;
    Ok(input_len / width)
}

fn read_input_line(
    input: &mut String,
    history: &mut Vec<String>,
    history_index: &mut usize,
    cpos_x: &mut usize,
    cpos_y: &mut u16,
) -> io::Result<()> {
    let completions = vec![
        "help", "exit", "clear", "create", "table", "database", "insert", "select", "into",
        "update", "delete",
    ];

    loop {
        if let event::Event::Key(KeyEvent {
            code, modifiers, ..
        }) = event::read()?
        {
            match code {
                KeyCode::Enter => {
                    *cpos_x = 0;
                    break;
                }
                KeyCode::Up => {
                    if *history_index > 0 {
                        *history_index -= 1;
                        input.clear();
                        input.push_str(history[*history_index].as_str());

                        let wraps = calculate_wraps(input)?;
                        let (_, term_height) = terminal::size()?;

                        execute!(std::io::stdout(), cursor::MoveToColumn(10));
                        execute!(std::io::stdout(), cursor::MoveToRow(*cpos_y))?;
                        execute!(
                            std::io::stdout(),
                            terminal::Clear(terminal::ClearType::FromCursorDown)
                        );
                        write!(io::stdout(), "{}", input);
                        io::stdout().flush()?;

                        if *cpos_y + wraps >= term_height {
                            *cpos_y -= term_height - (*cpos_y + wraps - 1);
                        }
                        *cpos_x = input.len();
                    }
                }
                KeyCode::Down => {
                    execute!(std::io::stdout(), cursor::MoveToColumn(10));
                    execute!(std::io::stdout(), cursor::MoveToRow(*cpos_y))?;
                    execute!(
                        std::io::stdout(),
                        terminal::Clear(terminal::ClearType::FromCursorDown)
                    );

                    let wraps = calculate_wraps(input)?;
                    let (_, term_height) = terminal::size()?;

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

                    if *cpos_y + wraps >= term_height {
                        *cpos_y -= term_height - (*cpos_y + wraps - 1);
                    }
                    *cpos_x = input.len();
                }
                KeyCode::Backspace => {
                    if input.len() > 0 && *cpos_x > 0 {
                        if *cpos_x == input.len() {
                            execute!(std::io::stdout(), cursor::MoveLeft(0));
                            execute!(
                                std::io::stdout(),
                                terminal::Clear(terminal::ClearType::UntilNewLine)
                            );
                        } else {
                            execute!(std::io::stdout(), cursor::MoveLeft(0));
                            execute!(
                                std::io::stdout(),
                                terminal::Clear(terminal::ClearType::UntilNewLine)
                            );
                            write!(io::stdout(), "{}", &input[*cpos_x..input.len()]);
                            execute!(
                                std::io::stdout(),
                                cursor::MoveToColumn(10 + ((*cpos_x - 1) as u16))
                            );
                        }

                        io::stdout().flush()?;
                        input.remove(*cpos_x - 1);
                        *cpos_x -= 1;
                    }
                }
                KeyCode::Left => {
                    if *cpos_x > 0 {
                        execute!(std::io::stdout(), cursor::MoveLeft(0));
                        *cpos_x -= 1;
                    }
                }
                KeyCode::Right => {
                    if *cpos_x < input.len() {
                        execute!(std::io::stdout(), cursor::MoveRight(0));
                        *cpos_x += 1;
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
                KeyCode::Char('l') if modifiers.contains(KeyModifiers::CONTROL) => {
                    io::stdout().execute(terminal::Clear(terminal::ClearType::FromCursorUp))?;
                    execute!(std::io::stdout(), cursor::MoveToColumn(0))?;
                    execute!(std::io::stdout(), cursor::MoveToRow(0))?;
                    print_prompt();
                    *cpos_y = 0;

                    write!(io::stdout(), "{}", &input);
                    execute!(std::io::stdout(), cursor::MoveToColumn(10 + *cpos_x as u16))?;
                    io::stdout().flush()?;
                }
                KeyCode::Tab => {
                    if let Some(last_word) = input.split_whitespace().last() {
                        let matches: Vec<&str> = completions
                            .iter()
                            .filter(|c| c.starts_with(&last_word.clone()))
                            .cloned()
                            .collect();
                        if matches.len() == 1 {
                            let completed = &matches[0][last_word.len()..];
                            input.push_str(&completed);
                            *cpos_x += &completed.len();
                            write!(io::stdout(), "{}", &completed);
                            io::stdout().flush()?;
                        }
                    }
                }
                KeyCode::Char(c) => {
                    if *cpos_x < input.len() {
                        input.insert(*cpos_x, c);
                        execute!(
                            std::io::stdout(),
                            cursor::MoveToColumn(10 + (*cpos_x as u16))
                        );
                        write!(io::stdout(), "{}", &input[*cpos_x..input.len()]);
                        io::stdout().flush()?;
                        *cpos_x += 1;
                        execute!(
                            std::io::stdout(),
                            cursor::MoveToColumn(10 + (*cpos_x as u16))
                        );
                    } else {
                        input.push(c);
                        write!(io::stdout(), "{}", c);
                        io::stdout().flush()?;
                        *cpos_x += 1;
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

    info!("Starting REPL session...");
    INIT.call_once(|| {
        std::panic::set_hook(Box::new(|_| match terminal::disable_raw_mode() {
            Ok(_) => {}
            Err(e) => {}
        }));
    });

    // execute!(
    //     std::io::stdout(),
    //     terminal::EnterAlternateScreen,
    //     terminal::Clear(terminal::ClearType::FromCursorDown),
    // )?;

    execute!(std::io::stdout(), cursor::EnableBlinking);
    io::stdout().execute(terminal::Clear(terminal::ClearType::FromCursorDown))?;

    let mut history: Vec<String> = load_history(".marble_history");
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

    match save_history(history, ".marble_history") {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to save history. {}", e);
        }
    }
    match session.close() {
        Ok(_) => {
            info!("Closed session.");
        }
        Err(e) => {
            error!("Failed to close session. {}", e);
        }
    }

    res
}
