use super::{buffer, prompt, status};
use crate::command;
use crate::database;
use crate::errors;
use crate::session;
use crate::sql;
use crossterm::{
    cursor, execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
    terminal,
};
use std::char;
use std::io::{self, BufRead, Write};
use std::sync::Once;
use std::time::Instant;
use tracing::{error, info};

static INIT: Once = Once::new();

const NAME: &str = env!("CARGO_PKG_NAME");

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

const BANNER: &str = r#"
Type "use <name>" to open a database.
Commands end with ; or \g. Type 'help;' or '\h' for help.
"#;

const HELP: &str = r#"List of all Marble commands:
Note that all text commands must be first on line and end with ';'

?         (\h) Synonym for `help'.
use       (\u) Use another database. Takes database name as argument.
quit      (\q) Quit Marble.
r"#;

/// Start REPL session in raw console mode.
pub fn start() -> Result<(), errors::Error> {
    let mut session = session::Session::open()?;

    info!(
        session_id = session.id.to_string(),
        "Starting REPL session..."
    );
    terminal::enable_raw_mode()?;
    execute!(std::io::stdout(), cursor::EnableBlinking)?;

    INIT.call_once(|| {
        std::panic::set_hook(Box::new(|_| match terminal::disable_raw_mode() {
            Ok(_) => {}
            Err(e) => {}
        }));
    });

    let mut console = Console::new(&mut session);
    match console.start() {
        Ok(_) => {
            terminal::disable_raw_mode()?;
            Ok(())
        }
        Err(e) => {
            terminal::disable_raw_mode()?;
            Err(e)
        }
    };

    match session.close() {
        Ok(_) => {
            info!("Closed session.");
        }
        Err(e) => {
            error!("Failed to close session. {}", e);
        }
    }

    Ok(())
}

struct Console<'a> {
    session: &'a mut session::Session,
    prompt: prompt::Prompt,
    status: status::StatusBar,
}

impl<'a> Console<'a> {
    pub fn new(session: &'a mut session::Session) -> Self {
        Console {
            session: session,
            prompt: prompt::Prompt::new(),
            status: status::StatusBar::new(),
        }
    }

    pub fn draw(&mut self) -> io::Result<()> {
        execute!(std::io::stdout(), terminal::Clear(terminal::ClearType::All))?;
        std::io::stdout().flush()?;
        Ok(())
    }

    pub fn start(&mut self) -> Result<(), errors::Error> {
        echo(format!("Welcome to the Marble {} REPL.\n", VERSION));
        echo_lines(format!("{}\n", BANNER));

        let mut continue_prompt: bool = false;

        let mut buffer = buffer::Buffer::new();
        let mut prompt = prompt::Prompt::new();

        loop {
            if !continue_prompt {
                buffer.clear();
                prompt.start_prompt()?;
            }
            buffer.newline();
            continue_prompt = false;

            self.status.update(
                Some(self.session.database.name.clone()),
                Some(prompt.x),
                Some(prompt.y),
                Some(buffer.len_total()),
            );
            self.status.draw()?;
            prompt.readline(&mut buffer, &mut self.status)?;
            let input = buffer.build();

            match input.as_str() {
                "exit" | "quit" | "\\q" => {
                    prompt.append_line(&input);
                    echo("\nBye\n".into());
                    break;
                }
                i if i.starts_with("use") | i.starts_with("\\u") => {
                    prompt.append_line(&input);

                    let dbname = i.split_whitespace().nth(1);

                    if let Some(n) = dbname {
                        let n = n.trim_end_matches(|c: char| c == ';');
                        match database::Database::get(&n.to_string()) {
                            Ok(d) => {
                                match self.session.set_database(d) {
                                    Ok(_) => {
                                        echo("Database changed\n".into());
                                    }
                                    Err(e) => {
                                        echo_error(format!("Failed to select database. {}\n", e));
                                    }
                                };
                            }
                            Err(e) => {
                                echo_error(format!("Failed to open database. {}\n", e));
                            }
                        };
                    } else {
                        echo_error("USE must be followed by a database name\n".into());
                    }
                }
                "version" | "\\v" => {
                    prompt.append_line(&input);

                    echo_error(format!("{} version: {}\n", NAME, VERSION));
                }
                "help" | "\\h" | "\\?" | "?" => {
                    prompt.append_line(&input);

                    echo_lines(format!("{}\n", HELP));
                }
                cmd => {
                    if cmd.starts_with("\\") {
                        echo(format!("Unrecognized command: {}", cmd));
                        echo_lines(format!("{}\n", HELP));
                        continue;
                    }

                    if cmd.ends_with(';') {
                        prompt.append_line(&input);

                        match sql::parse(cmd.to_string()) {
                            Ok(s) => {
                                let start = Instant::now();
                                match command::execute(&mut self.session, s) {
                                    Ok(result) => {
                                        let elapsed = start.elapsed().as_secs_f32();
                                        match result {
                                            command::SqlResult::Ok { affected_rows, .. } => {
                                                echo(format!(
                                                    "Query OK, {} row affected ({:.2} sec)\n",
                                                    affected_rows, elapsed
                                                ));
                                            }
                                            command::SqlResult::ResultSet { rows, .. } => {
                                                echo(format!(
                                                    "Query OK, {} row affected ({:.2} sec)\n",
                                                    rows.len(),
                                                    elapsed
                                                ));
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        echo_error(format!("{}\n", e));
                                    }
                                }
                            }
                            Err(e) => {
                                echo_error(format!("{}\n", e));
                            }
                        };
                        continue_prompt = false;
                    } else {
                        prompt.continue_prompt()?;
                        continue_prompt = true;
                    }
                }
            };
        }

        Ok(())
    }
}

pub fn echo(s: String) {
    execute!(
        io::stdout(),
        cursor::MoveTo(0, scroll_maybe(2)? + 1),
        terminal::Clear(terminal::ClearType::UntilNewLine),
        Print(format!("{}", s))
    );
    if let Err(e) = io::stdout().flush() {}
}

pub fn echo_error(s: String) {
    execute!(
        io::stdout(),
        cursor::MoveTo(0, scroll_maybe(2)? + 1),
        terminal::Clear(terminal::ClearType::UntilNewLine),
        SetForegroundColor(Color::Red),
        Print(s),
        ResetColor,
    );
    if let Err(e) = io::stdout().flush() {}
}

pub fn echo_lines(s: String) {
    for l in s.lines() {
        execute!(
            io::stdout(),
            cursor::MoveTo(0, scroll_maybe(2)? + 1),
            terminal::Clear(terminal::ClearType::UntilNewLine),
            Print(format!("{}", l))
        );
    }
    if let Err(e) = io::stdout().flush() {}
}

pub fn scroll_maybe(reserved_lines: u16) -> io::Result<u16> {
    let (_, y) = cursor::position()?;
    let (_, height) = terminal::size()?;
    let dest = y + reserved_lines;
    if dest >= height {
        let mut to_scroll = dest - height;
        if to_scroll == 0 {
            to_scroll = 1;
        }
        execute!(std::io::stdout(), terminal::ScrollUp(to_scroll),)?;
        return Ok(y - to_scroll);
    }
    Ok(y)
}

/// Build an ASCII table.
pub fn build_table(headers: &Vec<String>, rows: &[Vec<String>]) -> String {
    let mut result = String::new();

    // Determine the number of columns
    let num_columns = headers.len();

    // Determine the width of each column by finding the longest string in each
    let mut column_widths = vec![0; num_columns];
    for (i, header) in headers.iter().enumerate() {
        column_widths[i] = header.len();
    }
    for row in rows {
        for (i, col) in row.iter().enumerate() {
            column_widths[i] = column_widths[i].max(col.len());
        }
    }

    // Add top border
    result.push('+');
    for width in &column_widths {
        result.push_str(&"-".repeat(*width));
        result.push('+');
    }
    result.push('\n');

    // Add header rows (if any)
    if !headers.is_empty() {
        result.push('|');
        for (i, header) in headers.iter().enumerate() {
            result.push_str(&format!("{:<width$}|", header, width = column_widths[i]));
        }
        result.push('\n');

        // Add header/footer separator
        result.push('+');
        for width in &column_widths {
            result.push_str(&"-".repeat(*width));
            result.push('+');
        }
        result.push('\n');
    }

    // Add data rows
    for row in rows {
        result.push('|');
        for (i, col) in row.iter().enumerate() {
            result.push_str(&format!("{:<width$}|", col, width = column_widths[i]));
        }
        result.push('\n');
    }

    // Add bottom border
    result.push('+');
    for width in &column_widths {
        result.push_str(&"-".repeat(*width));
        result.push('+');
    }
    result.push('\n');

    result
}
