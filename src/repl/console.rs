use super::{buffer, prompt, status};
use crate::{command, database, errors, session, sql};
use crossterm::{
    cursor, execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
    terminal::{self, Clear, ClearType},
};
use std::io::{self, Write};
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

?         (\?) Synonym for `help'.
help      (\h) Display this help.
use       (\u) Use another database. Takes database name as argument.
version   (\v) Show version information.
quit      (\q) Quit Marble.
"#;

/// Starts a REPL session in raw console mode.
///
/// # Returns
/// A `Result` indicating success or an `errors::Error` if initialization or cleanup fails.
pub fn start() -> Result<(), errors::Error> {
    let mut session = session::Session::open()?;
    info!(session_id = %session.id, "Starting REPL session...");

    // Enable raw mode and blinking cursor
    terminal::enable_raw_mode()?;
    execute!(io::stdout(), cursor::EnableBlinking)?;

    // Set panic hook to disable raw mode on crash
    INIT.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            if let Err(e) = terminal::disable_raw_mode() {
                eprintln!("Failed to disable raw mode during panic: {}", e);
            }
            eprintln!("Panic occurred: {:?}", info);
        }));
    });

    let mut console = Console::new(&mut session);
    let result = console.start();

    // Ensure raw mode is disabled and session is closed, even on error
    terminal::disable_raw_mode()?;
    session.close().map_err(|e| {
        error!("Failed to close session: {}", e);
        e
    })?;

    info!("REPL session ended.");
    result
}

struct Console<'a> {
    session: &'a mut session::Session,
    prompt: prompt::Prompt,
    status: status::StatusBar,
    buffer: buffer::Buffer,
}

impl<'a> Console<'a> {
    /// Creates a new console instance.
    pub fn new(session: &'a mut session::Session) -> Self {
        Self {
            session,
            prompt: prompt::Prompt::new(),
            status: status::StatusBar::new(),
            buffer: buffer::Buffer::new(),
        }
    }

    /// Draws the initial console layout.
    fn draw(&self) -> io::Result<()> {
        execute!(io::stdout(), Clear(ClearType::All), cursor::MoveTo(0, 0))?;
        io::stdout().flush()?;
        Ok(())
    }

    /// Starts the REPL loop, handling user input and commands.
    pub fn start(&mut self) -> Result<(), errors::Error> {
        self.draw()?;
        echo_line(format!("Welcome to the {} {} REPL.", NAME, VERSION))?;
        echo_lines(BANNER.to_string())?;

        let mut continue_prompt = false;

        loop {
            if !continue_prompt {
                self.buffer.clear();
                self.prompt.start_prompt()?;
            } else {
                self.prompt.continue_prompt()?;
            }
            self.buffer.newline();

            self.update_status()?;
            self.prompt.readline(&mut self.buffer, &mut self.status)?;

            let input = self.buffer.build();
            continue_prompt = self.handle_input(&input)?;
            if !continue_prompt && input.as_str() == "quit" {
                break;
            }
        }

        echo_line("\nBye\n".to_string())?;
        Ok(())
    }

    /// Updates the status bar with current session and buffer information.
    fn update_status(&mut self) -> io::Result<()> {
        self.status.update(
            Some(self.session.database.name.clone()),
            Some(self.prompt.x),
            Some(self.prompt.y),
            Some(self.buffer.len_total()),
        );
        self.status.draw()
    }

    /// Handles user input and returns whether to continue the prompt.
    fn handle_input(&mut self, input: &str) -> Result<bool, errors::Error> {
        match input.trim() {
            "exit" | "quit" | "\\q" => {
                self.prompt.append_line(input);
                return Ok(false);
            }
            cmd if cmd.starts_with("use") || cmd.starts_with("\\u") => self.handle_use(cmd),
            "version" | "\\v" => self.handle_version(input),
            "help" | "\\h" | "\\?" | "?" => self.handle_help(input),
            _ => self.handle_command(input),
        }
    }

    fn handle_use(&mut self, cmd: &str) -> Result<bool, errors::Error> {
        self.prompt.append_line(cmd);
        let dbname = cmd
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| err!(InvalidOperation, "USE must be followed by a database name"))?;
        let dbname = dbname.trim_end_matches(|c: char| c == ';').to_string();
        let db = database::Database::get(&dbname)?;
        self.session.set_database(db)?;
        next_line()?;
        echo_line("Database changed".to_string())?;
        Ok(false)
    }

    fn handle_version(&mut self, cmd: &str) -> Result<bool, errors::Error> {
        self.prompt.append_line(cmd);
        next_line()?;
        echo_line(format!("{} version: {}", NAME, VERSION))?;
        Ok(false)
    }

    fn handle_help(&mut self, cmd: &str) -> Result<bool, errors::Error> {
        self.prompt.append_line(cmd);
        next_line()?;
        echo_lines(HELP.to_string())?;
        Ok(false)
    }

    fn handle_command(&mut self, cmd: &str) -> Result<bool, errors::Error> {
        if cmd.starts_with('\\') {
            next_line()?;
            echo_error(format!("Unrecognized command: {}", cmd))?;
            echo_lines(HELP.to_string())?;
            return Ok(false);
        }

        if cmd.ends_with(';') || cmd.ends_with("\\g") {
            self.prompt.append_line(cmd);
            let start = Instant::now();
            match sql::parser::parse(cmd.to_string()) {
                Ok(sql_cmd) => match command::execute(self.session, sql_cmd) {
                    Ok(result) => {
                        let elapsed = start.elapsed().as_secs_f32();
                        match result {
                            command::SqlResult::Ok { affected_rows } => {
                                next_line()?;
                                echo_line(format!(
                                    "Query OK, {} row{} affected ({:.2} sec)",
                                    affected_rows,
                                    if affected_rows == 1 { "" } else { "s" },
                                    elapsed
                                ))?;
                            }
                            command::SqlResult::ResultSet { columns, rows } => {
                                next_line()?;
                                echo_lines(build_table(&columns, &rows))?;
                                echo_line(format!(
                                    "{} row{} in set ({:.2} sec)",
                                    rows.len(),
                                    if rows.len() == 1 { "" } else { "s" },
                                    elapsed
                                ))?;
                            }
                        }
                    }
                    Err(e) => {
                        next_line()?;
                        echo_error(format!("{}\n", e))?;
                    }
                },
                Err(e) => {
                    next_line()?;
                    echo_error(format!("{}\n", e))?;
                }
            }
            Ok(false)
        } else {
            Ok(true) // Continue prompt for multi-line input
        }
    }
}

/// Echoes a string to the console at the current scroll position.
pub fn echo_line(s: String) -> io::Result<()> {
    execute!(
        io::stdout(),
        cursor::MoveToColumn(0),
        Print(s),
        terminal::Clear(terminal::ClearType::FromCursorDown),
        Print("\n"),
    )?;
    io::stdout().flush()?;
    Ok(())
}

/// Echoes an error message in red to the console.
pub fn echo_error(s: String) -> io::Result<()> {
    execute!(
        io::stdout(),
        cursor::MoveToColumn(0),
        SetForegroundColor(Color::Red),
        Print(s),
        terminal::Clear(terminal::ClearType::FromCursorDown),
        ResetColor
    )?;
    io::stdout().flush()?;
    Ok(())
}

/// Echoes multiple lines to the console, iterating by batches of 10 lines.
///
/// This function processes the input string in batches of 10 lines, scrolling the terminal as needed
/// to ensure all lines are visible while minimizing individual terminal commands.
///
/// # Arguments
/// * `s` - The string containing lines to echo.
///
/// # Returns
/// An `io::Result` indicating success or failure of the terminal operations.
pub fn echo_lines(s: String) -> io::Result<()> {
    let lines: Vec<&str> = s.lines().collect();
    if lines.is_empty() {
        return Ok(());
    }

    let (width, _) = terminal::size()?;
    for line in lines {
        execute!(
            io::stdout(),
            cursor::MoveToColumn(0),
            Print(format!("{:<width$}", line, width = width as usize)),
            Print("\n")
        )?;
    }
    // execute!(io::stdout(), Print("\n"))?;
    io::stdout().flush()?;
    Ok(())
}

/// Moves the cursor to the next line in the terminal, scrolling if necessary.
pub fn next_line() -> io::Result<()> {
    execute!(io::stdout(), Print("\n"))?;
    Ok(())
}

/// Scrolls the terminal if necessary, reserving lines at the bottom.
///
/// # Arguments
/// * `reserved_lines` - Number of lines to reserve at the bottom.
///
/// # Returns
/// The adjusted cursor Y position after scrolling.
pub fn scroll_maybe(reserved_lines: u16) -> io::Result<u16> {
    let (_, y) = cursor::position()?;
    let (_, height) = terminal::size()?;
    let dest = y + reserved_lines;
    if dest >= height {
        let to_scroll = (dest - height + 1).max(1);
        execute!(io::stdout(), terminal::ScrollUp(to_scroll))?;
        Ok(y - to_scroll)
    } else {
        Ok(y)
    }
}

/// Builds an ASCII table from headers and rows.
///
/// # Arguments
/// * `headers` - Column headers.
/// * `rows` - Data rows.
///
/// # Returns
/// A formatted ASCII table as a `String`.
pub fn build_table(headers: &[String], rows: &[Vec<String>]) -> String {
    if headers.is_empty() || rows.is_empty() {
        return String::new();
    }

    let mut result = String::new();
    let column_widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            rows.iter()
                .filter_map(|r| r.get(i))
                .fold(h.len(), |max, cell| max.max(cell.len()))
        })
        .collect();

    // Top border
    result.push_str(&format!(
        "+{}+\n",
        column_widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("+")
    ));

    // Headers
    result.push('|');
    for (i, header) in headers.iter().enumerate() {
        result.push_str(&format!("{:<width$}|", header, width = column_widths[i]));
    }
    result.push('\n');

    // Header separator
    result.push_str(&format!(
        "+{}+\n",
        column_widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("+")
    ));

    // Rows
    for row in rows {
        result.push('|');
        for (i, cell) in row.iter().enumerate() {
            result.push_str(&format!("{:<width$}|", cell, width = column_widths[i]));
        }
        result.push('\n');
    }

    // Bottom border
    result.push_str(&format!(
        "+{}+\n",
        column_widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("+")
    ));

    result
}
