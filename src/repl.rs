/// The REPL (Read-Eval-Print-Loop) module.
use crate::command;
use crate::database;
use crate::errors;
use crate::session;
use crate::sql;
use crossterm::{
    cursor,
    event::{self, KeyCode, KeyEvent, KeyModifiers},
    execute,
    style::{
        self, Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor,
        SetForegroundColor,
    },
    terminal, ExecutableCommand,
};
use dirs;
use std::char;
use std::collections::HashMap;
use std::fmt::format;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Once;
use std::time::Duration;
use std::time::Instant;
use tracing::{error, info, warn};

const NAME: &str = env!("CARGO_PKG_NAME");

static INIT: Once = Once::new();

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

pub struct Prompt {
    x: u16,
    y: u16,
    prompt_y: u16,
    history: Vec<String>,
    history_path: PathBuf,
    history_index: usize,
}

impl Prompt {
    pub fn new() -> Self {
        let history_path = get_home_file(format!(".{}_history", NAME));
        let history = load_history(&history_path);
        let history_index = history.len();

        Prompt {
            x: 0,
            y: 0,
            prompt_y: 0,
            history: history,
            history_path: history_path,
            history_index: history_index,
        }
    }
    pub fn append_line(&mut self, line: &String) {
        self.history.push(line.clone());
        self.history_index += 1;

        match append_history(line, &self.history_path) {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to save history. {}", e);
            }
        }
    }

    /// Start prompt on a newline.
    pub fn start_prompt(&mut self) -> io::Result<()> {
        self.y = 0;

        execute!(
            std::io::stdout(),
            cursor::MoveTo(0, scroll_maybe(2)?),
            style::SetAttribute(style::Attribute::Bold),
            SetForegroundColor(Color::Green),
            style::Print(format!("{}> ", NAME)),
            style::SetAttribute(style::Attribute::Reset),
            terminal::Clear(terminal::ClearType::UntilNewLine)
        )?;

        let (_, y) = cursor::position()?;
        self.prompt_y = y;

        Ok(())
    }

    /// Clear prompt without moving it to a newline.
    pub fn clear_prompt(&mut self) -> io::Result<()> {
        self.y = 0;
        execute!(
            std::io::stdout(),
            cursor::MoveTo(0, self.prompt_y),
            style::SetAttribute(style::Attribute::Bold),
            SetForegroundColor(Color::Green),
            style::Print(format!("{}> ", NAME)),
            style::SetAttribute(style::Attribute::Reset),
            terminal::Clear(terminal::ClearType::FromCursorDown)
        )?;
        Ok(())
    }

    /// Continue existing multiline propmnt.
    pub fn continue_prompt(&mut self) -> io::Result<()> {
        execute!(
            std::io::stdout(),
            cursor::MoveTo(0, scroll_maybe(2)?),
            style::SetAttribute(style::Attribute::Bold),
            SetForegroundColor(Color::Green),
            style::Print(format!("\n{}-> ", " ".repeat(NAME.len() - 1))),
            style::SetAttribute(style::Attribute::Reset),
            terminal::Clear(terminal::ClearType::UntilNewLine)
        )?;
        Ok(())
    }

    pub fn move_to_prompt(&self) -> io::Result<()> {
        execute!(
            std::io::stdout(),
            cursor::MoveTo((NAME.len() + 2) as u16, self.prompt_y),
            terminal::Clear(terminal::ClearType::FromCursorDown)
        );
        Ok(())
    }

    fn scroll_prompt_maybe(&mut self, input: &String) -> io::Result<()> {
        let (width, height) = terminal::size()?;
        let wraps = input.len() as u16 / width;

        if wraps > 0 {
            let dest = self.prompt_y + wraps + 1;
            if dest >= height {
                let mut to_scroll = (dest - height);
                if to_scroll == 0 {
                    to_scroll = 1;
                }
                if to_scroll > 0 {
                    self.prompt_y -= to_scroll;
                    execute!(
                        std::io::stdout(),
                        terminal::ScrollUp(to_scroll),
                        cursor::MoveToRow(self.prompt_y),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn readline(&mut self, buffer: &mut Buffer, status: &mut StatusBar) -> io::Result<()> {
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
                        self.x = 0;
                        self.y += 1;
                        break;
                    }
                    KeyCode::Up => {
                        if self.history_index > 0 {
                            self.history_index -= 1;
                            buffer.clear();
                            buffer.newline();
                            buffer.push_str(self.history[self.history_index].as_str());

                            let input = &buffer.build();

                            self.scroll_prompt_maybe(input);
                            self.clear_prompt();

                            write!(io::stdout(), "{}", input);
                            io::stdout().flush()?;

                            self.x = buffer.len() as u16;
                        }
                    }
                    KeyCode::Down => {
                        self.move_to_prompt()?;

                        if self.history_index < self.history.len().saturating_sub(1) {
                            self.history_index += 1;
                            buffer.clear();
                            buffer.newline();
                            buffer.push_str(self.history[self.history_index].as_str());

                            let input = &buffer.build();

                            self.scroll_prompt_maybe(input);
                            self.clear_prompt();

                            write!(io::stdout(), "{}", input);
                            io::stdout().flush()?;
                        } else {
                            buffer.clear();
                            io::stdout().flush()?;
                        }

                        self.x = buffer.len() as u16;
                    }
                    KeyCode::Backspace => {
                        if buffer.len() > 0 && self.x > 0 {
                            if self.x == buffer.len() as u16 {
                                execute!(std::io::stdout(), cursor::MoveLeft(0));
                                execute!(
                                    std::io::stdout(),
                                    terminal::Clear(terminal::ClearType::UntilNewLine)
                                );

                                buffer.remove(self.x as usize - 1);
                                self.x -= 1;
                                io::stdout().flush()?;
                            } else {
                                buffer.remove(self.x as usize - 1);
                                execute!(
                                    std::io::stdout(),
                                    cursor::MoveToColumn(NAME.len() as u16 + (self.x as u16) + 1),
                                    terminal::Clear(terminal::ClearType::UntilNewLine),
                                );
                                write!(
                                    io::stdout(),
                                    "{}",
                                    &buffer.current()[(self.x as usize) - 1..buffer.len() as usize]
                                );
                                io::stdout().flush()?;
                                self.x -= 1;
                                execute!(
                                    std::io::stdout(),
                                    cursor::MoveToColumn(NAME.len() as u16 + (self.x as u16) + 2)
                                );
                            }
                        }
                    }
                    KeyCode::Left => {
                        if self.x > 0 {
                            execute!(std::io::stdout(), cursor::MoveLeft(0));
                            self.x -= 1;
                        }
                    }
                    KeyCode::Right => {
                        if self.x < buffer.len() {
                            execute!(std::io::stdout(), cursor::MoveRight(0));
                            self.x += 1;
                        }
                    }
                    KeyCode::Esc | KeyCode::Char('c')
                        if modifiers.contains(KeyModifiers::CONTROL) =>
                    {
                        if buffer.len() != 0 {
                            buffer.clear();
                            self.clear_prompt()?;
                        } else {
                            echo("Bye".into());
                            return Err(io::Error::new(io::ErrorKind::Interrupted, "Ctrl-C"));
                        }
                    }
                    KeyCode::Char('d') if modifiers.contains(KeyModifiers::CONTROL) => {
                        echo("Bye".into());
                        return Err(io::Error::new(io::ErrorKind::Interrupted, "Ctrl-C"));
                    }
                    KeyCode::Char('l') if modifiers.contains(KeyModifiers::CONTROL) => {
                        io::stdout().execute(terminal::Clear(terminal::ClearType::FromCursorUp))?;
                        execute!(std::io::stdout(), cursor::MoveToColumn(0))?;
                        execute!(std::io::stdout(), cursor::MoveToRow(0))?;
                        self.start_prompt();
                        write!(io::stdout(), "{}", &buffer.current());
                        io::stdout().flush()?;
                    }
                    KeyCode::Tab => {
                        if let Some(last_word) = buffer.build().split_whitespace().last() {
                            let matches: Vec<&str> = completions
                                .iter()
                                .filter(|c| c.starts_with(&last_word.clone()))
                                .cloned()
                                .collect();
                            if matches.len() == 1 {
                                let completed = &matches[0][last_word.len()..];
                                buffer.push_str(&completed);
                                self.x += completed.len() as u16;
                                write!(io::stdout(), "{}", &completed);
                                io::stdout().flush()?;
                            }
                        }
                    }
                    KeyCode::Char(c) => {
                        if self.x < buffer.len() {
                            buffer.insert(self.x as usize, c);
                            execute!(
                                std::io::stdout(),
                                cursor::MoveToColumn(NAME.len() as u16 + (self.x as u16) + 2)
                            );
                            write!(
                                io::stdout(),
                                "{}",
                                &buffer.current()[(self.x as usize)..buffer.len() as usize]
                            );
                            io::stdout().flush()?;
                            self.x += 1;
                            execute!(
                                std::io::stdout(),
                                cursor::MoveToColumn(NAME.len() as u16 + (self.x as u16) + 2)
                            );
                        } else {
                            buffer.push(c);
                            write!(io::stdout(), "{}", c);
                            io::stdout().flush()?;
                            self.x += 1;
                        }
                    }
                    _ => {}
                }
            }

            status.update(None, Some(self.x), Some(self.y), Some(buffer.len_total()));
            status.draw()?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct StatusBar {
    database: Option<String>,
    x: Option<u16>,
    y: Option<u16>,
    buf: Option<u16>,
}

impl StatusBar {
    pub fn new() -> Self {
        Self {
            database: None,
            x: None,
            y: None,
            buf: None,
        }
    }

    pub fn update(
        &mut self,
        database: Option<String>,
        x: Option<u16>,
        y: Option<u16>,
        buf: Option<u16>,
    ) {
        if let Some(v) = database {
            self.database = Some(v);
        }
        if let Some(v) = x {
            self.x = Some(v);
        }
        if let Some(v) = y {
            self.y = Some(v);
        }
        if let Some(v) = buf {
            self.buf = Some(v);
        }
    }

    pub fn format(&self, width: u16) -> String {
        let status_text = format!(
            "DB: {} | Ln {}, Col {} | Buffer: {}",
            self.database.as_deref().unwrap_or("-"),
            self.x.unwrap_or(0),
            self.y.unwrap_or(0),
            self.buf.unwrap_or(0),
        );

        if status_text.len() as u16 > width {
            let mut truncated = status_text
                .chars()
                .take((width as usize).saturating_sub(3))
                .collect::<String>();
            truncated.push_str("...");
            truncated
        } else {
            status_text
        }
    }

    fn draw(&self) -> io::Result<()> {
        let (width, height) = terminal::size()?;
        let full_line = format!("{:<width$}", self.format(width), width = width as usize);

        execute!(
            std::io::stdout(),
            cursor::SavePosition,
            cursor::MoveTo(0, height - 1),
            terminal::Clear(terminal::ClearType::CurrentLine),
            SetForegroundColor(Color::White),
            SetBackgroundColor(Color::Yellow),
            SetAttribute(Attribute::Bold),
            Print(full_line),
            ResetColor,
            cursor::RestorePosition,
        )?;
        std::io::stdout().flush()?;

        Ok(())
    }
}

fn get_home_file(filename: String) -> PathBuf {
    let mut path = dirs::home_dir().expect("Failed to get home directory");
    path.push(filename);
    path
}

fn append_history(line: &String, path: &PathBuf) -> std::io::Result<()> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "{}", line)?;
    writer.flush()?;
    Ok(())
}

fn load_history(path: &PathBuf) -> Vec<String> {
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

struct Console<'a> {
    session: &'a mut session::Session,
    prompt: Prompt,
    status: StatusBar,
}

struct Buffer {
    lines: Vec<String>,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer {
            lines: Vec::from([String::new()]),
        }
    }

    pub fn clear(&mut self) {
        self.lines.clear();
    }

    pub fn newline(&mut self) {
        self.lines.push(String::new());
    }

    pub fn push(&mut self, ch: char) {
        if let Some(mut l) = self.lines.last_mut() {
            l.push(ch);
        }
    }

    pub fn push_str(&mut self, str: &str) {
        if let Some(mut l) = self.lines.last_mut() {
            l.push_str(str);
        }
    }

    pub fn insert(&mut self, idx: usize, ch: char) {
        if let Some(mut l) = self.lines.last_mut() {
            l.insert(idx, ch);
        }
    }

    pub fn remove(&mut self, idx: usize) {
        if let Some(mut l) = self.lines.last_mut() {
            l.remove(idx);
        }
    }

    pub fn build(&self) -> String {
        self.lines.join(" ").trim().to_string()
    }

    pub fn len(&mut self) -> u16 {
        if let Some(l) = self.lines.last() {
            return l.len() as u16;
        }
        0
    }

    pub fn len_total(&self) -> u16 {
        self.lines.iter().map(|l| l.len() as u16).sum()
    }

    pub fn current(&self) -> String {
        if let Some(l) = self.lines.last() {
            return l.clone();
        }
        String::new()
    }

    pub fn get_char(&self, index: usize) -> Option<char> {
        self.lines.last()?.chars().nth(index)
    }
}

impl<'a> Console<'a> {
    pub fn new(session: &'a mut session::Session) -> Self {
        Console {
            session: session,
            prompt: Prompt::new(),
            status: StatusBar::new(),
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

        let mut buffer = Buffer::new();
        let mut prompt = Prompt::new();

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

            match buffer.build().as_str() {
                "exit" | "quit" | "\\q" => {
                    echo("\nBye\n".into());
                    break;
                }
                i if i.starts_with("use") | i.starts_with("\\u") => {
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
                    echo_error(format!("{} version: {}\n", NAME, VERSION));
                }
                "help" | "\\h" | "\\?" | "?" => {
                    echo_lines(format!("{}\n", HELP));
                }
                (cmd) => {
                    if cmd.starts_with("\\") {
                        echo(format!("Unrecognized command: {}", cmd));
                        echo_lines(format!("{}\n", HELP));
                        continue;
                    }

                    if cmd.ends_with(';') {
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
        let mut to_scroll = (dest - height);
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
