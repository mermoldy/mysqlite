use crossterm::{
    cursor,
    event::{self, KeyCode, KeyEvent, KeyModifiers},
    execute,
    style::{self, Color, SetForegroundColor},
    terminal,
};
use std::io::{self, Write};
use std::path::PathBuf;
use tracing::warn;

const NAME: &str = env!("CARGO_PKG_NAME");

/// A terminal prompt structure for handling user input and command history.
#[derive(Debug)]
pub struct Prompt {
    pub x: u16,        // Cursor x position
    pub y: u16,        // Cursor y position
    pub prompt_y: u16, // Prompt starting y position
    history: Vec<String>,
    history_path: PathBuf,
    history_index: usize,
}

impl Prompt {
    /// Creates a new Prompt instance with history loaded from the user's home directory.
    pub fn new() -> Self {
        let history_path = super::history::get_home_file(format!(".{}_history", NAME));
        let history = super::history::load_history(&history_path);
        let history_index = history.len();

        Self {
            x: 0,
            y: 0,
            prompt_y: 0,
            history,
            history_path,
            history_index: history_index,
        }
    }

    /// Appends a line to the command history and saves it to the history file.
    ///
    /// # Arguments
    /// * `line` - The command string to append to history
    pub fn append_line(&mut self, line: &str) {
        if !line.trim().is_empty() {
            self.history.push(line.to_string());
            self.history_index = self.history.len();
            if let Err(e) = super::history::append_history(line, &self.history_path) {
                warn!("Failed to save history: {}", e);
            }
        }
    }

    /// Starts a new prompt on a new line with the application name.
    ///
    /// # Returns
    /// `io::Result<()>` indicating success or failure of the terminal operation
    pub fn start_prompt(&mut self) -> io::Result<()> {
        self.y = 0;
        self.prompt_y = super::console::scroll_maybe(1)?;
        self.render_prompt(format!("{}> ", NAME))?;
        Ok(())
    }

    /// Clears the current prompt and redraws it without moving to a new line.
    ///
    /// # Returns
    /// `io::Result<()>` indicating success or failure of the terminal operation
    pub fn clear_prompt(&mut self) -> io::Result<()> {
        self.y = 0;
        self.render_prompt(format!("{}> ", NAME))?;
        Ok(())
    }

    /// Continues a multi-line prompt with an aligned continuation marker.
    ///
    /// # Returns
    /// `io::Result<()>` indicating success or failure of the terminal operation
    pub fn continue_prompt(&mut self) -> io::Result<()> {
        self.prompt_y = super::console::scroll_maybe(2)? + 1;
        self.render_prompt(format!("{}-> ", " ".repeat(NAME.len() - 1)))?;
        Ok(())
    }

    /// Moves the cursor to the start of the prompt input area.
    ///
    /// # Returns
    /// `io::Result<()>` indicating success or failure of the terminal operation
    pub fn move_to_prompt(&self) -> io::Result<()> {
        execute!(
            io::stdout(),
            cursor::MoveTo((NAME.len() + 2) as u16, self.prompt_y),
            terminal::Clear(terminal::ClearType::FromCursorDown)
        )?;
        Ok(())
    }

    /// Renders the prompt text with styling to the terminal.
    ///
    /// # Arguments
    /// * `text` - The prompt text to render
    fn render_prompt(&self, text: String) -> io::Result<()> {
        execute!(
            io::stdout(),
            cursor::MoveTo(0, self.prompt_y),
            style::SetAttribute(style::Attribute::Bold),
            SetForegroundColor(Color::Green),
            style::Print(text),
            style::SetAttribute(style::Attribute::Reset),
            terminal::Clear(terminal::ClearType::FromCursorDown)
        )
    }

    /// Scrolls the terminal if the input would exceed the screen height.
    ///
    /// # Arguments
    /// * `input` - The current input string to check for wrapping
    fn scroll_prompt_if_needed(&mut self, input: &str) -> io::Result<()> {
        let (width, height) = terminal::size()?;
        let wraps = input.len() as u16 / width + 1;

        if self.prompt_y + wraps >= height {
            let to_scroll = (self.prompt_y + wraps - height + 1).max(1);
            self.prompt_y = self.prompt_y.saturating_sub(to_scroll);
            execute!(
                io::stdout(),
                terminal::ScrollUp(to_scroll),
                cursor::MoveToRow(self.prompt_y)
            )?;
        }
        Ok(())
    }

    /// Reads a line of input from the user with editing capabilities.
    ///
    /// # Arguments
    /// * `buffer` - The buffer to store the input
    /// * `status` - The status bar to update with cursor position
    ///
    /// # Returns
    /// `io::Result<()>` indicating success or an error (e.g., interrupt)
    pub fn readline(
        &mut self,
        buffer: &mut super::buffer::Buffer,
        status: &mut super::status::StatusBar,
    ) -> io::Result<()> {
        const COMPLETIONS: &[&str] = &[
            "help", "exit", "clear", "create", "table", "database", "insert", "select", "into",
            "update", "delete", "from",
        ];

        loop {
            match event::read()? {
                event::Event::Key(KeyEvent {
                    code, modifiers, ..
                }) => match (code, modifiers) {
                    (KeyCode::Enter, _) => {
                        self.x = 0;
                        self.y += 1;
                        break;
                    }
                    (KeyCode::Up, _) if self.history_index > 0 => {
                        self.handle_history(buffer, -1)?;
                    }
                    (KeyCode::Down, _) => {
                        self.handle_history(buffer, 1)?;
                    }
                    (KeyCode::Backspace, _) => {
                        self.handle_backspace(buffer)?;
                    }
                    (KeyCode::Char('b'), KeyModifiers::ALT) => {
                        self.handle_word_left(&buffer)?;
                    }
                    (KeyCode::Char('f'), KeyModifiers::ALT) => {
                        self.handle_word_right(&buffer)?;
                    }
                    (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                        self.handle_interrupt(buffer)?;
                    }
                    (KeyCode::Left, _) if self.x > 0 => {
                        self.handle_right()?;
                    }
                    (KeyCode::Right, _) if self.x < buffer.len() as u16 => {
                        self.handle_left()?;
                    }
                    (KeyCode::Char('d'), KeyModifiers::CONTROL) => {
                        super::console::echo_line("\nBye".into())?;
                        return Err(io::Error::new(io::ErrorKind::Interrupted, "Ctrl-D"));
                    }
                    (KeyCode::Char('l'), KeyModifiers::CONTROL) => {
                        self.handle_clear_screen(buffer)?;
                    }
                    (KeyCode::Tab, _) => {
                        self.handle_tab_completion(buffer, COMPLETIONS)?;
                    }
                    (KeyCode::Char(c), _) => {
                        self.handle_char_input(buffer, c)?;
                    }
                    _ => {}
                },
                _ => {}
            }
            status.update(None, Some(self.x), Some(self.y), Some(buffer.len_total()));
            status.draw()?;
        }
        Ok(())
    }

    /// Handles history navigation (up/down arrow keys).
    ///
    /// # Arguments
    /// * `buffer` - The input buffer to update
    /// * `delta` - Direction to move in history (-1 for up, 1 for down)
    fn handle_history(
        &mut self,
        buffer: &mut super::buffer::Buffer,
        delta: isize,
    ) -> io::Result<()> {
        let new_index = (self.history_index as isize + delta).clamp(0, self.history.len() as isize);
        self.history_index = new_index as usize;

        buffer.clear();
        buffer.newline();

        if self.history_index < self.history.len() {
            buffer.push_str(&self.history[self.history_index]);
        }

        let input = buffer.build();
        self.scroll_prompt_if_needed(&input)?;
        self.clear_prompt()?;

        write!(io::stdout(), "{}", input)?;
        io::stdout().flush()?;
        self.x = buffer.len() as u16;
        Ok(())
    }

    /// Handles interrupt signals (Ctrl+C or Esc).
    ///
    /// # Arguments
    /// * `buffer` - The input buffer to clear if non-empty
    fn handle_interrupt(&mut self, buffer: &mut super::buffer::Buffer) -> io::Result<()> {
        if buffer.len() > 0 {
            buffer.clear();
            self.clear_prompt()?;
        } else {
            super::console::echo_line("\nBye".into())?;
            return Err(io::Error::new(io::ErrorKind::Interrupted, "Ctrl-C"));
        }
        Ok(())
    }

    /// Handles screen clearing (Ctrl+L).
    ///
    /// # Arguments
    /// * `buffer` - The input buffer to redraw
    fn handle_clear_screen(&mut self, buffer: &mut super::buffer::Buffer) -> io::Result<()> {
        execute!(
            io::stdout(),
            terminal::Clear(terminal::ClearType::All),
            cursor::MoveTo(0, 0)
        )?;
        self.start_prompt()?;
        write!(io::stdout(), "{}", buffer.current())?;
        io::stdout().flush()?;
        self.x = buffer.len() as u16;
        Ok(())
    }

    /// Handles tab completion for commands.
    ///
    /// # Arguments
    /// * `buffer` - The input buffer to complete
    /// * `completions` - List of possible command completions
    fn handle_tab_completion(
        &mut self,
        buffer: &mut super::buffer::Buffer,
        completions: &[&str],
    ) -> io::Result<()> {
        if let Some(last_word) = buffer.build().split_whitespace().last() {
            let matches: Vec<_> = completions
                .iter()
                .filter(|c| c.starts_with(last_word))
                .collect();
            if matches.len() == 1 {
                let completed = &matches[0][last_word.len()..];
                buffer.push_str(completed);
                self.x += completed.len() as u16;
                write!(io::stdout(), "{}", completed)?;
                io::stdout().flush()?;
            }
        }
        Ok(())
    }

    /// Handles character input (typing).
    ///
    /// # Arguments
    /// * `buffer` - The input buffer to modify
    /// * `c` - The character to insert
    fn handle_char_input(&mut self, buffer: &mut super::buffer::Buffer, c: char) -> io::Result<()> {
        if self.x < buffer.len() as u16 {
            buffer.insert(self.x as usize, c);
            self.redraw_from_cursor(buffer)?;
        } else {
            buffer.push(c);
            write!(io::stdout(), "{}", c)?;
            io::stdout().flush()?;
        }
        self.x += 1;
        Ok(())
    }

    /// Handles backspace key press to remove characters.
    ///
    /// # Arguments
    /// * `buffer` - The input buffer to modify
    fn handle_backspace(&mut self, buffer: &mut super::buffer::Buffer) -> io::Result<()> {
        if self.x > 0 && buffer.len() > 0 {
            buffer.remove(self.x as usize - 1);
            self.x -= 1;

            let (width, _) = terminal::size()?;
            let (mut x, mut y) = cursor::position()?;

            if x == 0 && self.x > 0 {
                execute!(
                    io::stdout(),
                    cursor::MoveUp(1),
                    cursor::MoveToColumn(width),
                    terminal::Clear(terminal::ClearType::FromCursorDown)
                )?;
                (x, y) = cursor::position()?;
            } else {
                execute!(
                    io::stdout(),
                    cursor::MoveLeft(1),
                    terminal::Clear(terminal::ClearType::FromCursorDown)
                )?;
            }

            write!(io::stdout(), "{}", &buffer.current()[self.x as usize..])?;
            io::stdout().flush()?;
            execute!(io::stdout(), cursor::MoveTo(x - 1, y))?;
        }
        Ok(())
    }

    /// Handles Option+Left (Alt+B) word navigation
    fn handle_word_left(&mut self, buffer: &super::buffer::Buffer) -> io::Result<()> {
        if self.x > 0 {
            let current = buffer.current();
            let chars: Vec<char> = current.chars().collect();
            let mut new_x = self.x as usize;
            let (x, y) = cursor::position()?;
            let (width, _) = terminal::size()?;
            let prompt_offset = (NAME.len() + 2) as u16;

            // Skip trailing delimiters
            while new_x > 0
                && (chars[new_x - 1].is_whitespace()
                    || chars[new_x - 1] == '('
                    || chars[new_x - 1] == ')')
            {
                new_x -= 1;
            }
            // Find start of previous word
            while new_x > 0
                && !(chars[new_x - 1].is_whitespace()
                    || chars[new_x - 1] == '('
                    || chars[new_x - 1] == ')')
            {
                new_x -= 1;
            }

            let moves = self.x - new_x as u16;
            self.x = new_x as u16;

            let abs_pos = prompt_offset + self.x;
            let new_col = abs_pos % width;

            if moves > x {
                let lines_up = (moves - x + width - 1) / width;
                execute!(
                    io::stdout(),
                    cursor::MoveTo(new_col, y.saturating_sub(lines_up))
                )?;
            } else {
                execute!(io::stdout(), cursor::MoveLeft(moves))?;
            }
        }
        Ok(())
    }

    /// Handles right navigation.
    fn handle_right(&mut self) -> io::Result<()> {
        let (x, _) = cursor::position()?;
        let (width, _) = terminal::size()?;

        if x == 0 && self.x > 0 {
            execute!(io::stdout(), cursor::MoveUp(1), cursor::MoveToColumn(width))?;
        } else {
            execute!(io::stdout(), cursor::MoveLeft(1))?;
        }
        self.x -= 1;
        Ok(())
    }

    /// Handles left navigation.
    fn handle_left(&mut self) -> io::Result<()> {
        let (x, _) = cursor::position()?;
        let (width, _) = terminal::size()?;

        if x + 1 >= width {
            execute!(io::stdout(), cursor::MoveDown(1), cursor::MoveToColumn(0))?;
        } else {
            execute!(io::stdout(), cursor::MoveRight(1))?;
        }
        self.x += 1;
        Ok(())
    }
    /// Handles Option+Right (Alt+F) word navigation
    fn handle_word_right(&mut self, buffer: &super::buffer::Buffer) -> io::Result<()> {
        if self.x < buffer.len() as u16 {
            let current = buffer.current();
            let chars: Vec<char> = current.chars().collect();
            let len = chars.len();
            let mut new_x = self.x as usize;
            let (x, y) = cursor::position()?;
            let (width, _) = terminal::size()?;
            let prompt_offset = (NAME.len() + 2) as u16;

            // Skip current word
            while new_x < len
                && !(chars[new_x].is_whitespace() || chars[new_x] == '(' || chars[new_x] == ')')
            {
                new_x += 1;
            }
            // Skip following delimiters
            while new_x < len
                && (chars[new_x].is_whitespace() || chars[new_x] == '(' || chars[new_x] == ')')
            {
                new_x += 1;
            }

            let moves = (new_x as u16) - self.x;
            self.x = new_x as u16;

            let abs_pos = prompt_offset + self.x;
            let new_col = abs_pos % width;

            if x + moves >= width {
                let lines_down = (x + moves) / width;
                execute!(io::stdout(), cursor::MoveTo(new_col, y + lines_down))?;
                self.y = self.y.saturating_add(lines_down);
            } else {
                execute!(io::stdout(), cursor::MoveRight(moves))?;
            }
        }
        Ok(())
    }

    /// Redraws the buffer content from the cursor position.
    ///
    /// # Arguments
    /// * `buffer` - The input buffer to redraw
    fn redraw_from_cursor(&mut self, buffer: &mut super::buffer::Buffer) -> io::Result<()> {
        let (x, y) = cursor::position()?;
        execute!(
            io::stdout(),
            cursor::MoveToColumn(x),
            terminal::Clear(terminal::ClearType::FromCursorDown)
        )?;
        write!(io::stdout(), "{}", &buffer.current()[self.x as usize..])?;
        io::stdout().flush()?;
        execute!(io::stdout(), cursor::MoveTo(x + 1, y))?;
        Ok(())
    }
}
