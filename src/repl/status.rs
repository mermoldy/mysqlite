use crossterm::{
    cursor, execute,
    style::{
        Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    },
    terminal,
};
use std::io::{self, Write};

/// A status bar for displaying application state in a terminal interface.
///
/// The status bar shows information such as the current database, cursor position,
/// and buffer size, rendered at the bottom of the terminal.
#[derive(Debug, Default)]
pub struct StatusBar {
    database: Option<String>,
    x: Option<u16>,
    y: Option<u16>,
    buf: Option<usize>,
}

impl StatusBar {
    /// Creates a new `StatusBar` with all fields initialized to `None`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates the status bar with new values.
    ///
    /// # Arguments
    /// * `database` - Optional name of the current database
    /// * `x` - Optional x-coordinate of the cursor
    /// * `y` - Optional y-coordinate of the cursor
    /// * `buf` - Optional buffer size
    ///
    /// Only provided (Some) values will update the corresponding fields.
    pub fn update(
        &mut self,
        database: Option<String>,
        x: Option<u16>,
        y: Option<u16>,
        buf: Option<usize>,
    ) {
        self.database = database.or(self.database.clone());
        self.x = x.or(self.x);
        self.y = y.or(self.y);
        self.buf = buf.or(self.buf);
    }

    /// Formats the status bar content into a string suitable for display.
    ///
    /// # Arguments
    /// * `width` - The maximum width of the terminal in characters
    ///
    /// # Returns
    /// A formatted string, truncated with ellipsis if it exceeds the width
    fn format(&self, width: u16) -> String {
        let status_text = format!(
            "DB: {} | Ln {}, Col {} | Buffer: {}",
            self.database.as_deref().unwrap_or("-"),
            self.y.unwrap_or(0) + 1, // Line numbers typically start at 1
            self.x.unwrap_or(0) + 1, // Column numbers typically start at 1
            self.buf.unwrap_or(0),
        );

        let width = width as usize;
        if status_text.len() > width {
            let take_len = width.saturating_sub(3).max(0);
            format!(
                "{}...",
                status_text.chars().take(take_len).collect::<String>()
            )
        } else {
            status_text
        }
    }

    /// Draws the status bar at the bottom of the terminal.
    ///
    /// # Returns
    /// An `io::Result<()>` indicating success or failure of the terminal operation
    ///
    /// # Errors
    /// Returns an `io::Error` if terminal operations (e.g., size query, cursor movement) fail
    pub fn draw(&self) -> io::Result<()> {
        let (width, height) = terminal::size()?;
        let formatted = self.format(width);
        let full_line = format!("{:<width$}", formatted, width = width as usize);

        execute!(
            io::stdout(),
            cursor::SavePosition,
            cursor::MoveTo(0, height.saturating_sub(1)), // Ensure we don't go negative
            terminal::Clear(terminal::ClearType::CurrentLine),
            SetForegroundColor(Color::White),
            SetBackgroundColor(Color::DarkGrey), // Changed to DarkGrey for better contrast
            SetAttribute(Attribute::Bold),
            Print(&full_line),
            ResetColor,
            cursor::RestorePosition,
        )?;
        io::stdout().flush()?;
        Ok(())
    }

    /// Clears all status bar fields back to their initial state.
    pub fn clear(&mut self) {
        self.database = None;
        self.x = None;
        self.y = None;
        self.buf = None;
    }

    /// Gets the current database name, if set.
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Gets the current cursor x-position, if set.
    pub fn x(&self) -> Option<u16> {
        self.x
    }

    /// Gets the current cursor y-position, if set.
    pub fn y(&self) -> Option<u16> {
        self.y
    }

    /// Gets the current buffer size, if set.
    pub fn buf(&self) -> Option<usize> {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_truncation() {
        let mut bar = StatusBar::new();
        bar.update(Some("mydb".to_string()), Some(5), Some(3), Some(42));
        let formatted = bar.format(20);
        assert!(formatted.len() <= 20);
        assert!(formatted.contains("...") || formatted.len() <= 20);
    }

    #[test]
    fn test_update_partial() {
        let mut bar = StatusBar::new();
        bar.update(Some("test".to_string()), None, Some(2), None);
        assert_eq!(bar.database(), Some("test"));
        assert_eq!(bar.y(), Some(2));
        assert_eq!(bar.x(), None);
        assert_eq!(bar.buf(), None);
    }

    #[test]
    fn test_clear() {
        let mut bar = StatusBar::new();
        bar.update(Some("mydb".to_string()), Some(1), Some(2), Some(10));
        bar.clear();
        assert!(bar.database().is_none());
        assert!(bar.x().is_none());
        assert!(bar.y().is_none());
        assert!(bar.buf().is_none());
    }
}
