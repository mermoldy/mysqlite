/// A buffer structure that manages multiple lines of text.
#[derive(Debug, Default)]
pub struct Buffer {
    lines: Vec<String>,
}

impl Buffer {
    /// Creates a new Buffer with a single empty line.
    pub fn new() -> Self {
        Self {
            lines: vec![String::new()],
        }
    }

    /// Removes all lines from the buffer and resets it to a single empty line.
    pub fn clear(&mut self) {
        self.lines = vec![String::new()];
    }

    /// Adds a new empty line to the buffer.
    pub fn newline(&mut self) {
        self.lines.push(String::new());
    }

    /// Appends a character to the current (last) line.
    pub fn push(&mut self, ch: char) {
        self.lines
            .last_mut()
            .expect("Buffer should always have at least one line")
            .push(ch);
    }

    /// Appends a string to the current (last) line.
    pub fn push_str(&mut self, s: &str) {
        self.lines
            .last_mut()
            .expect("Buffer should always have at least one line")
            .push_str(s);
    }

    /// Inserts a character at the specified index in the current line.
    ///
    /// # Panics
    /// Panics if the index is out of bounds for the current line.
    pub fn insert(&mut self, idx: usize, ch: char) {
        let last_line = self
            .lines
            .last_mut()
            .expect("Buffer should always have at least one line");
        assert!(idx <= last_line.len(), "Index out of bounds");
        last_line.insert(idx, ch);
    }

    /// Removes a character at the specified index from the current line.
    ///
    /// # Panics
    /// Panics if the index is out of bounds for the current line.
    pub fn remove(&mut self, idx: usize) {
        let last_line = self
            .lines
            .last_mut()
            .expect("Buffer should always have at least one line");
        assert!(idx < last_line.len(), "Index out of bounds");
        last_line.remove(idx);
    }

    /// Combines all lines into a single String with spaces between lines.
    pub fn build(&self) -> String {
        self.lines
            .as_slice()
            .iter()
            .filter(|line| !line.trim().is_empty())
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>()
            .join(" ")
            .trim()
            .to_string()
    }

    /// Returns the length of the current (last) line.
    pub fn len(&self) -> usize {
        self.lines.last().map_or(0, String::len)
    }

    /// Returns the total length of all lines combined.
    pub fn len_total(&self) -> usize {
        self.lines.iter().map(String::len).sum()
    }

    /// Returns a copy of the current (last) line.
    pub fn current(&self) -> String {
        self.lines.last().cloned().unwrap_or_default()
    }

    /// Gets a character at the specified index from the current line.
    pub fn get_char(&self, index: usize) -> Option<char> {
        self.lines.last()?.chars().nth(index)
    }

    /// Returns the number of lines in the buffer.
    pub fn line_count(&self) -> usize {
        self.lines.len()
    }
}
