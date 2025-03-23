use dirs;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

/// Retrieves a file path in the user's home directory.
///
/// # Arguments
/// * `filename` - The name of the file to create the path for
///
/// # Panics
/// Panics if the home directory cannot be determined.
///
/// # Examples
/// ```
/// let path = get_home_file(".myapp_history".to_string());
/// ```
pub fn get_home_file(filename: String) -> PathBuf {
    dirs::home_dir()
        .expect("Failed to get home directory")
        .join(filename)
}

/// Appends a line to the history file.
///
/// # Arguments
/// * `line` - The line to append to the history file
/// * `path` - The path to the history file
///
/// # Returns
/// An `io::Result<()>` indicating success or failure of the write operation
///
/// # Errors
/// Returns an `io::Error` if the file cannot be opened or written to
pub fn append_history(line: &str, path: &PathBuf) -> io::Result<()> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;

    let mut writer = BufWriter::new(file);
    writeln!(writer, "{}", line.trim())?;
    writer.flush()?;
    Ok(())
}

/// Loads the command history from a file.
///
/// # Arguments
/// * `path` - The path to the history file
///
/// # Returns
/// A vector of strings containing the history lines. Returns an empty vector if the file
/// cannot be opened or read.
///
/// # Examples
/// ```
/// let history_path = get_home_file(".myapp_history".to_string());
/// let history = load_history(&history_path);
/// ```
pub fn load_history(path: &PathBuf) -> Vec<String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };

    let reader = BufReader::new(file);
    reader
        .lines()
        .filter_map(|line| {
            line.ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .collect()
}

/// Ensures the history file exists, creating it if necessary.
///
/// # Arguments
/// * `path` - The path to the history file
///
/// # Returns
/// An `io::Result<()>` indicating success or failure of the operation
///
/// # Errors
/// Returns an `io::Error` if the file cannot be created
pub fn ensure_history_file(path: &PathBuf) -> io::Result<()> {
    if !path.exists() {
        File::create(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_append_and_load_history() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Test appending
        append_history("command1", &path).unwrap();
        append_history("command2", &path).unwrap();

        // Test loading
        let history = load_history(&path);
        assert_eq!(history, vec!["command1", "command2"]);
    }

    #[test]
    fn test_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        let history = load_history(&path);
        assert!(history.is_empty());
    }

    #[test]
    fn test_nonexistent_file() {
        let path = PathBuf::from("/nonexistent/path/test_history");
        let history = load_history(&path);
        assert!(history.is_empty());
    }
}
