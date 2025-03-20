/// The RAW console module.
use crate::command;
use crate::errors;
use crossterm::{
    cursor,
    event::{self, KeyCode, KeyEvent, KeyModifiers},
    execute, style, terminal, ExecutableCommand,
};
use tracing::info;

use std::fmt::format;
use std::io::{self, Write};

const NAME: &str = env!("CARGO_PKG_NAME");

pub fn print_prompt() -> io::Result<()> {
    execute!(std::io::stdout(), cursor::MoveToColumn(0))?;
    io::stdout()
        .execute(style::SetAttribute(style::Attribute::Bold))?
        .execute(style::Print(format!("{}> ", NAME)))?
        .execute(style::SetAttribute(style::Attribute::Reset))?;
    io::stdout().flush()?;
    Ok(())
}

pub fn print_continue_prompt() -> io::Result<()> {
    execute!(std::io::stdout(), cursor::MoveToColumn(0))?;
    io::stdout()
        .execute(style::SetAttribute(style::Attribute::Bold))?
        .execute(style::Print(format!("\n       -> ")))?
        .execute(style::SetAttribute(style::Attribute::Reset))?;
    io::stdout().flush()?;
    io::stdout().flush()?;
    Ok(())
}

pub fn println(s: String) -> io::Result<()> {
    for l in s.lines() {
        io::stdout().execute(style::Print(format!("{}\n", l)))?;
        execute!(std::io::stdout(), cursor::MoveToColumn(0))?;
        io::stdout().flush()?;
    }
    Ok(())
}

pub fn echo(s: String) {
    if let Err(e) = io::stdout().execute(style::Print(format!("{}", s))) {}
    execute!(std::io::stdout(), cursor::MoveToColumn(0));
    if let Err(e) = io::stdout().flush() {}
}

pub fn error(s: String) {
    if let Err(e) = io::stdout().execute(style::Print(format!("{}", s))) {}
    if let Err(e) = io::stdout().flush() {}
    execute!(std::io::stdout(), cursor::MoveToColumn(0));
}

pub fn echo_lines(s: String) {
    for l in s.lines() {
        if let Err(e) = io::stdout().execute(style::Print(format!("{}\n", l))) {
            continue;
        }
        execute!(std::io::stdout(), cursor::MoveToColumn(0));
        if let Err(e) = io::stdout().flush() {
            continue;
        }
    }
}

pub fn echo_table(headers: &Vec<String>, rows: &[Vec<String>]) -> String {
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

    // Return formatted table
    result
}

#[macro_export]
macro_rules! echo {
    ($($arg:tt)*) => {
        crate::console::echo(format!($($arg)*))
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        crate::console::error(format!($($arg)*))
    };
}

#[macro_export]
macro_rules! echo_lines {
    ($($arg:tt)*) => {
        crate::console::echo_lines(format!($($arg)*))
    };
}
