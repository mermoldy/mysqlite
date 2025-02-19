/// The RAW console module.
use crate::cmdproc;
use crate::errors;
use crossterm::{
    cursor,
    event::{self, KeyCode, KeyEvent, KeyModifiers},
    execute, style, terminal, ExecutableCommand,
};

use std::fmt::format;
use std::io::{self, Write};

const NAME: &str = env!("CARGO_PKG_NAME");

pub fn print_prompt() -> io::Result<()> {
    execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
    io::stdout()
        .execute(style::SetAttribute(style::Attribute::Bold))?
        .execute(style::Print(format!("{}> ", NAME)))?
        .execute(style::SetAttribute(style::Attribute::Reset))?;
    io::stdout().flush()?;
    Ok(())
}

pub fn print_continue_prompt() -> io::Result<()> {
    execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
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
        execute!(std::io::stdout(), cursor::MoveToNextLine(0))?;
        io::stdout().flush()?;
    }
    Ok(())
}

pub fn echo(s: String) {
    if let Err(e) = io::stdout().execute(style::Print(format!("{}", s))) {}
    if let Err(e) = io::stdout().flush() {}
    execute!(std::io::stdout(), cursor::MoveToNextLine(0));
}

pub fn error(s: String) {
    if let Err(e) = io::stdout().execute(style::Print(format!("{}", s))) {}
    if let Err(e) = io::stdout().flush() {}
    execute!(std::io::stdout(), cursor::MoveToNextLine(0));
}

pub fn echo_lines(s: String) {
    for l in s.lines() {
        if let Err(e) = io::stdout().execute(style::Print(format!("{}\n", l))) {
            continue;
        }
        execute!(std::io::stdout(), cursor::MoveToNextLine(0));
        if let Err(e) = io::stdout().flush() {
            continue;
        }
    }
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
