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
