#![allow(warnings)]
#![allow(dead_code)]
mod cmdproc;
mod console;
mod errors;
mod repl;

fn main() {
    match repl::main() {
        Ok(_) => (),
        Err(e) => echo!("Error: {}", e),
    }
}
