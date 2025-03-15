use std::path::PathBuf;

use clap::builder::Str;

use crate::{errors, storage};

pub struct Database {
    pub path: PathBuf,
    pub tables: Vec<storage::Table>,
}

impl Database {
    pub fn open(name: String) -> Result<Database, errors::Error> {
        let path = PathBuf::from(format!("data/{}", name));
        std::fs::create_dir_all(&path)?;
        Ok(Database {
            path: path,
            tables: Vec::new(),
        })
    }

    pub fn flush(&mut self) -> Result<(), errors::Error> {
        for t in &mut self.tables {
            t.flush()?;
        }
        Ok(())
    }
}
