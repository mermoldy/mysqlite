use crate::{errors::Error, storage::engine};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, path::PathBuf};
use tracing::{info, warn};

pub struct Database {
    pub name: String,
    path: PathBuf,
    tables: HashMap<String, Arc<Mutex<engine::Table>>>,
}

impl Database {
    pub fn create(name: &String) -> Result<Self, Error> {
        let path = PathBuf::from(format!("data/{}", name));
        std::fs::create_dir_all(&path)?;

        if path.exists() && std::fs::read_dir(&path)?.next().is_some() {
            return Err(err!(Db, "Database '{}' already exists", name));
        }

        Self::load(name.to_string(), path)
    }

    pub fn get(name: &String) -> Result<Self, Error> {
        let path = PathBuf::from(format!("data/{}", name));
        if !path.exists() {
            return Err(err!(Db, "Database '{}' not found", name));
        }
        Self::load(name.to_string(), path)
    }

    pub fn get_or_create(name: &String) -> Result<Self, Error> {
        let path = PathBuf::from(format!("data/{}", name));
        std::fs::create_dir_all(&path)?;
        Self::load(name.to_string(), path)
    }

    fn load(name: String, path: PathBuf) -> Result<Self, Error> {
        let mut tables = HashMap::new();

        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();

                if path.extension() != Some("tbd".as_ref()) {
                    continue;
                }

                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    let table_name = stem.to_string();
                    let table = engine::load(&name, &table_name)?;
                    tables.insert(table_name, Arc::new(Mutex::new(table)));
                }
            }
        }

        Ok(Self { name, path, tables })
    }

    pub fn create_table(&mut self, name: &String) -> Result<(), Error> {
        if self.tables.contains_key(name) {
            return Err(err!(Db, "Table '{}.{}' already exists", self.name, name));
        }

        let table = engine::create_table(&self.name, name)?;
        self.tables
            .insert(name.to_string(), Arc::new(Mutex::new(table)));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &String) -> Result<(), Error> {
        self.tables
            .remove(name)
            .ok_or_else(|| err!(Db, "Table '{}.{}' doesn't exist", self.name, name))?;
        engine::drop_table(&self.name, name)?;
        Ok(())
    }

    pub fn find_table(&self, name: &String) -> Result<&Arc<Mutex<engine::Table>>, Error> {
        self.tables
            .get(name)
            .ok_or_else(|| err!(Db, "Table '{}.{}' doesn't exist", self.name, name))
    }

    pub fn flush(&self) -> Result<(), Error> {
        info!(name = %self.name, "Flushing database...");

        for (name, table) in &self.tables {
            info!(table = %name, "Flushing table...");
            let mut table = table.lock().map_err(|e| {
                err!(
                    LockTable,
                    "Failed to lock table '{}.{}': {}",
                    self.name,
                    name,
                    e
                )
            })?;

            if let Err(e) = table.flush() {
                warn!(table = %name, "Failed to flush table: {}", e);
            } else {
                info!(table = %name, "Flushed table");
            }
        }

        info!(name = %self.name, "Flushed database");
        Ok(())
    }
}

pub fn show_databases() -> Result<Vec<String>, Error> {
    let path = PathBuf::from("data");
    std::fs::create_dir_all(&path)?;

    let databases = std::fs::read_dir(&path)?
        .filter_map(|entry| {
            entry
                .ok()
                .and_then(|e| e.file_name().to_str().map(|s| s.to_string()))
        })
        .collect();

    Ok(databases)
}

pub fn drop_database(name: &str) -> Result<(), Error> {
    let path = PathBuf::from(format!("data/{}", name));
    if !path.exists() {
        return Err(err!(Db, "Unknown database '{}'", name));
    }
    std::fs::remove_dir_all(&path)?;
    Ok(())
}
