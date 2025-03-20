use std::sync::{Arc, Mutex};
use std::{collections::HashMap, path::PathBuf};

use tracing::{debug, info, warn};

use crate::{echo, errors, storage};

pub struct Database {
    pub name: String,
    pub path: PathBuf,
    pub tables: HashMap<String, Arc<Mutex<storage::Table>>>,
}

impl Database {
    pub fn create(name: &String) -> Result<Database, errors::Error> {
        let path = PathBuf::from(format!("data/{}", name));
        if path.exists() {
            return Err(errors::Error::Db(format!(
                "Database '{}' already exists",
                &name
            )));
        }
        std::fs::create_dir_all(&path)?;
        Ok(Database::load(name.clone(), path)?)
    }

    pub fn get(name: &String) -> Result<Database, errors::Error> {
        let path = PathBuf::from(format!("data/{}", name));
        if !path.exists() {
            return Err(errors::Error::Db(format!("Database '{}' not found", &name)));
        }
        Ok(Database::load(name.clone(), path)?)
    }

    pub fn get_or_create(name: &String) -> Result<Database, errors::Error> {
        let path = PathBuf::from(format!("data/{}", name));
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(Database::load(name.clone(), path)?)
    }

    fn load(name: String, path: PathBuf) -> Result<Database, errors::Error> {
        let mut tables = HashMap::new();
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|e| e.to_str()) != Some("tbd") {
                continue;
            }

            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                let table_name = stem.to_string();
                let table = Arc::new(Mutex::new(storage::load(&name, &table_name)?));
                tables.insert(table_name, table);
            }
        }

        Ok(Database {
            name: name,
            path: path,
            tables: tables,
        })
    }

    pub fn create_table(&mut self, name: &String) -> Result<(), errors::Error> {
        let table = Arc::new(Mutex::new(storage::create(&self.name, &name)?));
        self.tables.insert(name.clone(), table);
        Ok(())
    }

    pub fn drop_table(&mut self, name: &String) -> Result<(), errors::Error> {
        match self.tables.remove(&name.clone()) {
            Some(t) => Ok(storage::drop(&self.name, &name)?),
            None => Err(errors::Error::Db(format!(
                "Table '{}.{}' doesn't exist",
                self.name, name
            ))),
        }
    }

    pub fn find_table(
        &mut self,
        name: &String,
    ) -> Result<&Arc<Mutex<storage::Table>>, errors::Error> {
        let path = PathBuf::from(format!("data/{}/{}", self.name, name));
        match self.tables.get(name) {
            Some(t) => Ok(t),
            None => Err(errors::Error::Db(format!(
                "Table '{}.{}' doesn't exist",
                self.name, name
            ))),
        }
    }

    pub fn flush(&mut self) -> Result<(), errors::Error> {
        info!(name = self.name, "Flushing database...");
        for (name, table) in &mut self.tables {
            info!(name, "Flushing table...");
            match table.lock() {
                Ok(mut t) => {
                    match t.flush() {
                        Ok(_) => {
                            info!(name = self.name, "Flushed table.");
                        }
                        Err(e) => {
                            warn!(name = self.name, "Failed to flush table. {}", e);
                        }
                    };
                }
                Err(e) => {
                    warn!(
                        name,
                        "Failed to flush '{}.{}' table. {}", self.name, name, e
                    )
                }
            }
        }
        info!(self.name, "Flushed database.");
        Ok(())
    }
}

pub fn show_databases() -> Result<Vec<String>, errors::Error> {
    let path = PathBuf::from(format!("data/"));
    std::fs::create_dir_all(&path)?;
    let mut res = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(f) = path.file_name() {
            res.push(f.to_string_lossy().to_string());
        }
    }
    Ok(res)
}

pub fn drop_database(name: &String) -> Result<(), errors::Error> {
    let path = PathBuf::from(format!("data/{}", name));
    if !path.exists() {
        return Err(errors::Error::Db(format!("Unknown database '{}'", &name)));
    }
    std::fs::remove_dir_all(&path)?;
    Ok(())
}
