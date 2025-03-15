use tracing_subscriber::registry::Data;

use crate::{
    database::{self, Database},
    errors,
};

pub struct Session {
    database: database::Database,
}

impl Session {
    pub fn open() -> Result<Self, errors::Error> {
        Ok(Session {
            database: Database::open("default".into())?,
        })
    }
    pub fn close(&mut self) -> Result<(), errors::Error> {
        self.database.flush()?;
        Ok(())
    }

    pub fn set_database(&mut self, database: database::Database) -> Result<(), errors::Error> {
        self.database.flush()?;
        self.database = database;
        Ok(())
    }
}
