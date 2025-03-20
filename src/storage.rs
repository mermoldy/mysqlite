use super::schema::{self, TableSchema};
use crate::{database, errors};
use bincode::{config, Decode, Encode};
use clap::error;
use heapless::Vec;
use once_cell::sync::Lazy;
use std;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::info;

#[derive(Encode, Decode, Debug)]
pub struct TablespaceHeader {
    /// Number of actual records in the table.
    pub table_n_recs: u32,
    /// First page number.
    pub page_first: u32,
}

#[derive(Encode, Decode, Debug)]
pub struct PageHeader {
    /// Number of actual records in the page.
    pub page_n_recs: u16,
    /// Tracks the total number of records in the heap (including deleted).
    pub page_n_heap: u16,
    /// Offset of free space inside the page.
    pub page_free: u16,
    /// Number of deleted records (garbage).
    pub page_garbage: u16,
    /// Previous page number.
    pub page_prev: u32,
    /// Next page number.
    pub page_next: u32,
}

pub const TABLESPACE_HEADER_SIZE: usize = 16;
pub const PAGE_HEADER_SIZE: usize = 24;

#[derive(Debug)]
pub enum Value {
    Int(i8),
    Str(std::vec::Vec<u8>),
}

pub static SCHEMA: Lazy<schema::TableSchema> = Lazy::new(|| schema::TableSchema {
    columns: vec![
        schema::ColumnSchema {
            name: "id".into(),
            data_type: schema::DataType::INT,
            default: None,
            primary: true,
            nullable: false,
        },
        schema::ColumnSchema {
            name: "username".into(),
            data_type: schema::DataType::VARCHAR(32),
            default: Some("guest".into()),
            primary: false,
            nullable: false,
        },
        schema::ColumnSchema {
            name: "email".into(),
            data_type: schema::DataType::VARCHAR(255),
            default: None,
            primary: true,
            nullable: false,
        },
    ],
    version: 0,
    row_size: 295 + 1,
});

/// Page size 4 kilobytes because it’s the same size as a page used in
/// the virtual memory systems of most computer architectures.
pub const PAGE_SIZE: usize = 4096;

pub const TABLE_MAX_PAGES: usize = 100;

pub struct Table {
    pub name: String,
    pub path: PathBuf,
    pub database: String,
    pub num_rows: u32,
    pub pages: Vec<Arc<Mutex<[u8; PAGE_SIZE]>>, TABLE_MAX_PAGES>,
    pub schema: TableSchema,
}

pub fn decode_header<T: Decode<()>>(bytes: &[u8]) -> Result<T, errors::Error> {
    let (decoded, _): (T, usize) = bincode::decode_from_slice(&bytes, config::standard())
        .map_err(|e| errors::Error::Serialization(format!("Failed to encode header. {}", e)))?;
    Ok(decoded)
}

pub fn encode_header<T: Encode, const N: usize>(header: &T) -> Result<[u8; N], errors::Error> {
    let encoded = match bincode::encode_to_vec(header, config::standard()) {
        Ok(r) => Ok(r),
        Err(e) => Err(errors::Error::Serialization(format!(
            "Failed to decode header. {}",
            e
        ))),
    }?;

    if encoded.len() > N {
        return Err(errors::Error::Serialization(format!(
            "Header size ({}) does not fit within the frame ({}).",
            encoded.len(),
            N
        )));
    }

    let mut header = [0u8; N];
    header[..encoded.len()].copy_from_slice(&encoded);
    Ok(header)
}

impl Table {
    pub fn flush(&mut self) -> Result<(), errors::Error> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)?;

        let tablespace_header: [u8; TABLESPACE_HEADER_SIZE] = encode_header(&TablespaceHeader {
            table_n_recs: self.num_rows,
            page_first: 0,
        })?;
        file.write_all(&tablespace_header)?;

        for i in 0..self.pages.len() {
            let page = match self.pages.get(i) {
                Some(p) => p,
                None => return Err(errors::Error::Db(format!("Memory page {} not found.", i))),
            };

            let mut page_lock = page.lock().unwrap();
            let page_header: [u8; PAGE_HEADER_SIZE] = encode_header(&PageHeader {
                page_n_recs: 0,
                page_n_heap: 0,
                page_free: 0,
                page_garbage: 0,
                page_prev: 0,
                page_next: 0,
            })?;
            file.write_all(&page_header)?;
            file.write_all(&page_lock.as_slice())?;
        }
        info!(
            "Flushed {} rows and {} pages.",
            self.num_rows,
            self.pages.len()
        );

        Ok(())
    }
}

pub fn insert_row(table: &mut Table, row: &[u8]) -> Result<(), errors::Error> {
    if row.len() as u32 != table.schema.row_size {
        return Err(errors::Error::Db(format!(
            "Unexpected row size {}. Table row size is {}.",
            row.len(),
            table.schema.row_size
        )));
    }

    /// Rows should not cross page boundaries. Since pages probably won’t
    /// exist next to each other in memory, this assumption makes it
    /// easier to read/write rows.
    let rows_per_page = PAGE_SIZE as u32 / table.schema.row_size;
    let max_rows = rows_per_page * TABLE_MAX_PAGES as u32;

    if table.num_rows >= max_rows {
        return Err(errors::Error::Db(format!(
            "Table is full ({} rows).",
            table.num_rows
        )));
    }

    let page_num = table.num_rows / rows_per_page;

    // Allocate memory only when we try to access a new page.
    if page_num >= table.pages.len() as u32 {
        let p: [u8; 4096] = [0; 4096];
        if let Err(e) = table.pages.push(Arc::new(p.into())) {}
    }

    let mut page = match table.pages.get(page_num as usize) {
        Some(p) => p,
        None => {
            return Err(errors::Error::Db(
                format!("Memory page {} not found.", page_num).to_owned(),
            ))
        }
    };

    let mut l = 0;
    {
        let page_lock1 = page.lock().unwrap();
        l = page_lock1.len();
    }

    let mut page_lock = page.lock().unwrap();

    let row_offset = table.num_rows % rows_per_page;
    let byte_offset = (row_offset * table.schema.row_size) as usize;
    page_lock[byte_offset..byte_offset + row.len()].copy_from_slice(row);

    table.num_rows += 1;

    Ok(())
}

pub fn select_rows(table: &Table) -> Result<std::vec::Vec<schema::Row>, errors::Error> {
    let mut rows = std::vec::Vec::new();
    let rows_per_page = PAGE_SIZE as u32 / table.schema.row_size;

    for i in 0..table.num_rows {
        let page_num = i / rows_per_page;
        let row_offset = i % rows_per_page;
        let byte_offset = (row_offset * table.schema.row_size) as usize;

        let page = match table.pages.get(page_num as usize) {
            Some(p) => p,
            None => {
                return Err(errors::Error::Db(
                    format!("Memory page {} not found.", page_num).to_owned(),
                ))
            }
        };

        let mut page_lock = page.lock().unwrap();
        let row = schema::deserialize_row(
            &SCHEMA,
            &page_lock[byte_offset..byte_offset + table.schema.row_size as usize],
        )?;
        rows.push(row);
    }

    Ok(rows)
}

pub fn load(database: &String, name: &String) -> Result<Table, errors::Error> {
    let path = PathBuf::from(format!("data/{}/{}.tbd", database, name));
    let mut pages: Vec<Arc<Mutex<[u8; PAGE_SIZE]>>, TABLE_MAX_PAGES> = Vec::new();
    let mut file = std::fs::File::open(&path)?;

    let mut tablespace_header_buf = [0u8; TABLESPACE_HEADER_SIZE];
    file.read_exact(&mut tablespace_header_buf);
    let tablespace_header: TablespaceHeader = decode_header(&tablespace_header_buf)?;

    loop {
        let mut page_header_buf = [0u8; PAGE_HEADER_SIZE];
        let read = file.read(&mut page_header_buf)?;
        if read == 0 {
            break;
        }
        let page_header: PageHeader = decode_header(&page_header_buf)?;

        let mut page_buf: [u8; 4096] = [0; 4096];
        let read = file.read(&mut page_buf)?;
        if read == 0 {
            break;
        }

        pages.push(Arc::new(Mutex::new(page_buf)));
    }

    let mut table = Table {
        name: name.clone(),
        path: path,
        database: database.clone(),
        num_rows: tablespace_header.table_n_recs,
        pages: pages,
        schema: SCHEMA.clone(),
    };
    Ok(table)
}

pub fn create(database: &String, name: &String) -> Result<Table, errors::Error> {
    let mut pages: Vec<Arc<Mutex<[u8; PAGE_SIZE]>>, TABLE_MAX_PAGES> = Vec::new();
    let mut num_rows = 0;

    let path = PathBuf::from(format!("data/{}/{}.tbd", database, name));
    if path.exists() {
        return Err(errors::Error::Db(format!(
            "Table '{}.{}' already exists",
            &database, &name
        )));
    }
    File::create(&path)?;

    let mut table = Table {
        name: name.clone(),
        database: database.clone(),
        path: path,
        num_rows: num_rows,
        pages: pages,
        schema: SCHEMA.clone(),
    };
    Ok(table)
}

pub fn drop(database: &String, name: &String) -> Result<(), errors::Error> {
    let mut pages: Vec<Arc<Mutex<[u8; PAGE_SIZE]>>, TABLE_MAX_PAGES> = Vec::new();
    let mut num_rows = 0;

    let path = PathBuf::from(format!("data/{}/{}.tbd", database, name));
    if !path.exists() {
        return Err(errors::Error::Db(format!(
            "Unknown table '{}.{}'",
            &database, &name
        )));
    }
    std::fs::remove_dir_all(&path)?;
    Ok(())
}

pub fn show_tables(database: &String) -> Result<std::vec::Vec<String>, errors::Error> {
    let path = PathBuf::from(format!("data/{}", database));
    std::fs::create_dir_all(&path)?;
    let mut tables = std::vec::Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(f) = path.file_name() {
            tables.push(f.to_string_lossy().to_string());
        }
    }
    Ok(tables)
}
