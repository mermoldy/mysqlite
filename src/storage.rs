use crate::errors;
use clap::error;
use heapless::Vec;
use std;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::mem;
use std::sync::{Arc, Mutex};
use tracing::info;

#[repr(C)]
#[derive(Debug)]
pub struct TableHeader {
    pub num_rows: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct Row {
    pub id: i8,             // integer
    pub username: [u8; 32], // varchar(32)
    pub email: [u8; 255],   // varchar(255)
}

/// Page size 4 kilobytes because it’s the same size as a page used in
/// the virtual memory systems of most computer architectures.
pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

/// Rows should not cross page boundaries. Since pages probably won’t
/// exist next to each other in memory, this assumption makes it
/// easier to read/write rows.
pub const ROW_SIZE: usize = mem::size_of::<Row>();

pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub const HEADER_SIZE: usize = mem::size_of::<TableHeader>();

pub struct Table {
    pub name: String,
    pub num_rows: usize,
    pub pages: Vec<Arc<Mutex<[u8; PAGE_SIZE]>>, TABLE_MAX_PAGES>,
}

impl Table {
    fn new() -> Self {
        Self {
            name: "default".into(),
            num_rows: 0,
            pages: Vec::new(),
        }
    }
}

impl Table {
    pub fn flush(&mut self) -> Result<(), errors::Error> {
        for i in 0..self.pages.len() {
            let page = match self.pages.get(i) {
                Some(p) => p,
                None => {
                    return Err(errors::Error::Db(
                        format!("Memory page {} not found.", i).to_owned(),
                    ))
                }
            };

            let mut page_lock = page.lock().unwrap();
            std::fs::create_dir_all(format!("data/{}", self.name));
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(format!("data/{}/page.{}.bin", self.name, i))?;

            let mut header = serialize_header(&TableHeader {
                num_rows: self.num_rows as u64,
            });

            file.write_all(&header)?;
            file.write_all(&page_lock.as_slice())?;
        }

        Ok(())
    }
}

pub fn str_to_fixed_bytes<const N: usize>(input: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let bytes = input.as_bytes();
    let len = bytes.len().min(N);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

pub fn serialize_row(row: &Row) -> Vec<u8, ROW_SIZE> {
    let mut buf = Vec::new();
    let bytes = unsafe { std::slice::from_raw_parts(row as *const Row as *const u8, ROW_SIZE) };
    buf.extend_from_slice(bytes);
    buf
}

pub fn serialize_header(row: &TableHeader) -> Vec<u8, HEADER_SIZE> {
    let mut buf = Vec::new();
    let bytes =
        unsafe { std::slice::from_raw_parts(row as *const TableHeader as *const u8, HEADER_SIZE) };
    buf.extend_from_slice(bytes);
    buf
}

pub fn deserialize_row(buffer: &[u8]) -> Row {
    let mut new = Row {
        id: 0,
        username: [0; 32],
        email: [0; 255],
    };
    let mut cursor = Cursor::new(&buffer);
    cursor
        .read_exact(unsafe {
            std::slice::from_raw_parts_mut(&mut new as *mut Row as *mut u8, ROW_SIZE)
        })
        .unwrap();
    new
}

pub fn deserialize_header(buffer: &[u8]) -> TableHeader {
    let mut new = TableHeader { num_rows: 0 };
    let mut cursor = Cursor::new(&buffer);
    cursor
        .read_exact(unsafe {
            std::slice::from_raw_parts_mut(&mut new as *mut TableHeader as *mut u8, HEADER_SIZE)
        })
        .unwrap();
    new
}

pub fn insert_row(table: &mut Table, row: &[u8]) -> Result<(), errors::Error> {
    let page_num = table.num_rows / ROWS_PER_PAGE;

    // Allocate memory only when we try to access a new page.
    if page_num >= table.pages.len() {
        let p: [u8; 4096] = [0; 4096];
        if let Err(e) = table.pages.push(Arc::new(p.into())) {}
    }

    let mut page = match table.pages.get(page_num) {
        Some(p) => p,
        None => {
            return Err(errors::Error::Db(
                format!("Memory page {} not found.", page_num).to_owned(),
            ))
        }
    };

    let row_offset = table.num_rows % ROWS_PER_PAGE;
    let byte_offset = row_offset * ROW_SIZE;

    let mut l = 0;
    {
        let page_lock1 = page.lock().unwrap();
        l = page_lock1.len();
    }

    let mut page_lock = page.lock().unwrap();
    page_lock[byte_offset..byte_offset + row.len()].copy_from_slice(row);

    table.num_rows += 1;

    Ok(())
}

pub fn select_rows(table: &mut Table) -> Result<std::vec::Vec<Row>, errors::Error> {
    let mut rows = std::vec::Vec::new();
    for i in 0..table.num_rows {
        let page_num = i / ROWS_PER_PAGE;
        let row_offset = i % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let page = match table.pages.get(page_num) {
            Some(p) => p,
            None => {
                return Err(errors::Error::Db(
                    format!("Memory page {} not found.", page_num).to_owned(),
                ))
            }
        };

        let mut page_lock = page.lock().unwrap();
        let row = deserialize_row(&page_lock[byte_offset..byte_offset + ROW_SIZE]);
        rows.push(row);
    }

    Ok(rows)
}

pub fn load(name: String) -> Result<Table, errors::Error> {
    let mut pages: Vec<Arc<Mutex<[u8; PAGE_SIZE]>>, TABLE_MAX_PAGES> = Vec::new();
    let mut num_rows = 0;

    std::fs::create_dir_all(format!("data/{}", name));
    for entry in std::fs::read_dir(format!("data/{}", name))? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let mut f = std::fs::File::open(path)?;

            let mut header_buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
            f.read_exact(&mut header_buf);
            let header = deserialize_header(&header_buf);

            let mut buf: [u8; 4096] = [0; 4096];
            f.read(&mut buf);
            pages.push(Arc::new(Mutex::new(buf)));

            num_rows += header.num_rows
        }
    }

    let mut table = Table {
        name: name,
        num_rows: num_rows as usize,
        pages: pages,
    };
    Ok(table)
}
