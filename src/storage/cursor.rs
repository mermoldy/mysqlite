use super::table;
use crate::errors;

pub const PAGE_SIZE: usize = 4096;

pub struct Cursor<'a> {
    table: &'a mut table::Table,
    pub row_num: u32,
    // Indicates a position one past the last element
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn write_value(&mut self, row: &[u8]) -> Result<(), errors::Error> {
        let num_rows = self.table.num_rows as usize;
        let row_size = self.table.schema.get_row_size();

        let rows_per_page = PAGE_SIZE / row_size;
        let row_offset = num_rows % rows_per_page;
        let byte_offset = row_offset * row_size;

        let page_num = self.row_num / rows_per_page as u32;
        let mut page = table::get_page(self.table, page_num)?.lock().unwrap();
        page[byte_offset..byte_offset + row_size].copy_from_slice(row);

        Ok(())
    }

    pub fn read_value(&mut self, buf: &mut Vec<u8>) -> Result<(), errors::Error> {
        let num_rows = self.row_num as usize;
        let row_size = self.table.schema.get_row_size();

        let rows_per_page = PAGE_SIZE / row_size;
        let row_offset = num_rows % rows_per_page;
        let byte_offset = row_offset * row_size;

        let page_num = self.row_num / rows_per_page as u32;
        let page = table::get_page(self.table, page_num)?.lock().unwrap();

        buf.copy_from_slice(&page[byte_offset..byte_offset + row_size]);
        Ok(())
    }
}

pub fn table_start<'a>(table: &'a mut table::Table) -> Cursor<'a> {
    let row_num = 0;
    let end_of_table = table.num_rows == 0;

    Cursor {
        table,
        row_num,
        end_of_table,
    }
}

pub fn table_end<'a>(table: &'a mut table::Table) -> Cursor<'a> {
    let row_num = table.num_rows;
    let end_of_table = true;

    Cursor {
        table,
        row_num,
        end_of_table,
    }
}

pub fn cursor_advance(cursor: &mut Cursor) {
    cursor.row_num += 1;
    if cursor.row_num >= cursor.table.num_rows {
        cursor.end_of_table = true;
    }
}
