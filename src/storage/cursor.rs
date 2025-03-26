//! # Database Cursor Module
//!
//! Provides cursor-based navigation and manipulation of database tables,
//! enabling traversal and modification of table data.

use super::table;
use crate::errors::Error;

/// Represents a position within a database table
///
/// # Lifetime
/// The cursor borrows a mutable reference to the table for its entire lifetime
pub struct Cursor<'a> {
    /// Reference to the table being navigated
    pub table: &'a mut table::Table,

    /// Current page number in the table
    pub page_num: u32,

    /// Current cell number within the page
    pub cell_num: u32,

    /// Indicates a position one past the last element
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    /// Writes a row value at the current cursor position
    ///
    /// # Arguments
    /// * `row` - Byte slice representing the row data
    ///
    /// # Errors
    /// Returns an error if:
    /// - Page cannot be retrieved
    /// - Writing to the page fails
    // pub fn write_value(&mut self, row: &[u8]) -> Result<(), Error> {
    //     let page_num = self.page_num;
    //     let mut page = self
    //         .table
    //         .pager
    //         .get(page_num)
    //         .map_err(|e| Error::Storage(format!("Failed to get page: {}", e)))?
    //         .lock()
    //         .map_err(|_| Error::Storage("Page lock poisoned".into()))?;

    //     page.set_leaf_node_value(self.cell_num as usize, row)
    //         .map_err(|e| Error::Storage(format!("Failed to write value: {}", e)))
    // }

    /// Reads a row value from the current cursor position
    ///
    /// # Arguments
    /// * `buf` - Buffer to copy the row data into
    ///
    /// # Errors
    /// Returns an error if:
    /// - Page cannot be retrieved
    /// - Reading from the page fails
    pub fn read_value(&mut self, buf: &mut Vec<u8>) -> Result<(), Error> {
        let page_num = self.page_num;
        let page = self
            .table
            .pager
            .get(page_num)
            .map_err(|e| Error::Storage(format!("Failed to get page: {}", e)))?
            .lock()
            .map_err(|_| Error::Storage("Page lock poisoned".into()))?;

        let value = page
            .leaf_node_value(self.cell_num as usize)
            .map_err(|e| Error::Storage(format!("Failed to read value: {}", e)))?;

        buf.clear();
        buf.extend_from_slice(value);
        Ok(())
    }

    /// Creates a new cursor positioned at the start of the table
    ///
    /// # Arguments
    /// * `table` - Mutable reference to the table
    ///
    /// # Returns
    /// A new `Cursor` positioned at the table's first element
    pub fn start(table: &'a mut table::Table) -> Result<Self, Error> {
        let page_num = table.root_page_num;
        let cell_num = 0; // Start from the first cell (0-indexed)

        let num_cells = table
            .pager
            .get(table.root_page_num)
            .map_err(|e| Error::Storage(format!("Failed to get root page: {}", e)))?
            .lock()
            .map_err(|_| Error::Storage("Root page lock poisoned".into()))?
            .leaf_node_num_cells()?;

        let end_of_table = num_cells == 0;

        Ok(Cursor {
            table,
            page_num,
            cell_num,
            end_of_table,
        })
    }

    /// Creates a new cursor positioned at the end of the table
    ///
    /// # Arguments
    /// * `table` - Mutable reference to the table
    ///
    /// # Returns
    /// A new `Cursor` positioned after the last element
    pub fn end(table: &'a mut table::Table) -> Result<Self, Error> {
        let page_num = table.root_page_num;
        let cell_num = table
            .pager
            .get(table.root_page_num)
            .map_err(|e| Error::Storage(format!("Failed to get root page: {}", e)))?
            .lock()
            .map_err(|_| Error::Storage("Root page lock poisoned".into()))?
            .leaf_node_num_cells()?;

        Ok(Cursor {
            table,
            page_num,
            cell_num,
            end_of_table: true,
        })
    }

    /// Find a new cursor position for a given key ID.
    ///
    /// # Arguments
    /// * `table` - Mutable reference to the table
    /// * `key` - Key to find a position for
    ///
    /// # Returns
    /// A new `Cursor` positioned to a given key
    pub fn find(table: &'a mut table::Table, key: u32) -> Result<Self, Error> {
        let page_num = table.root_page_num;
        // let root_node = table::get_page(table, table.root_page_num)
        //     .map_err(|e| Error::Storage(format!("Failed to get root page: {}", e)))?
        //     .lock()
        //     .map_err(|_| Error::Storage("Root page lock poisoned".into()))?;

        // match root_node {
        //     LeafNode => Cursor::leaf_node_find(table, page_num, key),
        //     _ => Error::Storage("Need to implement searching an internal node".into()),
        // }

        Cursor::leaf_node_find(table, page_num, key)
    }

    pub fn leaf_node_find(
        table: &'a mut table::Table,
        page_num: u32,
        key: u32,
    ) -> Result<Self, Error> {
        let mut cell_num: Option<u32> = None;

        {
            let node = table
                .pager
                .get(page_num)
                .map_err(|e| Error::Storage(format!("Failed to get page: {}", e)))?
                .lock()
                .map_err(|_| Error::Storage("Root page lock poisoned".into()))?;

            let mut min_index = 0;
            let mut one_past_max_index = node.leaf_node_num_cells()?;

            // Binary search
            while one_past_max_index != min_index {
                let index = (min_index + one_past_max_index) / 2;
                let key_at_index = node.leaf_node_key(index as usize)?;
                if key == key_at_index {
                    cell_num = Some(index);
                    break;
                }

                if key < key_at_index {
                    one_past_max_index = index;
                } else {
                    min_index = index + 1;
                }
            }

            if cell_num.is_none() {
                cell_num = Some(min_index);
            }
        }

        return Ok(Cursor {
            table,
            page_num,
            cell_num: cell_num.unwrap_or(0),
            end_of_table: false,
        });
    }

    /// Advances the cursor to the next cell
    ///
    /// # Errors
    /// Returns an error if:
    /// - Page cannot be retrieved
    /// - Cannot read number of cells
    pub fn advance(&mut self) -> Result<(), Error> {
        let node = self
            .table
            .pager
            .get(self.page_num)
            .map_err(|e| Error::Storage(format!("Failed to get page: {}", e)))?
            .lock()
            .map_err(|_| Error::Storage("Page lock poisoned".into()))?;

        self.cell_num += 1;
        if self.cell_num >= node.leaf_node_num_cells()? {
            self.end_of_table = true;
        }
        Ok(())
    }
}
