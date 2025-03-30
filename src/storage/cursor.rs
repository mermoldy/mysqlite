//! # Database Cursor Module
//!
//! Provides cursor-based navigation and manipulation of database tables,
//! enabling traversal and modification of table data.
use super::{btree::NodeType, table};
use crate::errors::Error;
use tracing::{debug, trace};

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
        let page = self.table.pager.get(page_num)?;

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
        let mut cursor = Cursor::find(table, 0)?;

        debug!(
            page_num = cursor.page_num,
            "Created cursor on start of the table."
        );
        let num_cells = cursor
            .table
            .pager
            .get(cursor.page_num)?
            .leaf_node_num_cells()?;
        cursor.end_of_table = num_cells == 0;

        Ok(cursor)
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
            .get(table.root_page_num)?
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
        debug!(key, page_num, "Searching for a cursor position...");
        let root_node_type = table.pager.get(page_num)?.get_node_type()?;

        match root_node_type {
            NodeType::NodeLeaf => Cursor::leaf_node_find(table, page_num, key),
            NodeType::NodeInternal => Cursor::internal_node_find(table, page_num, key),
        }
    }

    pub fn leaf_node_find(
        table: &'a mut table::Table,
        page_num: u32,
        key: u32,
    ) -> Result<Self, Error> {
        let node = table.pager.get(page_num)?;
        let cell_num = node.leaf_node_find(key)?;
        drop(node);

        Ok(Cursor {
            table,
            page_num,
            cell_num,
            end_of_table: false,
        })
    }

    /// This function will perform binary search to find the child that should contain the given
    /// key. Remember that the key to the right of each child pointer is the maximum key contained
    /// by that child.
    pub fn internal_node_find(
        table: &'a mut table::Table,
        page_num: u32,
        key: u32,
    ) -> Result<Self, Error> {
        trace!(page_num, key, "Searching for a position on internal node");

        let child_num = {
            let node = table.pager.get(page_num)?;
            let child_index = node.internal_node_find_child(key)?;
            node.internal_node_child(child_index)?
        };

        let child_node_type = table.pager.get(child_num)?.get_node_type()?;
        trace!(
            child_node_type = child_node_type.to_string(),
            child_num,
            "Found child node"
        );
        match child_node_type {
            NodeType::NodeLeaf => Cursor::leaf_node_find(table, child_num, key),
            NodeType::NodeInternal => Cursor::internal_node_find(table, child_num, key),
        }
    }

    /// Advances the cursor to the next cell
    ///
    /// # Errors
    /// Returns an error if:
    /// - Page cannot be retrieved
    /// - Cannot read number of cells
    pub fn advance(&mut self) -> Result<(), Error> {
        let node = self.table.pager.get(self.page_num)?;

        self.cell_num += 1;
        if self.cell_num >= node.leaf_node_num_cells()? {
            // Advance to next leaf node
            let next_page_num = node.leaf_node_next_leaf()?;
            if next_page_num == 0 {
                // This was rightmost leaf
                self.end_of_table = true;
            } else {
                self.page_num = next_page_num;
                self.cell_num = 0
            }
        }
        Ok(())
    }
}
