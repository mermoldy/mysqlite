use super::btree;
use super::btree::Node;
use super::btree::NodeType;
use super::column::ColumnType;
use super::cursor;
use super::encoding;
use super::row;
use super::schema::{ColumnSchema, TableSchema};
use crate::errors::Error;
use bincode::{config, Decode, Encode};
use heapless;
use once_cell::sync::Lazy;
use std;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, trace, warn};

#[derive(Encode, Decode, Debug)]
pub struct TablespaceHeader {
    /// Number of actual records in the table.
    pub table_n_recs: u32,
    /// First page number.
    pub page_first: u32,
    /// Number of the root page
    pub root_page_num: u32,
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

/// Page size 4 kilobytes because it’s the same size as a page used in
/// the virtual memory systems of most computer architectures.
pub const PAGE_SIZE: usize = 4096;

pub const TABLE_MAX_PAGES: usize = 100;

pub static SCHEMA: Lazy<TableSchema> = Lazy::new(|| TableSchema {
    columns: vec![
        ColumnSchema {
            name: "id".into(),
            type_: ColumnType::INT,
            default: None,
            is_primary: true,
            is_nullable: false,
        },
        ColumnSchema {
            name: "username".into(),
            type_: ColumnType::VARCHAR(32),
            default: Some("guest".into()),
            is_primary: false,
            is_nullable: false,
        },
        ColumnSchema {
            name: "email".into(),
            type_: ColumnType::VARCHAR(255),
            default: None,
            is_primary: true,
            is_nullable: false,
        },
    ],
    version: 0,
});

pub struct Table {
    pub name: String,
    pub path: PathBuf,
    pub database: String,
    pub root_page_num: u32,
    pub pager: Pager,
    pub schema: TableSchema,
}

pub struct Pager {
    pages: heapless::Vec<Arc<Mutex<btree::Node>>, TABLE_MAX_PAGES>,
    row_size: u32,
}

impl Pager {
    pub fn new(row_size: u32) -> Self {
        let pages: heapless::Vec<Arc<Mutex<Node>>, TABLE_MAX_PAGES> = heapless::Vec::new();
        Pager { pages, row_size }
    }

    pub fn push(&mut self, node: Node) {
        if let Err(_) = self.pages.push(Arc::new(Mutex::new(node))) {}
    }

    pub fn try_create(&mut self, page_num: u32) -> Result<(), Error> {
        if page_num >= self.pages.len() as u32 {
            let p: [u8; 4096] = [0; 4096];
            let mut n = btree::Node::new(&p, self.row_size as usize);
            n.set_node_type(btree::NodeType::NodeLeaf);
            n.set_leaf_node_num_cells(0);
            n.set_node_root(self.pages.len() == 0);
            if let Err(_) = self.pages.push(Arc::new(Mutex::new(n))) {}
        }
        Ok(())
    }

    pub fn get(&self, page_num: u32) -> Result<MutexGuard<btree::Node>, Error> {
        let node_arc = match self.pages.get(page_num as usize) {
            Some(p) => p,
            None => {
                return Err(Error::Storage(
                    format!("Memory page {} not found.", page_num).to_owned(),
                ));
            }
        };

        node_arc
            .try_lock()
            .map_err(|_| Error::LockTable("Failed to lock the node".to_string()))
    }

    pub fn get_or_create(&mut self, page_num: u32) -> Result<MutexGuard<btree::Node>, Error> {
        self.try_create(page_num)?;
        self.get(page_num)
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn get_node_max_key(&self, node: &Node) -> Result<u32, Error> {
        if node.get_node_type()? == NodeType::NodeLeaf {
            let key = node.leaf_node_key(node.leaf_node_num_cells()? as usize - 1)?;
            return Ok(key);
        }
        let right_child = self.get(node.internal_node_right_child()?)?;
        return self.get_node_max_key(&right_child);
    }

    /// For now, we’re assuming that in a database with N pages, page numbers 0
    /// through N-1 are allocated. Therefore we can always allocate page number N
    /// for new pages. Eventually after we implement deletion, some pages may become
    /// empty and their page numbers unused. To be more efficient, we could re-allocate
    /// those free pages.
    pub fn get_unused_page_num(&self) -> usize {
        self.pages.len()
    }

    pub fn table_n_recs(&self) -> Result<u32, Error> {
        let mut total = 0;
        for i in 0..self.pages.len() {
            let node = self.get(i as u32)?;
            if node.get_node_type()? == NodeType::NodeLeaf {
                total += node.leaf_node_num_cells()?;
            }
        }
        Ok(total)
    }
}

impl Table {
    pub fn flush(&mut self) -> Result<(), Error> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)?;

        let tablespace_header: [u8; TABLESPACE_HEADER_SIZE] = encode_header(&TablespaceHeader {
            page_first: 0,
            table_n_recs: self.pager.table_n_recs()?,
            root_page_num: self.root_page_num,
        })?;
        file.write_all(&tablespace_header)?;

        for i in 0..self.pager.len() {
            let page = match self.pager.get(i as u32) {
                Ok(p) => p,
                Err(_) => return Err(Error::Storage(format!("Memory page {} not found.", i))),
            };

            let page_header: [u8; PAGE_HEADER_SIZE] = encode_header(&PageHeader {
                page_n_recs: 0,
                page_n_heap: 0,
                page_free: 0,
                page_garbage: 0,
                page_prev: 0,
                page_next: 0,
            })?;
            file.write_all(&page_header)?;
            file.write_all(&page.as_slice())?;
        }
        info!("Flushed {} pages.", self.pager.len());

        Ok(())
    }

    /// Builds a B-tree representation as columns and rows for all pages.
    ///
    /// Iterates over pages 0, 1, and 2 (or all available pages up to 3), collecting data from each.
    /// Returns a tuple of (columns, rows) where:
    /// - `columns` is a `Vec<String>` of column headers: "Page", "Index", "Key".
    /// - `rows` is a `Vec<Vec<String>>` of row data, with each row representing a cell from a page.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if a page cannot be accessed or if node data cannot be read.
    pub fn build_btree(&self) -> Result<(usize, Vec<String>, Vec<Vec<String>>), Error> {
        let total = self.pager.len();
        let columns = vec![
            "Type".to_string(),
            "Page".to_string(),
            "Index".to_string(),
            "Key".to_string(),
            "Parent".to_string(),
            "Is root".to_string(),
            "Capacity".to_string(),
            "Child".to_string(),
        ];

        let mut rows = Vec::new();
        for page_num in 0..total {
            let node = self.pager.get(page_num as u32)?;
            let num_cells = node.leaf_node_num_cells()?;

            for i in 0..num_cells {
                let (key, capacity, child) = if node.get_node_type()? == NodeType::NodeInternal {
                    (
                        node.internal_node_key(i as u32)?,
                        node.internal_node_num_keys()?,
                        node.internal_node_right_child()?,
                    )
                } else {
                    (
                        node.leaf_node_key(i as usize)?,
                        node.leaf_node_num_cells()?,
                        node.internal_node_right_child()?,
                    )
                };

                let parent = node.node_parent()?;
                let row = vec![
                    node.get_node_type()?.to_string(),
                    page_num.to_string(),    // Page number
                    i.to_string(),           // Cell index
                    format!("{:?}", key),    // Key value (debug-formatted)
                    format!("{:?}", parent), // Key value (debug-formatted)
                    format!("{:?}", node.is_node_root()?),
                    format!("{:?}", capacity),
                    format!("{:?}", child),
                ];
                rows.push(row);
            }
        }

        Ok((total, columns, rows))
    }
}

pub fn insert_row(table: &mut Table, row: &row::Row) -> Result<(), Error> {
    let row_size = table.schema.get_row_size();
    let row_id = row.get_id(&table.schema)?;
    let row_bin = encoding::encode_row(&table.schema, row)?;
    debug!(row_id = row_id, row_size = row_size, "Inserting a row...");

    if row_bin.len() != row_size {
        return Err(Error::Storage(format!(
            "Unexpected row size {}. Table row size is {}.",
            row_bin.len(),
            row_size
        )));
    }
    let mut cursor = cursor::Cursor::find(table, row_id)?;

    let mut node = cursor.table.pager.get(cursor.page_num)?;
    let num_cells = node.leaf_node_num_cells()?;

    if cursor.cell_num < num_cells {
        let key_at_index = node.leaf_node_key(cursor.cell_num as usize)?;
        if key_at_index == row_id {
            return Err(Error::Storage("Duplicate key".into()));
        }
    }

    if num_cells as usize >= node.max_cells() {
        warn!(
            page_num = cursor.page_num,
            num_cells,
            max_cells = node.max_cells(),
            "Node full. Splitting a leaf node..."
        );
        drop(node);
        leaf_node_split_and_insert(&mut cursor, row_id, row_bin.clone())?;
        return Ok(());
    }

    if cursor.cell_num < num_cells {
        // Make room for new cell
        for i in (cursor.cell_num + 1..=num_cells).rev() {
            let prev: Vec<u8>;
            {
                prev = node.leaf_node_cell(i as usize - 1)?.to_vec();
            }
            node.leaf_node_cell_mut(i as usize)?.copy_from_slice(&prev);
        }
    }

    node.set_leaf_node_num_cells(num_cells + 1);
    node.set_leaf_node_key(cursor.cell_num as usize, row_id)?;
    node.set_leaf_node_value(cursor.cell_num as usize, row_bin.as_slice())?;

    Ok(())
}

/// Create a new node and move half the cells over.
/// Insert the new value in one of the two nodes.
/// Update parent or create a new parent.
pub fn leaf_node_split_and_insert(
    cursor: &mut cursor::Cursor,
    row_id: u32,
    row_bin: Vec<u8>,
) -> Result<(), Error> {
    debug!("Splitting leaf node...");
    let new_page_num = cursor.table.pager.get_unused_page_num() as u32;
    cursor.table.pager.try_create(new_page_num)?;

    let mut old_node = cursor.table.pager.get(cursor.page_num)?;
    let old_max = old_node.get_node_max_key()?;
    let mut new_node = cursor.table.pager.get(new_page_num)?;

    initialize_leaf_node(&mut new_node)?;
    new_node.set_node_parent(old_node.node_parent()?);

    // Whenever we split a leaf node, update the sibling pointers.
    // The old leaf’s sibling becomes the new leaf, and the new leaf’s sibling becomes
    // whatever used to be the old leaf’s sibling.
    new_node.set_leaf_node_next_leaf(old_node.leaf_node_next_leaf()?);
    old_node.set_leaf_node_next_leaf(new_page_num);

    let leaf_node_left_split_count = old_node.leaf_node_left_split_count();
    let leaf_node_max_cells = old_node.leaf_node_max_cells;
    let old = old_node.clone();

    // All existing keys plus new key should be divided
    // evenly between old (left) and new (right) nodes.
    // Starting from the right, move each key to correct position.
    for i in (0..=leaf_node_max_cells).rev() {
        let dest_node = if i >= leaf_node_left_split_count {
            &mut new_node
        } else {
            &mut old_node
        };

        let cell_num = i % leaf_node_left_split_count;
        let dest = dest_node.leaf_node_cell_mut(cell_num)?;

        if i == cursor.cell_num as usize {
            dest_node.set_leaf_node_key(cell_num as usize, row_id)?;
            dest_node.set_leaf_node_value(cell_num as usize, row_bin.as_slice())?;
        } else if i > cursor.cell_num as usize {
            dest.copy_from_slice(old.leaf_node_cell(i - 1)?);
        } else {
            dest.copy_from_slice(old.leaf_node_cell(i)?);
        }
    }

    // Update cell count on both leaf nodes
    old_node.set_leaf_node_num_cells(old.leaf_node_left_split_count() as u32);
    new_node.set_leaf_node_num_cells(old.leaf_node_right_split_count() as u32);

    // We need to update the nodes’ parent. If the original node was the root,
    // it had no parent. In that case, create a new root node to act as the parent.
    if old_node.is_node_root()? {
        drop(old_node);
        drop(new_node);

        create_new_root(cursor, new_page_num)?;
        return Ok(());
    } else {
        let parent_page_num = old_node.node_parent()?;
        let new_max = old_node.get_node_max_key()?;

        drop(old_node);
        drop(new_node);

        {
            let mut parent = cursor.table.pager.get(parent_page_num)?;
            parent.update_internal_node_key(old_max, new_max)?;
        }

        internal_node_insert(cursor, parent_page_num, new_page_num)?;

        return Ok(());
    }
}

/// Splits an internal node and inserts a new child pointer into the resulting structure.
/// This operation is part of maintaining a balanced B-tree when an internal node becomes full.
///
/// When splitting an internal node, we perform the following steps:
/// - Create a sibling node to store `(n-1)/2` of the original node's keys.
/// - Move these keys and their associated child pointers from the original node to the sibling.
/// - Update the original node's key in the parent to reflect its new maximum key after the split.
/// - Insert the sibling node into the parent, which may trigger a recursive split if the parent overflows.
///
/// # Arguments
/// - `cursor`: A mutable reference to the B-tree cursor, used to access and modify the pager.
/// - `parent_page_num`: The page number of the internal node to split (initially the "old node").
/// - `child_page_num`: The page number of the child to insert after the split.
///
/// # Returns
/// - `Ok(())` on success, or an `Error` if any operation (e.g., page retrieval or initialization) fails.
///
/// # Notes
/// - If the node being split is the root, a new root is created, and the split nodes become its children.
/// - Borrowing is carefully managed to ensure immutable borrows are dropped before mutable operations.
pub fn internal_node_split_and_insert(
    cursor: &mut cursor::Cursor,
    parent_page_num: u32,
    child_page_num: u32,
) -> Result<(), Error> {
    debug!(
        parent_page_num,
        child_page_num, "Splitting internal node..."
    );

    let old_page_num = parent_page_num;

    // Precompute all values that require immutable borrows
    let (old_max, child_max, splitting_root, old_node_parent, old_num_keys, right_child_page_num) = {
        let old_node = cursor.table.pager.get(old_page_num)?;
        let child = cursor.table.pager.get(child_page_num)?;
        (
            cursor.table.pager.get_node_max_key(&old_node)?,
            cursor.table.pager.get_node_max_key(&child)?,
            old_node.is_node_root()?,
            old_node.node_parent()?,
            old_node.internal_node_num_keys()?,
            old_node.internal_node_right_child()?,
        )
    };
    let new_page_num = cursor.table.pager.get_unused_page_num() as u32;

    // Initialize the parent node
    let parent_id = {
        if splitting_root {
            create_new_root(cursor, new_page_num)?;
            cursor.table.root_page_num
        } else {
            let mut new_node = cursor.table.pager.get(new_page_num)?;
            initialize_internal_node(&mut new_node)?;
            old_node_parent
        }
    };

    // Split the old node and move keys/children
    {
        let mut old_node = cursor.table.pager.get(old_page_num)?;
        let mut current_num_keys = old_num_keys;

        // Move the right child to the new node
        {
            let mut cur = cursor.table.pager.get(right_child_page_num)?;
            cur.set_node_parent(new_page_num);
        }
        old_node.set_internal_node_right_child(btree::INVALID_PAGE_NUM);

        // Collect children to move to the new node
        let mut children_to_move = Vec::new();
        for i in (btree::INTERNAL_NODE_MAX_CELLS / 2 + 1..btree::INTERNAL_NODE_MAX_CELLS).rev() {
            let cur_page_num = old_node.internal_node_child(i as u32)?;
            children_to_move.push(cur_page_num);
            current_num_keys -= 1;
            old_node.set_internal_node_num_keys(current_num_keys);
        }

        // Update the old node's right child
        let new_right_child = old_node.internal_node_child(current_num_keys - 1)?;
        old_node
            .internal_node_right_child_mut()?
            .copy_from_slice(&new_right_child.to_le_bytes());
        current_num_keys -= 1;
        old_node.set_internal_node_num_keys(current_num_keys);

        // Drop old_node borrow before mutable operations
        drop(old_node);

        // Perform insertions into the new node
        for cur_page_num in children_to_move {
            internal_node_insert(cursor, new_page_num, cur_page_num)?;
            let mut cur = cursor.table.pager.get(cur_page_num)?;
            cur.set_node_parent(new_page_num);
        }
    }

    // Compute max_after_split and determine destination
    let max_after_split = {
        let old_node = cursor.table.pager.get(old_page_num)?;
        cursor.table.pager.get_node_max_key(&old_node)?
    };
    let destination_page_num = if child_max < max_after_split {
        old_page_num
    } else {
        new_page_num
    };

    // Insert the child
    internal_node_insert(cursor, destination_page_num, child_page_num)?;
    {
        let mut child = cursor.table.pager.get(child_page_num)?;
        child.set_node_parent(destination_page_num);
    }

    // Update parent key and handle root splitting
    {
        let old_node = cursor.table.pager.get(old_page_num)?;
        let mut parent = cursor.table.pager.get(parent_id)?;
        parent
            .update_internal_node_key(old_max, cursor.table.pager.get_node_max_key(&old_node)?)?;
    }
    if splitting_root {
        let parent_page_num = {
            let old_node = cursor.table.pager.get(old_page_num)?;
            old_node.node_parent()?
        };
        internal_node_insert(cursor, parent_page_num, new_page_num)?;
        let mut new_node = cursor.table.pager.get(new_page_num)?;
        new_node.set_node_parent(parent_page_num);
    }

    Ok(())
}

// Add a new child/key pair to parent that corresponds to child
// Because we store the rightmost child pointer separately from the rest of the child/key pairs, we have to handle
// things differently if the new child is going to become the rightmost child.
// In our example, we would get into the else block. First we make room for the new cell
// by shifting other cells one space to the right. (Although in our example there are 0 cells to shift)
// Next, we write the new child pointer and key into the cell determined by index.
pub fn internal_node_insert(
    cursor: &mut cursor::Cursor,
    parent_page_num: u32,
    child_page_num: u32,
) -> Result<(), Error> {
    debug!(parent_page_num, child_page_num, "Inserting internal node");

    let mut parent = cursor.table.pager.get(parent_page_num)?;
    let child = cursor.table.pager.get(child_page_num)?;
    let child_max_key: u32 = cursor.table.pager.get_node_max_key(&child)?;

    // The index where the new cell (child/key pair) should be inserted depends on the maximum key in the new child.
    let index = parent.internal_node_find_child(child_max_key)?;

    let original_num_keys = parent.internal_node_num_keys()?;
    parent.set_internal_node_num_keys(original_num_keys + 1);

    if original_num_keys >= btree::INTERNAL_NODE_MAX_CELLS as u32 {
        drop(parent);
        drop(child);
        internal_node_split_and_insert(cursor, parent_page_num, child_page_num)?;
        return Ok(());
    }

    let p2 = parent.clone(); // XXX: bullshit
    let right_child_page_num = p2.internal_node_right_child()?;
    // An internal node with a right child of INVALID_PAGE_NUM is empty
    if right_child_page_num == btree::INVALID_PAGE_NUM {
        parent.set_internal_node_right_child(child_page_num);
        return Ok(());
    }
    let right_child = cursor.table.pager.get(right_child_page_num)?;

    // If we are already at the max number of cells for a node, we cannot increment
    // before splitting. Incrementing without inserting a new key/child pair
    // and immediately calling internal_node_split_and_insert has the effect
    // of creating a new key at (max_cells + 1) with an uninitialized value.
    parent.set_internal_node_num_keys(original_num_keys + 1);

    if child_max_key > cursor.table.pager.get_node_max_key(&right_child)? {
        trace!("Replace right child");
        //
        parent
            .internal_node_child_mut(original_num_keys)?
            .copy_from_slice(&right_child_page_num.to_le_bytes());
        parent.set_internal_node_key(
            original_num_keys,
            cursor.table.pager.get_node_max_key(&right_child)?,
        )?;
        parent
            .internal_node_right_child_mut()?
            .copy_from_slice(&child_page_num.to_le_bytes());
    } else {
        trace!("Make room for the new cell");
        let source_parent = parent.clone();
        for i in (index + 1..=original_num_keys).rev() {
            let destination = parent.internal_node_cell_mut(i)?;
            let source = source_parent.internal_node_cell(i - 1)?;
            destination.copy_from_slice(source);
        }
        parent.set_internal_node_child(index, child_page_num)?;
        parent.set_internal_node_key(index, child_max_key)?;
    }

    Ok(())
}

// Creating a New Root
// Here’s how SQLite Database System explains the process of creating a new root node:
// Let N be the root node. First allocate two nodes, say L and R. Move lower half of N into L
// and the upper half into R. Now N is empty. Add 〈L, K,R〉 in N, where K is the max key in L.
// Page N remains the root. Note that the depth of the tree has increased by one, but the new
// tree remains height balanced without violating any B+-tree property.
// At this point, we’ve already allocated the right child and moved the upper half into it.
// Our function takes the right child as input and allocates a new page to store the left child.
pub fn create_new_root(
    cursor: &mut cursor::Cursor,
    right_child_page_num: u32,
) -> Result<(), Error> {
    // Handle splitting the root.
    // Old root copied to new page, becomes left child.
    // Address of right child passed in.
    // Re-initialize root page to contain the new root node.
    // New root node points to two children
    //
    debug!(right_child_page_num, "Creating a new root");
    let left_child_page_num = cursor.table.pager.get_unused_page_num();

    cursor.table.pager.try_create(right_child_page_num)?;
    cursor.table.pager.try_create(left_child_page_num as u32)?;

    let mut root = cursor.table.pager.get(cursor.table.root_page_num)?;
    let mut right_child = cursor.table.pager.get(right_child_page_num)?;
    let mut left_child = cursor.table.pager.get(left_child_page_num as u32)?;

    // The old root is copied to the left child so we can reuse the root page
    left_child.data.copy_from_slice(&root.data);
    left_child.set_node_root(false);

    // Finally we initialize the root page as a new internal node with two children.
    initialize_internal_node(&mut root)?;
    root.set_node_root(true);
    root.set_internal_node_num_keys(1);
    root.internal_node_child_mut(0)?
        .copy_from_slice((left_child_page_num as u32).to_le_bytes().as_slice());

    let left_child_max_key = cursor.table.pager.get_node_max_key(&left_child)?;
    root.set_internal_node_key(0, left_child_max_key)?;
    root.internal_node_right_child_mut()?
        .copy_from_slice((right_child_page_num as u32).to_le_bytes().as_slice());

    left_child.set_node_parent(cursor.table.root_page_num);
    right_child.set_node_parent(cursor.table.root_page_num);

    Ok(())
}

pub fn initialize_leaf_node(node: &mut Node) -> Result<(), Error> {
    node.set_node_type(btree::NodeType::NodeLeaf);
    node.set_node_root(false);
    node.set_leaf_node_num_cells(0);
    node.set_leaf_node_next_leaf(0); // 0 represents no sibling
    Ok(())
}

pub fn initialize_internal_node(node: &mut Node) -> Result<(), Error> {
    node.set_node_type(btree::NodeType::NodeInternal);
    node.set_node_root(false);
    node.set_internal_node_num_keys(0);

    // Necessary because the root page number is 0; by not initializing an internal
    // node's right child to an invalid page number when initializing the node, we may
    // end up with 0 as the node's right child, which makes the node a parent of the root
    node.set_internal_node_right_child(btree::INVALID_PAGE_NUM);

    Ok(())
}

pub fn select_rows(table: &mut Table) -> Result<std::vec::Vec<row::Row>, Error> {
    let mut rows = std::vec::Vec::new();
    let row_size = table.schema.get_row_size();

    let mut cursor = cursor::Cursor::start(table)?;
    while !cursor.end_of_table {
        let mut buf = vec![];
        buf.resize(row_size, 0);
        cursor.read_value(&mut buf)?;

        rows.push(encoding::decode_row(&SCHEMA, &buf)?);
        cursor.advance()?;
    }
    Ok(rows)
}

pub fn load_table(database: &String, name: &String) -> Result<Table, Error> {
    let path = PathBuf::from(format!("data/{}/{}.tbd", database, name));

    let row_size = SCHEMA.get_row_size();
    let mut pager = Pager::new(row_size as u32);
    let mut file = std::fs::File::open(&path)?;

    let mut tablespace_header_buf = [0u8; TABLESPACE_HEADER_SIZE];
    file.read_exact(&mut tablespace_header_buf)?;
    let tablespace_header: TablespaceHeader = decode_header(&tablespace_header_buf)?;

    loop {
        let mut page_header_buf = [0u8; PAGE_HEADER_SIZE];
        let read = file.read(&mut page_header_buf)?;
        if read == 0 {
            break;
        }
        let page_header: PageHeader = decode_header(&page_header_buf)?;
        debug!(page_n_recs = page_header.page_n_recs, "Read page");

        let mut page_buf: [u8; 4096] = [0; 4096];
        let read = file.read(&mut page_buf)?;
        if read == 0 {
            break;
        }
        let node = Node::new(&page_buf, row_size);
        pager.push(node);
    }

    debug!(
        database,
        name,
        root_page_num = tablespace_header.root_page_num,
        "Loaded table."
    );

    let table = Table {
        name: name.clone(),
        path,
        database: database.clone(),
        root_page_num: tablespace_header.root_page_num,
        pager,
        schema: SCHEMA.clone(),
    };
    Ok(table)
}

pub fn create_table(database: &String, name: &String) -> Result<Table, Error> {
    let root_page_num = 0;
    let row_size = SCHEMA.get_row_size();
    let mut pager = Pager::new(row_size as u32);
    pager.try_create(0)?;

    let path = PathBuf::from(format!("data/{}/{}.tbd", database, name));
    if path.exists() {
        return Err(Error::Storage(format!(
            "Table '{}.{}' already exists",
            &database, &name
        )));
    }
    File::create(&path)?;

    let table = Table {
        name: name.clone(),
        database: database.clone(),
        path,
        root_page_num,
        pager,
        schema: SCHEMA.clone(),
    };
    Ok(table)
}

pub fn drop_table(database: &String, name: &String) -> Result<(), Error> {
    let path = PathBuf::from(format!("data/{}/{}.tbd", database, name));
    if !path.exists() {
        return Err(Error::Storage(format!(
            "Unknown table '{}.{}'",
            &database, &name
        )));
    }
    std::fs::remove_file(&path)?;
    Ok(())
}

pub fn show_tables(database: &String) -> Result<std::vec::Vec<String>, Error> {
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

fn decode_header<T: Decode<()>>(bytes: &[u8]) -> Result<T, Error> {
    let (decoded, _): (T, usize) = bincode::decode_from_slice(&bytes, config::standard())
        .map_err(|e| Error::Encoding(format!("Failed to encode header. {}", e)))?;
    Ok(decoded)
}

fn encode_header<T: Encode, const N: usize>(header: &T) -> Result<[u8; N], Error> {
    let encoded = match bincode::encode_to_vec(header, config::standard()) {
        Ok(r) => Ok(r),
        Err(e) => Err(Error::Encoding(format!("Failed to decode header. {}", e))),
    }?;

    if encoded.len() > N {
        return Err(Error::Encoding(format!(
            "Header size ({}) does not fit within the frame ({}).",
            encoded.len(),
            N
        )));
    }

    let mut header = [0u8; N];
    header[..encoded.len()].copy_from_slice(&encoded);
    Ok(header)
}
