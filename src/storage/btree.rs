//! The B-Tree is the data structure to represent both tables and indexes.
//!
//! Why is a tree a good data structure for a database?
//! - Searching for a particular value is fast (logarithmic time)
//! - Inserting / deleting a value you’ve already found is fast (constant-ish time to rebalance)
//! - Traversing a range of values is fast (unlike a hash map)
//! A B-Tree is different from a binary tree (the "B" probably stands for the inventor’s name,
//! but could also stand for "balanced").
//!
//! Unlike a binary tree, each node in a B-Tree can have more than 2 children. Each node can have
//! up to m children, where m is called the tree’s "order". To keep the tree mostly balanced, we
//! also say nodes have to have at least m/2 children (rounded up).
//!
//! Nodes with children are called "internal" nodes. Internal nodes and leaf nodes are
//! structured differently:
//!
//! | **Property**            | **Internal Node**                  | **Leaf Node**              |
//! |-------------------------|------------------------------------|----------------------------|
//! | **Stores**              | Keys and pointers to children      | Keys and values            |
//! | **Number of keys**      | Up to `m-1`                        | As many as will fit        |
//! | **Number of pointers**  | Number of keys + 1                 | None                       |
//! | **Number of values**    | None                               | Number of keys             |
//! | **Key purpose**         | Used for routing                   | Paired with value          |
//! | **Stores values?**      | No                                 | Yes                        |
//!
//! ## Alternative Table Formats
//!
//! If we stored the table as an array, but kept rows sorted by id, we could use binary search to find
//! a particular id. However, insertion would be slow because we would have to move a lot of rows
//! to make space.
//!
//! With a tree structure each node in the tree can contain a variable number of rows,
//! so we have to store some information in each node to keep track of how many rows it contains.
//! Plus there is the storage overhead of all the internal nodes which don’t store any rows. In exchange
//! for a larger database file, we get fast insertion, deletion and lookup.
//!
//! |                   | Unsorted Array of rows | Sorted Array of rows | Tree of nodes               |
//! |-------------------|------------------------|----------------------|-----------------------------|
//! | **Pages contain** | only data              | only data            | metadata, primary keys, data|
//! | **Rows per page** | more                   | more                 | fewer                       |
//! | **Insertion**     | O(1)                   | O(n)                 | O(log(n))                   |
//! | **Deletion**      | O(n)                   | O(n)                 | O(log(n))                   |
//! | **Lookup by id**  | O(n)                   | O(log(n))            | O(log(n))                   |
//!
use super::table::PAGE_SIZE;
use crate::errors::Error;
use std::cmp::Ordering;
use std::fmt;

/// Represents the type of a B-tree node.
///
/// | Property           | Internal Node                  | Leaf Node              |
/// |--------------------|--------------------------------|------------------------|
/// | Stores             | keys and pointers to children  | keys and values        |
/// | Number of keys     | up to m-1                      | as many as will fit    |
/// | Number of pointers | number of keys + 1             | none                   |
/// | Number of values   | none                           | number of keys         |
/// | Key purpose        | used for routing               | paired with value      |
/// | Stores values?     | No                             | Yes                    |
#[derive(Debug, PartialEq)]
pub enum NodeType {
    /// A leaf node containing key-value pairs.
    NodeLeaf,
    /// An internal node containing keys and child pointers.
    NodeInternal,
}

// Common Node Header Layout

/// Offset of the node type field (starts at 0)
const NODE_TYPE_OFFSET: usize = 0;

/// Size of the node type field (1 byte, equivalent to uint8_t)
const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>() as usize;

/// Size of the is_root field (1 byte, equivalent to uint8_t)
const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>() as usize;

/// Offset of the is_root field (after node type)
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;

/// Size of the parent pointer field (4 bytes, equivalent to uint32_t)
const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>() as usize;

/// Offset of the parent pointer field (after is_root)
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;

/// Total size of the common node header (sum of all fields)
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout

/// Size of the num_cells field in a leaf node header (4 bytes, equivalent to uint32_t)
const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>() as usize;

/// Offset of the num_cells field in a leaf node header (after the common header)
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE as usize;

/// Size of the `next_leaf` field in a leaf node header (in bytes).
///
/// Represents a `u32` pointer to the next leaf node in the B-tree.
const LEAF_NODE_NEXT_LEAF_SIZE: usize = std::mem::size_of::<u32>();

/// Offset of the `next_leaf` field in a leaf node header.
///
/// Positioned immediately after the `num_cells` field.
const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;

/// Total size of a leaf node header (in bytes).
///
/// Includes the common header, `num_cells`, and `next_leaf` fields.
const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

// Keep it small for testing.
pub const INTERNAL_NODE_MAX_CELLS: usize = 3;
pub const INVALID_PAGE_NUM: u32 = u32::MAX;

// Leaf Node Body Layout

/// Size of the key field in a leaf node cell (4 bytes, equivalent to uint32_t)
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>() as usize;

/// Offset of the key field in a leaf node cell (starts at 0)
const LEAF_NODE_KEY_OFFSET: usize = 0;

/// Offset of the value field in a leaf node cell (after the key)
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;

/// Space available for cells in a leaf node (page size minus header)
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;

/// Total size of a cell in an internal node body (in bytes).
///
/// A cell consists of a child pointer followed by a key.
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

/// Size of the `num_keys` field in an internal node header (in bytes).
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = std::mem::size_of::<u32>();

/// Offset of the `num_keys` field in an internal node header.
///
/// Starts immediately after the common node header.
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;

/// Size of the `right_child` field in an internal node header (in bytes).
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = std::mem::size_of::<u32>();

/// Offset of the `right_child` field in an internal node header.
///
/// Follows the `num_keys` field.
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;

/// Total size of an internal node header (in bytes).
///
/// Includes the common header, `num_keys`, and `right_child` fields.
const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/// Size of a key in an internal node cell (in bytes).
///
/// Each key is a `u32`, typically representing a value used for splitting child nodes.
const INTERNAL_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();

/// Size of a child pointer in an internal node cell (in bytes).
///
/// Each child pointer is a `u32`, typically an index or offset to another node.
const INTERNAL_NODE_CHILD_SIZE: usize = std::mem::size_of::<u32>();

/// A leaf node in a B-tree, owning its data and managing key-value cells.
///
/// The node stores a fixed-size array of bytes (`[u8; PAGE_SIZE]`) and provides methods to read and
/// write cell data, including keys and values. The layout includes a header followed by a series
/// of cells, each containing a key and a value.
#[derive(Debug)]
pub struct Node {
    pub data: [u8; PAGE_SIZE],       // Owned data buffer
    pub leaf_node_value_size: usize, // Size of the value in each cell
    pub leaf_node_cell_size: usize,  // Total size of a cell (key + value)
    pub leaf_node_max_cells: usize,  // Maximum number of cells that fit in the node
}

impl Node {
    /// Creates a new `Node` from a byte array, copying the data.
    ///
    /// Initializes the node with a specified row size (value size), computing the cell size and maximum
    /// number of cells based on the buffer layout.
    ///
    /// # Arguments
    /// - `buf`: A reference to a `[u8; PAGE_SIZE]` array containing initial data.
    /// - `row_size`: The size of the value portion of each cell in bytes.
    ///
    /// # Examples
    /// ```
    /// let buffer = [0u8; PAGE_SIZE];
    /// let node = Node::new(&buffer, 256);
    /// assert_eq!(node.value_size(), 256);
    /// ```
    pub fn new(buf: &[u8; PAGE_SIZE], row_size: usize) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(buf);

        let leaf_node_value_size = row_size;
        let leaf_node_cell_size = LEAF_NODE_KEY_SIZE + leaf_node_value_size;
        let leaf_node_max_cells = LEAF_NODE_SPACE_FOR_CELLS / leaf_node_cell_size;

        Self {
            data,
            leaf_node_value_size,
            leaf_node_cell_size,
            leaf_node_max_cells,
        }
    }

    /// Returns the number of cells stored in the leaf node.
    ///
    /// Reads the value in little-endian format from `LEAF_NODE_NUM_CELLS_OFFSET`.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the data cannot be decoded into a `u32`.
    pub fn leaf_node_num_cells(&self) -> Result<u32, Error> {
        let bytes = self.slice_at(LEAF_NODE_NUM_CELLS_OFFSET, LEAF_NODE_NUM_CELLS_SIZE)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|e| {
            err!(Storage, "Failed to decode num_cells: {:?}", e)
        })?))
    }

    /// Sets the number of cells in the leaf node.
    ///
    /// Writes the value in little-endian format to `LEAF_NODE_NUM_CELLS_OFFSET`.
    ///
    /// # Arguments
    /// - `num`: The number of cells to set.
    pub fn set_leaf_node_num_cells(&mut self, num: u32) {
        self.data
            [LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
            .copy_from_slice(&num.to_le_bytes());
    }

    /// Computes the offset of a cell in the data buffer.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if `cell_num` exceeds `max_cells` or the offset exceeds the buffer size.
    fn get_leaf_node_cell_offset(&self, cell_num: usize) -> Result<usize, Error> {
        if cell_num >= self.leaf_node_max_cells {
            return Err(err!(
                Storage,
                "Cell index {} exceeds max_cells {}",
                cell_num,
                self.leaf_node_max_cells
            ));
        }
        let offset = LEAF_NODE_HEADER_SIZE + cell_num * self.leaf_node_cell_size;
        if offset + self.leaf_node_cell_size > self.data.len() {
            return Err(err!(
                Storage,
                "Cell offset {} exceeds buffer size {}",
                offset + self.leaf_node_cell_size,
                self.data.len()
            ));
        }
        Ok(offset)
    }

    /// Returns an immutable reference to the specified leaf node cell’s memory.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index or offset is invalid.
    pub fn leaf_node_cell(&self, cell_num: usize) -> Result<&[u8], Error> {
        let offset = self.get_leaf_node_cell_offset(cell_num)?;
        Ok(&self.data[offset..offset + self.leaf_node_cell_size])
    }

    /// Returns a mutable reference to the specified leaf node cell’s memory.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index or offset is invalid.
    pub fn leaf_node_cell_mut(&mut self, cell_num: usize) -> Result<&mut [u8], Error> {
        let offset = self.get_leaf_node_cell_offset(cell_num)?;
        Ok(&mut self.data[offset..offset + self.leaf_node_cell_size])
    }

    /// Returns an immutable reference to the value of the specified leaf node cell.
    ///
    /// The value follows the key in the cell layout.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index or value offset is invalid.
    pub fn leaf_node_value(&self, cell_num: usize) -> Result<&[u8], Error> {
        let cell = self.leaf_node_cell(cell_num)?;
        let offset = LEAF_NODE_KEY_SIZE;
        if offset + self.leaf_node_value_size > cell.len() {
            return Err(err!(
                Storage,
                "Value offset {} exceeds cell size {}",
                offset + self.leaf_node_value_size,
                cell.len()
            ));
        }
        Ok(&cell[offset..offset + self.leaf_node_value_size])
    }

    /// Sets the value of the specified leaf node cell.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    /// - `buf`: The value to write; its length must match `value_size()`.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index is invalid or `buf.len()` doesn’t match `value_size()`.
    pub fn set_leaf_node_value(&mut self, cell_num: usize, buf: &[u8]) -> Result<(), Error> {
        let size = self.leaf_node_value_size;
        if buf.len() != size {
            return Err(err!(
                Storage,
                "Value size mismatch (expected {}, got {})",
                size,
                buf.len()
            ));
        }
        let cell = self.leaf_node_cell_mut(cell_num)?;
        let offset = LEAF_NODE_KEY_SIZE;
        cell[offset..offset + size].copy_from_slice(buf);
        Ok(())
    }

    /// Returns the key of the specified leaf node cell as a `u32`.
    ///
    /// The key is stored in little-endian format at the start of the cell.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index is invalid or the key cannot be decoded.
    pub fn leaf_node_key(&self, cell_num: usize) -> Result<u32, Error> {
        let cell = self.leaf_node_cell(cell_num)?;
        let bytes = cell[..LEAF_NODE_KEY_SIZE]
            .try_into()
            .map_err(|e| err!(Storage, "Failed to decode key: {:?}", e))?;
        Ok(u32::from_le_bytes(bytes))
    }

    /// Sets the key of the specified leaf node cell.
    ///
    /// The key is stored in little-endian format at the start of the cell.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    /// - `key`: The key value to write.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index is invalid.
    pub fn set_leaf_node_key(&mut self, cell_num: usize, key: u32) -> Result<(), Error> {
        let cell = self.leaf_node_cell_mut(cell_num)?;
        cell[..LEAF_NODE_KEY_SIZE].copy_from_slice(&key.to_le_bytes());
        Ok(())
    }

    /// Returns an immutable reference to the raw data buffer.
    ///
    /// Useful for serialization or debugging.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Returns the maximum number of cells this leaf node can hold.
    pub fn max_cells(&self) -> usize {
        self.leaf_node_max_cells
    }

    /// Returns the size of each cell in this leaf node (key + value).
    pub fn cell_size(&self) -> usize {
        self.leaf_node_cell_size
    }

    /// Returns the size of the value portion in each leaf node cell.
    pub fn value_size(&self) -> usize {
        self.leaf_node_value_size
    }

    /// Returns the node type (leaf or internal).
    ///
    /// Reads a single byte at `NODE_TYPE_OFFSET`:
    /// - `0` indicates `NodeType::NodeLeaf`.
    /// - `1` indicates `NodeType::NodeInternal`.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the type cannot be decoded or is invalid.
    pub fn get_node_type(&self) -> Result<NodeType, Error> {
        let bytes = self.slice_at(NODE_TYPE_OFFSET, NODE_TYPE_SIZE)?;
        let value = u8::from_le_bytes(
            bytes
                .try_into()
                .map_err(|e| err!(Storage, "Failed to decode node type: {:?}", e))?,
        );
        match value {
            0 => Ok(NodeType::NodeLeaf),
            1 => Ok(NodeType::NodeInternal),
            invalid => Err(err!(Storage, "Invalid node type: {}", invalid)),
        }
    }

    /// Sets the node type (leaf or internal).
    ///
    /// Writes a single byte at `NODE_TYPE_OFFSET`:
    /// - `0` for `NodeType::NodeLeaf`.
    /// - `1` for `NodeType::NodeInternal`.
    ///
    /// # Arguments
    /// - `node_type`: The type to set.
    pub fn set_node_type(&mut self, node_type: NodeType) {
        let value = match node_type {
            NodeType::NodeLeaf => 0,
            NodeType::NodeInternal => 1,
        };
        self.data[NODE_TYPE_OFFSET] = value;
    }

    /// Checks if this node is the root of the B-tree.
    ///
    /// Reads a single byte at `IS_ROOT_OFFSET`:
    /// - `1` indicates it is the root.
    /// - `0` indicates it is not.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the flag cannot be decoded.
    pub fn is_node_root(&self) -> Result<bool, Error> {
        let bytes = self.slice_at(IS_ROOT_OFFSET, IS_ROOT_SIZE)?;
        let value = u8::from_le_bytes(
            bytes
                .try_into()
                .map_err(|e| err!(Storage, "Failed to decode is_root flag: {:?}", e))?,
        );
        Ok(value == 1)
    }

    /// Sets whether this node is the root of the B-tree.
    ///
    /// Writes a single byte at `IS_ROOT_OFFSET`:
    /// - `1` if `is_root` is `true`.
    /// - `0` if `is_root` is `false`.
    ///
    /// # Arguments
    /// - `is_root`: Whether this node is the root.
    pub fn set_node_root(&mut self, is_root: bool) {
        self.data[IS_ROOT_OFFSET] = is_root as u8;
    }

    /// Returns the number of keys in this internal node.
    ///
    /// Reads the value in little-endian format from `INTERNAL_NODE_NUM_KEYS_OFFSET`.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the data cannot be decoded into a `u32`.
    pub fn internal_node_num_keys(&self) -> Result<u32, Error> {
        let bytes = self.slice_at(INTERNAL_NODE_NUM_KEYS_OFFSET, INTERNAL_NODE_NUM_KEYS_SIZE)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|e| {
            err!(Storage, "Failed to decode num_keys: {:?}", e)
        })?))
    }

    /// Sets the number of keys in this internal node.
    ///
    /// Writes the value in little-endian format to `INTERNAL_NODE_NUM_KEYS_OFFSET`.
    ///
    /// # Arguments
    /// - `num_keys`: The number of keys to set.
    pub fn set_internal_node_num_keys(&mut self, num_keys: u32) {
        self.data[INTERNAL_NODE_NUM_KEYS_OFFSET
            ..INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE]
            .copy_from_slice(&num_keys.to_le_bytes());
    }

    /// Returns an immutable reference to the right child pointer of this internal node.
    ///
    /// The pointer is stored in little-endian format at `INTERNAL_NODE_RIGHT_CHILD_OFFSET`.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the slice cannot be accessed.
    pub fn internal_node_right_child(&self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.slice_at(
                INTERNAL_NODE_RIGHT_CHILD_OFFSET,
                INTERNAL_NODE_RIGHT_CHILD_SIZE,
            )?
            .try_into()
            .map_err(|e| err!(Storage, "Failed to decode right_child_page_num: {:?}", e))?,
        ))
    }

    /// Returns a mutable reference to the right child pointer of this internal node.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the slice cannot be accessed.
    pub fn internal_node_right_child_mut(&mut self) -> Result<&mut [u8], Error> {
        self.slice_at_mut(
            INTERNAL_NODE_RIGHT_CHILD_OFFSET,
            INTERNAL_NODE_RIGHT_CHILD_SIZE,
        )
    }

    /// Sets the right child pointer of this internal node.
    ///
    /// Writes the value in little-endian format to `INTERNAL_NODE_RIGHT_CHILD_OFFSET`.
    ///
    /// # Arguments
    /// - `right_child`: The page number of the right child.
    pub fn set_internal_node_right_child(&mut self, right_child: u32) {
        self.data[INTERNAL_NODE_RIGHT_CHILD_OFFSET
            ..INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE]
            .copy_from_slice(&right_child.to_le_bytes());
    }

    /// Returns an immutable reference to the specified internal node cell.
    ///
    /// Each cell contains a child pointer and a key.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index or offset is invalid.
    pub fn internal_node_cell(&self, cell_num: u32) -> Result<&[u8], Error> {
        let offset = INTERNAL_NODE_HEADER_SIZE + (cell_num as usize * INTERNAL_NODE_CELL_SIZE);
        self.slice_at(offset, INTERNAL_NODE_CELL_SIZE)
    }

    /// Returns a mutable reference to the specified internal node cell.
    ///
    /// Each cell contains a child pointer and a key.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the cell index or offset is invalid.
    pub fn internal_node_cell_mut(&mut self, cell_num: u32) -> Result<&mut [u8], Error> {
        let offset = INTERNAL_NODE_HEADER_SIZE + (cell_num as usize * INTERNAL_NODE_CELL_SIZE);
        self.slice_at_mut(offset, INTERNAL_NODE_CELL_SIZE)
    }

    /// Returns a mutable reference to the child pointer at the specified index.
    ///
    /// If `cell_num` equals the number of keys, returns the right child pointer.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the child index is invalid.
    pub fn internal_node_child_mut(&mut self, cell_num: u32) -> Result<&mut [u8], Error> {
        let num_keys = self.internal_node_num_keys()?;
        if cell_num > num_keys {
            return Err(err!(
                Storage,
                "Cell index {} exceeds num_keys {}",
                cell_num,
                num_keys
            ));
        }
        if cell_num == num_keys {
            self.internal_node_right_child_mut()
        } else {
            Ok(&mut self.internal_node_cell_mut(cell_num)?[..INTERNAL_NODE_CHILD_SIZE])
        }
    }

    /// Returns the child pointer at the specified index as a `u32`.
    ///
    /// # Arguments
    /// - `child_num`: The index of the child (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the child index is invalid or the pointer is `INVALID_PAGE_NUM`.
    pub fn internal_node_child(&self, child_num: u32) -> Result<u32, Error> {
        let num_keys = self.internal_node_num_keys()?;
        if child_num > num_keys {
            return Err(err!(
                Storage,
                "Child index {} exceeds num_keys {}",
                child_num,
                num_keys
            ));
        }
        let child = if child_num == num_keys {
            self.internal_node_right_child()?
        } else {
            let bytes: [u8; 4] = self.internal_node_cell(child_num)?[..INTERNAL_NODE_CHILD_SIZE]
                .try_into()
                .map_err(|e| err!(Storage, "Failed to decode child pointer: {:?}", e))?;

            u32::from_le_bytes(bytes)
        };

        if child == INVALID_PAGE_NUM {
            return Err(err!(Storage, "Child {} is invalid page number", child_num));
        }
        Ok(child)
    }
    pub fn set_internal_node_child(&mut self, cell_num: u32, child_num: u32) -> Result<(), Error> {
        self.internal_node_child_mut(cell_num)?
            .copy_from_slice(&child_num.to_le_bytes());
        Ok(())
    }

    /// Returns the key at the specified index in this internal node.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell (0-based).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the key index or offset is invalid.
    pub fn internal_node_key(&self, cell_num: u32) -> Result<u32, Error> {
        let cell = self.internal_node_cell(cell_num)?;
        let offset = INTERNAL_NODE_CHILD_SIZE;
        if offset + INTERNAL_NODE_KEY_SIZE > cell.len() {
            return Err(err!(
                Storage,
                "Key offset {} exceeds cell size {}",
                offset + INTERNAL_NODE_KEY_SIZE,
                cell.len()
            ));
        }
        let bytes = &cell[offset..offset + INTERNAL_NODE_KEY_SIZE];

        Ok(u32::from_le_bytes(bytes.try_into().map_err(|e| {
            err!(Storage, "Failed to decode key: {:?}", e)
        })?))
    }

    /// Sets the key at the specified index in this internal node.
    ///
    /// # Arguments
    /// - `key_num`: The index of the key (0-based).
    /// - `key_value`: The key value to write.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the key index or offset is invalid.
    pub fn set_internal_node_key(&mut self, cell_num: u32, key_value: u32) -> Result<(), Error> {
        let cell = self.internal_node_cell_mut(cell_num)?;
        let offset = INTERNAL_NODE_CHILD_SIZE;
        if offset + INTERNAL_NODE_KEY_SIZE > cell.len() {
            return Err(err!(
                Storage,
                "Key offset {} exceeds cell size {}",
                offset + INTERNAL_NODE_KEY_SIZE,
                cell.len()
            ));
        }
        cell[offset..offset + INTERNAL_NODE_KEY_SIZE].copy_from_slice(&key_value.to_le_bytes());
        Ok(())
    }

    /// Updates an existing key in this internal node.
    ///
    /// Finds the child index associated with `old_key` and replaces it with `new_key`.
    ///
    /// # Arguments
    /// - `old_key`: The key to replace.
    /// - `new_key`: The new key value.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the key cannot be found or updated.
    pub fn update_internal_node_key(&mut self, old_key: u32, new_key: u32) -> Result<(), Error> {
        let old_child_index = self.internal_node_find_child(old_key)?;
        // let parent_old_child = parent.internal_node_find_child(old_max)?;
        // parent.set_internal_node_key(parent_old_child, new_max)?;
        // drop(parent);
        //
        self.set_internal_node_key(old_child_index, new_key)
    }

    /// Returns the maximum key in this node.
    ///
    /// For leaf nodes, this is the key of the last cell. For internal nodes, it’s the last key.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the node type or key cannot be accessed.
    pub fn get_node_max_key(&self) -> Result<u32, Error> {
        match self.get_node_type()? {
            NodeType::NodeInternal => self.internal_node_key(self.internal_node_num_keys()? - 1),
            NodeType::NodeLeaf => self.leaf_node_key((self.leaf_node_num_cells()? - 1) as usize),
        }
    }

    /// Returns the number of cells assigned to the right sibling during a leaf node split.
    pub fn leaf_node_right_split_count(&self) -> usize {
        (self.leaf_node_max_cells + 1) / 2
    }

    /// Returns the number of cells assigned to the left sibling during a leaf node split.
    pub fn leaf_node_left_split_count(&self) -> usize {
        (self.leaf_node_max_cells + 1) - self.leaf_node_right_split_count()
    }

    /// Returns the page number of the next leaf node sibling.
    ///
    /// A value of `0` indicates no right sibling (e.g., this is the rightmost leaf).
    ///
    /// # Errors
    /// Returns `Error::Storage` if the pointer cannot be decoded.
    pub fn leaf_node_next_leaf(&self) -> Result<u32, Error> {
        let bytes = self.slice_at(LEAF_NODE_NEXT_LEAF_OFFSET, LEAF_NODE_NEXT_LEAF_SIZE)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|e| {
            err!(Storage, "Failed to decode next_leaf: {:?}", e)
        })?))
    }

    /// Sets the page number of the next leaf node sibling.
    ///
    /// A value of `0` indicates no right sibling.
    ///
    /// # Arguments
    /// - `next_leaf`: The page number of the next leaf sibling.
    pub fn set_leaf_node_next_leaf(&mut self, next_leaf: u32) {
        self.data
            [LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE]
            .copy_from_slice(&next_leaf.to_le_bytes());
    }

    /// Returns the page number of this node’s parent.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the pointer cannot be decoded.
    pub fn node_parent(&self) -> Result<u32, Error> {
        let bytes = self.slice_at(PARENT_POINTER_OFFSET, PARENT_POINTER_SIZE)?;
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|e| {
            err!(Storage, "Failed to decode parent: {:?}", e)
        })?))
    }

    /// Sets the page number of this node’s parent.
    ///
    /// # Arguments
    /// - `parent`: The page number of the parent node.
    pub fn set_node_parent(&mut self, parent: u32) {
        self.data[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
            .copy_from_slice(&parent.to_le_bytes());
    }

    /// Finds the insertion position for a key in this leaf node using binary search.
    ///
    /// Returns the index where the key should be inserted. If the key exists, returns its index.
    ///
    /// # Arguments
    /// - `key`: The key to find or insert.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the node data cannot be accessed.
    pub fn leaf_node_find(&self, key: u32) -> Result<u32, Error> {
        let mut min = 0;
        let mut max = self.leaf_node_num_cells()?;
        while min < max {
            let mid = (min + max) / 2;
            let key_at_mid = self.leaf_node_key(mid as usize)?;
            match key.cmp(&key_at_mid) {
                Ordering::Equal => return Ok(mid),
                Ordering::Less => max = mid,
                Ordering::Greater => min = mid + 1,
            }
        }
        Ok(min)
    }

    /// Returns the index of the child that should contain the given key in this internal node.
    ///
    /// Uses binary search to find the appropriate child pointer.
    ///
    /// # Arguments
    /// - `key`: The key to locate.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the node data cannot be accessed.
    pub fn internal_node_find_child(&self, key: u32) -> Result<u32, Error> {
        let num_keys = self.internal_node_num_keys()?;
        let mut min = 0;
        let mut max = num_keys;
        while min < max {
            let mid = (min + max) / 2;
            let key_at_mid = self.internal_node_key(mid)?;
            if key <= key_at_mid {
                max = mid;
            } else {
                min = mid + 1;
            }
        }
        Ok(min)
    }

    /// Returns the child page number that should contain the given key in this internal node.
    ///
    /// Uses binary search to locate the child.
    ///
    /// # Arguments
    /// - `key`: The key to find.
    ///
    /// # Errors
    /// Returns `Error::Storage` if the child cannot be accessed.
    pub fn internal_node_find(&self, key: u32) -> Result<u32, Error> {
        let child_index = self.internal_node_find_child(key)?;
        self.internal_node_child(child_index)
    }

    /// Helper method to safely slice the data buffer immutably.
    fn slice_at(&self, offset: usize, size: usize) -> Result<&[u8], Error> {
        if offset + size > self.data.len() {
            return Err(err!(
                Storage,
                "Offset {} exceeds buffer size {}",
                offset + size,
                self.data.len()
            ));
        }
        Ok(&self.data[offset..offset + size])
    }

    /// Helper method to safely slice the data buffer mutably.
    fn slice_at_mut(&mut self, offset: usize, size: usize) -> Result<&mut [u8], Error> {
        if offset + size > self.data.len() {
            return Err(err!(
                Storage,
                "Offset {} exceeds buffer size {}",
                offset + size,
                self.data.len()
            ));
        }
        Ok(&mut self.data[offset..offset + size])
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            data: self.data,
            leaf_node_value_size: self.leaf_node_value_size,
            leaf_node_cell_size: self.leaf_node_cell_size,
            leaf_node_max_cells: self.leaf_node_max_cells,
        }
    }
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeType::NodeLeaf => write!(f, "Leaf"),
            NodeType::NodeInternal => write!(f, "Internal"),
        }
    }
}
