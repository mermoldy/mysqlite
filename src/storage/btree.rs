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

/// Total size of the leaf node header (common header + num_cells)
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE as usize + LEAF_NODE_NUM_CELLS_SIZE;

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
    pub data: [u8; PAGE_SIZE],   // Owned data buffer
    leaf_node_value_size: usize, // Size of the value in each cell
    leaf_node_cell_size: usize,  // Total size of a cell (key + value)
    leaf_node_max_cells: usize,  // Maximum number of cells that fit in the node
}

impl Node {
    /// Creates a new `Node` from a byte array, copying the data.
    ///
    /// Initializes the node with a given row size (value size), computing the cell size and maximum
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
    /// assert_eq!(node.value_size, 256);
    /// ```
    pub fn new(buf: &[u8; PAGE_SIZE], row_size: usize) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(buf);

        let leaf_node_value_size = row_size;
        let leaf_node_cell_size = LEAF_NODE_KEY_SIZE + leaf_node_value_size;
        let leaf_node_max_cells = LEAF_NODE_SPACE_FOR_CELLS / leaf_node_cell_size;

        Node {
            data,
            leaf_node_value_size,
            leaf_node_cell_size,
            leaf_node_max_cells,
        }
    }

    /// Reads the number of cells stored in the node.
    ///
    /// The value is stored in little-endian format at `LEAF_NODE_NUM_CELLS_OFFSET`.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the byte slice cannot be decoded into a `u32`.
    pub fn leaf_node_num_cells(&self) -> Result<u32, Error> {
        let bytes = &self.data
            [LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE];
        let num_cells = u32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|e| err!(Storage, "Failed to decode num_cells: {:?}", e))?,
        );
        Ok(num_cells)
    }

    /// Writes the number of cells to the node.
    ///
    /// Stores the value in little-endian format at `LEAF_NODE_NUM_CELLS_OFFSET`.
    pub fn set_leaf_node_num_cells(&mut self, num: u32) -> Result<(), Error> {
        let bytes = &mut self.data
            [LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE];
        bytes.copy_from_slice(&num.to_le_bytes());
        Ok(())
    }

    /// Computes the offset of a cell in the data buffer.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell to locate.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if `cell_num` exceeds `max_cells` or if the resulting offset
    /// exceeds the buffer size.
    fn get_leaf_node_cell_offset(&self, cell_num: usize) -> Result<usize, Error> {
        if cell_num >= self.leaf_node_max_cells {
            return Err(Error::Storage(format!(
                "Cell number out of bounds (cell_num={}, max_cells={})",
                cell_num, self.leaf_node_max_cells
            )));
        }

        let offset = LEAF_NODE_HEADER_SIZE + cell_num * self.leaf_node_cell_size;
        if offset + self.leaf_node_cell_size > self.data.len() {
            return Err(Error::Storage(format!(
                "Cell offset exceeds buffer size (offset={}, buffer={})",
                offset + self.leaf_node_cell_size,
                self.data.len()
            )));
        }
        Ok(offset)
    }

    /// Returns an immutable reference to the specified cell’s memory.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell to read.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the cell index is out of bounds or the offset exceeds the buffer.
    pub fn leaf_node_cell(&self, cell_num: usize) -> Result<&[u8], Error> {
        let offset = self.get_leaf_node_cell_offset(cell_num)?;
        Ok(&self.data[offset..offset + self.leaf_node_cell_size])
    }

    /// Returns a mutable reference to the specified cell’s memory.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell to access.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the cell index is out of bounds or the offset exceeds the buffer.
    pub fn leaf_node_cell_mut(&mut self, cell_num: usize) -> Result<&mut [u8], Error> {
        let offset = self.get_leaf_node_cell_offset(cell_num)?;
        Ok(&mut self.data[offset..offset + self.leaf_node_cell_size])
    }

    /// Returns an immutable reference to the value of the specified cell.
    ///
    /// The value follows the key in the cell layout.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell whose value to read.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the cell index is invalid or the value offset is out of bounds.
    pub fn leaf_node_value(&self, cell_num: usize) -> Result<&[u8], Error> {
        let cell = self.leaf_node_cell(cell_num)?;
        let offset = LEAF_NODE_KEY_SIZE;
        if offset + self.leaf_node_value_size > cell.len() {
            return Err(Error::Storage(format!(
                "Value offset exceeds cell size (offset={}, cell_size={})",
                offset + self.leaf_node_value_size,
                cell.len()
            )));
        }
        Ok(&cell[offset..offset + self.leaf_node_value_size])
    }

    /// Writes a value to the specified cell.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell to write to.
    /// - `buf`: The byte slice containing the value to write (must match `value_size`).
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the cell index is invalid or if `buf` length doesn’t match `value_size`.
    pub fn set_leaf_node_value(&mut self, cell_num: usize, buf: &[u8]) -> Result<(), Error> {
        let value_size = self.leaf_node_value_size;

        if buf.len() != value_size {
            return Err(Error::Storage(format!(
                "Value size mismatch (expected={}, got={})",
                value_size,
                buf.len()
            )));
        }
        let cell = self.leaf_node_cell_mut(cell_num)?;
        let offset = LEAF_NODE_KEY_SIZE;
        if offset + value_size > cell.len() {
            return Err(Error::Storage(format!(
                "Value offset exceeds cell size (offset={}, cell_size={})",
                offset + value_size,
                cell.len()
            )));
        }
        cell[offset..offset + value_size].copy_from_slice(buf);
        Ok(())
    }

    /// Reads the key of the specified cell as a `u32`.
    ///
    /// The key is stored in little-endian format at the start of the cell.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell whose key to read.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the cell index is invalid or the key bytes cannot be decoded.
    pub fn leaf_node_key(&self, cell_num: usize) -> Result<u32, Error> {
        let cell = self.leaf_node_cell(cell_num)?;
        if cell.len() < LEAF_NODE_KEY_SIZE {
            return Err(Error::Storage(format!(
                "Cell too small for key (size={}, required={})",
                cell.len(),
                LEAF_NODE_KEY_SIZE
            )));
        }
        let bytes: [u8; LEAF_NODE_KEY_SIZE] = cell[..LEAF_NODE_KEY_SIZE]
            .try_into()
            .map_err(|e| err!(Storage, "Failed to decode key: {:?}", e))?;
        Ok(u32::from_le_bytes(bytes))
    }

    /// Writes a key to the specified cell.
    ///
    /// The key is stored in little-endian format at the start of the cell.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell to write to.
    /// - `key`: The `u32` key value to write.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the cell index is invalid.
    pub fn set_leaf_node_key(&mut self, cell_num: usize, key: u32) -> Result<(), Error> {
        let cell = self.leaf_node_cell_mut(cell_num)?;
        if cell.len() < LEAF_NODE_KEY_SIZE {
            return Err(Error::Storage(format!(
                "Cell too small for key (size={}, required={})",
                cell.len(),
                LEAF_NODE_KEY_SIZE
            )));
        }
        cell[..LEAF_NODE_KEY_SIZE].copy_from_slice(&key.to_le_bytes());
        Ok(())
    }

    /// Returns a reference to the raw data buffer.
    ///
    /// Useful for serialization or debugging purposes.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Returns the maximum number of cells this node can hold.
    pub fn max_cells(&self) -> usize {
        self.leaf_node_max_cells
    }

    /// Returns the cell size if this node.
    pub fn cell_size(&self) -> usize {
        self.leaf_node_cell_size
    }

    /// Retrieves the type of the node from its data buffer.
    ///
    /// Reads a single byte at `NODE_TYPE_OFFSET` and interprets it as a `NodeType`.
    /// The byte is expected to be in little-endian format, where:
    /// - `0` represents a `NodeLeaf`.
    /// - `1` represents a `NodeInternal`.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if:
    /// - The byte slice at `NODE_TYPE_OFFSET` cannot be decoded into a `u8` (e.g., out of bounds).
    /// - The decoded value does not correspond to a valid `NodeType` (i.e., neither 0 nor 1).
    pub fn get_node_type(&self) -> Result<NodeType, Error> {
        // Ensure the buffer has enough space for the node type
        if NODE_TYPE_OFFSET + NODE_TYPE_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Node type offset exceeds buffer size (offset={}, buffer={})",
                NODE_TYPE_OFFSET + NODE_TYPE_SIZE,
                self.data.len()
            ));
        }

        let bytes = &self.data[NODE_TYPE_OFFSET..NODE_TYPE_OFFSET + NODE_TYPE_SIZE];
        let node_type = u8::from_le_bytes(
            bytes
                .try_into()
                .map_err(|e| err!(Storage, "Failed to decode node type: {:?}", e))?,
        );

        match node_type {
            0 => Ok(NodeType::NodeLeaf),
            1 => Ok(NodeType::NodeInternal),
            invalid => Err(err!(Storage, "Invalid node type value: {}", invalid)),
        }
    }

    /// Sets the type of the node in its data buffer.
    ///
    /// Writes a single byte at `NODE_TYPE_OFFSET` in little-endian format, where:
    /// - `0` represents a `NodeLeaf`.
    /// - `1` represents a `NodeInternal`.
    ///
    /// # Arguments
    /// - `node_type`: The `NodeType` to set.
    ///
    /// # Panics
    /// Panics if the buffer is too small to store the node type at `NODE_TYPE_OFFSET`.
    /// This should never occur given the fixed-size `[u8; PAGE_SIZE]` buffer.
    pub fn set_node_type(&mut self, node_type: NodeType) -> Result<(), Error> {
        // Ensure the buffer has enough space for the node type
        if NODE_TYPE_OFFSET + NODE_TYPE_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Node type offset exceeds buffer size (offset={}, buffer={})",
                NODE_TYPE_OFFSET + NODE_TYPE_SIZE,
                self.data.len()
            ));
        }

        let value = match node_type {
            NodeType::NodeLeaf => 0,
            NodeType::NodeInternal => 1,
        };
        self.data[NODE_TYPE_OFFSET..NODE_TYPE_OFFSET + NODE_TYPE_SIZE].copy_from_slice(&[value]);
        Ok(())
    }

    /// Checks if the node is the root of the B-tree.
    ///
    /// Reads a single byte at `IS_ROOT_OFFSET`, interpreting it as a boolean flag:
    /// - `1` indicates the node is the root.
    /// - `0` indicates it is not the root.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the byte slice cannot be read or decoded.
    pub fn is_node_root(&self) -> Result<bool, Error> {
        // Ensure the buffer has enough space
        if IS_ROOT_OFFSET + IS_ROOT_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Root flag offset exceeds buffer size (offset={}, buffer={})",
                IS_ROOT_OFFSET + IS_ROOT_SIZE,
                self.data.len()
            ));
        }

        let bytes = &self.data[IS_ROOT_OFFSET..IS_ROOT_OFFSET + IS_ROOT_SIZE];
        let value = u8::from_le_bytes(
            bytes
                .try_into()
                .map_err(|e| err!(Storage, "Failed to decode is_root flag: {:?}", e))?,
        );
        Ok(value == 1)
    }

    /// Sets whether the node is the root of the B-tree.
    ///
    /// Writes a single byte at `IS_ROOT_OFFSET`:
    /// - `1` if `is_root` is `true`.
    /// - `0` if `is_root` is `false`.
    ///
    /// # Arguments
    /// - `is_root`: `true` if the node is the root, `false` otherwise.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the buffer is too small to write the flag.
    pub fn set_node_root(&mut self, is_root: bool) -> Result<(), Error> {
        // Ensure the buffer has enough space
        if IS_ROOT_OFFSET + IS_ROOT_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Root flag offset exceeds buffer size (offset={}, buffer={})",
                IS_ROOT_OFFSET + IS_ROOT_SIZE,
                self.data.len()
            ));
        }

        let value = is_root as u8; // true -> 1, false -> 0
        self.data[IS_ROOT_OFFSET..IS_ROOT_OFFSET + IS_ROOT_SIZE].copy_from_slice(&[value]);
        Ok(())
    }

    /// Reads the number of keys stored in the internal node.
    ///
    /// The value is stored in little-endian format at `INTERNAL_NODE_NUM_KEYS_OFFSET`.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the byte slice cannot be decoded into a `u32`.
    pub fn internal_node_num_keys(&self) -> Result<u32, Error> {
        if INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Num keys offset exceeds buffer size (offset={}, buffer={})",
                INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE,
                self.data.len()
            ));
        }
        let bytes = &self.data[INTERNAL_NODE_NUM_KEYS_OFFSET
            ..INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE];
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|e| {
            err!(Storage, "Failed to decode num_keys: {:?}", e)
        })?))
    }

    /// Sets the number of keys in the internal node.
    ///
    /// Writes the value in little-endian format at `INTERNAL_NODE_NUM_KEYS_OFFSET`.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the buffer is too small.
    pub fn set_internal_node_num_keys(&mut self, num_keys: u32) -> Result<(), Error> {
        if INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Num keys offset exceeds buffer size (offset={}, buffer={})",
                INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE,
                self.data.len()
            ));
        }
        self.data[INTERNAL_NODE_NUM_KEYS_OFFSET
            ..INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE]
            .copy_from_slice(&num_keys.to_le_bytes());
        Ok(())
    }

    /// Reads the right child pointer of the internal node.
    ///
    /// Returns a slice containing the `u32` pointer in little-endian format.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the byte slice cannot be accessed.
    pub fn internal_node_right_child(&mut self) -> Result<&mut [u8], Error> {
        if INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Right child offset exceeds buffer size (offset={}, buffer={})",
                INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE,
                self.data.len()
            ));
        }
        Ok(&mut self.data[INTERNAL_NODE_RIGHT_CHILD_OFFSET
            ..INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE])
    }

    /// Sets the right child pointer of the internal node.
    ///
    /// Writes the value in little-endian format at `INTERNAL_NODE_RIGHT_CHILD_OFFSET`.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the buffer is too small.
    pub fn set_internal_node_right_child(&mut self, right_child: u32) -> Result<(), Error> {
        if INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Right child offset exceeds buffer size (offset={}, buffer={})",
                INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE,
                self.data.len()
            ));
        }
        self.data[INTERNAL_NODE_RIGHT_CHILD_OFFSET
            ..INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE]
            .copy_from_slice(&right_child.to_le_bytes());
        Ok(())
    }

    /// Reads a cell (child pointer and key) at the specified index.
    ///
    /// # Arguments
    /// - `cell_num`: The index of the cell to read (0-based).
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the cell index is out of bounds or the slice cannot be accessed.
    pub fn internal_node_cell(&mut self, cell_num: u32) -> Result<&mut [u8], Error> {
        let offset = INTERNAL_NODE_HEADER_SIZE + (cell_num as usize * INTERNAL_NODE_CELL_SIZE);
        if offset + INTERNAL_NODE_CELL_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Cell offset exceeds buffer size (offset={}, buffer={})",
                offset + INTERNAL_NODE_CELL_SIZE,
                self.data.len()
            ));
        }
        Ok(&mut self.data[offset..offset + INTERNAL_NODE_CELL_SIZE])
    }

    /// Reads the child pointer at the specified index.
    ///
    /// If `child_num` equals the number of keys, returns the right child pointer.
    /// Otherwise, returns the child pointer from the specified cell.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if `child_num` exceeds the number of keys or the slice cannot be accessed.
    pub fn internal_node_child(&mut self, child_num: u32) -> Result<&mut [u8], Error> {
        let num_keys = self.internal_node_num_keys()?;
        if child_num > num_keys {
            return Err(err!(
                Storage,
                "Child index {} exceeds num_keys {}",
                child_num,
                num_keys
            ));
        }
        if child_num == num_keys {
            Ok(&mut self.internal_node_right_child()?[..])
        } else {
            Ok(&mut self.internal_node_cell(child_num)?[..INTERNAL_NODE_CHILD_SIZE])
        }
    }

    /// Reads the key at the specified index.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the key index is out of bounds or the slice cannot be decoded.
    pub fn internal_node_key(&self, key_num: u32) -> Result<u32, Error> {
        let num_keys = self.internal_node_num_keys()?;
        if key_num >= num_keys {
            return Err(err!(
                Storage,
                "Key index {} exceeds num_keys {}",
                key_num,
                num_keys
            ));
        }
        let offset = INTERNAL_NODE_HEADER_SIZE
            + (key_num as usize * INTERNAL_NODE_CELL_SIZE)
            + INTERNAL_NODE_CHILD_SIZE;
        if offset + INTERNAL_NODE_KEY_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Key offset exceeds buffer size (offset={}, buffer={})",
                offset + INTERNAL_NODE_KEY_SIZE,
                self.data.len()
            ));
        }
        let bytes = &self.data[offset..offset + INTERNAL_NODE_KEY_SIZE];
        Ok(u32::from_le_bytes(bytes.try_into().map_err(|e| {
            err!(Storage, "Failed to decode key: {:?}", e)
        })?))
    }

    /// Sets the key at the specified index.
    ///
    /// # Errors
    /// Returns an `Error::Storage` if the key index is out of bounds or the buffer is too small.
    pub fn set_internal_node_key(&mut self, key_num: u32, key_value: u32) -> Result<(), Error> {
        let num_keys = self.internal_node_num_keys()?;
        if key_num >= num_keys {
            return Err(err!(
                Storage,
                "Key index {} exceeds num_keys {}",
                key_num,
                num_keys
            ));
        }
        let offset = INTERNAL_NODE_HEADER_SIZE
            + (key_num as usize * INTERNAL_NODE_CELL_SIZE)
            + INTERNAL_NODE_CHILD_SIZE;
        if offset + INTERNAL_NODE_KEY_SIZE > self.data.len() {
            return Err(err!(
                Storage,
                "Key offset exceeds buffer size (offset={}, buffer={})",
                offset + INTERNAL_NODE_KEY_SIZE,
                self.data.len()
            ));
        }
        self.data[offset..offset + INTERNAL_NODE_KEY_SIZE]
            .copy_from_slice(&key_value.to_le_bytes());
        Ok(())
    }

    pub fn get_node_max_key(&self) -> Result<u32, Error> {
        match self.get_node_type()? {
            NodeType::NodeInternal => {
                Ok(self.internal_node_key(self.internal_node_num_keys()? - 1)?)
            }
            NodeType::NodeLeaf => {
                Ok(self.leaf_node_key((self.leaf_node_num_cells()? - 1) as usize)?)
            }
        }
    }

    // Leaf node methods (should ideally be in a separate LeafNode struct)
    /// Number of cells assigned to the right sibling when splitting a leaf node.
    pub fn leaf_node_right_split_count(&self) -> usize {
        (self.leaf_node_max_cells + 1) / 2
    }

    /// Number of cells assigned to the left sibling when splitting a leaf node.
    pub fn leaf_node_left_split_count(&self) -> usize {
        (self.leaf_node_max_cells + 1) - self.leaf_node_right_split_count()
    }

    /// Maximum number of cells that can fit in a leaf node.
    pub fn leaf_node_max_cells(&self) -> usize {
        self.leaf_node_max_cells
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
