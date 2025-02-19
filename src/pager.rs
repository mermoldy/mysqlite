/// The pager.
use std::collections::HashMap;

struct Row {
    id: i8,             // integer
    username: [u8; 32], // varchar(32)
    email: [u8; 255],   // varchar(255)
}

fn str_to_fixed_bytes<const N: usize>(input: &str) -> [u8; N] {
    let mut buffer = [0u8; N];
    let bytes = input.as_bytes();
    let len = bytes.len().min(N);
    buffer[..len].copy_from_slice(&bytes[..len]);
    buffer
}

fn serialize_row(row: Row) {}

// fn deserialize_row() -> Row {}

// +void serialize_row(Row* source, void* destination) {
//     +  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
//     +  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
//     +  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
// +}

// +void deserialize_row(void* source, Row* destination) {
//     +  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
//     +  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
//     +  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
// +}

/// We’ll start with a simpe array pager that will group rows into pages,
/// but instead of arranging those pages as a tree it will arrange them
/// as an array.
pub fn load() -> Vec<Row> {
    vec![Row {
        id: 0,
        username: str_to_fixed_bytes(""),
        email: str_to_fixed_bytes(""),
    }]
}
