/// Represents an SQL command with its parsed statement and original SQL string.
#[derive(Debug)]
pub struct SqlCommand {
    pub statement: Statement,
    pub sql: String,
}

/// Variants of `CREATE` statements.
#[derive(Debug)]
pub enum CreateStatement {
    CreateDatabaseStatement(CreateDatabaseStatement),
    CreateTableStatement(CreateTableStatement),
}

/// Variants of `DROP` statements.
#[derive(Debug)]
pub enum DropStatement {
    DropDatabasesStatement(String),
    DropTablesStatement(String),
}

/// Variants of `SHOW` statements.
#[derive(Debug)]
pub enum ShowStatement {
    ShowDatabasesStatement,
    ShowTablesStatement,
}

/// Core SQL statement types supported by the parser.
#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
    Drop(DropStatement),
    Show(ShowStatement),
}

/// SQL clauses (currently unused but included for future expansion).
#[derive(Debug)]
pub enum Clause {
    Join,
    Where,
}

/// Represents an `INSERT` statement with table, columns, and values.
#[derive(Debug)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

/// Represents column selection in a `SELECT` statement.
#[derive(Debug)]
pub enum Columns {
    All,
    List(Vec<String>),
}

/// Represents a `SELECT` statement with table and columns.
#[derive(Debug)]
pub struct SelectStatement {
    pub table: String,
    pub columns: Columns,
}

/// Represents a `CREATE DATABASE` statement.
#[derive(Debug)]
pub struct CreateDatabaseStatement {
    pub name: String,
}

/// Represents a `CREATE TABLE` statement with table name and column schemas.
#[derive(Debug)]
pub struct CreateTableStatement {
    pub name: String,
    pub columns_schemas: Vec<ColumnSchema>,
}

/// Supported SQL column data types.
#[derive(Debug)]
pub enum ColumnType {
    Int,          // i64, equivalent to SQL's BIGINT
    SmallInt,     // i16
    TinyInt,      // i8
    BigInt,       // i128
    Float,        // 32-bit floating point
    Double,       // 64-bit floating point
    VarChar(u16), // Variable-length string with max length
    Text,         // Unbounded string
    DateTime,     // Date and time combined
    Timestamp,    // Date and time with timezone information
    Boolean,      // True/False value
}

/// Represents a column schema in a `CREATE TABLE` statement.
#[derive(Debug)]
pub struct ColumnSchema {
    pub name: String,
    pub is_primary: bool,
    pub type_: ColumnType,
    pub default: Option<String>,
}

/// Represents a `DELETE` statement with table and optional WHERE clause.
#[derive(Debug)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<String>,
}

/// Represents an `UPDATE` statement with table, column-value pairs, and optional WHERE clause.
#[derive(Debug)]
pub struct UpdateStatement {
    pub table: String,
    pub sets: Vec<(String, String)>,
    pub where_clause: Option<String>,
}
