use super::column::ColumnType;

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
    pub version: u32,
}

impl TableSchema {
    pub fn get_row_size(&self) -> usize {
        self.columns.iter().map(|c| c.type_.fixed_size()).sum()
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub type_: ColumnType,
    pub default: Option<String>,
    pub is_primary: bool,
    pub is_nullable: bool,
}
