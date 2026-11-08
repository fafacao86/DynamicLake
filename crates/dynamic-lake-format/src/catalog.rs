use async_trait::async_trait;
use datafusion::arrow::datatypes::{FieldRef, SchemaRef};
use dynamic_lake_core::TableIdentifier;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LakeFormatKind {
    Paimon,
    Delta,
    Iceberg,
    Other(String),
}

impl LakeFormatKind {
    pub fn from_name(name: &str) -> Self {
        if name.eq_ignore_ascii_case("paimon") {
            Self::Paimon
        } else if name.eq_ignore_ascii_case("delta") {
            Self::Delta
        } else if name.eq_ignore_ascii_case("iceberg") {
            Self::Iceberg
        } else {
            Self::Other(name.to_string())
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Paimon => "paimon",
            Self::Delta => "delta",
            Self::Iceberg => "iceberg",
            Self::Other(name) => name,
        }
    }
}

/// Format-neutral table exposed by a lake catalog.
///
/// Implementations own their metadata representation. Callers only depend on
/// this interface and never on a format-specific metadata payload.
pub trait LakeTable: Send + Sync + Debug {
    fn identifier(&self) -> &TableIdentifier;
    fn format(&self) -> &LakeFormatKind;
    fn location(&self) -> Option<&str>;
    fn latest_version(&self) -> Option<i64>;
    fn schema(&self) -> &SchemaRef;
    fn properties(&self) -> &[(String, String)];
}

#[derive(Clone)]
pub struct CreateTableRequest {
    pub table: TableIdentifier,
    pub schema: SchemaRef,
    pub partition_by: Vec<String>,
    pub primary_key: Vec<String>,
    pub properties: Vec<(String, String)>,
    pub comment: Option<String>,
    pub ignore_if_exists: bool,
}

#[derive(Clone)]
pub struct CreateDatabaseRequest {
    pub database: String,
    pub properties: Vec<(String, String)>,
    pub if_not_exists: bool,
}

#[derive(Clone)]
pub struct DropDatabaseRequest {
    pub database: String,
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Clone)]
pub enum TableAlteration {
    AddColumn(FieldRef),
    DropColumn(String),
    RenameColumn { from: String, to: String },
    SetProperty { key: String, value: String },
}

#[derive(Clone)]
pub struct AlterTableRequest {
    pub table: TableIdentifier,
    pub alterations: Vec<TableAlteration>,
}

#[derive(Clone)]
pub struct DropTableRequest {
    pub table: TableIdentifier,
    pub if_exists: bool,
    pub purge: bool,
}

/// Catalog and DDL boundary. Implementations translate a format-neutral
/// request into format-specific metadata operations.
#[async_trait]
pub trait LakeCatalog: Send + Sync {
    async fn create_database(&self, request: CreateDatabaseRequest) -> super::LakeFormatResult<()>;

    async fn drop_database(&self, request: DropDatabaseRequest) -> super::LakeFormatResult<()>;

    async fn get_table(
        &self,
        table: &TableIdentifier,
    ) -> super::LakeFormatResult<std::sync::Arc<dyn LakeTable>>;

    async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> super::LakeFormatResult<std::sync::Arc<dyn LakeTable>>;

    async fn alter_table(&self, request: AlterTableRequest) -> super::LakeFormatResult<()>;

    async fn drop_table(&self, request: DropTableRequest) -> super::LakeFormatResult<()>;

    async fn rename_table(
        &self,
        _from: &TableIdentifier,
        _to: &TableIdentifier,
        _if_exists: bool,
    ) -> super::LakeFormatResult<()> {
        Err(super::LakeFormatError::Unsupported(
            "rename table".to_string(),
        ))
    }
}
