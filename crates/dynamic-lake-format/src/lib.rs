//! Open lake table format SPI.
//!
//! Format implementations (Paimon, Delta, Iceberg, ...) live in
//! separate crates. The DAG/refresh layer only consumes
//! [`dynamic_lake_core::PipelineCatalog`]; this crate adds the catalog DDL and
//! DataFusion plan-node boundaries needed by a concrete backend.

pub mod catalog;
pub mod error;
pub mod planner;
pub mod registry;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use catalog::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, DropDatabaseRequest,
    DropTableRequest, LakeCatalog, LakeFormatKind, LakeTable, TableAlteration,
};
pub use error::{LakeFormatError, LakeFormatResult};
pub use planner::{
    CHANGE_KIND_COLUMN, ChangeReadPlanRequest, LakeTablePlanner, ReadPlan, ReadPlanRequest,
    ScanAuthority, WriteMode, WritePlan, WritePlanRequest,
};
pub use registry::LakeFormatRegistry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FormatCapability {
    CreateTable,
    AlterTable,
    DropTable,
    SnapshotVersion,
    FullScan,
    IncrementalScan,
    Append,
    Overwrite,
    Upsert,
    CatalogCommit,
}

/// A registered format implementation.
///
/// The registry resolves table names to a format, while `catalog` and `planner`
/// keep DDL/metadata and read/write planning as independent implementations.
pub trait LakeFormat: Send + Sync {
    fn name(&self) -> &str;
    fn kind(&self) -> LakeFormatKind;
    fn capabilities(&self) -> &[FormatCapability];
    fn catalog(&self) -> &dyn LakeCatalog;
    fn planner(&self) -> &dyn LakeTablePlanner;
}

/// Hook for formats whose version provider requires runtime access beyond the
/// catalog trait.
#[async_trait]
pub trait TableVersionProvider: Send + Sync {
    async fn latest_version(
        &self,
        table: &dynamic_lake_core::TableIdentifier,
    ) -> LakeFormatResult<i64>;
}
