use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use dynamic_lake_core::{IncrementInterval, TableIdentifier};
use serde::{Deserialize, Serialize};

/// Standard change kind emitted by [`LakeTablePlanner::plan_changes`].
pub const CHANGE_KIND_COLUMN: &str = "__dl_change_kind";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ScanAuthority {
    #[default]
    Client,
    Catalog,
    Provider,
}

#[derive(Clone)]
pub struct ReadPlanRequest {
    pub table: TableIdentifier,
    pub snapshot: Option<i64>,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
}

#[derive(Clone)]
pub struct ChangeReadPlanRequest {
    pub table: TableIdentifier,
    pub interval: IncrementInterval,
}

#[derive(Clone)]
pub struct ReadPlan {
    pub execution: Arc<dyn ExecutionPlan>,
    pub snapshot_version: Option<i64>,
    pub scan_authority: ScanAuthority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Append,
    Overwrite,
    Change,
}

pub struct WritePlanRequest {
    pub table: TableIdentifier,
    pub input: Arc<dyn ExecutionPlan>,
    pub mode: WriteMode,
    pub expected_snapshot: Option<i64>,
}

#[derive(Clone)]
pub struct WritePlan {
    pub execution: Arc<dyn ExecutionPlan>,
    pub commit_authority: String,
}

/// DataFusion physical-plan boundary for lake table reads and writes.
///
/// Format crates own scan/sink nodes and commit semantics. DynamicLake's DAG
/// scheduler only sees completed node executions.
#[async_trait]
pub trait LakeTablePlanner: Send + Sync {
    async fn plan_read(
        &self,
        session: &dyn Session,
        request: ReadPlanRequest,
    ) -> super::LakeFormatResult<ReadPlan>;

    /// Plan a native change read over (before_exclusive, after_inclusive].
    ///
    /// The first output column must be [`CHANGE_KIND_COLUMN`] as non-null Int32:
    /// [`dynamic_lake_core::RowKind::Negative`] for a removed row or before image, and
    /// [`dynamic_lake_core::RowKind::Positive`] for an added row or after image.
    async fn plan_changes(
        &self,
        session: &dyn Session,
        request: ChangeReadPlanRequest,
    ) -> super::LakeFormatResult<ReadPlan> {
        let _ = (session, request);
        Err(super::LakeFormatError::Unsupported(
            "native change reads".to_string(),
        ))
    }

    async fn plan_write(
        &self,
        session: &dyn Session,
        request: WritePlanRequest,
    ) -> super::LakeFormatResult<WritePlan>;
}
