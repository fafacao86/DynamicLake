use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use dynamic_lake_core::TableIdentifier;
use serde::{Deserialize, Serialize};

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
pub struct ReadPlan {
    pub execution: Arc<dyn ExecutionPlan>,
    pub snapshot_version: Option<i64>,
    pub scan_authority: ScanAuthority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Append,
    Overwrite,
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

    async fn plan_write(
        &self,
        session: &dyn Session,
        request: WritePlanRequest,
    ) -> super::LakeFormatResult<WritePlan>;
}
