use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use dynamic_lake_core::{
    DynamicTable, ExecutionStatus, NoopExecutionPlanner, PipelineCatalog, PipelineExecution,
    PipelineGraph, PipelineResult, PipelineService, RefreshMode, RefreshRequest, TableIdentifier,
};

struct Catalog;

#[async_trait]
impl PipelineCatalog for Catalog {
    async fn latest_version(&self, _table: &TableIdentifier) -> PipelineResult<i64> {
        Ok(7)
    }
}

#[tokio::test]
async fn plans_incremental_closure_and_executes_dag() -> PipelineResult<()> {
    let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
        .build()
        .map_err(|error| dynamic_lake_core::PipelineError::Execution(error.to_string()))?;
    let clean_id: TableIdentifier = "paimon.default.clean".parse()?;
    let summary_id: TableIdentifier = "paimon.default.summary".parse()?;
    let raw: TableIdentifier = "paimon.default.raw".parse()?;
    let clean = DynamicTable::new(clean_id.clone(), plan.clone()).with_external_input(raw);
    let summary = DynamicTable::new(summary_id.clone(), plan).with_upstream(clean_id.clone());
    let graph = PipelineGraph::try_new(BTreeMap::from([
        (clean_id, clean),
        (summary_id.clone(), summary),
    ]))?;
    let service = PipelineService::new(Arc::new(Catalog));
    let request = RefreshRequest {
        mode: RefreshMode::Incremental {
            roots: vec!["clean".to_string()],
        },
    };
    let plan = service.plan(&graph, &request).await?;
    assert_eq!(plan.source_versions.len(), 1);
    assert_eq!(plan.selected_nodes().count(), 2);
    let report = PipelineExecution::new(&graph, &plan, &NoopExecutionPlanner)
        .run()
        .await?;
    assert!(report.failures.is_empty());
    assert_eq!(report.statuses[&summary_id], ExecutionStatus::Successful);

    Ok(())
}
