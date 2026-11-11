use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use dynamic_lake_core::{
    DynamicTable, GraphExecution, NodeExecution, NodeExecutionPlanner, PipelineGraph,
    PipelineResult, RefreshMode,
};

struct Recorder(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl NodeExecutionPlanner for Recorder {
    async fn plan(
        &self,
        node: &DynamicTable,
        _mode: RefreshMode,
    ) -> PipelineResult<Option<Box<dyn NodeExecution>>> {
        self.0
            .lock()
            .unwrap()
            .push(format!("plan:{}", node.id.table()));
        Ok(Some(Box::new(RecordExecution(
            Arc::clone(&self.0),
            node.id.table().to_string(),
        ))))
    }
}

struct RecordExecution(Arc<Mutex<Vec<String>>>, String);

#[async_trait]
impl NodeExecution for RecordExecution {
    async fn execute(&self) -> PipelineResult<()> {
        self.0.lock().unwrap().push(format!("exec:{}", self.1));
        Ok(())
    }
}

#[tokio::test]
async fn plans_each_node_after_its_upstream_executes() -> PipelineResult<()> {
    let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
        .build()
        .map_err(|error| dynamic_lake_core::PipelineError::Execution(error.to_string()))?;
    let first = DynamicTable::new("paimon.default.first".parse()?, plan.clone());
    let second = DynamicTable::new("paimon.default.second".parse()?, plan.clone())
        .with_upstream(first.id.clone());
    let third =
        DynamicTable::new("paimon.default.third".parse()?, plan).with_upstream(second.id.clone());
    let graph = PipelineGraph::try_new(BTreeMap::from([
        (third.id.clone(), third),
        (first.id.clone(), first),
        (second.id.clone(), second),
    ]))?;
    let events = Arc::new(Mutex::new(Vec::new()));
    GraphExecution::new(&graph, &Recorder(Arc::clone(&events)))
        .run(RefreshMode::Incremental)
        .await?;
    assert_eq!(
        *events.lock().unwrap(),
        [
            "plan:first",
            "exec:first",
            "plan:second",
            "exec:second",
            "plan:third",
            "exec:third",
        ]
    );
    Ok(())
}
