use async_trait::async_trait;

use crate::error::{PipelineError, PipelineResult};
use crate::graph::{GraphOperations, PipelineGraph};
use crate::model::DynamicTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMode {
    Full,
    Incremental,
}

#[async_trait]
pub trait NodeExecution: Send + Sync {
    async fn execute(&self) -> PipelineResult<()>;
}

#[async_trait]
pub trait NodeExecutionPlanner: Send + Sync {
    /// Return None when this node has no changes to refresh.
    async fn plan(
        &self,
        node: &DynamicTable,
        mode: RefreshMode,
    ) -> PipelineResult<Option<Box<dyn NodeExecution>>>;
}

/// Plan and execute each node after its upstream nodes have committed.
pub struct GraphExecution<'a> {
    graph: &'a PipelineGraph,
    planner: &'a dyn NodeExecutionPlanner,
}

impl<'a> GraphExecution<'a> {
    pub fn new(graph: &'a PipelineGraph, planner: &'a dyn NodeExecutionPlanner) -> Self {
        Self { graph, planner }
    }

    pub async fn run(&self, mode: RefreshMode) -> PipelineResult<()> {
        self.graph.validate()?;
        for id in self.graph.topological_order() {
            let node = self.graph.node(id).ok_or_else(|| {
                PipelineError::Invalid(format!("dynamic table `{id}` disappeared"))
            })?;
            if let Some(execution) = self.planner.plan(node, mode).await? {
                execution.execute().await?;
            }
        }
        Ok(())
    }
}
