use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{PipelineError, PipelineResult};
use crate::graph::{GraphOperations, PipelineGraph};
use crate::model::{DynamicTable, NodeId, TableName};
use crate::refresh::{NodeRefreshMode, RefreshPlan, RefreshStep};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecutionResult {
    /// The output table snapshot returned by the format writer, if one exists.
    pub output_version: Option<i64>,
    pub rows_written: u64,
}

#[derive(Debug, Clone)]
pub struct RefreshTask {
    pub definition: DynamicTable,
    pub step: RefreshStep,
    pub source_versions: BTreeMap<TableName, i64>,
}

#[async_trait]
pub trait NodeExecution: Send + Sync {
    async fn execute(&self) -> PipelineResult<NodeExecutionResult>;
}

pub trait NodeExecutionPlanner: Send + Sync {
    fn plan(&self, task: &RefreshTask) -> PipelineResult<Box<dyn NodeExecution>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionStatus {
    Queued,
    Running,
    Successful,
    Failed,
    Skipped,
    Excluded,
}

#[derive(Debug, Clone)]
pub struct GraphExecutionResult {
    pub statuses: BTreeMap<NodeId, ExecutionStatus>,
    pub node_versions: BTreeMap<NodeId, Option<i64>>,
    pub failures: BTreeMap<NodeId, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub statuses: BTreeMap<NodeId, ExecutionStatus>,
    pub node_versions: BTreeMap<NodeId, Option<i64>>,
    pub failures: BTreeMap<NodeId, String>,
}

/// Run the selected part of a refresh plan with a DAG ready queue.
pub struct GraphExecution<'a> {
    graph: &'a PipelineGraph,
    plan: &'a RefreshPlan,
    planner: &'a dyn NodeExecutionPlanner,
}

impl<'a> GraphExecution<'a> {
    pub fn new(
        graph: &'a PipelineGraph,
        plan: &'a RefreshPlan,
        planner: &'a dyn NodeExecutionPlanner,
    ) -> Self {
        Self {
            graph,
            plan,
            planner,
        }
    }

    pub async fn execute(&self) -> PipelineResult<GraphExecutionResult> {
        let mut statuses = self
            .graph
            .nodes()
            .keys()
            .map(|id| {
                let status = if self.plan.excluded.contains(id) {
                    ExecutionStatus::Excluded
                } else if self
                    .plan
                    .steps
                    .get(id)
                    .is_some_and(|step| step.mode == NodeRefreshMode::Skip)
                {
                    ExecutionStatus::Skipped
                } else {
                    ExecutionStatus::Queued
                };
                (id.clone(), status)
            })
            .collect::<BTreeMap<_, _>>();
        let mut failures = BTreeMap::new();
        let mut node_versions = BTreeMap::new();
        let mut pending = self
            .plan
            .steps
            .values()
            .filter(|step| step.mode != NodeRefreshMode::Skip)
            .collect::<Vec<_>>();

        loop {
            let ready = pending
                .iter()
                .copied()
                .filter(|step| statuses.get(&step.node_id) == Some(&ExecutionStatus::Queued))
                .filter(|step| {
                    step.required_outputs.iter().all(|upstream| {
                        matches!(
                            statuses.get(upstream),
                            Some(ExecutionStatus::Successful | ExecutionStatus::Excluded)
                        )
                    })
                })
                .collect::<Vec<_>>();
            if ready.is_empty() {
                break;
            }

            for step in ready {
                statuses.insert(step.node_id.clone(), ExecutionStatus::Running);
                let Some(definition) = self.graph.node(&step.node_id) else {
                    return Err(PipelineError::Invalid(format!(
                        "dynamic table `{}` disappeared",
                        step.node_id
                    )));
                };
                let task = RefreshTask {
                    definition: definition.clone(),
                    step: step.clone(),
                    source_versions: self.plan.source_versions.clone(),
                };
                let result = match self.planner.plan(&task) {
                    Ok(execution) => execution.execute().await,
                    Err(error) => Err(error),
                };
                match result {
                    Ok(result) => {
                        node_versions.insert(step.node_id.clone(), result.output_version);
                        statuses.insert(step.node_id.clone(), ExecutionStatus::Successful);
                    }
                    Err(error) => {
                        failures.insert(step.node_id.clone(), error.to_string());
                        statuses.insert(step.node_id.clone(), ExecutionStatus::Failed);
                        self.skip_downstream(&step.node_id, &mut statuses);
                    }
                }
            }
            pending.retain(|step| {
                matches!(
                    statuses.get(&step.node_id),
                    Some(ExecutionStatus::Queued | ExecutionStatus::Running)
                )
            });
        }

        Ok(GraphExecutionResult {
            statuses,
            node_versions,
            failures,
        })
    }

    fn skip_downstream(&self, node: &NodeId, statuses: &mut BTreeMap<NodeId, ExecutionStatus>) {
        let downstream = self
            .graph
            .downstream_closure([node.clone()])
            .into_iter()
            .filter(|id| id != node);
        for child in downstream {
            if self.plan.steps.contains_key(&child) {
                statuses.insert(child, ExecutionStatus::Skipped);
            }
        }
    }
}

pub struct PipelineExecution<'a> {
    graph: &'a PipelineGraph,
    plan: &'a RefreshPlan,
    planner: &'a dyn NodeExecutionPlanner,
}

impl<'a> PipelineExecution<'a> {
    pub fn new(
        graph: &'a PipelineGraph,
        plan: &'a RefreshPlan,
        planner: &'a dyn NodeExecutionPlanner,
    ) -> Self {
        Self {
            graph,
            plan,
            planner,
        }
    }

    pub async fn run(&self) -> PipelineResult<RunReport> {
        self.graph.validate()?;
        let result = GraphExecution::new(self.graph, self.plan, self.planner)
            .execute()
            .await?;
        Ok(RunReport {
            statuses: result.statuses,
            node_versions: result.node_versions,
            failures: result.failures,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopExecutionPlanner;

impl NodeExecutionPlanner for NoopExecutionPlanner {
    fn plan(&self, _task: &RefreshTask) -> PipelineResult<Box<dyn NodeExecution>> {
        Ok(Box::new(NoopExecution))
    }
}

struct NoopExecution;

#[async_trait]
impl NodeExecution for NoopExecution {
    async fn execute(&self) -> PipelineResult<NodeExecutionResult> {
        Ok(NodeExecutionResult {
            output_version: None,
            rows_written: 0,
        })
    }
}
