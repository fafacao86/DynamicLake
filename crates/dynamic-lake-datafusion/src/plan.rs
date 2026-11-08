use std::collections::BTreeMap;

use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use dynamic_lake_core::{
    DynamicTable, GraphOperations, NodeId, PipelineError, PipelineGraph, PipelineResult,
};

/// Applies DataFusion's logical optimizer before IVM normalizing a definition.
#[derive(Clone)]
pub struct DataFusionPlanOptimizer {
    state: SessionState,
}

impl DataFusionPlanOptimizer {
    pub fn new(state: SessionState) -> Self {
        Self { state }
    }

    pub fn optimize(&self, plan: LogicalPlan) -> PipelineResult<LogicalPlan> {
        self.state
            .optimize(&plan)
            .map_err(|error| PipelineError::Execution(error.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct PlannedPipelineNode {
    pub definition: DynamicTable,
    pub input_plan: LogicalPlan,
    pub execution_plan: LogicalPlan,
}

pub fn plan_pipeline_node(node: DynamicTable) -> PlannedPipelineNode {
    let input_plan = node.query.clone();
    let execution_plan = input_plan.clone();
    PlannedPipelineNode {
        definition: node,
        input_plan,
        execution_plan,
    }
}

#[derive(Clone)]
pub struct DataFusionPipelinePlan {
    nodes: BTreeMap<NodeId, PlannedPipelineNode>,
}

impl DataFusionPipelinePlan {
    pub fn new(nodes: BTreeMap<NodeId, PlannedPipelineNode>) -> Self {
        Self { nodes }
    }

    pub fn node(&self, id: &NodeId) -> Option<&PlannedPipelineNode> {
        self.nodes.get(id)
    }

    pub fn nodes(&self) -> &BTreeMap<NodeId, PlannedPipelineNode> {
        &self.nodes
    }
}

pub fn compile_pipeline_plan(graph: &PipelineGraph) -> PipelineResult<DataFusionPipelinePlan> {
    let mut nodes = BTreeMap::new();
    for id in graph.topological_order() {
        let definition = graph
            .node(id)
            .cloned()
            .ok_or_else(|| PipelineError::Invalid(format!("dynamic table `{id}` disappeared")))?;
        nodes.insert(id.clone(), plan_pipeline_node(definition));
    }
    Ok(DataFusionPipelinePlan::new(nodes))
}
