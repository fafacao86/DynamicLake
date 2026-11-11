use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use dynamic_lake_core::{PipelineError, PipelineResult};

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
