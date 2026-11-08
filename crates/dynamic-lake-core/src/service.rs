use std::sync::Arc;

use crate::error::PipelineResult;
use crate::graph::{GraphOperations, PipelineGraph};
use crate::refresh::{PipelineCatalog, RefreshPlan, RefreshPlanner, RefreshRequest};

#[derive(Clone)]
pub struct PipelineService {
    catalog: Arc<dyn PipelineCatalog>,
}

impl PipelineService {
    pub fn new(catalog: Arc<dyn PipelineCatalog>) -> Self {
        Self { catalog }
    }

    pub async fn plan(
        &self,
        graph: &PipelineGraph,
        request: &RefreshRequest,
    ) -> PipelineResult<RefreshPlan> {
        graph.validate()?;
        RefreshPlanner
            .plan(graph, self.catalog.as_ref(), request)
            .await
    }
}
