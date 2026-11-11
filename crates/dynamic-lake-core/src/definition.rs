use std::collections::{BTreeMap, BTreeSet};

use crate::graph::PipelineGraph;
use crate::model::{DynamicTable, TableIdentifier};
use crate::{PipelineError, PipelineResult};
use datafusion::logical_expr::LogicalPlan;

#[derive(Debug, Clone)]
pub struct ViewDefinition {
    pub name: String,
    pub sql: String,
}

/// Classify view references and create the dependency graph.
pub fn build_view_graph(
    views: Vec<(TableIdentifier, LogicalPlan, BTreeSet<TableIdentifier>)>,
) -> PipelineResult<PipelineGraph> {
    let ids = views
        .iter()
        .map(|(id, _, _)| id.clone())
        .collect::<BTreeSet<_>>();
    if ids.len() != views.len() {
        return Err(PipelineError::Invalid(
            "duplicate dynamic table name".to_string(),
        ));
    }

    let nodes = views
        .into_iter()
        .map(|(id, query, references)| {
            let (upstream, external_inputs): (BTreeSet<_>, BTreeSet<_>) = references
                .into_iter()
                .partition(|reference| ids.contains(reference));
            (
                id.clone(),
                DynamicTable {
                    id,
                    query,
                    upstream,
                    external_inputs,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    PipelineGraph::try_new(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphOperations;

    #[test]
    fn classifies_view_and_external_sources() -> PipelineResult<()> {
        let query = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
            .build()
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        let source: TableIdentifier = "paimon.default.source".parse()?;
        let first: TableIdentifier = "paimon.default.first".parse()?;
        let second: TableIdentifier = "paimon.default.second".parse()?;
        let graph = build_view_graph(vec![
            (
                second.clone(),
                query.clone(),
                BTreeSet::from([first.clone()]),
            ),
            (first.clone(), query, BTreeSet::from([source.clone()])),
        ])?;
        assert_eq!(graph.topological_order(), &[first.clone(), second.clone()]);
        assert!(
            graph
                .node(&first)
                .unwrap()
                .external_inputs
                .contains(&source)
        );
        assert!(graph.node(&second).unwrap().upstream.contains(&first));
        Ok(())
    }
}
