use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{PipelineError, PipelineResult};
use crate::graph::{GraphOperations, PipelineGraph};
use crate::model::{NodeId, TableName};

#[async_trait]
pub trait PipelineCatalog: Send + Sync {
    /// Return the current format-owned version/snapshot for a source table.
    async fn latest_version(&self, table: &TableName) -> PipelineResult<i64>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RefreshMode {
    FullAll,
    Full { targets: Vec<String> },
    Incremental { roots: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshRequest {
    pub mode: RefreshMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeRefreshMode {
    Full,
    Incremental,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RefreshReason {
    FullAll,
    ExplicitFull,
    DownstreamOfFull,
    ExplicitIncrementalRoot,
    DownstreamOfIncremental,
    Excluded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshStep {
    pub node_id: NodeId,
    pub mode: NodeRefreshMode,
    pub reason: RefreshReason,
    pub required_outputs: BTreeSet<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshPlan {
    /// A planning-time snapshot of source-table versions.
    pub source_versions: BTreeMap<TableName, i64>,
    pub steps: BTreeMap<NodeId, RefreshStep>,
    pub excluded: BTreeSet<NodeId>,
}

impl RefreshPlan {
    pub fn selected_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.steps
            .iter()
            .filter_map(|(id, step)| (step.mode != NodeRefreshMode::Skip).then_some(id))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RefreshPlanner;

impl RefreshPlanner {
    pub async fn plan(
        &self,
        graph: &PipelineGraph,
        catalog: &dyn PipelineCatalog,
        request: &RefreshRequest,
    ) -> PipelineResult<RefreshPlan> {
        graph.validate()?;
        let (selected, requested_roots) = select_nodes(graph, &request.mode)?;
        if selected.is_empty() {
            return Err(PipelineError::Invalid(
                "refresh request does not select any dynamic table".to_string(),
            ));
        }

        let source_tables = selected
            .iter()
            .filter_map(|id| graph.node(id))
            .flat_map(|node| node.external_inputs.iter().cloned())
            .collect::<BTreeSet<TableName>>();
        let mut source_versions = BTreeMap::new();
        for source in source_tables {
            let version = catalog.latest_version(&source).await?;
            source_versions.insert(source, version);
        }

        let excluded = graph
            .nodes()
            .keys()
            .filter(|id| !selected.contains(*id))
            .cloned()
            .collect::<BTreeSet<_>>();
        let (full_nodes, explicit_full) = match &request.mode {
            RefreshMode::FullAll => (selected.clone(), selected.clone()),
            RefreshMode::Full { .. } => (
                graph.downstream_closure(requested_roots.iter().cloned()),
                requested_roots.clone(),
            ),
            RefreshMode::Incremental { .. } => (BTreeSet::new(), BTreeSet::new()),
        };
        let explicit_incremental = requested_roots
            .difference(&full_nodes)
            .cloned()
            .collect::<BTreeSet<_>>();

        let mut steps = BTreeMap::new();
        for id in graph.topological_order() {
            let node = graph.node(id).ok_or_else(|| {
                PipelineError::Invalid(format!("dynamic table `{id}` disappeared"))
            })?;
            let required_outputs = node
                .upstream
                .intersection(&selected)
                .cloned()
                .collect::<BTreeSet<_>>();
            let (mode, reason) = if excluded.contains(id) {
                (NodeRefreshMode::Skip, RefreshReason::Excluded)
            } else if full_nodes.contains(id) {
                let reason = if matches!(request.mode, RefreshMode::FullAll) {
                    RefreshReason::FullAll
                } else if explicit_full.contains(id) {
                    RefreshReason::ExplicitFull
                } else {
                    RefreshReason::DownstreamOfFull
                };
                (NodeRefreshMode::Full, reason)
            } else {
                let reason = if explicit_incremental.contains(id) {
                    RefreshReason::ExplicitIncrementalRoot
                } else {
                    RefreshReason::DownstreamOfIncremental
                };
                (NodeRefreshMode::Incremental, reason)
            };
            steps.insert(
                id.clone(),
                RefreshStep {
                    node_id: id.clone(),
                    mode,
                    reason,
                    required_outputs,
                },
            );
        }

        Ok(RefreshPlan {
            source_versions,
            steps,
            excluded,
        })
    }
}

fn select_nodes(
    graph: &PipelineGraph,
    mode: &RefreshMode,
) -> PipelineResult<(BTreeSet<NodeId>, BTreeSet<NodeId>)> {
    match mode {
        RefreshMode::FullAll => Ok((graph.nodes().keys().cloned().collect(), BTreeSet::new())),
        RefreshMode::Full { targets } | RefreshMode::Incremental { roots: targets } => {
            let roots = targets
                .iter()
                .map(|target| graph.resolve_node(target))
                .collect::<PipelineResult<BTreeSet<_>>>()?;
            Ok((graph.downstream_closure(roots.iter().cloned()), roots))
        }
    }
}
