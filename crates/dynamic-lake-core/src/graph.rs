use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::error::{PipelineError, PipelineResult};
use crate::model::{DynamicTable, NodeId};

#[derive(Debug, Clone)]
pub struct DynamicPipelineGraph {
    nodes: BTreeMap<NodeId, DynamicTable>,
    topological_order: Vec<NodeId>,
}

impl DynamicPipelineGraph {
    pub fn try_new(nodes: BTreeMap<NodeId, DynamicTable>) -> PipelineResult<Self> {
        for (id, node) in &nodes {
            if id != &node.id {
                return Err(PipelineError::Invalid(format!(
                    "graph key `{id}` does not match node id `{}`",
                    node.id
                )));
            }
        }
        let topological_order = topological_order(&nodes)?;
        let graph = Self {
            nodes,
            topological_order,
        };
        graph.validate()?;
        Ok(graph)
    }

    pub fn into_nodes(self) -> BTreeMap<NodeId, DynamicTable> {
        self.nodes
    }
}

pub trait GraphOperations {
    fn nodes(&self) -> &BTreeMap<NodeId, DynamicTable>;
    fn topological_order(&self) -> &[NodeId];

    fn node(&self, id: &NodeId) -> Option<&DynamicTable> {
        self.nodes().get(id)
    }

    fn validate(&self) -> PipelineResult<()> {
        let mut visited = BTreeSet::new();
        for id in self.topological_order() {
            let node = self.node(id).ok_or_else(|| {
                PipelineError::Invalid(format!("topological node `{id}` is missing"))
            })?;
            for upstream in &node.upstream {
                if !self.nodes().contains_key(upstream) {
                    return Err(PipelineError::Invalid(format!(
                        "node `{id}` depends on unknown node `{upstream}`"
                    )));
                }
                if !visited.contains(upstream) {
                    return Err(PipelineError::Invalid(format!(
                        "node `{id}` appears before upstream node `{upstream}`"
                    )));
                }
            }
            visited.insert(id.clone());
        }
        Ok(())
    }
}

impl GraphOperations for DynamicPipelineGraph {
    fn nodes(&self) -> &BTreeMap<NodeId, DynamicTable> {
        &self.nodes
    }

    fn topological_order(&self) -> &[NodeId] {
        &self.topological_order
    }
}

pub type PipelineGraph = DynamicPipelineGraph;

fn topological_order(nodes: &BTreeMap<NodeId, DynamicTable>) -> PipelineResult<Vec<NodeId>> {
    let mut indegree = nodes
        .keys()
        .map(|id| (id.clone(), 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut downstream = nodes
        .keys()
        .map(|id| (id.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();

    for (id, node) in nodes {
        for upstream in &node.upstream {
            if !nodes.contains_key(upstream) {
                return Err(PipelineError::Invalid(format!(
                    "node `{id}` depends on unknown node `{upstream}`"
                )));
            }
            *indegree
                .get_mut(id)
                .ok_or_else(|| PipelineError::Invalid(format!("missing node `{id}`")))? += 1;
            if let Some(children) = downstream.get_mut(upstream) {
                children.insert(id.clone());
            }
        }
    }

    let mut queue = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<VecDeque<_>>();
    let mut order = Vec::with_capacity(nodes.len());
    while let Some(id) = queue.pop_front() {
        order.push(id.clone());
        for child in downstream.get(&id).into_iter().flatten() {
            let count = indegree.get_mut(child).ok_or_else(|| {
                PipelineError::Invalid(format!("unknown downstream node `{child}`"))
            })?;
            *count -= 1;
            if *count == 0 {
                queue.push_back(child.clone());
            }
        }
    }

    if order.len() != nodes.len() {
        return Err(PipelineError::Invalid(
            "pipeline graph contains a dependency cycle".to_string(),
        ));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, upstream: &[&str]) -> DynamicTable {
        let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false)
            .build()
            .expect("empty logical plan");
        let mut node = DynamicTable::new(id.parse().expect("valid node id"), plan);
        node.upstream = upstream
            .iter()
            .map(|id| id.parse().expect("valid upstream id"))
            .collect();
        node
    }

    #[test]
    fn orders_diamond_dependencies() -> PipelineResult<()> {
        let graph = DynamicPipelineGraph::try_new(BTreeMap::from([
            ("a".parse().expect("valid id"), node("a", &[])),
            ("b".parse().expect("valid id"), node("b", &["a"])),
            ("c".parse().expect("valid id"), node("c", &["a"])),
            ("d".parse().expect("valid id"), node("d", &["b", "c"])),
        ]))?;
        let order = graph.topological_order();
        assert_eq!(order.first().map(NodeId::table), Some("a"));
        assert_eq!(order.last().map(NodeId::table), Some("d"));
        Ok(())
    }

    #[test]
    fn rejects_cycles() {
        let result = DynamicPipelineGraph::try_new(BTreeMap::from([
            ("a".parse().expect("valid id"), node("a", &["b"])),
            ("b".parse().expect("valid id"), node("b", &["a"])),
        ]));
        assert!(matches!(result, Err(PipelineError::Invalid(_))));
    }
}
