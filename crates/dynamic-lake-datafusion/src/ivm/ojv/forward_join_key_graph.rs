use std::collections::{BTreeMap, BTreeSet};

use crate::ivm::error::IvmError;

use super::{OjvInputId, OjvJoinKeyBinding, OjvMultiJoin};

/// Forward dependency graph between OJV multi-join inputs.
///
/// A LEFT join contributes an edge from its preserved input to its
/// null-extended input. Other join types are rejected by the OJV analyzer.
/// The secondary planner uses reachability to find affected orphan terms.
///
/// https://www.vldb.org/pvldb/vol18/p5153-zhou.pdf
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardJoinKeyGraph {
    inputs: Vec<OjvInputId>,
    successors: BTreeMap<OjvInputId, BTreeSet<OjvInputId>>,
}

impl ForwardJoinKeyGraph {
    pub fn try_new(
        inputs: impl IntoIterator<Item = OjvInputId>,
        bindings: impl IntoIterator<Item = OjvJoinKeyBinding>,
    ) -> Result<Self, IvmError> {
        let inputs = inputs.into_iter().collect::<BTreeSet<_>>();
        if inputs.is_empty() {
            return Ok(Self {
                inputs: Vec::new(),
                successors: BTreeMap::new(),
            });
        }

        let mut successors = inputs
            .iter()
            .copied()
            .map(|input| (input, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();

        for binding in bindings {
            if !inputs.contains(&binding.left_input) {
                return Err(IvmError::MissingJoinInput(binding.left_input));
            }
            if !inputs.contains(&binding.right_input) {
                return Err(IvmError::MissingJoinInput(binding.right_input));
            }
            successors
                .get_mut(&binding.left_input)
                .expect("validated OJV input must exist")
                .insert(binding.right_input);
        }

        Ok(Self {
            inputs: inputs.into_iter().collect(),
            successors,
        })
    }

    pub fn from_multi_joins(multi_joins: &[OjvMultiJoin]) -> Result<Self, IvmError> {
        let mut inputs = BTreeSet::new();
        let mut bindings = Vec::new();
        for multi_join in multi_joins {
            inputs.extend(multi_join.input_ids());
            bindings.extend(multi_join.join_keys.iter().cloned());
        }
        Self::try_new(inputs, bindings)
    }

    pub fn inputs(&self) -> &[OjvInputId] {
        &self.inputs
    }

    pub fn successors(&self, input: OjvInputId) -> Result<&BTreeSet<OjvInputId>, IvmError> {
        self.successors
            .get(&input)
            .ok_or(IvmError::MissingGraphInput(input))
    }

    /// Return all inputs reachable from `start`, including `start` itself.
    pub fn reachable_from(&self, start: OjvInputId) -> Result<BTreeSet<OjvInputId>, IvmError> {
        if !self.successors.contains_key(&start) {
            return Err(IvmError::MissingGraphInput(start));
        }

        let mut reachable = BTreeSet::new();
        let mut pending = vec![start];
        while let Some(current) = pending.pop() {
            if !reachable.insert(current) {
                continue;
            }
            if let Some(successors) = self.successors.get(&current) {
                pending.extend(successors.iter().copied());
            }
        }
        Ok(reachable)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::JoinType;

    use super::*;

    fn id(value: usize) -> OjvInputId {
        OjvInputId::new(value)
    }

    #[test]
    fn left_join_edges_follow_preserved_to_null_side() {
        let binding = OjvJoinKeyBinding::for_join(id(0), id(1), JoinType::Left)
            .expect("left join is supported");
        let graph = ForwardJoinKeyGraph::try_new([id(0), id(1)], [binding]).expect("valid graph");

        assert_eq!(
            graph.successors(id(0)).expect("input 0"),
            &BTreeSet::from([id(1)])
        );
        assert!(graph.successors(id(1)).expect("input 1").is_empty());
        assert_eq!(
            graph.reachable_from(id(0)).expect("reachable inputs"),
            BTreeSet::from([id(0), id(1)])
        );
        assert_eq!(
            graph.reachable_from(id(1)).expect("reachable inputs"),
            BTreeSet::from([id(1)])
        );
    }

    #[test]
    fn one_input_can_have_multiple_successors() {
        let bindings = vec![
            OjvJoinKeyBinding::new(id(0), id(1)),
            OjvJoinKeyBinding::new(id(0), id(2)),
        ];
        let graph =
            ForwardJoinKeyGraph::try_new([id(0), id(1), id(2)], bindings).expect("valid graph");

        assert_eq!(
            graph.reachable_from(id(0)).expect("reachable inputs"),
            BTreeSet::from([id(0), id(1), id(2)])
        );
    }

    #[test]
    fn reachable_from_terminates_when_graph_has_a_cycle() {
        let bindings = vec![
            OjvJoinKeyBinding::new(id(0), id(1)),
            OjvJoinKeyBinding::new(id(1), id(0)),
        ];
        let graph = ForwardJoinKeyGraph::try_new([id(0), id(1)], bindings).expect("valid graph");

        assert_eq!(
            graph.reachable_from(id(0)).expect("reachable inputs"),
            BTreeSet::from([id(0), id(1)])
        );
    }

    #[test]
    fn non_left_join_bindings_are_rejected() {
        for join_type in [JoinType::Inner, JoinType::Right, JoinType::Full] {
            assert!(matches!(
                OjvJoinKeyBinding::for_join(id(0), id(1), join_type),
                Err(IvmError::UnsupportedJoinType(rejected)) if rejected == join_type
            ));
        }
    }

    #[test]
    fn graph_rejects_inputs_outside_the_multi_join() {
        let binding = OjvJoinKeyBinding::new(id(0), id(9));
        assert!(matches!(
            ForwardJoinKeyGraph::try_new([id(0), id(1)], [binding]),
            Err(IvmError::MissingJoinInput(missing)) if missing == id(9)
        ));
    }
}
