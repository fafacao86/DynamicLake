use std::collections::{BTreeMap, BTreeSet};

use crate::ivm::error::IvmError;

use super::{OjvInputId, OjvJoinKeyBinding, OjvMultiJoin};

/// Forward dependency graph between OJV multi-join inputs.
///
/// For a LEFT OUTER JOIN, changes propagate from the preserved left input to
/// the null-producing right input. The graph is used by the secondary delta
/// planner to determine which orphan terms may be affected by one base-table
/// change.
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
            for left in &binding.left_inputs {
                if !inputs.contains(left) {
                    return Err(IvmError::MissingJoinInput(*left));
                }
            }
            for right in &binding.right_inputs {
                if !inputs.contains(right) {
                    return Err(IvmError::MissingJoinInput(*right));
                }
            }
            for left in binding.left_inputs {
                let targets = successors
                    .get_mut(&left)
                    .expect("validated OJV input must exist");
                targets.extend(binding.right_inputs.iter().copied());
            }
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
        let bindings = OjvJoinKeyBinding::for_join([id(0)], [id(1)], JoinType::Left)
            .expect("left join is supported");
        let graph = ForwardJoinKeyGraph::try_new([id(0), id(1)], bindings).expect("valid graph");

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
    fn inner_join_edges_are_bidirectional() {
        let bindings = OjvJoinKeyBinding::for_join([id(0)], [id(1)], JoinType::Inner)
            .expect("inner join is supported");
        let graph = ForwardJoinKeyGraph::try_new([id(0), id(1)], bindings).expect("valid graph");

        assert_eq!(
            graph.successors(id(0)).expect("input 0"),
            &BTreeSet::from([id(1)])
        );
        assert_eq!(
            graph.successors(id(1)).expect("input 1"),
            &BTreeSet::from([id(0)])
        );
    }

    #[test]
    fn computed_branch_can_contribute_multiple_sources() {
        let bindings = vec![
            OjvJoinKeyBinding::try_new([id(0)], [id(1), id(2)]).expect("binding"),
            OjvJoinKeyBinding::try_new([id(1)], [id(2)]).expect("binding"),
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
            OjvJoinKeyBinding::try_new([id(0)], [id(1)]).expect("binding"),
            OjvJoinKeyBinding::try_new([id(1)], [id(0)]).expect("binding"),
        ];
        let graph = ForwardJoinKeyGraph::try_new([id(0), id(1)], bindings).expect("valid graph");

        assert_eq!(
            graph.reachable_from(id(0)).expect("reachable inputs"),
            BTreeSet::from([id(0), id(1)])
        );
    }

    #[test]
    fn right_and_full_joins_are_rejected_for_now() {
        assert!(matches!(
            OjvJoinKeyBinding::for_join([id(0)], [id(1)], JoinType::Right),
            Err(IvmError::UnsupportedJoinType(JoinType::Right))
        ));
        assert!(matches!(
            OjvJoinKeyBinding::for_join([id(0)], [id(1)], JoinType::Full),
            Err(IvmError::UnsupportedJoinType(JoinType::Full))
        ));
    }

    #[test]
    fn graph_rejects_inputs_outside_the_multi_join() {
        let binding = OjvJoinKeyBinding::try_new([id(0)], [id(9)]).expect("binding");
        assert!(matches!(
            ForwardJoinKeyGraph::try_new([id(0), id(1)], [binding]),
            Err(IvmError::MissingJoinInput(missing)) if missing == id(9)
        ));
    }
}
