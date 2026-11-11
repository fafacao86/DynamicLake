use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use datafusion::common::NullEquality;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, logical_plan::JoinConstraint};

use crate::ivm::error::IvmError;

use super::ForwardJoinKeyGraph;

/// Input leaf number assigned left-to-right across one OJV analysis.
/// It remains attached to that input during rewrite, but is not persistent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OjvInputId(usize);

impl OjvInputId {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

impl fmt::Display for OjvInputId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

pub fn input_row_id_name(input: OjvInputId) -> String {
    format!("__ivm_input_{}_row_id", input.get())
}

/// One input leaf of a maximal OJV multi-join.
///
/// Every input plan is expected to expose the normalized row-id column by
/// [`crate::ivm::ROW_ID_COLUMN`]; no per-input column handle is stored.
#[derive(Debug, Clone)]
pub struct OjvJoinInput {
    pub id: OjvInputId,
    pub plan: LogicalPlan,
}

/// One directional edge in the forward join-key graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OjvJoinKeyEdge {
    pub left_input: OjvInputId,
    pub right_input: OjvInputId,
}

impl OjvJoinKeyEdge {
    pub fn new(left_input: OjvInputId, right_input: OjvInputId) -> Self {
        Self {
            left_input,
            right_input,
        }
    }

    /// Build the directional edge for the currently supported join shapes.
    ///
    /// LEFT joins propagate changes from their preserved input to the
    /// null-extended input. Other join types are not supported yet.
    pub fn for_join(
        left_input: OjvInputId,
        right_input: OjvInputId,
        join_type: JoinType,
    ) -> Result<Self, IvmError> {
        match join_type {
            JoinType::Left => Ok(Self::new(left_input, right_input)),
            other => Err(IvmError::UnsupportedJoinType(other)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OjvJoinNode {
    pub join_type: JoinType,
    pub on: Vec<(Expr, Expr)>,
    pub filter: Option<Expr>,
    pub join_constraint: JoinConstraint,
    pub null_equality: NullEquality,
    pub null_aware: bool,
    pub left: Box<OjvJoinTree>,
    pub right: Box<OjvJoinTree>,
}

/// Explicit multi-join shape with stable input IDs.
#[derive(Debug, Clone)]
pub enum OjvJoinTree {
    Input(OjvInputId),
    Join(OjvJoinNode),
}

/// A maximal connected join component after OJV normalization.
#[derive(Debug, Clone)]
pub struct OjvMultiJoin {
    pub original_root: LogicalPlan,
    pub root: OjvJoinTree,
    pub inputs: BTreeMap<OjvInputId, OjvJoinInput>,
    pub join_keys: Vec<OjvJoinKeyEdge>,
}

impl OjvMultiJoin {
    pub fn try_new(
        original_root: LogicalPlan,
        root: OjvJoinTree,
        inputs: impl IntoIterator<Item = OjvJoinInput>,
        join_keys: impl IntoIterator<Item = OjvJoinKeyEdge>,
    ) -> Result<Self, IvmError> {
        let mut input_map = BTreeMap::new();
        for input in inputs {
            let input_id = input.id;
            if input_map.insert(input_id, input).is_some() {
                return Err(IvmError::DuplicateInput(input_id));
            }
        }
        let join_keys = join_keys.into_iter().collect::<Vec<_>>();
        for edge in &join_keys {
            for id in [edge.left_input, edge.right_input] {
                if !input_map.contains_key(&id) {
                    return Err(IvmError::MissingJoinInput(id));
                }
            }
        }
        Ok(Self {
            original_root,
            root,
            inputs: input_map,
            join_keys,
        })
    }

    pub fn input_ids(&self) -> BTreeSet<OjvInputId> {
        self.inputs.keys().copied().collect()
    }
}

/// Statement-scoped OJV analysis shared by primary and secondary planners.
#[derive(Debug, Clone)]
pub struct OjvAnalysis {
    pub multi_joins: Vec<OjvMultiJoin>,
    pub forward_join_key_graph: ForwardJoinKeyGraph,
}

impl OjvAnalysis {
    pub fn try_new(multi_joins: Vec<OjvMultiJoin>) -> Result<Self, IvmError> {
        let forward_join_key_graph = ForwardJoinKeyGraph::from_multi_joins(&multi_joins)?;
        Ok(Self {
            multi_joins,
            forward_join_key_graph,
        })
    }
}
