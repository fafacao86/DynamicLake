use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use datafusion::common::NullEquality;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, logical_plan::JoinConstraint};

use crate::ivm::error::IvmError;

use super::ForwardJoinKeyGraph;

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

/// A stable input of one maximal OJV multi-join.
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
pub struct OjvJoinKeyBinding {
    pub left_inputs: Vec<OjvInputId>,
    pub right_inputs: Vec<OjvInputId>,
}

impl OjvJoinKeyBinding {
    pub fn try_new(
        left_inputs: impl IntoIterator<Item = OjvInputId>,
        right_inputs: impl IntoIterator<Item = OjvInputId>,
    ) -> Result<Self, IvmError> {
        let left_inputs = left_inputs.into_iter().collect::<Vec<_>>();
        let right_inputs = right_inputs.into_iter().collect::<Vec<_>>();
        if left_inputs.is_empty() || right_inputs.is_empty() {
            return Err(IvmError::EmptyJoinKeyBinding);
        }
        Ok(Self {
            left_inputs,
            right_inputs,
        })
    }

    /// Build the directional bindings for the currently supported join shapes.
    ///
    /// LEFT OUTER JOIN only propagates changes from its preserved left side to
    /// its null-producing right side. INNER JOIN is retained because it is a
    /// valid segment of the normalized OJV left-deep path.
    pub fn for_join(
        left_inputs: impl IntoIterator<Item = OjvInputId>,
        right_inputs: impl IntoIterator<Item = OjvInputId>,
        join_type: JoinType,
    ) -> Result<Vec<Self>, IvmError> {
        let left_to_right = Self::try_new(left_inputs, right_inputs)?;
        match join_type {
            JoinType::Left => Ok(vec![left_to_right]),
            JoinType::Inner => {
                let right_to_left = Self::try_new(
                    left_to_right.right_inputs.clone(),
                    left_to_right.left_inputs.clone(),
                )?;
                Ok(vec![left_to_right, right_to_left])
            }
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

/// Explicit multi-join shape with stable input bindings.
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
    pub join_keys: Vec<OjvJoinKeyBinding>,
}

impl OjvMultiJoin {
    pub fn try_new(
        original_root: LogicalPlan,
        root: OjvJoinTree,
        inputs: impl IntoIterator<Item = OjvJoinInput>,
        join_keys: impl IntoIterator<Item = OjvJoinKeyBinding>,
    ) -> Result<Self, IvmError> {
        let mut input_map = BTreeMap::new();
        for input in inputs {
            let input_id = input.id;
            if input_map.insert(input_id, input).is_some() {
                return Err(IvmError::DuplicateInput(input_id));
            }
        }
        let join_keys = join_keys.into_iter().collect::<Vec<_>>();
        for binding in &join_keys {
            for id in binding
                .left_inputs
                .iter()
                .chain(binding.right_inputs.iter())
            {
                if !input_map.contains_key(id) {
                    return Err(IvmError::MissingJoinInput(*id));
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
