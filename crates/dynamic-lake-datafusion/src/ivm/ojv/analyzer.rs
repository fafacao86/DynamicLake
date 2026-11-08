use std::collections::{BTreeMap, BTreeSet};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};

use crate::ivm::error::IvmError;

use super::{
    OjvAnalysis, OjvInputId, OjvJoinInput, OjvJoinKeyBinding, OjvJoinNode, OjvJoinTree,
    OjvMultiJoin,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct OjvAnalyzer;

impl OjvAnalyzer {
    pub fn analyze(&self, plan: &LogicalPlan) -> Result<OjvAnalysis, IvmError> {
        let mut state = AnalyzerState::default();
        let multi_joins = state.collect_multi_joins(plan, false)?;
        OjvAnalysis::try_new(multi_joins)
    }
}

#[derive(Default)]
struct AnalyzerState {
    next_input_id: usize,
}

impl AnalyzerState {
    fn collect_multi_joins(
        &mut self,
        plan: &LogicalPlan,
        inside_join: bool,
    ) -> Result<Vec<OjvMultiJoin>, IvmError> {
        let mut multi_joins = Vec::new();
        if matches!(plan, LogicalPlan::Join(_)) && !inside_join {
            let mut join_keys = Vec::new();
            let mut inputs = BTreeMap::new();
            let (root, _) = self.collect_join(plan, &mut join_keys, &mut inputs)?;
            multi_joins.push(OjvMultiJoin::try_new(
                plan.clone(),
                root,
                inputs.values().cloned(),
                join_keys,
            )?);
        }

        for input in plan.inputs() {
            multi_joins
                .extend(self.collect_multi_joins(input, matches!(plan, LogicalPlan::Join(_)))?);
        }
        Ok(multi_joins)
    }

    fn collect_join(
        &mut self,
        plan: &LogicalPlan,
        join_keys: &mut Vec<OjvJoinKeyBinding>,
        inputs: &mut BTreeMap<OjvInputId, OjvJoinInput>,
    ) -> Result<(OjvJoinTree, Vec<OjvInputId>), IvmError> {
        let LogicalPlan::Join(join) = plan else {
            let id = OjvInputId::new(self.next_input_id);
            self.next_input_id += 1;
            inputs.insert(
                id,
                OjvJoinInput {
                    id,
                    plan: plan.clone(),
                },
            );
            return Ok((OjvJoinTree::Input(id), vec![id]));
        };

        if join.null_aware {
            return Err(IvmError::UnsupportedOperator(
                "null-aware OJV join".to_string(),
            ));
        }

        let (left, left_sources) = self.collect_join(&join.left, join_keys, inputs)?;
        let (right, right_sources) = self.collect_join(&join.right, join_keys, inputs)?;
        for (left_expr, right_expr) in &join.on {
            let left_bound = self.sources_of(left_expr, &left_sources, inputs)?;
            let right_bound = self.sources_of(right_expr, &right_sources, inputs)?;
            join_keys.extend(OjvJoinKeyBinding::for_join(
                left_bound,
                right_bound,
                join.join_type,
            )?);
        }
        if let Some(filter) = &join.filter {
            self.collect_filter_bindings(
                filter,
                &left_sources,
                &right_sources,
                join.join_type,
                join_keys,
                inputs,
            )?;
        }

        let mut all_sources = left_sources;
        all_sources.extend(right_sources);
        let all_sources = all_sources
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Ok((
            OjvJoinTree::Join(OjvJoinNode {
                join_type: join.join_type,
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_constraint: join.join_constraint,
                null_equality: join.null_equality,
                null_aware: join.null_aware,
                left: Box::new(left),
                right: Box::new(right),
            }),
            all_sources,
        ))
    }

    fn sources_of(
        &self,
        expr: &Expr,
        candidates: &[OjvInputId],
        inputs: &BTreeMap<OjvInputId, OjvJoinInput>,
    ) -> Result<Vec<OjvInputId>, IvmError> {
        let mut columns = Vec::new();
        expr.apply(&mut |expr: &Expr| {
            if let Expr::Column(column) = expr {
                columns.push(column.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let columns = columns;
        let sources = candidates
            .iter()
            .copied()
            .filter(|id| {
                let input = inputs.get(id).expect("candidate input must be collected");
                columns
                    .iter()
                    .any(|column| input.plan.schema().has_column(column))
            })
            .collect::<Vec<_>>();
        if sources.is_empty() {
            return Err(IvmError::UnsupportedOperator(format!(
                "join key does not reference a collected input: {expr}"
            )));
        }
        Ok(sources)
    }

    fn collect_filter_bindings(
        &self,
        filter: &Expr,
        left_sources: &[OjvInputId],
        right_sources: &[OjvInputId],
        join_type: datafusion::logical_expr::JoinType,
        join_keys: &mut Vec<OjvJoinKeyBinding>,
        inputs: &BTreeMap<OjvInputId, OjvJoinInput>,
    ) -> Result<(), IvmError> {
        match filter {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                self.collect_filter_bindings(
                    &binary.left,
                    left_sources,
                    right_sources,
                    join_type,
                    join_keys,
                    inputs,
                )?;
                self.collect_filter_bindings(
                    &binary.right,
                    left_sources,
                    right_sources,
                    join_type,
                    join_keys,
                    inputs,
                )
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                let left_bound = self.sources_of(&binary.left, left_sources, inputs)?;
                let right_bound = self.sources_of(&binary.right, right_sources, inputs)?;
                join_keys.extend(OjvJoinKeyBinding::for_join(
                    left_bound,
                    right_bound,
                    join_type,
                )?);
                Ok(())
            }
            other => Err(IvmError::UnsupportedOperator(format!(
                "join-attached filter is not an equality conjunction: {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;

    use crate::ivm::{ROW_ID_COLUMN, normalize::IvmNormalizer};

    use super::*;

    #[tokio::test]
    async fn analyzes_nested_left_join_as_one_multi_join() -> Result<(), Box<dyn std::error::Error>>
    {
        let session = SessionContext::new();
        for (name, key) in [("a", "a_id"), ("b", "b_id"), ("c", "c_id")] {
            session.register_table(
                name,
                Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                    Field::new(ROW_ID_COLUMN, DataType::Int64, false),
                    Field::new(key, DataType::Int64, false),
                ])))),
            )?;
        }
        let plan = session
            .state()
            .create_logical_plan(
                "SELECT a_id, b_id, c_id
                 FROM a
                 LEFT JOIN b ON a_id = b_id
                 LEFT JOIN c ON b_id = c_id",
            )
            .await?;
        let normalized = IvmNormalizer::default().normalize(plan)?;
        let analysis = OjvAnalyzer.analyze(&normalized.plan)?;
        assert_eq!(analysis.multi_joins.len(), 1);
        let multi_join = &analysis.multi_joins[0];
        assert_eq!(
            multi_join.input_ids(),
            BTreeSet::from([OjvInputId::new(0), OjvInputId::new(1), OjvInputId::new(2)])
        );
        assert_eq!(multi_join.join_keys.len(), 2);
        assert_eq!(
            analysis
                .forward_join_key_graph
                .reachable_from(OjvInputId::new(0))?,
            BTreeSet::from([OjvInputId::new(0), OjvInputId::new(1), OjvInputId::new(2)])
        );
        assert_eq!(
            analysis
                .forward_join_key_graph
                .reachable_from(OjvInputId::new(1))?,
            BTreeSet::from([OjvInputId::new(1), OjvInputId::new(2)])
        );
        Ok(())
    }
}
