use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::common::DFSchema;
use datafusion::logical_expr::logical_plan::{Join, Projection};
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::{ROW_ID_COLUMN, RowIdDomain, ivm_sha1_expr, single_row_id};

use super::{OjvInputId, OjvJoinNode, OjvJoinTree, OjvMultiJoin, input_row_id_name};

#[derive(Debug, Clone, Copy, Default)]
pub struct PrimaryDeltaPlanner;

impl PrimaryDeltaPlanner {
    pub fn plan(
        &self,
        multi_join: &OjvMultiJoin,
        changed_input: OjvInputId,
        rewritten_inputs: &BTreeMap<OjvInputId, LogicalPlan>,
    ) -> Result<LogicalPlan, IvmError> {
        if !multi_join.inputs.contains_key(&changed_input) {
            return Err(IvmError::MissingChangedInput(changed_input));
        }
        for input in multi_join.inputs.keys() {
            if !rewritten_inputs.contains_key(input) {
                return Err(IvmError::MissingRewriteInput(*input));
            }
        }

        let (plan, contains_changed) =
            rewrite_tree(&multi_join.root, changed_input, rewritten_inputs)?;
        if !contains_changed {
            return Err(IvmError::MissingChangedInput(changed_input));
        }

        let mut exprs = plan
            .schema()
            .columns()
            .into_iter()
            .filter(|column| column.name != ROW_ID_COLUMN)
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let mut input_row_ids = Vec::with_capacity(multi_join.inputs.len());
        for input_id in multi_join.inputs.keys() {
            let input = rewritten_inputs
                .get(input_id)
                .ok_or(IvmError::MissingRewriteInput(*input_id))?;
            let row_id = single_row_id(input)?;
            exprs.push(Expr::Column(row_id.clone()).alias(input_row_id_name(*input_id)));
            input_row_ids.push(Expr::Column(row_id));
        }
        exprs.push(ivm_sha1_expr(RowIdDomain::Join, input_row_ids).alias(ROW_ID_COLUMN));
        let fields = exprs
            .iter()
            .map(|expr| expr.to_field(plan.schema()))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            plan.schema().metadata().clone(),
        )?);
        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            exprs,
            Arc::new(plan),
            schema,
        )?))
    }
}

fn rewrite_tree(
    tree: &OjvJoinTree,
    changed_input: OjvInputId,
    rewritten_inputs: &BTreeMap<OjvInputId, LogicalPlan>,
) -> Result<(LogicalPlan, bool), IvmError> {
    match tree {
        OjvJoinTree::Input(input_id) => {
            let plan = rewritten_inputs
                .get(input_id)
                .ok_or(IvmError::MissingRewriteInput(*input_id))?
                .clone();
            Ok((plan, *input_id == changed_input))
        }
        OjvJoinTree::Join(join) => {
            let (left, left_changed) = rewrite_tree(&join.left, changed_input, rewritten_inputs)?;
            let (right, right_changed) =
                rewrite_tree(&join.right, changed_input, rewritten_inputs)?;
            if left_changed && right_changed {
                return Err(IvmError::MultipleChangedInputs);
            }
            if left_changed {
                Ok((
                    rebuild_join(join, simplify(join.join_type), left, right, join.on.clone())?,
                    true,
                ))
            } else if right_changed {
                let on = join
                    .on
                    .iter()
                    .map(|(left, right)| (right.clone(), left.clone()))
                    .collect();
                Ok((
                    rebuild_join(join, simplify(commute(join.join_type)), right, left, on)?,
                    true,
                ))
            } else {
                Ok((
                    rebuild_join(join, join.join_type, left, right, join.on.clone())?,
                    false,
                ))
            }
        }
    }
}

fn rebuild_join(
    source: &OjvJoinNode,
    join_type: JoinType,
    left: LogicalPlan,
    right: LogicalPlan,
    on: Vec<(Expr, Expr)>,
) -> Result<LogicalPlan, IvmError> {
    Ok(LogicalPlan::Join(Join::try_new(
        Arc::new(left),
        Arc::new(right),
        on,
        source.filter.clone(),
        join_type,
        source.join_constraint,
        source.null_equality,
        source.null_aware,
    )?))
}

fn commute(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Inner => JoinType::Inner,
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        other => other,
    }
}

fn simplify(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Right => JoinType::Inner,
        JoinType::Full => JoinType::Left,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;

    use crate::ivm::normalize::IvmNormalizer;

    use super::*;
    use crate::ivm::ojv::OjvAnalyzer;

    #[tokio::test]
    async fn changed_right_input_commutes_left_join_to_inner_primary_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        for (name, key) in [("a", "a_id"), ("b", "b_id")] {
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
            .create_logical_plan("SELECT a_id, b_id FROM a LEFT JOIN b ON a_id = b_id")
            .await?;
        let normalized = IvmNormalizer::default()
            .normalize(plan)
            .expect("normalize left join");
        let analysis = OjvAnalyzer
            .analyze(&normalized.plan)
            .expect("analyze left join");
        let multi_join = &analysis.multi_joins[0];
        let rewritten = multi_join
            .inputs
            .iter()
            .map(|(id, input)| (*id, input.plan.clone()))
            .collect::<BTreeMap<_, _>>();
        let primary = PrimaryDeltaPlanner
            .plan(multi_join, OjvInputId::new(1), &rewritten)
            .expect("plan primary delta");

        let display = primary.display_indent().to_string();
        assert!(display.contains("Inner Join"));
        assert!(display.contains("_ivm_sha1"));
        Ok(())
    }
}
