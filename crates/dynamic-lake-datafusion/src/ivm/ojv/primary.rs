use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::logical_plan::{Aggregate, Join};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, when};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::{
    ROW_ID_COLUMN, RowIdDomain, ivm_sha1_expr, row_id_columns, single_row_id,
};
use crate::ivm::plan_utils::project_plan;

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
        let plan = left_deepen_main_path(plan)?;

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
        Ok(project_plan(plan, exprs)?)
    }
}

/// Move the changed input to the left of every ancestor join, creating the
/// main path for the later rotations.
///
/// If the right child contains the delta, swap the children and ON keys,
/// commute LEFT/RIGHT, then simplify the join type. If the left child contains
/// the delta, only the simplification is needed.
///
/// ```text
///     Before:             J(p)             After:       simplify(commute(J))(p)
///                        /    \                         /                 \
///                       L    Delta                    Delta                L
///
///     ON:             (L.k, Delta.k)                 (Delta.k, L.k)
/// ```
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
                    rebuild_join(
                        join,
                        simplify(join.join_type)?,
                        left,
                        right,
                        join.on.clone(),
                    )?,
                    true,
                ))
            } else if right_changed {
                let on = join
                    .on
                    .iter()
                    .map(|(left, right)| (right.clone(), left.clone()))
                    .collect();
                Ok((
                    rebuild_join(join, simplify(commute(join.join_type)?)?, right, left, on)?,
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

fn commute(join_type: JoinType) -> Result<JoinType, IvmError> {
    match join_type {
        JoinType::Inner => Ok(JoinType::Inner),
        JoinType::Left => Ok(JoinType::Right),
        JoinType::Right => Ok(JoinType::Left),
        JoinType::Full => Ok(JoinType::Full),
        other => Err(IvmError::UnsupportedJoinType(other)),
    }
}

/// The main path keeps only rows derived from the delta. Remove join branches
/// that preserve old rows from the right side alone.
///
/// ```text
///     Delta RIGHT old  -> Delta INNER old
///     Delta FULL  old  -> Delta LEFT  old
///     Delta LEFT  old  -> Delta LEFT  old
///     Delta INNER old  -> Delta INNER old
/// ```
fn simplify(join_type: JoinType) -> Result<JoinType, IvmError> {
    match join_type {
        JoinType::Inner => Ok(JoinType::Inner),
        JoinType::Left => Ok(JoinType::Left),
        JoinType::Right => Ok(JoinType::Inner),
        JoinType::Full => Ok(JoinType::Left),
        other => Err(IvmError::UnsupportedJoinType(other)),
    }
}

/// Walk up the main path from the delta, flattening each bushy right subtree.
///
/// ```text
///     Before:         J3                 After:           J4
///                    /  \                                /  \
///                   J2   J4                             J3   R4
///                  / \  / \                            /  \
///             Delta R2 R3 R4                          J2   R3
///                                                    /  \
///                                                 Delta R2
/// ```
///
/// Process the left path through J2 first, then rotate the right-side J4
/// above J3.
fn left_deepen_main_path(plan: LogicalPlan) -> Result<LogicalPlan, IvmError> {
    match plan {
        LogicalPlan::Join(join) => {
            let left = left_deepen_main_path((*join.left).clone())?;
            let current = rebuild_plan_join(
                &join,
                join.join_type,
                left,
                (*join.right).clone(),
                join.on.clone(),
            )?;
            pull_up_right_bushes(current)
        }
        other => Ok(other),
    }
}

/// Pull one level of a bushy right subtree onto the main path, following
/// associativity Rules 2–5 from the paper.
///
/// The parent predicate p12 must reference exactly one branch of its right
/// subtree. Call that branch near and the other branch far. If p12 references
/// the original right branch, swap the bushy join's children and ON keys and
/// commute LEFT/RIGHT before performing the rotation below.
///
/// ```text
///     Before:          main(p12)            After:         lifted(p23)
///                     /         \                         /          \
///                main_path     bushy(p23)             main(p12)       far
///                             /        \              /       \
///                           near       far        main_path   near
/// ```
///
/// p12 joins main_path with near; p23 joins near with far.
///
/// When main is LEFT, the resulting join types and compensation are:
///
/// ```text
///     bushy FULL/LEFT  ->  (main_path LEFT near) LEFT far
///     bushy RIGHT/INNER -> DISTINCT(NULL_IF((main_path LEFT near) LEFT far))
/// ```
///
/// When main is INNER, the lifted join uses simplify(bushy_type).
/// Near may itself be a join. Recursively process the rotated result until
/// each join on the main path has a non-join right input. Apply NULL_IF and
/// DISTINCT for Rules 4/5 after that recursion.
fn pull_up_right_bushes(plan: LogicalPlan) -> Result<LogicalPlan, IvmError> {
    let LogicalPlan::Join(main) = plan else {
        return Ok(plan);
    };
    let LogicalPlan::Join(bushy) = main.right.as_ref() else {
        return Ok(LogicalPlan::Join(main));
    };
    let left_keys = join_right_keys_belong_to(&main, bushy.left.schema());
    let right_keys = join_right_keys_belong_to(&main, bushy.right.schema());
    let (near, far, bushy_on, bushy_type) = match (left_keys, right_keys) {
        (true, false) => (
            (*bushy.left).clone(),
            (*bushy.right).clone(),
            bushy.on.clone(),
            bushy.join_type,
        ),
        (false, true) => (
            (*bushy.right).clone(),
            (*bushy.left).clone(),
            bushy
                .on
                .iter()
                .map(|(left, right)| (right.clone(), left.clone()))
                .collect(),
            commute(bushy.join_type)?,
        ),
        _ => {
            return Err(IvmError::UnsupportedOperator(
                "left-deep reorder needs the parent join key to reference exactly one right branch"
                    .into(),
            ));
        }
    };
    let (lifted_type, repair_nulls) = match (main.join_type, bushy_type) {
        (JoinType::Inner, kind) => (simplify(kind)?, false),
        (JoinType::Left, JoinType::Left | JoinType::Full) => (JoinType::Left, false),
        (JoinType::Left, JoinType::Inner | JoinType::Right) => (JoinType::Left, true),
        (main_type, bushy_type) => {
            return Err(IvmError::UnsupportedOperator(format!(
                "unsupported left-deep associativity: main {main_type}, right {bushy_type}"
            )));
        }
    };

    let lower = rebuild_plan_join(
        &main,
        main.join_type,
        (*main.left).clone(),
        near,
        main.on.clone(),
    )?;
    let far_row_ids = row_id_columns(far.schema());
    let lifted = rebuild_plan_join(bushy, lifted_type, lower, far, bushy_on)?;
    let left_deep = left_deepen_main_path(lifted)?;
    if !repair_nulls {
        return Ok(left_deep);
    }

    let missing_far = far_row_ids
        .into_iter()
        .map(|column| Expr::Column(column).is_null())
        .reduce(Expr::and)
        .ok_or(IvmError::UnexpectedRowIdCount(0))?;
    null_if_and_distinct(left_deep, missing_far, &bushy.schema)
}

fn join_right_keys_belong_to(join: &Join, schema: &datafusion::common::DFSchema) -> bool {
    let mut columns = join
        .on
        .iter()
        .flat_map(|(_, right)| right.column_refs())
        .collect::<Vec<_>>();
    if let Some(filter) = &join.filter {
        columns.extend(
            filter
                .column_refs()
                .into_iter()
                .filter(|column| !join.left.schema().has_column(column)),
        );
    }
    !columns.is_empty() && columns.into_iter().all(|column| schema.has_column(column))
}

fn rebuild_plan_join(
    template: &Join,
    join_type: JoinType,
    left: LogicalPlan,
    right: LogicalPlan,
    on: Vec<(Expr, Expr)>,
) -> Result<LogicalPlan, IvmError> {
    Ok(LogicalPlan::Join(Join::try_new(
        Arc::new(left),
        Arc::new(right),
        on,
        template.filter.clone(),
        join_type,
        template.join_constraint,
        template.null_equality,
        template.null_aware,
    )?))
}

/// Compensation for Rules 4/5: when far has no matching row, set the columns
/// of the original bushy subtree to NULL, then deduplicate by all output
/// columns to collapse duplicate orphan rows introduced by the rotation.
///
/// ```text
///     rotated rows
///         |
///         v
///     NULL_IF(all far row IDs are NULL, bushy columns)
///         |
///         v
///     DISTINCT(all output columns)
/// ```
fn null_if_and_distinct(
    plan: LogicalPlan,
    missing_far: Expr,
    bushy_schema: &datafusion::common::DFSchema,
) -> Result<LogicalPlan, IvmError> {
    let exprs = plan
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            let column = Column::new(qualifier.cloned(), field.name());
            let value = Expr::Column(column.clone());
            if bushy_schema.has_column(&column) {
                Ok(when(
                    missing_far.clone(),
                    Expr::Literal(ScalarValue::try_from(field.data_type())?, None),
                )
                .otherwise(value)?
                .alias_qualified(qualifier.cloned(), field.name()))
            } else {
                Ok(value)
            }
        })
        .collect::<Result<Vec<_>, IvmError>>()?;
    let nulled = project_plan(plan, exprs)?;
    let keys = nulled
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    Ok(LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(nulled),
        keys,
        Vec::new(),
    )?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;

    use crate::ivm::normalize::{IvmNormalizer, register_ivm_functions};

    use super::*;
    use crate::ivm::ojv::OjvAnalyzer;

    #[test]
    fn simplifies_main_path_outer_joins() -> Result<(), IvmError> {
        assert_eq!(simplify(JoinType::Right)?, JoinType::Inner);
        assert_eq!(simplify(JoinType::Full)?, JoinType::Left);
        assert_eq!(simplify(commute(JoinType::Left)?)?, JoinType::Inner);
        assert_eq!(simplify(commute(JoinType::Full)?)?, JoinType::Left);
        Ok(())
    }

    #[tokio::test]
    async fn pulls_left_right_subtree_into_left_deep_path() -> Result<(), Box<dyn std::error::Error>>
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
                "SELECT a_id, b_id, c_id FROM a \
                 LEFT JOIN (b LEFT JOIN c ON b_id = c_id) ON a_id = b_id",
            )
            .await?;
        let normalized = IvmNormalizer::default().normalize(plan)?;
        let analysis = OjvAnalyzer.analyze(&normalized.plan)?;
        let multi_join = &analysis.multi_joins[0];
        assert_eq!(multi_join.inputs.len(), 3);
        let rewritten = multi_join
            .inputs
            .iter()
            .map(|(id, input)| (*id, input.plan.clone()))
            .collect();
        let primary = PrimaryDeltaPlanner.plan(multi_join, OjvInputId::new(0), &rewritten)?;
        let LogicalPlan::Projection(project) = primary else {
            panic!("primary needs row-id projection")
        };
        let LogicalPlan::Join(root) = project.input.as_ref() else {
            panic!("LEFT bushy subtree should rotate to a join")
        };
        assert!(!matches!(root.right.as_ref(), LogicalPlan::Join(_)));
        let LogicalPlan::Join(lower) = root.left.as_ref() else {
            panic!("pulled subtree needs a lower join")
        };
        assert!(!matches!(lower.right.as_ref(), LogicalPlan::Join(_)));
        Ok(())
    }

    #[tokio::test]
    async fn orients_left_subtree_by_parent_key() -> Result<(), Box<dyn std::error::Error>> {
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
                "SELECT a_id, b_id, c_id FROM a \
                 LEFT JOIN (b LEFT JOIN c ON b_id = c_id) ON a_id = c_id",
            )
            .await?;
        let normalized = IvmNormalizer::default().normalize(plan)?;
        let analysis = OjvAnalyzer.analyze(&normalized.plan)?;
        let multi_join = &analysis.multi_joins[0];
        assert_eq!(multi_join.inputs.len(), 3);
        let rewritten = multi_join
            .inputs
            .iter()
            .map(|(id, input)| (*id, input.plan.clone()))
            .collect();
        let primary = PrimaryDeltaPlanner.plan(multi_join, OjvInputId::new(0), &rewritten)?;
        let LogicalPlan::Projection(project) = primary else {
            panic!("primary needs row-id projection")
        };
        let LogicalPlan::Aggregate(repair) = project.input.as_ref() else {
            panic!("swapped LEFT subtree needs null-if and distinct")
        };
        let LogicalPlan::Projection(null_if) = repair.input.as_ref() else {
            panic!("repair needs null-if projection")
        };
        let LogicalPlan::Join(root) = null_if.input.as_ref() else {
            panic!("repaired path needs a join")
        };
        assert_eq!(root.join_type, JoinType::Left);
        assert!(!matches!(root.right.as_ref(), LogicalPlan::Join(_)));
        let LogicalPlan::Join(lower) = root.left.as_ref() else {
            panic!("rotated join needs a lower join")
        };
        assert!(!matches!(lower.right.as_ref(), LogicalPlan::Join(_)));
        Ok(())
    }

    #[tokio::test]
    async fn left_bushy_join_matches_expected_rows() -> Result<(), Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        register_ivm_functions(&session);
        for (name, key, ids) in [
            ("a", "a_id", vec![1_i64, 2_i64]),
            ("b", "b_id", vec![1_i64, 2_i64]),
            ("c", "c_id", vec![1_i64]),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new(ROW_ID_COLUMN, DataType::Int64, false),
                Field::new(key, DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Int64Array::from(ids)),
                ],
            )?;
            session.register_table(
                name,
                Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
            )?;
        }
        let plan = session
            .state()
            .create_logical_plan(
                "SELECT a_id, b_id, c_id FROM a \
                 LEFT JOIN (b LEFT JOIN c ON b_id = c_id) ON a_id = b_id",
            )
            .await?;
        let normalized = IvmNormalizer::default().normalize(plan)?;
        let analysis = OjvAnalyzer.analyze(&normalized.plan)?;
        let multi_join = &analysis.multi_joins[0];
        let rewritten = multi_join
            .inputs
            .iter()
            .map(|(id, input)| (*id, input.plan.clone()))
            .collect();
        let primary = PrimaryDeltaPlanner.plan(multi_join, OjvInputId::new(0), &rewritten)?;
        let batches = session
            .execute_logical_plan(primary)
            .await?
            .collect()
            .await?;
        let mut rows = Vec::new();
        for batch in batches {
            let a = batch
                .column(batch.schema().index_of("a_id")?)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let b = batch
                .column(batch.schema().index_of("b_id")?)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let c = batch
                .column(batch.schema().index_of("c_id")?)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    a.value(row),
                    (!b.is_null(row)).then(|| b.value(row)),
                    (!c.is_null(row)).then(|| c.value(row)),
                ));
            }
        }
        rows.sort();
        assert_eq!(rows, vec![(1, Some(1), Some(1)), (2, Some(2), None)]);
        Ok(())
    }

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
