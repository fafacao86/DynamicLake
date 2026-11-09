use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::{Column, DFSchema, NullEquality, ScalarValue};
use datafusion::logical_expr::logical_plan::{Aggregate, Filter, Join, JoinConstraint, Union};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator, lit};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::{
    DeltaRowKind, ROW_ID_COLUMN, ROW_KIND_COLUMN, RowIdDomain, ivm_sha1_expr, single_row_id,
};
use crate::ivm::plan_utils::{project_plan, resolve_column};

use super::{OjvAnalysis, OjvInputId, OjvMultiJoin, input_row_id_name};

const STABLE_KEY_COLUMN: &str = "__ivm_secondary_stable_key";

pub struct OjvSecondaryDeltaPlanner<'a> {
    multi_join: &'a OjvMultiJoin,
    changed_input: OjvInputId,
    affected_inputs: BTreeSet<OjvInputId>,
}

impl<'a> OjvSecondaryDeltaPlanner<'a> {
    pub fn new(
        analysis: &'a OjvAnalysis,
        multi_join: &'a OjvMultiJoin,
        changed_input: OjvInputId,
    ) -> Result<Self, IvmError> {
        let affected_inputs = analysis
            .forward_join_key_graph
            .reachable_from(changed_input)?;
        Ok(Self {
            multi_join,
            changed_input,
            affected_inputs,
        })
    }

    pub fn changed_input(&self) -> OjvInputId {
        self.changed_input
    }

    pub fn affected_inputs(&self) -> &BTreeSet<OjvInputId> {
        &self.affected_inputs
    }

    /// If forward join key graph indicates that all inputs tables are covered by reachability closure
    /// then secondary delta is not needed, else needed
    pub fn needs_secondary(&self) -> bool {
        self.multi_join
            .inputs
            .keys()
            .any(|input| !self.affected_inputs.contains(input))
    }

    /// Apply the primary delta to the old view to obtain V1, the intermediate
    /// view used to compute the secondary delta.
    ///
    /// ```text
    ///     V0 -------------------- LEFT ANTI D- ---+
    ///                                            UNION ALL ---> V1
    ///     D+ ------------------------------------+
    /// ```
    ///
    /// D- removes old primary rows; D+ adds new primary rows. Both
    /// ceased_orphans and new_orphans use V1 to identify orphan changes.
    pub fn apply_primary(
        &self,
        before_view: &LogicalPlan,
        negative_primary: &LogicalPlan,
        positive_primary: &LogicalPlan,
    ) -> Result<LogicalPlan, IvmError> {
        let before_rows = self.view_state(before_view)?;
        let row_id = Column::from_name(ROW_ID_COLUMN);
        let retained = LogicalPlan::Join(Join::try_new(
            Arc::new(before_rows.clone()),
            Arc::new(negative_primary.clone()),
            vec![(Expr::Column(row_id.clone()), Expr::Column(row_id))],
            None,
            JoinType::LeftAnti,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?);

        let mut exprs = Vec::new();
        let stable_parts = self
            .multi_join
            .inputs
            .keys()
            .filter(|input| !self.affected_inputs.contains(input))
            .map(|input| Expr::Column(Column::from_name(input_row_id_name(*input))))
            .collect::<Vec<_>>();
        for column in before_rows.schema().columns() {
            let expr = if column.name == STABLE_KEY_COLUMN {
                ivm_sha1_expr(RowIdDomain::Join, stable_parts.clone())
            } else {
                Expr::Column(
                    resolve_column(positive_primary, &column)
                        .ok_or_else(|| IvmError::MissingHiddenColumn(column.name.clone()))?,
                )
            };
            exprs.push(expr.alias_qualified(column.relation.clone(), &column.name));
        }
        let inserted = project_plan(positive_primary.clone(), exprs)?;
        Ok(LogicalPlan::Union(Union::try_new(vec![
            Arc::new(retained),
            Arc::new(inserted),
        ])?))
    }

    /// Positive primary rows make existing orphan rows cease to be orphans.
    pub fn ceased_orphans(
        &self,
        positive_primary: &LogicalPlan,
        applied_view: &LogicalPlan,
    ) -> Result<LogicalPlan, IvmError> {
        let candidate = self.candidate(positive_primary)?;
        let old_orphans = filter_affected_all_null(applied_view, &self.affected_inputs)?;
        let stable_key = Column::from_name(STABLE_KEY_COLUMN);
        let semi = Join::try_new(
            Arc::new(old_orphans),
            Arc::new(candidate),
            vec![(Expr::Column(stable_key.clone()), Expr::Column(stable_key))],
            None,
            JoinType::LeftSemi,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        with_row_kind(LogicalPlan::Join(semi), DeltaRowKind::Negative)
    }

    /// Negative primary rows may create new orphan rows.
    pub fn new_orphans(
        &self,
        negative_primary: &LogicalPlan,
        applied_view: &LogicalPlan,
    ) -> Result<LogicalPlan, IvmError> {
        let candidate = self.candidate(negative_primary)?;
        let covering = filter_any_affected_not_null(applied_view, &self.affected_inputs)?;
        let stable_key = Column::from_name(STABLE_KEY_COLUMN);
        let anti = Join::try_new(
            Arc::new(candidate),
            Arc::new(covering),
            vec![(Expr::Column(stable_key.clone()), Expr::Column(stable_key))],
            None,
            JoinType::LeftAnti,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        with_row_kind(LogicalPlan::Join(anti), DeltaRowKind::Positive)
    }

    fn candidate(&self, primary: &LogicalPlan) -> Result<LogicalPlan, IvmError> {
        let mut exprs = Vec::new();
        let mut final_parts = Vec::new();
        let mut stable_parts = Vec::new();
        for input_id in self.multi_join.inputs.keys() {
            let row_id = Column::from_name(input_row_id_name(*input_id));
            if self.affected_inputs.contains(input_id) {
                exprs.push(
                    Expr::Literal(null_value(primary.schema(), &row_id)?, None)
                        .alias(input_row_id_name(*input_id)),
                );
                final_parts.push(Expr::Literal(null_value(primary.schema(), &row_id)?, None));
            } else {
                exprs.push(Expr::Column(row_id.clone()).alias(input_row_id_name(*input_id)));
                final_parts.push(Expr::Column(row_id.clone()));
                stable_parts.push(Expr::Column(row_id));
            }
        }
        for column in primary.schema().columns() {
            if column.name == ROW_ID_COLUMN
                || column.name == ROW_KIND_COLUMN
                || is_input_row_id(&column.name)
                || column.name == STABLE_KEY_COLUMN
            {
                continue;
            }
            let expr = if self.column_is_affected(&column) {
                Expr::Literal(null_value(primary.schema(), &column)?, None)
            } else {
                Expr::Column(column.clone())
            };
            exprs.push(expr.alias_qualified(column.relation.clone(), &column.name));
        }
        exprs.push(ivm_sha1_expr(RowIdDomain::Join, final_parts).alias(ROW_ID_COLUMN));
        exprs.push(ivm_sha1_expr(RowIdDomain::Join, stable_parts).alias(STABLE_KEY_COLUMN));
        distinct(project_plan(primary.clone(), exprs)?)
    }

    fn view_state(&self, before_view: &LogicalPlan) -> Result<LogicalPlan, IvmError> {
        let mut exprs = Vec::new();
        let mut final_parts = Vec::new();
        let mut stable_parts = Vec::new();
        for input_id in self.multi_join.inputs.keys() {
            let input = self
                .multi_join
                .inputs
                .get(input_id)
                .ok_or(IvmError::MissingRewriteInput(*input_id))?;
            let source_row_id = single_row_id(&input.plan)?;
            let resolved = resolve_column(before_view, &source_row_id)
                .ok_or_else(|| IvmError::MissingHiddenColumn(source_row_id.name.clone()))?;
            exprs.push(Expr::Column(resolved.clone()).alias(input_row_id_name(*input_id)));
            final_parts.push(Expr::Column(resolved.clone()));
            if !self.affected_inputs.contains(input_id) {
                stable_parts.push(Expr::Column(resolved));
            }
        }
        for column in before_view.schema().columns() {
            if column.name == ROW_ID_COLUMN
                || column.name == ROW_KIND_COLUMN
                || is_input_row_id(&column.name)
                || column.name == STABLE_KEY_COLUMN
            {
                continue;
            }
            let resolved = resolve_column(before_view, &column)
                .ok_or_else(|| IvmError::MissingHiddenColumn(column.name.clone()))?;
            exprs.push(
                Expr::Column(resolved).alias_qualified(column.relation.clone(), &column.name),
            );
        }
        exprs.push(ivm_sha1_expr(RowIdDomain::Join, final_parts).alias(ROW_ID_COLUMN));
        exprs.push(ivm_sha1_expr(RowIdDomain::Join, stable_parts).alias(STABLE_KEY_COLUMN));
        Ok(project_plan(before_view.clone(), exprs)?)
    }

    fn column_is_affected(&self, column: &Column) -> bool {
        self.affected_inputs.iter().any(|input_id| {
            self.multi_join
                .inputs
                .get(input_id)
                .is_some_and(|input| input.plan.schema().has_column(column))
        })
    }
}

fn filter_affected_all_null(
    view: &LogicalPlan,
    affected: &BTreeSet<OjvInputId>,
) -> Result<LogicalPlan, IvmError> {
    let predicate = affected.iter().fold(None, |predicate, input_id| {
        let expr = Expr::Column(Column::from_name(input_row_id_name(*input_id))).is_null();
        Some(match predicate {
            Some(predicate) => Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                Box::new(predicate),
                Operator::And,
                Box::new(expr),
            )),
            None => expr,
        })
    });
    Ok(LogicalPlan::Filter(Filter::try_new(
        predicate.unwrap_or_else(|| lit(false)),
        Arc::new(view.clone()),
    )?))
}

fn filter_any_affected_not_null(
    view: &LogicalPlan,
    affected: &BTreeSet<OjvInputId>,
) -> Result<LogicalPlan, IvmError> {
    let predicate = affected.iter().fold(None, |predicate, input_id| {
        let expr = Expr::Column(Column::from_name(input_row_id_name(*input_id))).is_not_null();
        Some(match predicate {
            Some(predicate) => Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                Box::new(predicate),
                Operator::Or,
                Box::new(expr),
            )),
            None => expr,
        })
    });
    Ok(LogicalPlan::Filter(Filter::try_new(
        predicate.unwrap_or_else(|| lit(false)),
        Arc::new(view.clone()),
    )?))
}

fn with_row_kind(plan: LogicalPlan, row_kind: DeltaRowKind) -> Result<LogicalPlan, IvmError> {
    let mut exprs = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    exprs.push(lit(row_kind.code()).alias(ROW_KIND_COLUMN));
    Ok(project_plan(plan, exprs)?)
}

fn distinct(plan: LogicalPlan) -> Result<LogicalPlan, IvmError> {
    let group_expr = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    Ok(LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(plan),
        group_expr,
        Vec::new(),
    )?))
}

fn null_value(schema: &DFSchema, column: &Column) -> Result<ScalarValue, IvmError> {
    let index = schema.index_of_column(column)?;
    Ok(ScalarValue::try_from(schema.field(index).data_type())?)
}

fn is_input_row_id(name: &str) -> bool {
    name.starts_with("__ivm_input_") && name.ends_with("_row_id")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::logical_plan::TableScan;
    use datafusion::prelude::SessionContext;

    use crate::ivm::normalize::{IvmNormalizer, register_ivm_functions};
    use crate::ivm::ojv::{OjvAnalyzer, PrimaryDeltaPlanner};

    use super::*;

    #[tokio::test]
    async fn inserted_match_deletes_old_orphan_from_applied_primary_view()
    -> Result<(), Box<dyn std::error::Error>> {
        check_secondary(vec![], false).await
    }

    #[tokio::test]
    async fn deleted_match_inserts_new_orphan_from_applied_primary_view()
    -> Result<(), Box<dyn std::error::Error>> {
        check_secondary(vec![(200, 1)], true).await
    }

    async fn check_secondary(
        old_b_rows: Vec<(i64, i64)>,
        deleting: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        register_ivm_functions(&session);
        for (name, key, rows) in [
            ("a", "a_id", vec![(100_i64, 1_i64)]),
            ("b", "b_id", old_b_rows),
            ("b_delta", "b_id", vec![(200_i64, 1_i64)]),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new(ROW_ID_COLUMN, DataType::Int64, false),
                Field::new(key, DataType::Int64, false),
            ]));
            let batches = if rows.is_empty() {
                vec![]
            } else {
                vec![RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(
                            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            rows.iter().map(|(_, key)| *key).collect::<Vec<_>>(),
                        )),
                    ],
                )?]
            };
            session.register_table(name, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
        }
        let plan = session
            .state()
            .create_logical_plan("SELECT a_id, b_id FROM a LEFT JOIN b ON a_id = b_id")
            .await?;
        let normalized = IvmNormalizer::default().normalize(plan)?;
        let analysis = OjvAnalyzer.analyze(&normalized.plan)?;
        let multi_join = &analysis.multi_joins[0];
        let LogicalPlan::TableScan(delta_scan) =
            session.table("b_delta").await?.into_unoptimized_plan()
        else {
            panic!("delta source should be a scan")
        };
        let mut rewritten = multi_join
            .inputs
            .iter()
            .map(|(id, input)| (*id, input.plan.clone()))
            .collect::<BTreeMap<_, _>>();
        let LogicalPlan::TableScan(old_b) = &multi_join.inputs[&OjvInputId::new(1)].plan else {
            panic!("b input should be a scan")
        };
        rewritten.insert(
            OjvInputId::new(1),
            LogicalPlan::TableScan(TableScan::try_new(
                old_b.table_name.clone(),
                delta_scan.source,
                old_b.projection.clone(),
                vec![],
                None,
            )?),
        );
        let delta = PrimaryDeltaPlanner.plan(multi_join, OjvInputId::new(1), &rewritten)?;
        let empty = LogicalPlan::Filter(Filter::try_new(lit(false), Arc::new(delta.clone()))?);
        let (negative, positive) = if deleting {
            (delta, empty)
        } else {
            (empty, delta)
        };
        let secondary = OjvSecondaryDeltaPlanner::new(&analysis, multi_join, OjvInputId::new(1))?;
        let applied = secondary.apply_primary(&multi_join.original_root, &negative, &positive)?;
        let secondary_delta = if deleting {
            secondary.new_orphans(&negative, &applied)?
        } else {
            secondary.ceased_orphans(&positive, &applied)?
        };
        let batches = session
            .execute_logical_plan(secondary_delta)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        Ok(())
    }
}
