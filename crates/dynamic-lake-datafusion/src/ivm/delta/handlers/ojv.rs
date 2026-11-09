use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::logical_plan::{Filter, Sort, Union};
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr, lit};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::{DeltaRowKind, ROW_KIND_COLUMN};
use crate::ivm::ojv::{OjvInputId, OjvSecondaryDeltaPlanner, PrimaryDeltaPlanner};
use crate::ivm::plan_utils::cte_ref;
use crate::ivm::plan_utils::project_plan;

use super::super::IvmDeltaRewriter;

const APPLY_PHASE_COLUMN: &str = "__ivm_ojv_apply_phase";

pub(crate) struct OjvDeltaHandler;

impl OjvDeltaHandler {
    pub(crate) fn rewrite(
        &self,
        join_root: &LogicalPlan,
        rewriter: &IvmDeltaRewriter<'_>,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        let context = rewriter.context();
        let multi_join = context
            .analysis
            .multi_joins
            .iter()
            .find(|multi_join| multi_join.original_root == *join_root)
            .ok_or(IvmError::MissingOjvJoinRoot)?;
        let mut changed_inputs = Vec::new();
        for input in multi_join.inputs.values() {
            if rewriter.may_change(&input.plan) {
                changed_inputs.push(input.id);
            }
        }
        if changed_inputs.is_empty() {
            return Ok(None);
        }
        if changed_inputs.len() > 1 {
            return Err(IvmError::MultipleChangedInputs);
        }
        let changed_input = changed_inputs[0];

        let mut rewritten_inputs: BTreeMap<OjvInputId, LogicalPlan> = BTreeMap::new();
        for input in multi_join.inputs.values() {
            let plan = if input.id == changed_input {
                rewriter
                    .rewrite(&input.plan)?
                    .ok_or(IvmError::MissingDelta)?
            } else {
                context.snapshots.before(&input.plan)?
            };
            rewritten_inputs.insert(input.id, plan);
        }
        let primary =
            cte_ref(PrimaryDeltaPlanner.plan(multi_join, changed_input, &rewritten_inputs)?)?;
        let secondary = OjvSecondaryDeltaPlanner::new(context.analysis, multi_join, changed_input)?;
        if !secondary.needs_secondary() {
            return Ok(Some(self.plan_primary_only(primary)?));
        }
        let before_view = context.snapshots.before(join_root)?;
        Ok(Some(self.plan_apply(primary, &secondary, &before_view)?))
    }

    fn plan_primary_only(&self, primary: LogicalPlan) -> Result<LogicalPlan, IvmError> {
        let primary_delete = filter_row_kind(&primary, DeltaRowKind::Negative)?;
        let primary_insert = filter_row_kind(&primary, DeltaRowKind::Positive)?;
        let branches = primary_branches(&primary, &primary_delete, &primary_insert)?;
        finish_apply(&primary, branches)
    }

    fn plan_apply(
        &self,
        primary: LogicalPlan,
        secondary: &OjvSecondaryDeltaPlanner<'_>,
        before_view: &LogicalPlan,
    ) -> Result<LogicalPlan, IvmError> {
        let primary_delete = filter_row_kind(&primary, DeltaRowKind::Negative)?;
        let primary_insert = filter_row_kind(&primary, DeltaRowKind::Positive)?;
        let mut branches = primary_branches(&primary, &primary_delete, &primary_insert)?;
        // V1 is the view after applying the primary delta. The secondary
        // planner uses it to find removed and newly created orphan rows.
        let applied_view =
            cte_ref(secondary.apply_primary(before_view, &primary_delete, &primary_insert)?)?;
        let secondary_delete = secondary.ceased_orphans(&primary_insert, &applied_view)?;
        let secondary_insert = secondary.new_orphans(&primary_delete, &applied_view)?;
        branches.push(canonical_branch(
            &secondary_delete,
            &primary,
            DeltaRowKind::Negative,
            2,
        )?);
        branches.push(canonical_branch(
            &secondary_insert,
            &primary,
            DeltaRowKind::Positive,
            3,
        )?);
        finish_apply(&primary, branches)
    }
}

fn primary_branches(
    primary: &LogicalPlan,
    primary_delete: &LogicalPlan,
    primary_insert: &LogicalPlan,
) -> Result<Vec<LogicalPlan>, IvmError> {
    Ok(vec![
        canonical_branch(primary_delete, primary, DeltaRowKind::Negative, 0)?,
        canonical_branch(primary_insert, primary, DeltaRowKind::Positive, 1)?,
    ])
}

fn finish_apply(
    primary: &LogicalPlan,
    branches: Vec<LogicalPlan>,
) -> Result<LogicalPlan, IvmError> {
    let union = LogicalPlan::Union(Union::try_new(
        branches.into_iter().map(Arc::new).collect(),
    )?);
    let phase = Column::from_name(APPLY_PHASE_COLUMN);
    // DataFusion's Union drops field qualifiers. Restore the primary
    // schema so outer projections can still resolve qualified columns.
    let exprs = union
        .schema()
        .columns()
        .into_iter()
        .zip(
            primary
                .schema()
                .columns()
                .into_iter()
                .chain([phase.clone()]),
        )
        .map(|(actual, target)| Expr::Column(actual).alias_qualified(target.relation, target.name))
        .collect();
    let qualified = project_plan(union, exprs)?;
    Ok(LogicalPlan::Sort(Sort {
        expr: vec![SortExpr::new(Expr::Column(phase), true, false)],
        input: Arc::new(qualified),
        fetch: None,
    }))
}

fn filter_row_kind(plan: &LogicalPlan, row_kind: DeltaRowKind) -> Result<LogicalPlan, IvmError> {
    Ok(LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::from_name(ROW_KIND_COLUMN)).eq(lit(row_kind.code())),
        Arc::new(plan.clone()),
    )?))
}

fn canonical_branch(
    branch: &LogicalPlan,
    primary: &LogicalPlan,
    row_kind: DeltaRowKind,
    phase: usize,
) -> Result<LogicalPlan, IvmError> {
    let mut exprs = Vec::new();
    for column in primary.schema().columns() {
        let expr = if column.name == ROW_KIND_COLUMN {
            lit(row_kind.code()).alias(ROW_KIND_COLUMN)
        } else {
            let fallback = resolve_by_name(branch, &column.name)?;
            match fallback {
                Some(resolved) => {
                    Expr::Column(resolved).alias_qualified(column.relation.clone(), &column.name)
                }
                None => {
                    let index = primary.schema().index_of_column(&column)?;
                    Expr::Literal(
                        ScalarValue::try_from(primary.schema().field(index).data_type())?,
                        None,
                    )
                    .alias_qualified(column.relation.clone(), &column.name)
                }
            }
        };
        exprs.push(expr);
    }
    exprs.push(lit(phase as i32).alias(APPLY_PHASE_COLUMN));
    Ok(project_plan(branch.clone(), exprs)?)
}

fn resolve_by_name(plan: &LogicalPlan, name: &str) -> Result<Option<Column>, IvmError> {
    let columns = plan
        .schema()
        .iter()
        .filter(|(_, field)| field.name() == name)
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect::<Vec<_>>();
    match columns.as_slice() {
        [] => Ok(None),
        [column] => Ok(Some(column.clone())),
        _ => Err(IvmError::MissingHiddenColumn(name.to_string())),
    }
}
