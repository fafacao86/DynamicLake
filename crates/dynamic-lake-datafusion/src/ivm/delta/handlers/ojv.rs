use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::logical_expr::logical_plan::{Filter, Projection, Sort, Union};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, SortExpr, lit};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::{DeltaRowKind, ROW_KIND_COLUMN};
use crate::ivm::ojv::{OjvInputId, OjvSecondaryDeltaPlanner, PrimaryDeltaPlanner};

use super::super::IvmDeltaRewriter;

const APPLY_PHASE_COLUMN: &str = "__ivm_apply_phase";

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
        let primary = PrimaryDeltaPlanner.plan(multi_join, changed_input, &rewritten_inputs)?;
        let after_view = context.snapshots.after(join_root)?;
        Ok(Some(self.plan_apply(
            primary,
            context.analysis,
            multi_join,
            changed_input,
            &after_view,
        )?))
    }

    fn plan_apply(
        &self,
        primary: LogicalPlan,
        analysis: &crate::ivm::ojv::OjvAnalysis,
        multi_join: &crate::ivm::ojv::OjvMultiJoin,
        changed_input: OjvInputId,
        after_view: &LogicalPlan,
    ) -> Result<LogicalPlan, IvmError> {
        let primary_delete = filter_row_kind(&primary, DeltaRowKind::Negative)?;
        let primary_insert = filter_row_kind(&primary, DeltaRowKind::Positive)?;
        let secondary = OjvSecondaryDeltaPlanner::new(analysis, multi_join, changed_input)?;
        let secondary_delete = if secondary.is_no_op() {
            empty_branch(&primary, DeltaRowKind::Negative)?
        } else {
            secondary.ceased_orphans(&primary_insert, after_view)?
        };
        let secondary_insert = if secondary.is_no_op() {
            empty_branch(&primary, DeltaRowKind::Positive)?
        } else {
            secondary.new_orphans(&primary_delete, after_view)?
        };

        let branches = [
            canonical_branch(&primary_delete, &primary, DeltaRowKind::Negative, 0)?,
            canonical_branch(&primary_insert, &primary, DeltaRowKind::Positive, 1)?,
            canonical_branch(&secondary_delete, &primary, DeltaRowKind::Negative, 2)?,
            canonical_branch(&secondary_insert, &primary, DeltaRowKind::Positive, 3)?,
        ];
        let union = LogicalPlan::Union(Union::try_new(
            branches.into_iter().map(Arc::new).collect(),
        )?);
        let phase = Column::from_name(APPLY_PHASE_COLUMN);
        Ok(LogicalPlan::Sort(Sort {
            expr: vec![SortExpr::new(Expr::Column(phase), true, false)],
            input: Arc::new(union),
            fetch: None,
        }))
    }
}

fn filter_row_kind(plan: &LogicalPlan, row_kind: DeltaRowKind) -> Result<LogicalPlan, IvmError> {
    Ok(LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::from_name(ROW_KIND_COLUMN)).eq(lit(row_kind.code())),
        Arc::new(plan.clone()),
    )?))
}

fn empty_branch(primary: &LogicalPlan, row_kind: DeltaRowKind) -> Result<LogicalPlan, IvmError> {
    let empty = LogicalPlan::Filter(Filter::try_new(lit(false), Arc::new(primary.clone()))?);
    canonical_branch(&empty, primary, row_kind, usize::MAX)
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
                Some(column) => Expr::Column(column.clone()).alias(&column.name),
                None => {
                    let index = primary.schema().index_of_column(&column)?;
                    Expr::Literal(
                        ScalarValue::try_from(primary.schema().field(index).data_type())?,
                        None,
                    )
                    .alias(column.name)
                }
            }
        };
        exprs.push(expr);
    }
    exprs.push(lit(phase as i32).alias(APPLY_PHASE_COLUMN));
    let fields = exprs
        .iter()
        .map(|expr| expr.to_field(branch.schema()))
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    let schema = Arc::new(DFSchema::new_with_metadata(
        fields,
        branch.schema().metadata().clone(),
    )?);
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        exprs,
        Arc::new(branch.clone()),
        schema,
    )?))
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
