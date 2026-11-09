//! Materialized-view normalization contracts.

pub mod hash;
pub mod row_id;

use std::sync::Arc;

use datafusion::logical_expr::logical_plan::{Aggregate, Join, Projection, TableScan, Union};
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan};

use crate::ivm::error::IvmError;

pub use hash::{IvmSha1, ivm_sha1, ivm_sha1_expr, register_ivm_functions};
pub use row_id::{
    DeltaRowKind, ROW_ID_COLUMN, ROW_KIND_COLUMN, RowIdDomain, hidden_row_id, row_id_columns,
    single_row_id,
};

/// The logical plan persisted for an incrementally maintained materialized view.
///
/// Normalization guarantees that every OJV input exposes [`ROW_ID_COLUMN`].
/// Joins retain one row-id per input; the OJV planner adds the final join hash.
#[derive(Debug, Clone)]
pub struct NormalizedIvmPlan {
    pub plan: LogicalPlan,
}

impl NormalizedIvmPlan {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }
}

/// Adds stable hidden row identities to a materialized-view definition.
///
/// This follows the Polars IVM normalizer: scans expose the configured source
/// row-id, joins retain one row-id for every input, and unions create a
/// branch-scoped hash identity. A final join hash is intentionally left to the
/// OJV planner.
#[derive(Debug, Clone)]
pub struct IvmNormalizer {
    source_row_id: String,
}

impl Default for IvmNormalizer {
    fn default() -> Self {
        Self::new(ROW_ID_COLUMN)
    }
}

impl IvmNormalizer {
    pub fn new(source_row_id: impl Into<String>) -> Self {
        Self {
            source_row_id: source_row_id.into(),
        }
    }

    pub fn normalize(&self, plan: LogicalPlan) -> Result<NormalizedIvmPlan, IvmError> {
        Ok(NormalizedIvmPlan::new(self.normalize_plan(plan)?))
    }

    fn normalize_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan, IvmError> {
        match plan {
            LogicalPlan::TableScan(scan) => self.normalize_scan(scan),
            LogicalPlan::Projection(projection) => self.normalize_projection(projection),
            LogicalPlan::Filter(filter) => {
                let input = Arc::new(self.normalize_plan((*filter.input).clone())?);
                Ok(LogicalPlan::Filter(
                    datafusion::logical_expr::logical_plan::Filter::try_new(
                        filter.predicate,
                        input,
                    )?,
                ))
            }
            LogicalPlan::Join(join) => self.normalize_join(join),
            LogicalPlan::Union(union) => self.normalize_union(union),
            LogicalPlan::Aggregate(aggregate) => self.normalize_aggregate(aggregate),
            LogicalPlan::EmptyRelation(empty) => Ok(LogicalPlan::Projection(Projection::try_new(
                vec![datafusion::logical_expr::lit(0_i64).alias(ROW_ID_COLUMN)],
                Arc::new(LogicalPlan::EmptyRelation(empty)),
            )?)),
            other => Err(IvmError::UnsupportedOperator(other.display().to_string())),
        }
    }

    fn normalize_scan(&self, scan: TableScan) -> Result<LogicalPlan, IvmError> {
        let source_schema = scan.source.schema();
        let (row_id_index, row_id_field) = source_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == &self.source_row_id)
            .ok_or_else(|| IvmError::MissingSourceRowId {
                table: scan.table_name.to_string(),
                column: self.source_row_id.clone(),
            })?;
        if row_id_field.is_nullable() {
            return Err(IvmError::NullableSourceRowId(self.source_row_id.clone()));
        }

        let mut projection = scan
            .projection
            .clone()
            .unwrap_or_else(|| (0..source_schema.fields().len()).collect::<Vec<_>>());
        if !projection.contains(&row_id_index) {
            projection.push(row_id_index);
        }
        let scan = TableScan::try_new(
            scan.table_name.clone(),
            Arc::clone(&scan.source),
            Some(projection),
            scan.filters,
            scan.fetch,
        )?;

        if self.source_row_id == ROW_ID_COLUMN {
            Ok(LogicalPlan::TableScan(scan))
        } else {
            let mut exprs = scan
                .projected_schema
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect::<Vec<_>>();
            exprs.push(
                Expr::Column(datafusion::common::Column::new(
                    Some(scan.table_name.clone()),
                    self.source_row_id.clone(),
                ))
                .alias_qualified(Some(scan.table_name.clone()), ROW_ID_COLUMN),
            );
            Ok(LogicalPlan::Projection(Projection::try_new(
                exprs,
                Arc::new(LogicalPlan::TableScan(scan)),
            )?))
        }
    }

    fn normalize_projection(&self, projection: Projection) -> Result<LogicalPlan, IvmError> {
        let input = Arc::new(self.normalize_plan((*projection.input).clone())?);
        let mut exprs = projection.expr;
        for column in row_id_columns(input.schema()) {
            if !projection.schema.has_column(&column) {
                exprs.push(Expr::Column(column));
            }
        }
        Ok(LogicalPlan::Projection(Projection::try_new(exprs, input)?))
    }

    fn normalize_join(&self, join: Join) -> Result<LogicalPlan, IvmError> {
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
        ) {
            return Err(IvmError::UnsupportedJoinType(join.join_type));
        }
        let left = Arc::new(self.normalize_plan((*join.left).clone())?);
        let right = Arc::new(self.normalize_plan((*join.right).clone())?);
        Ok(LogicalPlan::Join(Join::try_new(
            left,
            right,
            join.on,
            join.filter,
            join.join_type,
            join.join_constraint,
            join.null_equality,
            join.null_aware,
        )?))
    }

    fn normalize_union(&self, union: Union) -> Result<LogicalPlan, IvmError> {
        let output_width = union.schema.fields().len();
        let mut inputs = Vec::with_capacity(union.inputs.len());
        for (branch, input) in union.inputs.into_iter().enumerate() {
            let input = Arc::new(self.normalize_plan((*input).clone())?);
            let row_id = single_row_id(&input)?;
            let mut exprs = input
                .schema()
                .columns()
                .into_iter()
                .take(output_width)
                .zip(union.schema.fields().iter())
                .map(|(column, field)| Expr::Column(column).alias(field.name()))
                .collect::<Vec<_>>();
            exprs.push(
                hash::ivm_sha1_expr(RowIdDomain::UnionBranch(branch), [Expr::Column(row_id)])
                    .alias(ROW_ID_COLUMN),
            );
            inputs.push(Arc::new(LogicalPlan::Projection(Projection::try_new(
                exprs, input,
            )?)));
        }
        Ok(LogicalPlan::Union(Union::try_new(inputs)?))
    }

    fn normalize_aggregate(&self, aggregate: Aggregate) -> Result<LogicalPlan, IvmError> {
        let group_key_count = aggregate.group_expr.len();
        let input = Arc::new(self.normalize_plan((*aggregate.input).clone())?);
        let aggregate = Aggregate::try_new(input, aggregate.group_expr, aggregate.aggr_expr)?;
        let group_keys = aggregate
            .schema
            .columns()
            .into_iter()
            .take(group_key_count)
            .map(Expr::Column)
            .collect::<Vec<_>>();
        let mut exprs = aggregate
            .schema
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        exprs.push(hash::ivm_sha1_expr(RowIdDomain::Aggregate, group_keys).alias(ROW_ID_COLUMN));
        Ok(LogicalPlan::Projection(Projection::try_new(
            exprs,
            Arc::new(LogicalPlan::Aggregate(aggregate)),
        )?))
    }
}

pub fn normalize_ivm_plan(plan: LogicalPlan) -> Result<NormalizedIvmPlan, IvmError> {
    IvmNormalizer::default().normalize(plan)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn session_with_tables(
        tables: &[(&str, &[&str])],
    ) -> Result<SessionContext, Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        for (name, columns) in tables {
            let fields = columns
                .iter()
                .map(|column| Field::new(*column, DataType::Int64, false))
                .collect::<Vec<_>>();
            session.register_table(
                *name,
                Arc::new(EmptyTable::new(Arc::new(Schema::new(fields)))),
            )?;
        }
        Ok(session)
    }

    #[tokio::test]
    async fn left_join_keeps_one_row_id_per_input() -> Result<(), Box<dyn std::error::Error>> {
        let session = session_with_tables(&[
            ("a", &[ROW_ID_COLUMN, "a_id"]),
            ("b", &[ROW_ID_COLUMN, "b_id"]),
        ])
        .await?;
        let plan = session
            .state()
            .create_logical_plan("SELECT a_id, b_id FROM a LEFT JOIN b ON a_id = b_id")
            .await?;
        let normalized = normalize_ivm_plan(plan).expect("normalize left join");

        assert_eq!(row_id_columns(normalized.plan.schema()).len(), 2);
        assert!(
            normalized
                .plan
                .display_indent()
                .to_string()
                .contains("Left Join")
        );
        Ok(())
    }

    #[tokio::test]
    async fn union_hashes_each_branch_identity() -> Result<(), Box<dyn std::error::Error>> {
        let session =
            session_with_tables(&[("a", &[ROW_ID_COLUMN, "k"]), ("b", &[ROW_ID_COLUMN, "k"])])
                .await?;
        let plan = session
            .state()
            .create_logical_plan("SELECT k FROM a UNION ALL SELECT k FROM b")
            .await?;
        let normalized = normalize_ivm_plan(plan).expect("normalize union");
        let display = normalized.plan.display_indent().to_string();

        assert_eq!(row_id_columns(normalized.plan.schema()).len(), 1);
        assert!(display.contains("_ivm_sha1"));
        Ok(())
    }

    #[tokio::test]
    async fn configured_source_row_id_is_exposed_as_ivm_row_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let session = session_with_tables(&[("a", &["_ROW_ID", "k"])]).await?;
        let plan = session
            .state()
            .create_logical_plan("SELECT k FROM a")
            .await?;
        let normalized = IvmNormalizer::new("_ROW_ID").normalize(plan)?;

        assert_eq!(row_id_columns(normalized.plan.schema()).len(), 1);
        Ok(())
    }
}
