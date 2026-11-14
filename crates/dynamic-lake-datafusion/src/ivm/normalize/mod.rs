//! Materialized-view normalization contracts.

pub mod hash;
pub mod row_id;

use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Column;
use datafusion::logical_expr::logical_plan::{Aggregate, Join, Projection, TableScan, Union};
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan};
use dynamic_lake_format::LakeFormatRegistry;

use crate::ivm::error::IvmError;

use super::plan_utils::table_identifier;

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
#[derive(Debug, Clone, Default)]
pub struct IvmNormalizer;

impl IvmNormalizer {
    pub async fn normalize(
        &self,
        plan: LogicalPlan,
        formats: &LakeFormatRegistry,
    ) -> Result<NormalizedIvmPlan, IvmError> {
        Ok(NormalizedIvmPlan::new(
            self.normalize_plan(plan, formats).await?,
        ))
    }

    #[async_recursion]
    async fn normalize_plan(
        &self,
        plan: LogicalPlan,
        formats: &LakeFormatRegistry,
    ) -> Result<LogicalPlan, IvmError> {
        match plan {
            LogicalPlan::TableScan(scan) => self.normalize_scan(scan, formats).await,
            LogicalPlan::Projection(projection) => {
                self.normalize_projection(projection, formats).await
            }
            LogicalPlan::Filter(filter) => {
                let input = Arc::new(
                    self.normalize_plan((*filter.input).clone(), formats)
                        .await?,
                );
                Ok(LogicalPlan::Filter(
                    datafusion::logical_expr::logical_plan::Filter::try_new(
                        filter.predicate,
                        input,
                    )?,
                ))
            }
            LogicalPlan::Join(join) => self.normalize_join(join, formats).await,
            LogicalPlan::Union(union) => self.normalize_union(union, formats).await,
            LogicalPlan::Aggregate(aggregate) => self.normalize_aggregate(aggregate, formats).await,
            LogicalPlan::EmptyRelation(empty) => Ok(LogicalPlan::Projection(Projection::try_new(
                vec![datafusion::logical_expr::lit(0_i64).alias(ROW_ID_COLUMN)],
                Arc::new(LogicalPlan::EmptyRelation(empty)),
            )?)),
            other => Err(IvmError::UnsupportedOperator(other.display().to_string())),
        }
    }

    async fn normalize_scan(
        &self,
        scan: TableScan,
        formats: &LakeFormatRegistry,
    ) -> Result<LogicalPlan, IvmError> {
        let source_schema = scan.source.schema();
        let identity_columns = if source_schema
            .fields()
            .iter()
            .any(|field| field.name() == ROW_ID_COLUMN)
        {
            vec![ROW_ID_COLUMN.to_string()]
        } else {
            let table = table_identifier(&scan.table_name)?;
            let catalog = table.parts().first().ok_or_else(|| {
                IvmError::SourceMetadata(format!("table `{table}` has no catalog"))
            })?;
            let format = formats.format(catalog).ok_or_else(|| {
                IvmError::SourceMetadata(format!("no lake format registered for `{catalog}`"))
            })?;
            format
                .get_identity_columns(&table)
                .await
                .map_err(|error| IvmError::SourceMetadata(error.to_string()))?
        };
        self.add_identity_projection(scan, identity_columns)
    }

    fn add_identity_projection(
        &self,
        scan: TableScan,
        identity_columns: Vec<String>,
    ) -> Result<LogicalPlan, IvmError> {
        let source_schema = scan.source.schema();
        let original_projection = scan
            .projection
            .clone()
            .unwrap_or_else(|| (0..source_schema.fields().len()).collect::<Vec<_>>());
        if identity_columns.is_empty() {
            return Err(IvmError::MissingSourceRowId {
                table: scan.table_name.to_string(),
                column: ROW_ID_COLUMN.to_string(),
            });
        }

        let mut projection = original_projection.clone();
        for column in &identity_columns {
            let (index, field) = source_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name() == column)
                .ok_or_else(|| IvmError::MissingSourceRowId {
                    table: scan.table_name.to_string(),
                    column: column.clone(),
                })?;
            if field.is_nullable() {
                return Err(IvmError::NullableSourceRowId(column.clone()));
            }
            if !projection.contains(&index) {
                projection.push(index);
            }
        }
        let scan = TableScan::try_new(
            scan.table_name.clone(),
            Arc::clone(&scan.source),
            Some(projection),
            scan.filters,
            scan.fetch,
        )?;
        if identity_columns.len() == 1 && identity_columns[0] == ROW_ID_COLUMN {
            return Ok(LogicalPlan::TableScan(scan));
        }

        let qualifier = Some(scan.table_name.clone());
        let mut exprs = original_projection
            .into_iter()
            .map(|index| {
                Expr::Column(Column::new(
                    qualifier.clone(),
                    source_schema.field(index).name(),
                ))
            })
            .collect::<Vec<_>>();
        let identity = identity_columns
            .into_iter()
            .map(|column| Expr::Column(Column::new(qualifier.clone(), column)))
            .collect::<Vec<_>>();
        exprs.push(
            ivm_sha1_expr(RowIdDomain::Source, identity)
                .alias_qualified(qualifier.clone(), ROW_ID_COLUMN),
        );
        Ok(LogicalPlan::Projection(Projection::try_new(
            exprs,
            Arc::new(LogicalPlan::TableScan(scan)),
        )?))
    }

    async fn normalize_projection(
        &self,
        projection: Projection,
        formats: &LakeFormatRegistry,
    ) -> Result<LogicalPlan, IvmError> {
        let input = Arc::new(
            self.normalize_plan((*projection.input).clone(), formats)
                .await?,
        );
        let mut exprs = projection.expr;
        for column in row_id_columns(input.schema()) {
            if !projection.schema.has_column(&column) {
                exprs.push(Expr::Column(column));
            }
        }
        Ok(LogicalPlan::Projection(Projection::try_new(exprs, input)?))
    }

    async fn normalize_join(
        &self,
        join: Join,
        formats: &LakeFormatRegistry,
    ) -> Result<LogicalPlan, IvmError> {
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
        ) {
            return Err(IvmError::UnsupportedJoinType(join.join_type));
        }
        let left = Arc::new(self.normalize_plan((*join.left).clone(), formats).await?);
        let right = Arc::new(self.normalize_plan((*join.right).clone(), formats).await?);
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

    async fn normalize_union(
        &self,
        union: Union,
        formats: &LakeFormatRegistry,
    ) -> Result<LogicalPlan, IvmError> {
        let output_width = union.schema.fields().len();
        let mut inputs = Vec::with_capacity(union.inputs.len());
        for (branch, input) in union.inputs.into_iter().enumerate() {
            let input = Arc::new(self.normalize_plan((*input).clone(), formats).await?);
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

    async fn normalize_aggregate(
        &self,
        aggregate: Aggregate,
        formats: &LakeFormatRegistry,
    ) -> Result<LogicalPlan, IvmError> {
        let group_key_count = aggregate.group_expr.len();
        let input = Arc::new(
            self.normalize_plan((*aggregate.input).clone(), formats)
                .await?,
        );
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

pub async fn normalize_ivm_plan(
    plan: LogicalPlan,
    formats: &LakeFormatRegistry,
) -> Result<NormalizedIvmPlan, IvmError> {
    IvmNormalizer::default().normalize(plan, formats).await
}

/// Build the persisted schema of a normalized IVM view.
pub fn materialized_view_schema(plan: &LogicalPlan) -> SchemaRef {
    let source_row_ids = plan
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() == ROW_ID_COLUMN)
        .count();
    if source_row_ids <= 1 {
        return Arc::new(plan.schema().as_arrow().clone());
    }
    let mut input = 0;
    let mut fields = plan
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == ROW_ID_COLUMN {
                let renamed = field
                    .as_ref()
                    .clone()
                    .with_name(format!("__ivm_input_{input}_row_id"));
                input += 1;
                renamed
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

/// Add a stable output identity and expose each input identity for an OJV.
pub fn materialized_view_plan(plan: LogicalPlan) -> Result<LogicalPlan, IvmError> {
    let row_ids = row_id_columns(plan.schema());
    if row_ids.len() <= 1 {
        return Ok(plan);
    }
    let mut exprs = plan
        .schema()
        .columns()
        .into_iter()
        .filter(|column| column.name != ROW_ID_COLUMN)
        .map(Expr::Column)
        .collect::<Vec<_>>();
    for (index, column) in row_ids.iter().enumerate() {
        exprs.push(Expr::Column(column.clone()).alias(format!("__ivm_input_{index}_row_id")));
    }
    exprs.push(
        ivm_sha1_expr(RowIdDomain::Join, row_ids.into_iter().map(Expr::Column))
            .cast_to(&DataType::Utf8, plan.schema())?
            .alias(ROW_ID_COLUMN),
    );
    Ok(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(plan),
    )?))
}

/// OJV deltas carry input identities; derive the persisted join identity.
pub fn materialized_view_delta_plan(
    plan: LogicalPlan,
    input_count: usize,
) -> Result<LogicalPlan, IvmError> {
    if plan
        .schema()
        .has_column_with_unqualified_name(ROW_ID_COLUMN)
    {
        return Ok(plan);
    }
    let columns = plan.schema().columns();
    let mut identities = Vec::with_capacity(input_count);
    for index in 0..input_count {
        let name = format!("__ivm_input_{index}_row_id");
        let column = columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| IvmError::UnsupportedOperator(format!("delta is missing `{name}`")))?;
        identities.push(Expr::Column(column.clone()));
    }
    let mut exprs = columns.into_iter().map(Expr::Column).collect::<Vec<_>>();
    exprs.push(
        ivm_sha1_expr(RowIdDomain::Join, identities)
            .cast_to(&DataType::Utf8, plan.schema())?
            .alias(ROW_ID_COLUMN),
    );
    Ok(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(plan),
    )?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::TableReference;
    use datafusion::datasource::default_table_source::DefaultTableSource;
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
        let normalized = normalize_ivm_plan(plan, &LakeFormatRegistry::new())
            .await
            .expect("normalize left join");

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
        let normalized = normalize_ivm_plan(plan, &LakeFormatRegistry::new())
            .await
            .expect("normalize union");
        let display = normalized.plan.display_indent().to_string();

        assert_eq!(row_id_columns(normalized.plan.schema()).len(), 1);
        assert!(display.contains("_ivm_sha1"));
        Ok(())
    }

    #[tokio::test]
    async fn primary_key_is_hashed_into_hidden_row_id() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, false),
        ]));
        let source = Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema))));
        let scan = TableScan::try_new(
            TableReference::bare("a"),
            source,
            Some(vec![1]),
            Vec::new(),
            None,
        )?;
        let normalized =
            IvmNormalizer::default().add_identity_projection(scan, vec!["id".to_string()])?;

        let names = normalized
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["k", ROW_ID_COLUMN]);
        assert!(
            normalized
                .display_indent()
                .to_string()
                .contains("_ivm_sha1")
        );
        Ok(())
    }
}
