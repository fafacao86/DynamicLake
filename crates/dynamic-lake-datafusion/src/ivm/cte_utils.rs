//! Materialize explicitly shared IVM subplans with DataFusion's cache API.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::logical_plan::SubqueryAlias;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;

use super::plan_utils::project_plan;

const CTE_PREFIX: &str = "__ivm_cte_";
static NEXT_CTE_ID: AtomicU64 = AtomicU64::new(1);

/// Mark one logical subplan for reuse without changing its output schema.
pub(crate) fn cte_ref(input: LogicalPlan) -> Result<LogicalPlan> {
    let name = format!(
        "{CTE_PREFIX}{}",
        NEXT_CTE_ID.fetch_add(1, Ordering::Relaxed)
    );
    let original = input.schema().columns();
    // A CTE may contain duplicate field names under different qualifiers.
    // Give the cache unique names, then restore the original qualified schema.
    let names = (0..original.len())
        .map(|index| format!("__ivm_cte_column_{index}"))
        .collect::<Vec<_>>();
    let renamed = project_plan(
        input,
        original
            .iter()
            .zip(&names)
            .map(|(column, name)| Expr::Column(column.clone()).alias(name))
            .collect(),
    )?;
    let aliased = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(renamed), name)?);
    project_plan(
        aliased,
        original
            .into_iter()
            .zip(names)
            .map(|(column, name)| {
                Expr::Column(Column::from_name(name)).alias_qualified(column.relation, column.name)
            })
            .collect(),
    )
}

/// Cache CTEs in dependency order: if B references A, cache A before B.
/// After caching A, replace its references so B reads the cached result.
pub async fn materialize_ctes(
    session: &SessionContext,
    mut plan: LogicalPlan,
) -> Result<LogicalPlan> {
    loop {
        let mut candidate = None;
        plan.apply(|node| {
            if let LogicalPlan::SubqueryAlias(alias) = node {
                if alias.alias.table().starts_with(CTE_PREFIX) && !has_cte(&alias.input)? {
                    candidate = Some((alias.alias.table().to_string(), (*alias.input).clone()));
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let Some((name, producer)) = candidate else {
            break;
        };
        let cached = session
            .execute_logical_plan(producer)
            .await?
            .cache()
            .await?
            .into_unoptimized_plan();
        plan = plan
            .transform_up(|node| match node {
                LogicalPlan::SubqueryAlias(alias) if alias.alias.table() == name => Ok(
                    Transformed::yes(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                        Arc::new(cached.clone()),
                        format!("__ivm_cached_{name}"),
                    )?)),
                ),
                other => Ok(Transformed::no(other)),
            })?
            .data;
    }
    Ok(plan)
}

fn has_cte(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::SubqueryAlias(alias) if alias.alias.table().starts_with(CTE_PREFIX)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::logical_plan::Union;

    use super::*;

    #[tokio::test]
    async fn caches_nested_ctes_and_restores_duplicate_column_names() -> Result<()> {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for table in ["a", "b"] {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )?;
            session.register_table(
                table,
                Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])?),
            )?;
        }
        let source = session
            .sql("SELECT a.id, b.id FROM a CROSS JOIN b")
            .await?
            .into_unoptimized_plan();
        let expected_schema = source.schema().clone();
        let primary = cte_ref(source)?;
        assert_eq!(primary.schema(), &expected_schema);
        let applied = cte_ref(primary)?;
        assert_eq!(applied.schema(), &expected_schema);
        let plan = LogicalPlan::Union(Union::try_new(vec![
            Arc::new(applied.clone()),
            Arc::new(applied),
        ])?);
        let resolved = materialize_ctes(&session, plan).await?;
        assert!(!has_cte(&resolved)?);
        let batches = session
            .execute_logical_plan(resolved)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(batches[0].num_columns(), 2);
        Ok(())
    }
}
