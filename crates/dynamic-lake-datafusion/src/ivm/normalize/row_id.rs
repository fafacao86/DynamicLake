use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::LogicalPlan;
pub use dynamic_lake_core::RowKind as DeltaRowKind;

use crate::ivm::error::IvmError;

pub const ROW_ID_COLUMN: &str = "__ivm_row_id";
pub const ROW_KIND_COLUMN: &str = "__ivm_row_kind";

/// Domain separation for deterministic row-id hashing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowIdDomain {
    Source,
    Join,
    UnionBranch(usize),
    Aggregate,
}

impl RowIdDomain {
    pub fn label(self) -> String {
        match self {
            Self::Source => "source".to_string(),
            Self::Join => "join".to_string(),
            Self::UnionBranch(branch) => format!("union-branch:{branch}"),
            Self::Aggregate => "aggregate".to_string(),
        }
    }
}

pub fn hidden_row_id() -> Column {
    Column::from_name(ROW_ID_COLUMN)
}

pub fn row_id_columns(schema: &DFSchema) -> Vec<Column> {
    schema
        .iter()
        .filter(|(_, field)| field.name() == ROW_ID_COLUMN)
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect()
}

pub fn single_row_id(plan: &LogicalPlan) -> Result<Column, IvmError> {
    let row_ids = row_id_columns(plan.schema());
    if row_ids.len() != 1 {
        return Err(IvmError::UnexpectedRowIdCount(row_ids.len()));
    }
    Ok(row_ids.into_iter().next().expect("row-id count is one"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_id_domains_are_separated() {
        assert_ne!(RowIdDomain::Join.label(), RowIdDomain::Aggregate.label());
        assert_ne!(
            RowIdDomain::UnionBranch(0).label(),
            RowIdDomain::UnionBranch(1).label()
        );
        assert_eq!(hidden_row_id().name, ROW_ID_COLUMN);
    }
}
