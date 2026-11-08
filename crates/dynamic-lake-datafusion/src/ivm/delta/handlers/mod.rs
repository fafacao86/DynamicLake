mod linear;
mod ojv;
mod union;

pub(super) use linear::LinearDeltaHandler;
pub(super) use ojv::OjvDeltaHandler;
pub(super) use union::UnionDeltaHandler;

use crate::ivm::error::IvmError;
use datafusion::common::Column;

pub(super) fn column_by_name(
    schema: &datafusion::common::DFSchema,
    name: &str,
) -> Result<Column, IvmError> {
    let columns = schema
        .iter()
        .filter(|(_, field)| field.name() == name)
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect::<Vec<_>>();
    match columns.as_slice() {
        [column] => Ok(column.clone()),
        _ => Err(IvmError::MissingHiddenColumn(name.to_string())),
    }
}
