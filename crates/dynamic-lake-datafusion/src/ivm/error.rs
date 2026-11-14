use datafusion::error::DataFusionError;
use datafusion::logical_expr::JoinType;
use thiserror::Error;

use super::ojv::OjvInputId;

#[derive(Debug, Error)]
pub enum IvmError {
    #[error("OJV join type `{0}` is not supported in the incremental path")]
    UnsupportedJoinType(JoinType),
    #[error("OJV join-key binding references input `{0}` which is not part of the multi-join")]
    MissingJoinInput(OjvInputId),
    #[error("duplicate OJV input id `{0}`")]
    DuplicateInput(OjvInputId),
    #[error("OJV input `{0}` is not part of the forward join-key graph")]
    MissingGraphInput(OjvInputId),
    #[error("source table `{table}` has no row-id column `{column}`")]
    MissingSourceRowId { table: String, column: String },
    #[error("failed to resolve row identity: {0}")]
    SourceMetadata(String),
    #[error("source row-id column `{0}` must be non-null")]
    NullableSourceRowId(String),
    #[error("expected exactly one IVM row-id column, found {0}")]
    UnexpectedRowIdCount(usize),
    #[error("unsupported IVM normalize operator: {0}")]
    UnsupportedOperator(String),
    #[error("OJV input `{0}` is missing from the delta rewrite")]
    MissingRewriteInput(OjvInputId),
    #[error("changed OJV input `{0}` is absent from the multi-join")]
    MissingChangedInput(OjvInputId),
    #[error(
        "multiple OJV inputs changed in one multi-join; self-join maintenance is not supported"
    )]
    MultipleChangedInputs,
    #[error("OJV input `{0}` has no rows to rewrite")]
    EmptyRewriteInput(OjvInputId),
    #[error("join root is not registered in the OJV analysis")]
    MissingOjvJoinRoot,
    #[error("IVM delta rewrite produced no delta for a changed input")]
    MissingDelta,
    #[error("required IVM column `{0}` is missing")]
    MissingHiddenColumn(String),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
}
