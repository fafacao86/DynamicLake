//! Dynamic table graph, execution order, and persistent metadata.

pub mod definition;
pub mod error;
pub mod graph;
pub mod graph_execution;
pub mod meta;
pub mod model;
pub mod row_kind;

pub use definition::{ViewDefinition, build_view_graph};
pub use error::{PipelineError, PipelineResult};
pub use graph::{DynamicPipelineGraph, GraphOperations, PipelineGraph};
pub use graph_execution::{GraphExecution, NodeExecution, NodeExecutionPlanner, RefreshMode};
pub use meta::{
    IncrementInterval, IvmStreamObject, KvTransaction, MaterializedViewDefinition, MetastoreError,
    MetastoreFactory, MetastoreRepository, MetastoreResult, StreamStatus, TransactionalKvStore,
};
pub use model::{DynamicTable, NodeId, PipelineBuildOptions, TableIdentifier, TableName};
pub use row_kind::{IVM_ROW_KIND_COLUMN, RowKind};
