//! Incremental view maintenance planning.
//!
//! This module is intentionally separate from the lake-format read/write SPI.
//! It owns materialized-view normalization, OJV analysis, and incremental plan
//! construction over DataFusion logical plans.

pub mod delta;
pub mod error;
pub mod normalize;
pub mod ojv;
pub mod physical;
mod plan_utils;
pub mod signature;
mod view_compiler;

pub use delta::{
    IncrementInterval, IvmDeltaRewriter, IvmRefreshContext, IvmRewriter, IvmScanKind, IvmScanNode,
    IvmSnapshotPlanner, plan_incremental_view,
};
pub use error::IvmError;
pub use normalize::{IvmNormalizer, NormalizedIvmPlan, ROW_ID_COLUMN, normalize_ivm_plan};
pub use ojv::{
    ForwardJoinKeyGraph, OjvAnalysis, OjvAnalyzer, OjvInputId, OjvJoinInput, OjvJoinKeyEdge,
    OjvJoinNode, OjvJoinTree, OjvMultiJoin, PrimaryDeltaPlanner,
};
pub use plan_utils::materialize_ctes;
pub use signature::{IvmPlanSignature, IvmPlanSignatureGenerator};
pub use view_compiler::{canonical_view_id, compile_view_definitions};
