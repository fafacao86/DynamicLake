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

pub use delta::{
    IncrementInterval, IvmDeltaRewriter, IvmRefreshContext, IvmRewriter, IvmScanKind, IvmScanNode,
    IvmSnapshotPlanner,
};
pub use error::IvmError;
pub use normalize::{IvmNormalizer, NormalizedIvmPlan, ROW_ID_COLUMN, normalize_ivm_plan};
pub use ojv::{
    ForwardJoinKeyGraph, OjvAnalysis, OjvAnalyzer, OjvInputId, OjvJoinInput, OjvJoinKeyBinding,
    OjvJoinNode, OjvJoinTree, OjvMultiJoin, PrimaryDeltaPlanner,
};
pub use plan_utils::materialize_ctes;
pub use signature::{IvmPlanSignature, IvmPlanSignatureGenerator};
