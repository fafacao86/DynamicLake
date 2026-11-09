//! Outer-join view (OJV) incremental maintenance.
//!
//! https://www.cs.columbia.edu/~jrzhou/pub/OJViewMaintenance.pdf

mod analyzer;
mod forward_join_key_graph;
mod multi_join;
mod primary;
mod secondary;

pub use analyzer::OjvAnalyzer;
pub use forward_join_key_graph::ForwardJoinKeyGraph;
pub use multi_join::{
    OjvAnalysis, OjvInputId, OjvJoinInput, OjvJoinKeyBinding, OjvJoinNode, OjvJoinTree,
    OjvMultiJoin, input_row_id_name,
};
pub use primary::PrimaryDeltaPlanner;
pub use secondary::OjvSecondaryDeltaPlanner;
