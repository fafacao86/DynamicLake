//! Transactional metadata persistence primitives for IVM pipelines.

mod error;
mod key;
mod model;
mod repository;
mod store;

pub use error::{MetastoreError, MetastoreResult};
pub use model::{IncrementInterval, IvmStreamObject, MaterializedViewDefinition, StreamStatus};
pub use repository::MetastoreRepository;
pub use store::{KvTransaction, TransactionalKvStore};
