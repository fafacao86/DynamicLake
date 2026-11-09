//! Paimon table property keys exposed by DynamicLake's Paimon adapter.
//!
//! The upstream `CoreOptions` type exposes readers but not these key constants.

pub const BUCKET: &str = "bucket";
pub const BUCKET_KEY: &str = "bucket-key";
pub const FILE_FORMAT: &str = "file.format";
pub const WRITE_ONLY: &str = "write-only";
pub const COMMIT_FORCE_COMPACT: &str = "commit.force-compact";
pub const COMPACTION_FORCE_UP_LEVEL_0: &str = "compaction.force-up-level-0";
pub const WRITE_BUFFER_SPILLABLE: &str = "write-buffer.spillable";
pub const DELETION_VECTORS_ENABLED: &str = "deletion-vectors.enabled";
pub const LOOKUP_REMOTE_FILE_ENABLED: &str = "lookup.remote-file.enabled";
