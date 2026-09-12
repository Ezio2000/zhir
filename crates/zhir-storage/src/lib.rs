//! Native atomic run stores with incremental history persistence.
pub mod artifacts;
mod codec;
#[cfg(feature = "artifacts-filesystem")]
pub use artifacts::FilesystemArtifactStore;
pub use artifacts::MemoryArtifactStore;
pub mod memory;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(any(feature = "sqlite", feature = "mysql"))]
mod sql;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub use memory::MemoryRunStore;
