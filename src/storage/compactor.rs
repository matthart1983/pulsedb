//! Background segment compaction.
//!
//! Merges small segments within the same partition into larger ones,
//! re-compresses data, and drops tombstoned entries.

use tracing::info;

pub struct Compactor;

impl Compactor {
    pub fn new() -> Self {
        Self
    }

    pub fn compact(&self) -> anyhow::Result<()> {
        info!("compaction not yet implemented");
        Ok(())
    }
}

impl Default for Compactor {
    fn default() -> Self {
        Self::new()
    }
}
