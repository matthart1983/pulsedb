use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{debug, warn};

use crate::model::DataPoint;

use super::config::FsyncPolicy;

/// Entry type discriminator stored in the WAL.
const ENTRY_TYPE_WRITE: u8 = 1;

/// Append-only Write-Ahead Log backed by a single file.
pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
    fsync: FsyncPolicy,
}

impl Wal {
    /// Open (or create) the WAL file inside `dir`.
    pub fn open(dir: &Path, fsync: FsyncPolicy) -> Result<Self> {
        fs::create_dir_all(dir).context("creating WAL directory")?;
        let path = dir.join("wal.log");

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening WAL file at {}", path.display()))?;

        debug!(path = %path.display(), "WAL opened");

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            fsync,
        })
    }

    /// Append a batch of data points as a single WAL entry.
    pub fn append(&mut self, points: &[DataPoint]) -> Result<()> {
        let payload = serde_json::to_vec(points).context("serializing WAL payload")?;

        let crc = crc32fast::hash(&payload);

        let len = (1 + payload.len()) as u32; // entry_type + payload

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&[ENTRY_TYPE_WRITE])?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;

        if self.fsync == FsyncPolicy::Every {
            self.writer.get_ref().sync_all()?;
        }

        debug!(points = points.len(), bytes = payload.len(), "WAL entry appended");

        Ok(())
    }

    /// Replay the WAL and return all valid data points, skipping corrupted entries.
    pub fn recover(&mut self) -> Result<Vec<DataPoint>> {
        let file = File::open(&self.path).with_context(|| {
            format!("opening WAL for recovery at {}", self.path.display())
        })?;

        let mut reader = BufReader::new(file);
        let mut all_points = Vec::new();

        loop {
            // Read header: len(4) + crc(4)
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e).context("reading WAL entry length"),
            }

            let len = u32::from_le_bytes(len_buf) as usize;

            let mut crc_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut crc_buf) {
                warn!("truncated WAL entry (missing CRC): {e}");
                break;
            }
            let stored_crc = u32::from_le_bytes(crc_buf);

            if len == 0 {
                warn!("WAL entry with zero length, stopping recovery");
                break;
            }

            // Read entry_type + payload
            let mut body = vec![0u8; len];
            if let Err(e) = reader.read_exact(&mut body) {
                warn!("truncated WAL entry body: {e}");
                break;
            }

            let entry_type = body[0];
            let payload = &body[1..];

            let actual_crc = crc32fast::hash(payload);
            if actual_crc != stored_crc {
                warn!(
                    stored_crc,
                    actual_crc, "CRC mismatch in WAL entry, skipping"
                );
                continue;
            }

            if entry_type != ENTRY_TYPE_WRITE {
                warn!(entry_type, "unknown WAL entry type, skipping");
                continue;
            }

            match serde_json::from_slice::<Vec<DataPoint>>(payload) {
                Ok(points) => {
                    debug!(points = points.len(), "recovered WAL entry");
                    all_points.extend(points);
                }
                Err(e) => {
                    warn!("failed to deserialize WAL entry: {e}");
                }
            }
        }

        debug!(total = all_points.len(), "WAL recovery complete");
        Ok(all_points)
    }

    /// Truncate the WAL file (called after a successful flush).
    pub fn truncate(&mut self) -> Result<()> {
        // Drop the old writer, truncate, and re-open.
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
            .context("truncating WAL file")?;

        self.writer = BufWriter::new(file);

        debug!(path = %self.path.display(), "WAL truncated");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample_points(n: usize) -> Vec<DataPoint> {
        (0..n)
            .map(|i| DataPoint {
                measurement: "cpu".into(),
                tags: BTreeMap::from([("host".into(), format!("server-{i}"))]),
                fields: BTreeMap::from([(
                    "usage".into(),
                    crate::model::FieldValue::Float(42.5 + i as f64),
                )]),
                timestamp: 1_700_000_000 + i as i64,
            })
            .collect()
    }

    #[test]
    fn append_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), FsyncPolicy::None).unwrap();

        let points = sample_points(5);
        wal.append(&points).unwrap();

        let mut wal2 = Wal::open(dir.path(), FsyncPolicy::None).unwrap();
        let recovered = wal2.recover().unwrap();

        assert_eq!(recovered.len(), 5);
        assert_eq!(recovered[0].measurement, "cpu");
        assert_eq!(recovered[4].timestamp, 1_700_000_004);
    }

    #[test]
    fn recover_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), FsyncPolicy::None).unwrap();

        wal.append(&sample_points(3)).unwrap();
        wal.append(&sample_points(2)).unwrap();

        let mut wal2 = Wal::open(dir.path(), FsyncPolicy::None).unwrap();
        let recovered = wal2.recover().unwrap();
        assert_eq!(recovered.len(), 5);
    }

    #[test]
    fn truncate_clears_wal() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), FsyncPolicy::None).unwrap();

        wal.append(&sample_points(3)).unwrap();
        wal.truncate().unwrap();

        let mut wal2 = Wal::open(dir.path(), FsyncPolicy::None).unwrap();
        let recovered = wal2.recover().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn recover_handles_corrupted_crc() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(dir.path(), FsyncPolicy::None).unwrap();
        wal.append(&sample_points(2)).unwrap();
        drop(wal);

        // Corrupt the CRC bytes (bytes 4..8) of the first entry.
        let wal_path = dir.path().join("wal.log");
        let mut data = fs::read(&wal_path).unwrap();
        data[4] ^= 0xFF;
        data[5] ^= 0xFF;
        let mut f = File::create(&wal_path).unwrap();
        f.write_all(&data).unwrap();

        let mut wal2 = Wal::open(dir.path(), FsyncPolicy::None).unwrap();
        let recovered = wal2.recover().unwrap();

        // The corrupted entry should be skipped.
        assert_eq!(recovered.len(), 0);
    }
}
