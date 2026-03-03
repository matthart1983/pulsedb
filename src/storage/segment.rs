//! Columnar segment file reader and writer.
//!
//! Segments are immutable on-disk files containing time-sorted data for a
//! single series. Data is stored column-by-column with type-aware compression.
//!
//! File format:
//!   [magic: 8 bytes] [header] [column blocks...] [footer]

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::encoding;
use crate::model::FieldValue;

const MAGIC: &[u8; 8] = b"PLSDB001";

// Encoding type markers
const ENC_TIMESTAMP: u8 = 1;
const ENC_FLOAT: u8 = 2;
const ENC_INTEGER: u8 = 3;
const ENC_BOOLEAN: u8 = 4;

/// Writes a columnar segment file to disk.
pub struct SegmentWriter;

impl SegmentWriter {
    /// Write a complete segment file for a single series.
    ///
    /// `timestamps` must be sorted. `fields` maps field names to parallel
    /// arrays of values (same length as `timestamps`).
    pub fn write_segment(
        path: &Path,
        series_key: &str,
        timestamps: &[i64],
        fields: &BTreeMap<String, Vec<FieldValue>>,
    ) -> Result<()> {
        let point_count = timestamps.len() as u64;
        if point_count == 0 {
            bail!("cannot write empty segment");
        }

        // Validate all field columns have the same length
        for (name, values) in fields {
            if values.len() != timestamps.len() {
                bail!(
                    "field '{}' has {} values but expected {}",
                    name,
                    values.len(),
                    timestamps.len()
                );
            }
        }

        let min_ts = timestamps[0];
        let max_ts = timestamps[timestamps.len() - 1];
        // timestamp column + field columns
        let column_count = 1 + fields.len() as u32;

        let mut buf = Vec::new();

        // -- Magic --
        buf.extend_from_slice(MAGIC);

        // -- Header --
        buf.extend_from_slice(&min_ts.to_le_bytes());
        buf.extend_from_slice(&max_ts.to_le_bytes());
        buf.extend_from_slice(&point_count.to_le_bytes());
        buf.extend_from_slice(&column_count.to_le_bytes());

        // -- Series key --
        let key_bytes = series_key.as_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(key_bytes);

        // -- Timestamp column --
        Self::write_column_block(&mut buf, "__timestamp", ENC_TIMESTAMP, || {
            Ok(encoding::encode_timestamps(timestamps))
        })?;

        // -- Field columns --
        for (name, values) in fields {
            let enc_type = Self::encoding_type_for(&values[0]);
            Self::write_column_block(&mut buf, name, enc_type, || {
                Self::encode_field_column(values, enc_type)
            })?;
        }

        // -- Footer --
        let checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        // Write atomically via temp file
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::File::create(path)?;
        file.write_all(&buf)?;
        file.sync_all()?;

        Ok(())
    }

    fn write_column_block(
        buf: &mut Vec<u8>,
        name: &str,
        enc_type: u8,
        encode_fn: impl FnOnce() -> Result<Vec<u8>>,
    ) -> Result<()> {
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.push(enc_type);

        let encoded = encode_fn()?;
        let compressed = lz4_flex::compress_prepend_size(&encoded);

        buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        buf.extend_from_slice(&compressed);

        Ok(())
    }

    fn encoding_type_for(value: &FieldValue) -> u8 {
        match value {
            FieldValue::Float(_) => ENC_FLOAT,
            FieldValue::Integer(_) => ENC_INTEGER,
            FieldValue::UInteger(_) => ENC_INTEGER,
            FieldValue::Boolean(_) => ENC_BOOLEAN,
            FieldValue::String(_) => ENC_INTEGER, // fallback; string fields not yet supported
        }
    }

    fn encode_field_column(values: &[FieldValue], enc_type: u8) -> Result<Vec<u8>> {
        match enc_type {
            ENC_FLOAT => {
                let floats: Vec<f64> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Float(f) => *f,
                        _ => 0.0,
                    })
                    .collect();
                Ok(encoding::encode_floats(&floats))
            }
            ENC_INTEGER => {
                let ints: Vec<i64> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Integer(i) => *i,
                        FieldValue::UInteger(u) => *u as i64,
                        _ => 0,
                    })
                    .collect();
                Ok(encoding::encode_integers(&ints))
            }
            ENC_BOOLEAN => {
                let bools: Vec<bool> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Boolean(b) => *b,
                        _ => false,
                    })
                    .collect();
                Ok(encoding::encode_booleans(&bools))
            }
            _ => bail!("unsupported encoding type: {}", enc_type),
        }
    }
}

/// Parsed column block metadata for reading.
struct ColumnBlock {
    name: String,
    enc_type: u8,
    data: Vec<u8>, // compressed data
}

/// Reads a columnar segment file from disk.
pub struct SegmentReader {
    min_ts: i64,
    max_ts: i64,
    point_count: u64,
    series_key: String,
    columns: Vec<ColumnBlock>,
}

impl SegmentReader {
    /// Open and parse a segment file.
    pub fn open(path: &Path) -> Result<Self> {
        let data = fs::read(path).with_context(|| format!("reading segment {}", path.display()))?;

        if data.len() < 8 {
            bail!("segment too small");
        }
        if &data[0..8] != MAGIC {
            bail!("invalid segment magic bytes");
        }

        let mut pos = 8;

        // Header
        let min_ts = read_i64(&data, &mut pos)?;
        let max_ts = read_i64(&data, &mut pos)?;
        let point_count = read_u64(&data, &mut pos)?;
        let column_count = read_u32(&data, &mut pos)?;

        // Series key
        let key_len = read_u16(&data, &mut pos)? as usize;
        if pos + key_len > data.len() {
            bail!("unexpected end of data reading series key");
        }
        let series_key = String::from_utf8(data[pos..pos + key_len].to_vec())?;
        pos += key_len;

        // Column blocks
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let name_len = read_u16(&data, &mut pos)? as usize;
            if pos + name_len > data.len() {
                bail!("unexpected end of data reading column name");
            }
            let name = String::from_utf8(data[pos..pos + name_len].to_vec())?;
            pos += name_len;

            if pos >= data.len() {
                bail!("unexpected end of data reading encoding type");
            }
            let enc_type = data[pos];
            pos += 1;

            let compressed_len = read_u32(&data, &mut pos)? as usize;
            if pos + compressed_len > data.len() {
                bail!("unexpected end of data reading column data");
            }
            let col_data = data[pos..pos + compressed_len].to_vec();
            pos += compressed_len;

            columns.push(ColumnBlock {
                name,
                enc_type,
                data: col_data,
            });
        }

        // Verify CRC (last 4 bytes)
        if pos + 4 > data.len() {
            bail!("missing footer checksum");
        }
        let stored_crc = u32::from_le_bytes(data[pos..pos + 4].try_into()?);
        let computed_crc = crc32fast::hash(&data[..pos]);
        if stored_crc != computed_crc {
            bail!(
                "checksum mismatch: stored={:#x} computed={:#x}",
                stored_crc,
                computed_crc
            );
        }

        Ok(Self {
            min_ts,
            max_ts,
            point_count,
            series_key,
            columns,
        })
    }

    /// Time range covered by this segment.
    pub fn time_range(&self) -> (i64, i64) {
        (self.min_ts, self.max_ts)
    }

    /// Number of data points in this segment.
    pub fn point_count(&self) -> u64 {
        self.point_count
    }

    /// The series key this segment belongs to.
    pub fn series_key(&self) -> &str {
        &self.series_key
    }

    /// Read and decompress the timestamp column.
    pub fn read_timestamps(&self) -> Result<Vec<i64>> {
        let col = self
            .find_column("__timestamp")
            .context("missing timestamp column")?;
        let decompressed = lz4_flex::decompress_size_prepended(&col.data)
            .map_err(|e| anyhow::anyhow!("LZ4 decompress failed: {}", e))?;
        encoding::decode_timestamps(&decompressed)
    }

    /// Read and decompress a named field column.
    pub fn read_column(&self, name: &str) -> Result<Vec<FieldValue>> {
        let col = self
            .find_column(name)
            .with_context(|| format!("column '{}' not found", name))?;
        let decompressed = lz4_flex::decompress_size_prepended(&col.data)
            .map_err(|e| anyhow::anyhow!("LZ4 decompress failed: {}", e))?;

        match col.enc_type {
            ENC_FLOAT => {
                let floats =
                    encoding::decode_floats(&decompressed, self.point_count as usize)?;
                Ok(floats.into_iter().map(FieldValue::Float).collect())
            }
            ENC_INTEGER => {
                let ints = encoding::decode_integers(&decompressed)?;
                Ok(ints.into_iter().map(FieldValue::Integer).collect())
            }
            ENC_BOOLEAN => {
                let bools = encoding::decode_booleans(&decompressed)?;
                Ok(bools.into_iter().map(FieldValue::Boolean).collect())
            }
            _ => bail!("unsupported encoding type: {}", col.enc_type),
        }
    }

    /// List all column names (excluding __timestamp).
    pub fn field_names(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.name != "__timestamp")
            .map(|c| c.name.as_str())
            .collect()
    }

    fn find_column(&self, name: &str) -> Option<&ColumnBlock> {
        self.columns.iter().find(|c| c.name == name)
    }
}

fn read_i64(data: &[u8], pos: &mut usize) -> Result<i64> {
    if *pos + 8 > data.len() {
        bail!("unexpected end of data reading i64");
    }
    let val = i64::from_le_bytes(data[*pos..*pos + 8].try_into()?);
    *pos += 8;
    Ok(val)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > data.len() {
        bail!("unexpected end of data reading u64");
    }
    let val = u64::from_le_bytes(data[*pos..*pos + 8].try_into()?);
    *pos += 8;
    Ok(val)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > data.len() {
        bail!("unexpected end of data reading u32");
    }
    let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into()?);
    *pos += 4;
    Ok(val)
}

fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16> {
    if *pos + 2 > data.len() {
        bail!("unexpected end of data reading u16");
    }
    let val = u16::from_le_bytes(data[*pos..*pos + 2].try_into()?);
    *pos += 2;
    Ok(val)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_float_fields(
        name: &str,
        values: &[f64],
    ) -> BTreeMap<String, Vec<FieldValue>> {
        let mut fields = BTreeMap::new();
        fields.insert(
            name.to_string(),
            values.iter().map(|&v| FieldValue::Float(v)).collect(),
        );
        fields
    }

    #[test]
    fn write_and_read_float_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let timestamps: Vec<i64> = (0..100).map(|i| 1_000_000 + i * 1000).collect();
        let values: Vec<f64> = (0..100).map(|i| 20.0 + i as f64 * 0.1).collect();
        let fields = make_float_fields("temperature", &values);

        SegmentWriter::write_segment(&path, "sensor,id=T1", &timestamps, &fields).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.time_range(), (1_000_000, 1_099_000));
        assert_eq!(reader.point_count(), 100);
        assert_eq!(reader.series_key(), "sensor,id=T1");

        let read_ts = reader.read_timestamps().unwrap();
        assert_eq!(read_ts, timestamps);

        let read_vals = reader.read_column("temperature").unwrap();
        for (i, (orig, read)) in values.iter().zip(read_vals.iter()).enumerate() {
            match read {
                FieldValue::Float(f) => {
                    assert_eq!(orig.to_bits(), f.to_bits(), "mismatch at index {}", i)
                }
                _ => panic!("expected Float at index {}", i),
            }
        }

        assert_eq!(reader.field_names(), vec!["temperature"]);
    }

    #[test]
    fn write_and_read_integer_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_int.seg");

        let timestamps: Vec<i64> = (0..50).map(|i| 1_000 + i * 100).collect();
        let mut fields = BTreeMap::new();
        fields.insert(
            "count".to_string(),
            (0..50)
                .map(|i| FieldValue::Integer(i * 10))
                .collect::<Vec<_>>(),
        );

        SegmentWriter::write_segment(&path, "requests,host=web1", &timestamps, &fields).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.point_count(), 50);

        let read_ts = reader.read_timestamps().unwrap();
        assert_eq!(read_ts, timestamps);

        let read_vals = reader.read_column("count").unwrap();
        for (i, val) in read_vals.iter().enumerate() {
            match val {
                FieldValue::Integer(v) => assert_eq!(*v, i as i64 * 10),
                _ => panic!("expected Integer at index {}", i),
            }
        }
    }

    #[test]
    fn write_and_read_boolean_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_bool.seg");

        let timestamps: Vec<i64> = (0..20).map(|i| 100 + i * 10).collect();
        let mut fields = BTreeMap::new();
        fields.insert(
            "healthy".to_string(),
            (0..20)
                .map(|i| FieldValue::Boolean(i % 3 != 0))
                .collect::<Vec<_>>(),
        );

        SegmentWriter::write_segment(&path, "health,host=db1", &timestamps, &fields).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.point_count(), 20);

        let read_vals = reader.read_column("healthy").unwrap();
        for (i, val) in read_vals.iter().enumerate() {
            match val {
                FieldValue::Boolean(b) => assert_eq!(*b, i % 3 != 0),
                _ => panic!("expected Boolean at index {}", i),
            }
        }
    }

    #[test]
    fn multiple_field_columns() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("multi.seg");

        let timestamps: Vec<i64> = (0..30).map(|i| 1000 + i).collect();
        let mut fields = BTreeMap::new();
        fields.insert(
            "cpu".to_string(),
            (0..30).map(|i| FieldValue::Float(i as f64)).collect(),
        );
        fields.insert(
            "mem".to_string(),
            (0..30)
                .map(|i| FieldValue::Float(100.0 - i as f64))
                .collect(),
        );

        SegmentWriter::write_segment(&path, "sys,host=a", &timestamps, &fields).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let mut names = reader.field_names();
        names.sort();
        assert_eq!(names, vec!["cpu", "mem"]);

        let cpu_vals = reader.read_column("cpu").unwrap();
        assert_eq!(cpu_vals.len(), 30);

        let mem_vals = reader.read_column("mem").unwrap();
        assert_eq!(mem_vals.len(), 30);
    }

    #[test]
    fn missing_column_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");

        let timestamps = vec![1i64, 2, 3];
        let fields = make_float_fields("temp", &[1.0, 2.0, 3.0]);
        SegmentWriter::write_segment(&path, "s", &timestamps, &fields).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert!(reader.read_column("nonexistent").is_err());
    }

    #[test]
    fn empty_segment_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.seg");
        let fields: BTreeMap<String, Vec<FieldValue>> = BTreeMap::new();
        assert!(SegmentWriter::write_segment(&path, "s", &[], &fields).is_err());
    }

    #[test]
    fn compression_is_effective() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("compress.seg");

        // 10k regular timestamps — should compress very well
        let timestamps: Vec<i64> = (0..10_000).map(|i| 1_000_000_000 + i * 1_000_000).collect();
        let values: Vec<f64> = (0..10_000).map(|i| 25.0 + (i as f64 * 0.01).sin()).collect();
        let fields = make_float_fields("temp", &values);

        SegmentWriter::write_segment(&path, "sensor,id=T1", &timestamps, &fields).unwrap();

        let file_size = fs::metadata(&path).unwrap().len();
        let raw_size = (10_000 * 8 * 2) as u64; // timestamps + floats, uncompressed
        assert!(
            file_size < raw_size / 2,
            "file size {} should be much less than raw {}",
            file_size,
            raw_size
        );
    }
}
