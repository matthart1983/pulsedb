//! Delta-of-delta encoding for timestamps.
//!
//! Timestamps in time-series data are typically monotonically increasing with
//! roughly constant intervals. Delta-of-delta encoding exploits this by storing
//! the difference between consecutive deltas, which are usually very small
//! (often zero for regular intervals). Combined with zigzag encoding for signed
//! values and varint encoding for compactness, this achieves excellent
//! compression ratios for timestamp columns.

use anyhow::{bail, Result};

/// Encodes a signed integer using zigzag encoding so that small-magnitude
/// values (both positive and negative) map to small unsigned values.
///   0 => 0, -1 => 1, 1 => 2, -2 => 3, 2 => 4, ...
#[inline]
pub fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// Decodes a zigzag-encoded unsigned integer back to a signed integer.
#[inline]
pub fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

/// Encodes a u64 value as a variable-length integer (LEB128).
/// Smaller values use fewer bytes: values < 128 use 1 byte, < 16384 use 2, etc.
#[inline]
pub fn encode_varint(value: u64, buf: &mut Vec<u8>) {
    let mut v = value;
    loop {
        if v < 0x80 {
            buf.push(v as u8);
            break;
        }
        buf.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
}

/// Decodes a variable-length integer from `data` starting at `pos`.
/// Advances `pos` past the consumed bytes.
#[inline]
pub fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            bail!("unexpected end of data while decoding varint");
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint too long");
        }
    }
}

/// Accumulates timestamps and encodes them using delta-of-delta compression.
///
/// The encoding format is:
///   - varint: number of timestamps
///   - raw i64 (8 bytes LE): first timestamp (if any)
///   - For each subsequent timestamp: zigzag + varint encoded delta-of-delta
pub struct TimestampEncoder {
    timestamps: Vec<i64>,
}

impl TimestampEncoder {
    pub fn new() -> Self {
        Self {
            timestamps: Vec::new(),
        }
    }

    /// Adds a timestamp to the encoder.
    pub fn push(&mut self, ts: i64) {
        self.timestamps.push(ts);
    }

    /// Encodes all accumulated timestamps into a byte vector.
    pub fn encode(&self) -> Vec<u8> {
        encode_timestamps(&self.timestamps)
    }
}

impl Default for TimestampEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Encodes a slice of timestamps using delta-of-delta compression.
///
/// Layout:
///   [count: varint] [first_ts: 8 bytes LE] [delta-of-deltas as zigzag varints...]
///
/// The first delta (between ts[1] and ts[0]) is also stored as a zigzag varint.
/// Subsequent values store the delta-of-delta (current_delta - previous_delta).
pub fn encode_timestamps(timestamps: &[i64]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Store count
    encode_varint(timestamps.len() as u64, &mut buf);

    if timestamps.is_empty() {
        return buf;
    }

    // Store first timestamp as raw LE bytes
    buf.extend_from_slice(&timestamps[0].to_le_bytes());

    if timestamps.len() == 1 {
        return buf;
    }

    // First delta (ts[1] - ts[0]), stored as zigzag varint
    let mut prev_delta = timestamps[1] - timestamps[0];
    encode_varint(zigzag_encode(prev_delta), &mut buf);

    // Subsequent values: store delta-of-delta
    for i in 2..timestamps.len() {
        let delta = timestamps[i] - timestamps[i - 1];
        let delta_of_delta = delta - prev_delta;
        encode_varint(zigzag_encode(delta_of_delta), &mut buf);
        prev_delta = delta;
    }

    buf
}

/// Decodes timestamps that were encoded with `encode_timestamps`.
pub fn decode_timestamps(data: &[u8]) -> Result<Vec<i64>> {
    let mut pos = 0;

    let count = decode_varint(data, &mut pos)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut timestamps = Vec::with_capacity(count);

    // Read first timestamp (raw LE i64)
    if pos + 8 > data.len() {
        bail!("unexpected end of data reading first timestamp");
    }
    let first = i64::from_le_bytes(data[pos..pos + 8].try_into()?);
    pos += 8;
    timestamps.push(first);

    if count == 1 {
        return Ok(timestamps);
    }

    // Read first delta
    let zz = decode_varint(data, &mut pos)?;
    let mut prev_delta = zigzag_decode(zz);
    timestamps.push(first + prev_delta);

    // Read remaining delta-of-deltas
    for _ in 2..count {
        let zz = decode_varint(data, &mut pos)?;
        let delta_of_delta = zigzag_decode(zz);
        let delta = prev_delta + delta_of_delta;
        let ts = *timestamps.last().unwrap() + delta;
        timestamps.push(ts);
        prev_delta = delta;
    }

    Ok(timestamps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag_roundtrip() {
        for &v in &[0i64, 1, -1, 2, -2, 100, -100, i64::MAX, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    #[test]
    fn test_varint_roundtrip() {
        for &v in &[0u64, 1, 127, 128, 16383, 16384, u64::MAX / 2, u64::MAX] {
            let mut buf = Vec::new();
            encode_varint(v, &mut buf);
            let mut pos = 0;
            let decoded = decode_varint(&buf, &mut pos).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn test_monotonically_increasing_timestamps() {
        // Simulates regular 10-second interval timestamps
        let base = 1_700_000_000_000i64; // epoch millis
        let timestamps: Vec<i64> = (0..1000).map(|i| base + i * 10_000).collect();

        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(decoded, timestamps);

        // Delta-of-delta for constant intervals should be very compact:
        // count varint + 8 bytes first ts + 1 varint first delta + 998 × 1 byte (zero dod)
        // Much smaller than 1000 × 8 = 8000 bytes raw
        assert!(encoded.len() < 1100, "encoded size: {}", encoded.len());
    }

    #[test]
    fn test_irregular_timestamps() {
        let timestamps = vec![
            1_000_000i64,
            1_000_010,
            1_000_015, // gap changes: 10, 5
            1_000_100, // gap 85
            1_000_101, // gap 1
            1_000_101, // gap 0 (duplicate)
            1_000_200, // gap 99
        ];

        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_empty_timestamps() {
        let encoded = encode_timestamps(&[]);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_single_timestamp() {
        let timestamps = vec![42i64];
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_two_timestamps() {
        let timestamps = vec![100i64, 200];
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_decreasing_timestamps() {
        let timestamps = vec![1000i64, 900, 800, 700, 600];
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(decoded, timestamps);
    }

    #[test]
    fn test_encoder_struct() {
        let mut encoder = TimestampEncoder::new();
        encoder.push(100);
        encoder.push(200);
        encoder.push(300);

        let encoded = encoder.encode();
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(decoded, vec![100, 200, 300]);
    }
}
