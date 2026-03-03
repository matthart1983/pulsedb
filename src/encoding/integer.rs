//! Delta + zigzag + varint encoding for integer values.
//!
//! Integer time-series data (counters, gauges) often changes by small amounts
//! between consecutive points. This codec delta-encodes the values, applies
//! zigzag encoding to handle signed deltas, then varint-encodes for compactness.

use anyhow::{bail, Result};

use super::timestamp::{decode_varint, encode_varint, zigzag_decode, zigzag_encode};

/// Encodes a slice of i64 values using delta + zigzag + varint compression.
///
/// Layout:
///   [count: varint] [first_value: 8 bytes LE] [deltas as zigzag varints...]
pub fn encode_integers(values: &[i64]) -> Vec<u8> {
    let mut buf = Vec::new();

    encode_varint(values.len() as u64, &mut buf);

    if values.is_empty() {
        return buf;
    }

    // Store first value as raw LE bytes
    buf.extend_from_slice(&values[0].to_le_bytes());

    // Store deltas as zigzag varints
    for i in 1..values.len() {
        let delta = values[i].wrapping_sub(values[i - 1]);
        encode_varint(zigzag_encode(delta), &mut buf);
    }

    buf
}

/// Decodes integer values that were encoded with `encode_integers`.
pub fn decode_integers(data: &[u8]) -> Result<Vec<i64>> {
    let mut pos = 0;

    let count = decode_varint(data, &mut pos)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    if pos + 8 > data.len() {
        bail!("unexpected end of data reading first integer value");
    }

    let mut values = Vec::with_capacity(count);

    // Read first value
    let first = i64::from_le_bytes(data[pos..pos + 8].try_into()?);
    pos += 8;
    values.push(first);

    // Read deltas
    for _ in 1..count {
        let zz = decode_varint(data, &mut pos)?;
        let delta = zigzag_decode(zz);
        let val = values.last().unwrap().wrapping_add(delta);
        values.push(val);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonically_increasing() {
        let values: Vec<i64> = (0..500).collect();
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded).unwrap();
        assert_eq!(decoded, values);

        // Each delta is 1, zigzag(1)=2, varint(2)=1 byte
        // So ~1 byte per value + overhead — much smaller than 500*8=4000 bytes
        assert!(encoded.len() < 600, "encoded size: {}", encoded.len());
    }

    #[test]
    fn test_constant_values() {
        let values = vec![42i64; 200];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded).unwrap();
        assert_eq!(decoded, values);

        // All deltas are 0, zigzag(0)=0, varint(0)=1 byte each
        assert!(encoded.len() < 220, "encoded size: {}", encoded.len());
    }

    #[test]
    fn test_negative_deltas() {
        let values = vec![1000i64, 900, 800, 700, 600, 500];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_mixed_deltas() {
        let values = vec![10i64, 20, 15, 25, 0, -100, 50];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_large_values() {
        let values = vec![i64::MIN, 0, i64::MAX, -1, 1];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_empty() {
        let encoded = encode_integers(&[]);
        let decoded = decode_integers(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_single_value() {
        let values = vec![999i64];
        let encoded = encode_integers(&values);
        let decoded = decode_integers(&encoded).unwrap();
        assert_eq!(decoded, values);
    }
}
