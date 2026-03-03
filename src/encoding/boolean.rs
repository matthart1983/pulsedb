//! Bit-packing codec for boolean values.
//!
//! Booleans are packed 8 per byte, with the count stored as a u32 LE prefix
//! so the decoder knows how many valid bits are in the final byte.

use anyhow::{bail, Result};

/// Encodes a slice of booleans as bit-packed bytes.
///
/// Layout:
///   [count: 4 bytes LE u32] [packed bits, 8 booleans per byte, MSB first]
///
/// The final byte is zero-padded on the right if `count` is not a multiple of 8.
pub fn encode_booleans(values: &[bool]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Store count as u32 LE
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

    let mut current_byte: u8 = 0;
    let mut bit_count: u8 = 0;

    for &val in values {
        current_byte = (current_byte << 1) | (val as u8);
        bit_count += 1;
        if bit_count == 8 {
            buf.push(current_byte);
            current_byte = 0;
            bit_count = 0;
        }
    }

    // Flush remaining bits, left-aligned in the final byte
    if bit_count > 0 {
        current_byte <<= 8 - bit_count;
        buf.push(current_byte);
    }

    buf
}

/// Decodes booleans that were encoded with `encode_booleans`.
pub fn decode_booleans(data: &[u8]) -> Result<Vec<bool>> {
    if data.len() < 4 {
        bail!("data too short for boolean header");
    }

    let count = u32::from_le_bytes(data[0..4].try_into()?) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let packed = &data[4..];
    let expected_bytes = (count + 7) / 8;
    if packed.len() < expected_bytes {
        bail!(
            "not enough packed bytes: need {} but got {}",
            expected_bytes,
            packed.len()
        );
    }

    let mut values = Vec::with_capacity(count);
    let mut remaining = count;

    for &byte in &packed[..expected_bytes] {
        let bits_in_byte = remaining.min(8);
        for i in 0..bits_in_byte {
            let bit = (byte >> (7 - i)) & 1 == 1;
            values.push(bit);
        }
        remaining -= bits_in_byte;
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_true() {
        let values = vec![true; 100];
        let encoded = encode_booleans(&values);
        let decoded = decode_booleans(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_all_false() {
        let values = vec![false; 100];
        let encoded = encode_booleans(&values);
        let decoded = decode_booleans(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_alternating() {
        let values: Vec<bool> = (0..100).map(|i| i % 2 == 0).collect();
        let encoded = encode_booleans(&values);
        let decoded = decode_booleans(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_non_multiple_of_8() {
        // 13 values — tests partial final byte handling
        let values = vec![
            true, false, true, true, false, true, false, false, true, true, false, true, true,
        ];
        let encoded = encode_booleans(&values);
        let decoded = decode_booleans(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_exactly_8() {
        let values = vec![true, false, true, false, true, false, true, false];
        let encoded = encode_booleans(&values);
        let decoded = decode_booleans(&encoded).unwrap();
        assert_eq!(decoded, values);
        // 4 bytes header + 1 byte packed = 5 bytes total
        assert_eq!(encoded.len(), 5);
    }

    #[test]
    fn test_single() {
        let values = vec![true];
        let encoded = encode_booleans(&values);
        let decoded = decode_booleans(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_empty() {
        let encoded = encode_booleans(&[]);
        let decoded = decode_booleans(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_compact_size() {
        // 1000 booleans should pack into 4 + 125 = 129 bytes
        let values = vec![true; 1000];
        let encoded = encode_booleans(&values);
        assert_eq!(encoded.len(), 4 + 125);
    }
}
