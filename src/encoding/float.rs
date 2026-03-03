//! Gorilla XOR compression for floating-point values.
//!
//! Based on the Facebook Gorilla paper (Pelkonen et al., 2015). The key insight
//! is that consecutive float values in time-series data often share many bits.
//! XOR-ing consecutive values produces results with long runs of zeros, which
//! compress extremely well with a simple variable-length bit encoding.
//!
//! Encoding scheme for each value after the first:
//!   - XOR the value with the previous value
//!   - If XOR == 0: write a single 0 bit (values are identical)
//!   - If XOR != 0: write a 1 bit, then:
//!     - 6 bits: number of leading zeros (capped at 63)
//!     - 6 bits: length of meaningful bits (1-64, stored as length-1)
//!     - N bits: the meaningful (non-zero) bits

use anyhow::{bail, Result};

/// Writes individual bits into a byte buffer.
struct BitWriter {
    buf: Vec<u8>,
    current_byte: u8,
    bit_count: u8, // bits written into current_byte (0..8)
}

impl BitWriter {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            current_byte: 0,
            bit_count: 0,
        }
    }

    /// Writes a single bit (0 or 1).
    #[inline]
    fn write_bit(&mut self, bit: bool) {
        self.current_byte = (self.current_byte << 1) | (bit as u8);
        self.bit_count += 1;
        if self.bit_count == 8 {
            self.buf.push(self.current_byte);
            self.current_byte = 0;
            self.bit_count = 0;
        }
    }

    /// Writes `num_bits` bits from the least-significant end of `value`.
    #[inline]
    fn write_bits(&mut self, value: u64, num_bits: u8) {
        debug_assert!(num_bits <= 64);
        for i in (0..num_bits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    /// Flushes any remaining partial byte, padding with zero bits on the right.
    fn finish(mut self) -> Vec<u8> {
        if self.bit_count > 0 {
            // Left-align the remaining bits in the final byte
            self.current_byte <<= 8 - self.bit_count;
            self.buf.push(self.current_byte);
        }
        self.buf
    }
}

/// Reads individual bits from a byte buffer.
struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8, // next bit to read within data[byte_pos] (0..8, MSB first)
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    /// Reads a single bit. Returns an error if data is exhausted.
    #[inline]
    fn read_bit(&mut self) -> Result<bool> {
        if self.byte_pos >= self.data.len() {
            bail!("unexpected end of data reading bit");
        }
        let bit = (self.data[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.bit_pos = 0;
            self.byte_pos += 1;
        }
        Ok(bit)
    }

    /// Reads `num_bits` bits and returns them as a u64 (MSB first).
    #[inline]
    fn read_bits(&mut self, num_bits: u8) -> Result<u64> {
        debug_assert!(num_bits <= 64);
        let mut value: u64 = 0;
        for _ in 0..num_bits {
            value = (value << 1) | (self.read_bit()? as u64);
        }
        Ok(value)
    }
}

/// Encodes a slice of f64 values using Gorilla XOR compression.
///
/// Layout:
///   - 4 bytes LE: count of values
///   - 8 bytes: first value as raw f64 bits (if count > 0)
///   - Bit-packed XOR stream for remaining values
pub fn encode_floats(values: &[f64]) -> Vec<u8> {
    let mut result = Vec::new();

    // Store count as u32 LE
    result.extend_from_slice(&(values.len() as u32).to_le_bytes());

    if values.is_empty() {
        return result;
    }

    // Store first value as raw 64-bit LE
    result.extend_from_slice(&values[0].to_bits().to_le_bytes());

    if values.len() == 1 {
        return result;
    }

    let mut writer = BitWriter::new();
    let mut prev_bits = values[0].to_bits();

    for &val in &values[1..] {
        let curr_bits = val.to_bits();
        let xor = prev_bits ^ curr_bits;

        if xor == 0 {
            // Values are identical — store a single 0 bit
            writer.write_bit(false);
        } else {
            writer.write_bit(true);

            let leading = xor.leading_zeros().min(63) as u8;
            let trailing = xor.trailing_zeros() as u8;
            let meaningful_bits = 64 - leading - trailing;

            // 6 bits for leading zeros count
            writer.write_bits(leading as u64, 6);
            // 6 bits for meaningful bit length (store length - 1 since it's at least 1)
            writer.write_bits((meaningful_bits - 1) as u64, 6);
            // Write the meaningful bits
            writer.write_bits(xor >> trailing, meaningful_bits);
        }

        prev_bits = curr_bits;
    }

    result.extend_from_slice(&writer.finish());
    result
}

/// Decodes f64 values that were encoded with `encode_floats`.
///
/// `count` is provided for verification but the actual count is read from the
/// encoded header. If the counts don't match, an error is returned.
pub fn decode_floats(data: &[u8], count: usize) -> Result<Vec<f64>> {
    if data.len() < 4 {
        bail!("data too short for float header");
    }

    let stored_count = u32::from_le_bytes(data[0..4].try_into()?) as usize;
    if stored_count != count {
        bail!(
            "count mismatch: header says {} but caller expects {}",
            stored_count,
            count
        );
    }

    if count == 0 {
        return Ok(Vec::new());
    }

    if data.len() < 12 {
        bail!("data too short for first float value");
    }

    let mut values = Vec::with_capacity(count);

    // Read first value
    let first_bits = u64::from_le_bytes(data[4..12].try_into()?);
    values.push(f64::from_bits(first_bits));

    if count == 1 {
        return Ok(values);
    }

    let mut reader = BitReader::new(&data[12..]);
    let mut prev_bits = first_bits;

    for _ in 1..count {
        let is_nonzero = reader.read_bit()?;
        if !is_nonzero {
            // XOR is zero — same value as previous
            values.push(f64::from_bits(prev_bits));
        } else {
            let leading = reader.read_bits(6)? as u8;
            let meaningful_bits = reader.read_bits(6)? as u8 + 1;
            let trailing = 64 - leading - meaningful_bits;

            let meaningful = reader.read_bits(meaningful_bits)?;
            let xor = meaningful << trailing;
            let curr_bits = prev_bits ^ xor;
            values.push(f64::from_bits(curr_bits));
            prev_bits = curr_bits;
        }
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_values() {
        let values = vec![42.0; 100];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len()).unwrap();
        assert_eq!(decoded, values);

        // Constant values: 4 (count) + 8 (first) + ~12 bytes for 99 zero bits
        // Should be extremely compact
        assert!(
            encoded.len() < 30,
            "constant encoding too large: {}",
            encoded.len()
        );
    }

    #[test]
    fn test_slowly_changing() {
        // Simulates a temperature sensor with small fluctuations
        let values: Vec<f64> = (0..100).map(|i| 20.0 + (i as f64) * 0.01).collect();
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len()).unwrap();

        for (i, (&orig, &dec)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(orig.to_bits(), dec.to_bits(), "mismatch at index {}", i);
        }

        // Should compress to less than raw storage (100 * 8 = 800 bytes)
        let raw_size = values.len() * 8;
        assert!(
            encoded.len() < raw_size,
            "slowly changing: encoded {} vs raw {}",
            encoded.len(),
            raw_size
        );
    }

    #[test]
    fn test_random_values() {
        // Random-ish values — worst case, but should still roundtrip correctly
        let values: Vec<f64> = (0..50)
            .map(|i| ((i * 7 + 13) as f64).sin() * 1000.0)
            .collect();
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len()).unwrap();

        for (i, (&orig, &dec)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(orig.to_bits(), dec.to_bits(), "mismatch at index {}", i);
        }
    }

    #[test]
    fn test_empty() {
        let encoded = encode_floats(&[]);
        let decoded = decode_floats(&encoded, 0).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_single_value() {
        let values = vec![std::f64::consts::PI];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, 1).unwrap();
        assert_eq!(decoded[0].to_bits(), std::f64::consts::PI.to_bits());
    }

    #[test]
    fn test_special_floats() {
        let values = vec![0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY, 1.0];
        let encoded = encode_floats(&values);
        let decoded = decode_floats(&encoded, values.len()).unwrap();

        for (i, (&orig, &dec)) in values.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(orig.to_bits(), dec.to_bits(), "mismatch at index {}", i);
        }
    }

    #[test]
    fn test_count_mismatch_error() {
        let values = vec![1.0, 2.0, 3.0];
        let encoded = encode_floats(&values);
        assert!(decode_floats(&encoded, 5).is_err());
    }

    #[test]
    fn test_bit_writer_reader_roundtrip() {
        let mut writer = BitWriter::new();
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bits(0b110011, 6);
        writer.write_bits(255, 8);
        let data = writer.finish();

        let mut reader = BitReader::new(&data);
        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert_eq!(reader.read_bits(6).unwrap(), 0b110011);
        assert_eq!(reader.read_bits(8).unwrap(), 255);
    }
}
