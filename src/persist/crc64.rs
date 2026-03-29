//! CRC64 using the JONES polynomial, matching Redis's `crc64.c`.
//!
//! REDIS: Redis uses CRC64 (not CRC32) for RDB checksums because it provides
//! stronger error detection for multi-gigabyte files. The JONES polynomial
//! (0xad93d23594c935a9) was chosen for its good error-detection properties
//! across a wide range of message lengths.
//!
//! Parameters: reflected in/out, init 0x0, xor-out 0x0.

// Lookup table generated from the reflected polynomial.
// Redis uses poly 0xad93d23594c935a9 in MSB-first mode, then reflects the
// result. The LSB-first (reflected) equivalent is 0x95ac9329ac4bc9b5.
const TABLE: [u64; 256] = {
    const POLY: u64 = 0x95ac9329ac4bc9b5;
    let mut table = [0u64; 256];
    let mut i: usize = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Compute CRC64 over `data`, starting from `crc`.
///
/// To checksum a single buffer: `crc64(0, data)`.
/// To accumulate across buffers: pass the previous result as `crc`.
///
/// REDIS: CRC-64/REDIS parameters from the CRC RevEng catalogue:
/// init=0, refin=true, refout=true, xorout=0. No pre/post inversion.
pub(crate) fn crc64(crc: u64, data: &[u8]) -> u64 {
    data.iter().fold(crc, |acc, &byte| {
        TABLE[((acc ^ byte as u64) & 0xff) as usize] ^ (acc >> 8)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known test vector from Redis's CRC64 implementation.
    /// See redis/src/crc64.c `crc64Test()`.
    #[test]
    fn known_test_vector() {
        let result = crc64(0, b"123456789");
        assert_eq!(result, 0xe9c6d914c4b8d9ca);
    }

    #[test]
    fn empty_input_returns_initial() {
        assert_eq!(crc64(0, b""), 0);
    }

    #[test]
    fn incremental_matches_single_pass() {
        let data = b"Hello, CRC64!";
        let single = crc64(0, data);
        let incremental = crc64(crc64(0, &data[..5]), &data[5..]);
        assert_eq!(single, incremental);
    }
}
