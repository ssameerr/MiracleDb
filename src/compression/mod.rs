//! Compression Module - Data compression utilities

use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

/// Compression algorithm
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Zstd,
    Lz4,
    Snappy,
    Gzip,
}

/// Compression level
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum CompressionLevel {
    Fast,
    Default,
    Best,
    Custom(i32),
}

/// Compressor
pub struct Compressor {
    algorithm: CompressionAlgorithm,
    level: CompressionLevel,
}

impl Compressor {
    pub fn new(algorithm: CompressionAlgorithm, level: CompressionLevel) -> Self {
        Self { algorithm, level }
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Zstd => {
                let level = match self.level {
                    CompressionLevel::Fast => 1,
                    CompressionLevel::Default => 3,
                    CompressionLevel::Best => 19,
                    CompressionLevel::Custom(l) => l,
                };
                zstd::encode_all(data, level)
                    .map_err(|e| format!("Zstd compression failed: {}", e))
            }
            CompressionAlgorithm::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(match self.level {
                        CompressionLevel::Fast => 1,
                        CompressionLevel::Default => 4,
                        CompressionLevel::Best => 16,
                        CompressionLevel::Custom(l) => l as u32,
                    })
                    .build(Vec::new())
                    .map_err(|e| format!("LZ4 encoder creation failed: {}", e))?;
                
                encoder.write_all(data)
                    .map_err(|e| format!("LZ4 write failed: {}", e))?;
                
                let (output, result) = encoder.finish();
                result.map_err(|e| format!("LZ4 finish failed: {}", e))?;
                Ok(output)
            }
            CompressionAlgorithm::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                encoder.compress_vec(data)
                    .map_err(|e| format!("Snappy compression failed: {}", e))
            }
            CompressionAlgorithm::Gzip => {
                use flate2::Compression;
                use flate2::write::GzEncoder;
                
                let level = match self.level {
                    CompressionLevel::Fast => Compression::fast(),
                    CompressionLevel::Default => Compression::default(),
                    CompressionLevel::Best => Compression::best(),
                    CompressionLevel::Custom(l) => Compression::new(l as u32),
                };
                
                let mut encoder = GzEncoder::new(Vec::new(), level);
                encoder.write_all(data)
                    .map_err(|e| format!("Gzip write failed: {}", e))?;
                encoder.finish()
                    .map_err(|e| format!("Gzip finish failed: {}", e))
            }
        }
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Zstd => {
                zstd::decode_all(data)
                    .map_err(|e| format!("Zstd decompression failed: {}", e))
            }
            CompressionAlgorithm::Lz4 => {
                let mut decoder = lz4::Decoder::new(data)
                    .map_err(|e| format!("LZ4 decoder creation failed: {}", e))?;
                let mut output = Vec::new();
                decoder.read_to_end(&mut output)
                    .map_err(|e| format!("LZ4 read failed: {}", e))?;
                Ok(output)
            }
            CompressionAlgorithm::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                decoder.decompress_vec(data)
                    .map_err(|e| format!("Snappy decompression failed: {}", e))
            }
            CompressionAlgorithm::Gzip => {
                use flate2::read::GzDecoder;
                
                let mut decoder = GzDecoder::new(data);
                let mut output = Vec::new();
                decoder.read_to_end(&mut output)
                    .map_err(|e| format!("Gzip decompression failed: {}", e))?;
                Ok(output)
            }
        }
    }

    /// Get compression ratio
    pub fn ratio(&self, original: usize, compressed: usize) -> f64 {
        if original == 0 {
            1.0
        } else {
            compressed as f64 / original as f64
        }
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new(CompressionAlgorithm::Zstd, CompressionLevel::Default)
    }
}

/// Auto-select best algorithm based on data
pub fn auto_select_algorithm(data: &[u8]) -> CompressionAlgorithm {
    // For small data, overhead isn't worth it
    if data.len() < 100 {
        return CompressionAlgorithm::None;
    }

    // For very large data, use faster algorithm
    if data.len() > 10_000_000 {
        return CompressionAlgorithm::Lz4;
    }

    // Default to zstd for best ratio
    CompressionAlgorithm::Zstd
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_roundtrip() {
        let compressor = Compressor::new(CompressionAlgorithm::Zstd, CompressionLevel::Default);
        let data = b"Hello, World! This is test data for compression.".repeat(100);
        
        let compressed = compressor.compress(&data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_slice());
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_snappy_roundtrip() {
        let compressor = Compressor::new(CompressionAlgorithm::Snappy, CompressionLevel::Default);
        let data = b"Snappy test data".repeat(50);
        
        let compressed = compressor.compress(&data).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        
        assert_eq!(data, decompressed.as_slice());
    }
}

// ---------------------------------------------------------------------------
// Lightweight column-store codecs (no external dependencies)
// ---------------------------------------------------------------------------

/// RLE (Run-Length Encoding) for repeated byte values
pub fn rle_encode(data: &[u8]) -> Vec<u8> {
    if data.is_empty() { return Vec::new(); }

    let mut result = Vec::new();
    let mut count = 1u8;

    for i in 1..data.len() {
        if data[i] == data[i-1] && count < 255 {
            count += 1;
        } else {
            result.push(count);
            result.push(data[i-1]);
            count = 1;
        }
    }
    result.push(count);
    result.push(*data.last().unwrap());
    result
}

pub fn rle_decode(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    let mut i = 0;
    while i + 1 < data.len() {
        let count = data[i] as usize;
        let value = data[i+1];
        for _ in 0..count {
            result.push(value);
        }
        i += 2;
    }
    result
}

/// Delta encoding for time-series integer data (e.g. Unix timestamps)
pub fn delta_encode_i64(data: &[i64]) -> Vec<i64> {
    if data.is_empty() { return Vec::new(); }
    let mut result = vec![data[0]];
    for i in 1..data.len() {
        result.push(data[i] - data[i-1]);
    }
    result
}

pub fn delta_decode_i64(data: &[i64]) -> Vec<i64> {
    if data.is_empty() { return Vec::new(); }
    let mut result = vec![data[0]];
    for i in 1..data.len() {
        result.push(result[i-1] + data[i]);
    }
    result
}

/// Dictionary encoding for low-cardinality string columns
pub struct DictionaryEncoder {
    dict: std::collections::HashMap<String, u32>,
    reverse: Vec<String>,
}

impl DictionaryEncoder {
    pub fn new() -> Self {
        Self {
            dict: std::collections::HashMap::new(),
            reverse: Vec::new(),
        }
    }

    pub fn encode(&mut self, value: &str) -> u32 {
        if let Some(&code) = self.dict.get(value) {
            code
        } else {
            let code = self.reverse.len() as u32;
            self.dict.insert(value.to_string(), code);
            self.reverse.push(value.to_string());
            code
        }
    }

    pub fn decode(&self, code: u32) -> Option<&str> {
        self.reverse.get(code as usize).map(|s| s.as_str())
    }

    pub fn dict_size(&self) -> usize {
        self.reverse.len()
    }
}

impl Default for DictionaryEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple bit packing for small unsigned integer values
pub fn bit_pack(values: &[u32], bits_per_value: u8) -> Vec<u8> {
    assert!(bits_per_value <= 32 && bits_per_value > 0);
    let total_bits = values.len() * bits_per_value as usize;
    let total_bytes = (total_bits + 7) / 8;
    let mut result = vec![0u8; total_bytes];
    let mask: u32 = if bits_per_value == 32 { u32::MAX } else { (1u32 << bits_per_value) - 1 };

    for (i, &val) in values.iter().enumerate() {
        let v = val & mask;
        let bit_pos = i * bits_per_value as usize;
        let byte_pos = bit_pos / 8;
        let bit_offset = bit_pos % 8;

        let bytes_needed = (bit_offset + bits_per_value as usize + 7) / 8;
        let mut v64 = (v as u64) << bit_offset;
        for j in 0..bytes_needed.min(total_bytes.saturating_sub(byte_pos)) {
            result[byte_pos + j] |= (v64 & 0xFF) as u8;
            v64 >>= 8;
        }
    }
    result
}

/// Compression statistics helper
#[derive(Debug)]
pub struct CompressionStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub ratio: f64,
}

impl CompressionStats {
    pub fn compute(original: usize, compressed: usize) -> Self {
        CompressionStats {
            original_size: original,
            compressed_size: compressed,
            ratio: if original > 0 { compressed as f64 / original as f64 } else { 1.0 },
        }
    }

    pub fn savings_percent(&self) -> f64 {
        (1.0 - self.ratio) * 100.0
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn test_rle() {
        let data = vec![1u8, 1, 1, 2, 2, 3, 3, 3, 3];
        let encoded = rle_encode(&data);
        let decoded = rle_decode(&encoded);
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_delta_encoding() {
        let ts = vec![1000i64, 1001, 1002, 1003, 1010, 1011];
        let encoded = delta_encode_i64(&ts);
        let decoded = delta_decode_i64(&encoded);
        assert_eq!(decoded, ts);
    }

    #[test]
    fn test_dictionary_encoding() {
        let mut enc = DictionaryEncoder::new();
        let c1 = enc.encode("apple");
        let c2 = enc.encode("banana");
        let c3 = enc.encode("apple"); // duplicate
        assert_eq!(c1, c3);
        assert_ne!(c1, c2);
        assert_eq!(enc.decode(c1), Some("apple"));
        assert_eq!(enc.dict_size(), 2);
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::compute(1000, 400);
        assert!((stats.ratio - 0.4).abs() < 0.001);
        assert!((stats.savings_percent() - 60.0).abs() < 0.001);
    }
}
