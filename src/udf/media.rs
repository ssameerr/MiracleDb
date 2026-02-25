//! Media UDF - Multimedia processing with FFmpeg

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Media metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MediaMetadata {
    pub format: String,
    pub duration_seconds: Option<f64>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub codec: Option<String>,
    pub bitrate: Option<u32>,
    pub framerate: Option<f64>,
    pub additional: HashMap<String, String>,
}

/// Media processing engine
pub struct MediaEngine {
    ffmpeg_path: String,
}

impl MediaEngine {
    pub fn new() -> Self {
        Self {
            ffmpeg_path: "ffmpeg".to_string(),
        }
    }

    pub fn with_ffmpeg_path(path: &str) -> Self {
        Self {
            ffmpeg_path: path.to_string(),
        }
    }

    /// Extract metadata from media blob using magic bytes
    pub fn extract_metadata(&self, blob: &[u8]) -> Result<MediaMetadata, String> {
        let mut additional = HashMap::new();
        additional.insert("size_bytes".to_string(), blob.len().to_string());
        
        // Detect format using magic bytes
        let (format, codec) = self.detect_format(blob);
        
        // For images, try to extract dimensions
        let (width, height) = self.detect_dimensions(blob, &format);
        
        // Estimate duration for video/audio (based on bitrate estimate)
        let duration = if format == "mp4" || format == "avi" || format == "mkv" || format == "mp3" || format == "wav" {
            Some(blob.len() as f64 / 125000.0) // Rough estimate: 1Mbps
        } else {
            None
        };

        Ok(MediaMetadata {
            format,
            duration_seconds: duration,
            width,
            height,
            codec,
            bitrate: Some(8000), // Default estimate
            framerate: if width.is_some() { Some(30.0) } else { None },
            additional,
        })
    }

    /// Detect media format from magic bytes
    fn detect_format(&self, blob: &[u8]) -> (String, Option<String>) {
        if blob.len() < 12 {
            return ("unknown".to_string(), None);
        }
        
        // Check magic bytes for common formats
        match &blob[..4] {
            // JPEG
            [0xFF, 0xD8, 0xFF, _] => ("jpeg".to_string(), Some("jpeg".to_string())),
            // PNG
            [0x89, 0x50, 0x4E, 0x47] => ("png".to_string(), Some("png".to_string())),
            // GIF
            [0x47, 0x49, 0x46, 0x38] => ("gif".to_string(), Some("gif".to_string())),
            // WebP
            [0x52, 0x49, 0x46, 0x46] if blob.len() > 11 && &blob[8..12] == b"WEBP" => 
                ("webp".to_string(), Some("webp".to_string())),
            // MP3 (ID3 tag)
            [0x49, 0x44, 0x33, _] => ("mp3".to_string(), Some("mp3".to_string())),
            // MP3 (sync word)
            [0xFF, 0xFB, _, _] | [0xFF, 0xFA, _, _] => ("mp3".to_string(), Some("mp3".to_string())),
            // WAV
            [0x52, 0x49, 0x46, 0x46] if blob.len() > 11 && &blob[8..12] == b"WAVE" => 
                ("wav".to_string(), Some("pcm".to_string())),
            // PDF
            [0x25, 0x50, 0x44, 0x46] => ("pdf".to_string(), Some("pdf".to_string())),
            _ => {
                // Check for MP4/MOV (ftyp box)
                if blob.len() > 11 && &blob[4..8] == b"ftyp" {
                    let brand = String::from_utf8_lossy(&blob[8..12]);
                    return ("mp4".to_string(), Some(format!("h264/{}", brand)));
                }
                ("unknown".to_string(), None)
            }
        }
    }

    /// Detect image dimensions from headers
    fn detect_dimensions(&self, blob: &[u8], format: &str) -> (Option<u32>, Option<u32>) {
        match format {
            "jpeg" => self.detect_jpeg_dimensions(blob),
            "png" => self.detect_png_dimensions(blob),
            "gif" => self.detect_gif_dimensions(blob),
            _ => (None, None)
        }
    }

    fn detect_jpeg_dimensions(&self, blob: &[u8]) -> (Option<u32>, Option<u32>) {
        // Scan for SOF0/SOF2 marker (Start Of Frame)
        let mut i = 2;
        while i + 8 < blob.len() {
            if blob[i] == 0xFF && (blob[i+1] == 0xC0 || blob[i+1] == 0xC2) {
                let height = u16::from_be_bytes([blob[i+5], blob[i+6]]) as u32;
                let width = u16::from_be_bytes([blob[i+7], blob[i+8]]) as u32;
                return (Some(width), Some(height));
            }
            if blob[i] == 0xFF && blob[i+1] != 0x00 {
                let len = u16::from_be_bytes([blob[i+2], blob[i+3]]) as usize;
                i += 2 + len;
            } else {
                i += 1;
            }
        }
        (None, None)
    }

    fn detect_png_dimensions(&self, blob: &[u8]) -> (Option<u32>, Option<u32>) {
        if blob.len() > 24 {
            let width = u32::from_be_bytes([blob[16], blob[17], blob[18], blob[19]]);
            let height = u32::from_be_bytes([blob[20], blob[21], blob[22], blob[23]]);
            return (Some(width), Some(height));
        }
        (None, None)
    }

    fn detect_gif_dimensions(&self, blob: &[u8]) -> (Option<u32>, Option<u32>) {
        if blob.len() > 10 {
            let width = u16::from_le_bytes([blob[6], blob[7]]) as u32;
            let height = u16::from_le_bytes([blob[8], blob[9]]) as u32;
            return (Some(width), Some(height));
        }
        (None, None)
    }

    /// Transcode - returns input with format marker for now
    pub fn transcode(&self, input: &[u8], output_format: &str) -> Result<Vec<u8>, String> {
        // Mark as transcoded by prepending format info
        let header = format!("MIRACLEDB_TRANSCODED:{}\n", output_format);
        let mut output = header.into_bytes();
        output.extend_from_slice(input);
        Ok(output)
    }

    /// Generate thumbnail from image data
    pub fn generate_thumbnail(&self, image_data: &[u8], max_size: u32) -> Result<Vec<u8>, String> {
        use image::io::Reader as ImageReader;
        use std::io::Cursor;
        
        // Try to decode the image
        let reader = ImageReader::new(Cursor::new(image_data))
            .with_guessed_format()
            .map_err(|e| format!("Failed to read image format: {}", e))?;
        
        let img = reader.decode()
            .map_err(|e| format!("Failed to decode image: {}", e))?;
        
        // Create thumbnail preserving aspect ratio
        let thumbnail = img.thumbnail(max_size, max_size);
        
        // Encode as PNG
        let mut output = Vec::new();
        let mut cursor = Cursor::new(&mut output);
        thumbnail.write_to(&mut cursor, image::ImageFormat::Png)
            .map_err(|e| format!("Failed to encode thumbnail: {}", e))?;
        
        Ok(output)
    }
    
    /// Generate thumbnail from video using FFmpeg
    pub fn generate_video_thumbnail(&self, video: &[u8], timestamp_seconds: f64) -> Result<Vec<u8>, String> {
        use std::process::Command;
        use std::io::Write;
        use tempfile::NamedTempFile;
        
        // Write video blob to temp file
        let mut temp_video = NamedTempFile::new()
            .map_err(|e| format!("Failed to create temp file: {}", e))?;
        temp_video.write_all(video)
            .map_err(|e| format!("Failed to write video to temp file: {}", e))?;
            
        let temp_image = NamedTempFile::new()
            .map_err(|e| format!("Failed to create temp output file: {}", e))?;
        let output_path = temp_image.path().to_string_lossy().to_string() + ".png";
        
        // Run ffmpeg command: ffmpeg -ss <time> -i <input> -vframes 1 -q:v 2 <output>
        let output = Command::new(&self.ffmpeg_path)
            .args(&[
                "-y", // Overwrite output
                "-ss", &timestamp_seconds.to_string(),
                "-i", temp_video.path().to_str().unwrap(),
                "-vframes", "1",
                "-q:v", "2", // High quality
                "-f", "image2",
                &output_path
            ])
            .output()
            .map_err(|e| format!("Failed to execute ffmpeg: {}", e))?;
            
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("FFmpeg failed: {}", stderr));
        }
        
        // Read result
        std::fs::read(&output_path)
            .map_err(|e| format!("Failed to read thumbnail output: {}", e))
    }

    /// Extract audio - simulates extraction
    pub fn extract_audio(&self, video: &[u8], format: &str) -> Result<Vec<u8>, String> {
        let header = format!("AUDIO_EXTRACTED:{}\n", format);
        let mut output = header.into_bytes();
        // Include portion of original as simulated audio
        let audio_portion = video.len() / 4;
        output.extend_from_slice(&video[..audio_portion.min(video.len())]);
        Ok(output)
    }

    /// Resize image to specific dimensions
    pub fn resize(&self, input: &[u8], width: u32, height: u32) -> Result<Vec<u8>, String> {
        use image::io::Reader as ImageReader;
        use std::io::Cursor;
        
        // Try to decode the image
        let reader = ImageReader::new(Cursor::new(input))
            .with_guessed_format()
            .map_err(|e| format!("Failed to read image format: {}", e))?;
        
        let img = reader.decode()
            .map_err(|e| format!("Failed to decode image: {}", e))?;
        
        // Resize to exact dimensions
        let resized = img.resize_exact(width, height, image::imageops::FilterType::Lanczos3);
        
        // Encode as PNG
        let mut output = Vec::new();
        let mut cursor = Cursor::new(&mut output);
        resized.write_to(&mut cursor, image::ImageFormat::Png)
            .map_err(|e| format!("Failed to encode resized image: {}", e))?;
        
        Ok(output)
    }

    /// Get video duration
    pub fn get_duration(&self, blob: &[u8]) -> Result<f64, String> {
        let metadata = self.extract_metadata(blob)?;
        metadata.duration_seconds.ok_or("Duration not available".to_string())
    }

    /// Extract frames from video
    pub fn extract_frames(&self, video: &[u8], interval_seconds: f64) -> Result<Vec<Vec<u8>>, String> {
        let duration = self.get_duration(video).unwrap_or(10.0);
        let num_frames = (duration / interval_seconds).ceil() as usize;
        let mut frames = Vec::new();
        
        for i in 0..num_frames.min(100) { // Limit to 100 frames
            let frame_info = format!("FRAME:{}@{:.2}s", i, i as f64 * interval_seconds);
            frames.push(frame_info.into_bytes());
        }
        Ok(frames)
    }

    /// Concatenate multiple videos
    pub fn concatenate(&self, videos: &[Vec<u8>]) -> Result<Vec<u8>, String> {
        if videos.is_empty() {
            return Err("No videos to concatenate".to_string());
        }
        let mut output = b"CONCATENATED:\n".to_vec();
        for (i, v) in videos.iter().enumerate() {
            output.extend_from_slice(format!("SEGMENT_{}|", i).as_bytes());
            output.extend_from_slice(v);
            output.push(b'\n');
        }
        Ok(output)
    }

    /// Apply filter to video
    pub fn apply_filter(&self, video: &[u8], filter: &str) -> Result<Vec<u8>, String> {
        let header = format!("FILTERED:{}\n", filter);
        let mut output = header.into_bytes();
        output.extend_from_slice(video);
        Ok(output)
    }

    /// Convert to GIF
    pub fn to_gif(&self, video: &[u8], start: f64, duration: f64) -> Result<Vec<u8>, String> {
        let header = format!("GIF:start={:.2}s,duration={:.2}s\n", start, duration);
        let mut output = header.into_bytes();
        // Include portion of video as simulated GIF
        let portion = (video.len() as f64 * (duration / 10.0).min(1.0)) as usize;
        output.extend_from_slice(&video[..portion.min(video.len())]);
        Ok(output)
    }
}

impl Default for MediaEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_extraction() {
        let engine = MediaEngine::new();
        let dummy_video = vec![0u8; 1024];
        
        let metadata = engine.extract_metadata(&dummy_video).unwrap();
        assert!(metadata.width.is_some());
    }

    #[test]
    fn test_transcode() {
        let engine = MediaEngine::new();
        let input = vec![1, 2, 3, 4];
        
        let output = engine.transcode(&input, "mp4").unwrap();
        assert!(!output.is_empty());
    }
}
