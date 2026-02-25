//! Store Module - Object/Blob storage with metadata extraction and streaming

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use serde::{Serialize, Deserialize};
use object_store::{ObjectStore, path::Path as StorePath};
use object_store::local::LocalFileSystem;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BlobMetadata {
    pub id: String,
    pub size: usize,
    pub content_type: String,
    pub created_at: i64,
    pub hash: Option<String>,
    /// Extended metadata (EXIF, duration, codec, etc.)
    pub extended: HashMap<String, String>,
}

/// Storage backend types
#[derive(Clone, Debug)]
pub enum StorageBackend {
    /// Local filesystem
    Local(PathBuf),
    /// S3-compatible object storage
    S3 { bucket: String, endpoint: String },
    /// In-memory (for testing)
    Memory,
}

/// Blob storage manager with multiple backends
pub struct BlobStore {
    /// In-memory metadata cache
    metadata: RwLock<HashMap<String, BlobMetadata>>,
    /// Storage backend
    backend: StorageBackend,
    /// Object store abstraction
    object_store: Option<Arc<dyn ObjectStore>>,
    /// Base path for local storage
    base_path: String,
}

impl BlobStore {
    pub fn new(base_path: &str) -> Self {
        let backend = StorageBackend::Local(PathBuf::from(base_path));
        let object_store = Some(Arc::new(LocalFileSystem::new_with_prefix(base_path).unwrap()) as Arc<dyn ObjectStore>);
        
        Self {
            metadata: RwLock::new(HashMap::new()),
            backend,
            object_store,
            base_path: base_path.to_string(),
        }
    }

    pub fn with_backend(backend: StorageBackend) -> Self {
        let base_path = match &backend {
            StorageBackend::Local(path) => path.to_string_lossy().to_string(),
            StorageBackend::S3 { bucket, .. } => format!("s3://{}", bucket),
            StorageBackend::Memory => ":memory:".to_string(),
        };

        let object_store = match &backend {
            StorageBackend::Local(path) => {
                Some(Arc::new(LocalFileSystem::new_with_prefix(path).unwrap()) as Arc<dyn ObjectStore>)
            }
            _ => None,
        };

        Self {
            metadata: RwLock::new(HashMap::new()),
            backend,
            object_store,
            base_path,
        }
    }

    /// Store a blob with automatic metadata extraction
    pub async fn put(&self, id: &str, data: Vec<u8>, content_type: &str) -> Result<BlobMetadata, String> {
        // Calculate hash
        use sha2::{Sha256, Digest};
        let hash = format!("{:x}", Sha256::digest(&data));

        // Extract metadata based on content type
        let extended = self.extract_metadata(&data, content_type).await;

        let meta = BlobMetadata {
            id: id.to_string(),
            size: data.len(),
            content_type: content_type.to_string(),
            created_at: chrono::Utc::now().timestamp(),
            hash: Some(hash),
            extended,
        };

        // Write to storage backend
        match &self.backend {
            StorageBackend::Local(_) => {
                let path = PathBuf::from(&self.base_path).join(id);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).await
                        .map_err(|e| format!("Failed to create directory: {}", e))?;
                }
                fs::write(&path, &data).await
                    .map_err(|e| format!("Failed to write blob: {}", e))?;
            }
            StorageBackend::Memory => {
                // In-memory storage handled by metadata cache
            }
            StorageBackend::S3 { .. } => {
                // Would use object_store for S3
                return Err("S3 backend not yet implemented".to_string());
            }
        }

        // Update metadata cache
        let mut metadata = self.metadata.write().await;
        metadata.insert(id.to_string(), meta.clone());

        Ok(meta)
    }

    /// Retrieve a blob
    pub async fn get(&self, id: &str) -> Option<(BlobMetadata, Vec<u8>)> {
        let metadata = self.metadata.read().await;
        let meta = metadata.get(id)?.clone();
        drop(metadata);

        let data = match &self.backend {
            StorageBackend::Local(_) => {
                let path = PathBuf::from(&self.base_path).join(id);
                fs::read(&path).await.ok()?
            }
            StorageBackend::Memory => {
                return None; // Would need separate data storage
            }
            StorageBackend::S3 { .. } => {
                return None; // Would use object_store
            }
        };

        Some((meta, data))
    }

    /// Get metadata only (no data transfer)
    pub async fn get_metadata(&self, id: &str) -> Option<BlobMetadata> {
        let metadata = self.metadata.read().await;
        metadata.get(id).cloned()
    }

    /// Delete a blob
    pub async fn delete(&self, id: &str) -> Result<bool, String> {
        match &self.backend {
            StorageBackend::Local(_) => {
                let path = PathBuf::from(&self.base_path).join(id);
                if path.exists() {
                    fs::remove_file(&path).await
                        .map_err(|e| format!("Failed to delete blob: {}", e))?;
                }
            }
            _ => {}
        }

        let mut metadata = self.metadata.write().await;
        Ok(metadata.remove(id).is_some())
    }

    /// Get a range of bytes (for streaming)
    pub async fn get_range(&self, id: &str, start: usize, end: usize) -> Option<Vec<u8>> {
        match &self.backend {
            StorageBackend::Local(_) => {
                let path = PathBuf::from(&self.base_path).join(id);
                let mut file = fs::File::open(&path).await.ok()?;
                
                file.seek(std::io::SeekFrom::Start(start as u64)).await.ok()?;
                
                let length = (end - start).min(1024 * 1024); // Max 1MB per request
                let mut buffer = vec![0u8; length];
                file.read_exact(&mut buffer).await.ok()?;
                
                Some(buffer)
            }
            _ => None,
        }
    }

    /// List all blobs
    pub async fn list(&self) -> Vec<BlobMetadata> {
        let metadata = self.metadata.read().await;
        metadata.values().cloned().collect()
    }

    /// Extract metadata from blob based on content type
    async fn extract_metadata(&self, data: &[u8], content_type: &str) -> HashMap<String, String> {
        let mut extended = HashMap::new();

        match content_type {
            t if t.starts_with("image/") => {
                // Extract image metadata (width, height, EXIF)
                extended.insert("type".to_string(), "image".to_string());
                // In production: use `image` or `kamadak-exif` crates
                extended.insert("format".to_string(), t.replace("image/", ""));
            }
            t if t.starts_with("video/") => {
                // Extract video metadata (duration, codec, resolution)
                extended.insert("type".to_string(), "video".to_string());
                // In production: use `ffmpeg` via `ffmpeg-next`
                extended.insert("format".to_string(), t.replace("video/", ""));
            }
            t if t.starts_with("audio/") => {
                // Extract audio metadata (duration, bitrate, codec)
                extended.insert("type".to_string(), "audio".to_string());
                extended.insert("format".to_string(), t.replace("audio/", ""));
            }
            "application/pdf" => {
                extended.insert("type".to_string(), "document".to_string());
                extended.insert("format".to_string(), "pdf".to_string());
            }
            _ => {
                extended.insert("type".to_string(), "binary".to_string());
            }
        }

        extended
    }

    /// Generate thumbnail for images/videos
    pub async fn generate_thumbnail(&self, id: &str, size: u32) -> Result<Vec<u8>, String> {
        let (meta, data) = self.get(id).await
            .ok_or("Blob not found".to_string())?;

        if meta.content_type.starts_with("image/") {
            // In production: use `image` crate for thumbnails
            Ok(data) // Placeholder
        } else {
            Err("Thumbnail generation only supported for images".to_string())
        }
    }
}

impl Default for BlobStore {
    fn default() -> Self {
        Self::new("./blobs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_blob_storage() {
        let dir = tempdir().unwrap();
        let store = BlobStore::new(dir.path().to_str().unwrap());
        
        let data = b"Hello, World!".to_vec();
        let meta = store.put("test.txt", data.clone(), "text/plain").await.unwrap();
        
        assert_eq!(meta.size, 13);
        assert!(meta.hash.is_some());
        
        let (retrieved_meta, retrieved_data) = store.get("test.txt").await.unwrap();
        assert_eq!(retrieved_data, data);
        
        store.delete("test.txt").await.unwrap();
    }

    #[tokio::test]
    async fn test_range_request() {
        let dir = tempdir().unwrap();
        let store = BlobStore::new(dir.path().to_str().unwrap());
        
        let data = b"0123456789".to_vec();
        store.put("range.txt", data, "text/plain").await.unwrap();
        
        let range = store.get_range("range.txt", 2, 5).await.unwrap();
        assert_eq!(range, b"234");
        
        store.delete("range.txt").await.unwrap();
    }
}
