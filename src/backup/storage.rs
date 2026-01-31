//! Storage Backend Abstraction for Backups
//!
//! Provides a unified interface for different storage backends:
//! - Local filesystem
//! - Amazon S3
//! - Google Cloud Storage
//! - Azure Blob Storage

use std::path::{Path, PathBuf};
use async_trait::async_trait;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Storage backend trait
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Write data to storage
    async fn write(&self, key: &str, data: &[u8]) -> Result<(), String>;

    /// Read data from storage
    async fn read(&self, key: &str) -> Result<Vec<u8>, String>;

    /// Delete data from storage
    async fn delete(&self, key: &str) -> Result<(), String>;

    /// List keys with prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>, String>;

    /// Check if key exists
    async fn exists(&self, key: &str) -> Result<bool, String>;

    /// Get storage backend name
    fn backend_name(&self) -> &str;
}

/// Local filesystem storage backend
pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    fn resolve_path(&self, key: &str) -> PathBuf {
        self.base_path.join(key)
    }
}

#[async_trait]
impl StorageBackend for LocalStorage {
    async fn write(&self, key: &str, data: &[u8]) -> Result<(), String> {
        let path = self.resolve_path(key);

        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await
                .map_err(|e| format!("Failed to create directories: {}", e))?;
        }

        fs::write(&path, data).await
            .map_err(|e| format!("Failed to write file: {}", e))
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, String> {
        let path = self.resolve_path(key);
        fs::read(&path).await
            .map_err(|e| format!("Failed to read file: {}", e))
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        let path = self.resolve_path(key);
        if path.is_dir() {
            fs::remove_dir_all(&path).await
                .map_err(|e| format!("Failed to delete directory: {}", e))
        } else {
            fs::remove_file(&path).await
                .map_err(|e| format!("Failed to delete file: {}", e))
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let prefix_path = self.resolve_path(prefix);
        let mut results = Vec::new();

        if prefix_path.exists() && prefix_path.is_dir() {
            let mut entries = fs::read_dir(&prefix_path).await
                .map_err(|e| format!("Failed to read directory: {}", e))?;

            while let Some(entry) = entries.next_entry().await
                .map_err(|e| format!("Failed to read entry: {}", e))? {
                let path = entry.path();
                let file_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                results.push(format!("{}/{}", prefix, file_name));
            }
        }

        Ok(results)
    }

    async fn exists(&self, key: &str) -> Result<bool, String> {
        let path = self.resolve_path(key);
        Ok(path.exists())
    }

    fn backend_name(&self) -> &str {
        "local"
    }
}

/// Amazon S3 storage backend
#[cfg(feature = "cloud")]
pub struct S3Storage {
    bucket: String,
    region: String,
    prefix: Option<String>,
    client: Option<aws_sdk_s3::Client>,
}

#[cfg(feature = "cloud")]
impl S3Storage {
    pub fn new(bucket: String, region: String, prefix: Option<String>) -> Self {
        Self {
            bucket,
            region,
            prefix,
            client: None,
        }
    }

    pub async fn initialize(&mut self) -> Result<(), String> {
        use aws_config::BehaviorVersion;

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(self.region.clone()))
            .load()
            .await;

        self.client = Some(aws_sdk_s3::Client::new(&config));
        Ok(())
    }

    fn get_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key),
            None => key.to_string(),
        }
    }
}

#[cfg(feature = "cloud")]
#[async_trait]
impl StorageBackend for S3Storage {
    async fn write(&self, key: &str, data: &[u8]) -> Result<(), String> {
        let client = self.client.as_ref().ok_or("S3 client not initialized")?;
        let s3_key = self.get_key(key);

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .body(data.to_vec().into())
            .send()
            .await
            .map_err(|e| format!("S3 write failed: {}", e))?;

        Ok(())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, String> {
        let client = self.client.as_ref().ok_or("S3 client not initialized")?;
        let s3_key = self.get_key(key);

        let response = client
            .get_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await
            .map_err(|e| format!("S3 read failed: {}", e))?;

        let data = response.body.collect().await
            .map_err(|e| format!("S3 read body failed: {}", e))?;

        Ok(data.to_vec())
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        let client = self.client.as_ref().ok_or("S3 client not initialized")?;
        let s3_key = self.get_key(key);

        client
            .delete_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await
            .map_err(|e| format!("S3 delete failed: {}", e))?;

        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let client = self.client.as_ref().ok_or("S3 client not initialized")?;
        let s3_prefix = self.get_key(prefix);

        let response = client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&s3_prefix)
            .send()
            .await
            .map_err(|e| format!("S3 list failed: {}", e))?;

        let keys = response.contents()
            .iter()
            .filter_map(|obj| obj.key())
            .map(|k| k.to_string())
            .collect();

        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool, String> {
        let client = self.client.as_ref().ok_or("S3 client not initialized")?;
        let s3_key = self.get_key(key);

        match client
            .head_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn backend_name(&self) -> &str {
        "s3"
    }
}

/// Google Cloud Storage backend
#[cfg(feature = "cloud")]
pub struct GcsStorage {
    bucket: String,
    prefix: Option<String>,
}

#[cfg(feature = "cloud")]
impl GcsStorage {
    pub fn new(bucket: String, prefix: Option<String>) -> Self {
        Self { bucket, prefix }
    }

    fn get_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key),
            None => key.to_string(),
        }
    }
}

#[cfg(feature = "cloud")]
#[async_trait]
impl StorageBackend for GcsStorage {
    async fn write(&self, key: &str, data: &[u8]) -> Result<(), String> {
        // GCS implementation would go here
        // Using google-cloud-storage crate
        Err("GCS not yet implemented".to_string())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, String> {
        Err("GCS not yet implemented".to_string())
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        Err("GCS not yet implemented".to_string())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        Err("GCS not yet implemented".to_string())
    }

    async fn exists(&self, key: &str) -> Result<bool, String> {
        Err("GCS not yet implemented".to_string())
    }

    fn backend_name(&self) -> &str {
        "gcs"
    }
}

/// Azure Blob Storage backend
#[cfg(feature = "cloud")]
pub struct AzureStorage {
    container: String,
    account: String,
    prefix: Option<String>,
}

#[cfg(feature = "cloud")]
impl AzureStorage {
    pub fn new(container: String, account: String, prefix: Option<String>) -> Self {
        Self {
            container,
            account,
            prefix,
        }
    }

    fn get_key(&self, key: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key),
            None => key.to_string(),
        }
    }
}

#[cfg(feature = "cloud")]
#[async_trait]
impl StorageBackend for AzureStorage {
    async fn write(&self, key: &str, data: &[u8]) -> Result<(), String> {
        // Azure implementation would go here
        // Using azure_storage_blobs crate
        Err("Azure not yet implemented".to_string())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, String> {
        Err("Azure not yet implemented".to_string())
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        Err("Azure not yet implemented".to_string())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        Err("Azure not yet implemented".to_string())
    }

    async fn exists(&self, key: &str) -> Result<bool, String> {
        Err("Azure not yet implemented".to_string())
    }

    fn backend_name(&self) -> &str {
        "azure"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_storage_write_read() {
        let temp_dir = tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_path_buf());

        let test_data = b"Hello, World!";
        storage.write("test.txt", test_data).await.unwrap();

        let read_data = storage.read("test.txt").await.unwrap();
        assert_eq!(read_data, test_data);
    }

    #[tokio::test]
    async fn test_local_storage_exists() {
        let temp_dir = tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_path_buf());

        assert!(!storage.exists("nonexistent.txt").await.unwrap());

        storage.write("exists.txt", b"data").await.unwrap();
        assert!(storage.exists("exists.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_storage_delete() {
        let temp_dir = tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path().to_path_buf());

        storage.write("delete_me.txt", b"data").await.unwrap();
        assert!(storage.exists("delete_me.txt").await.unwrap());

        storage.delete("delete_me.txt").await.unwrap();
        assert!(!storage.exists("delete_me.txt").await.unwrap());
    }
}
