use std::sync::Arc;
use object_store::{ObjectStore, path::Path as ObjPath};
// use object_store::local::LocalFileSystem;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use futures::StreamExt;

pub struct BlobManager {
    store: Arc<dyn ObjectStore>,
}

impl BlobManager {
    // For MVP, we use memory or local fs. But object_store crate is abstract.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn put(&self, path: &str, data:  Bytes) -> Result<(), Box<dyn std::error::Error>> {
        let location = ObjPath::from(path);
        self.store.put(&location, data.into()).await?;
        Ok(())
    }

    pub async fn get(&self, path: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
        let location = ObjPath::from(path);
        let result = self.store.get(&location).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }
    
    pub async fn delete(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
         let location = ObjPath::from(path);
         self.store.delete(&location).await?;
         Ok(())
    }
    
    // Stub for metadata extraction (Phase 12 feature)
    pub fn extract_metadata(&self, _data: &[u8]) -> std::collections::HashMap<String, String> {
        let mut meta = std::collections::HashMap::new();
        meta.insert("type".to_string(), "unknown".to_string());
        // In real impl, check magic bytes or use ffmpeg/image crate
        meta
    }
}
