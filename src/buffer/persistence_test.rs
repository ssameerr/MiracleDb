
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Arc;
    use tempfile::tempdir;
    use crate::smgr::{StorageManagerInterface, RelFileNode, ForkNum as ForkNumber};
    use crate::buffer::{BufferPool, BufferTag};
    use crate::storage::Page;

    #[tokio::test]
    async fn test_buffer_persistence() {
        let dir = tempdir().unwrap();
        let smgr = Arc::new(StorageManagerInterface::new(dir.path().to_path_buf()));
        let buffer_pool = BufferPool::new(10, smgr.clone());

        let node = RelFileNode { spc_oid: 1, db_oid: 1, rel_oid: 1 };
        let tag = BufferTag {
            rel_file_node: crate::smgr::RelFileNode { spc_oid: 1, db_oid: 1, rel_oid: 1 },
            fork_number: ForkNumber::Main,
            block_number: 0,
        };

        // Create relation by extending it with 1 empty block
        smgr.extend(&tag.rel_file_node, ForkNumber::Main, &[0u8; 8192]).await.unwrap();

        // Write to buffer
        let buf_id = buffer_pool.read_buffer(tag.clone()).await.unwrap();
        let mut page = Page::new();
        page.raw[0] = 42;
        page.raw[1] = 99;
        buffer_pool.write_page(buf_id, page).await;
        buffer_pool.mark_dirty(&tag).await;

        // Flush
        buffer_pool.flush().await;

        // Verify file exists
        let path = smgr.get_path(&tag.rel_file_node, crate::smgr::ForkNum::Main);
        assert!(path.exists());
        let meta = fs::metadata(&path).unwrap();
        assert!(meta.len() >= 8192);

        // Re-read with new buffer pool (simulate restart)
        let buffer_pool2 = BufferPool::new(10, smgr.clone());
        let buf_id2: u32 = buffer_pool2.read_buffer(tag.clone()).await.unwrap();
        let page2 = buffer_pool2.get_page(buf_id2).await;
        
        assert_eq!(page2.raw[0], 42);
        assert_eq!(page2.raw[1], 99);
    }
}
