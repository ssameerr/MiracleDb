//! Search Module - Full-text search with Tantivy and RRF Hybrid Search

use std::path::Path;
use std::collections::HashMap;
use tantivy::{Index, IndexWriter, IndexReader, doc, schema::*};
use tantivy::schema::{TantivyDocument, Document as DocumentTrait};

use tantivy::query::{QueryParser, BooleanQuery, TermQuery, FuzzyTermQuery};
use tantivy::collector::{TopDocs, Count};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

pub mod rrf;
pub use rrf::{RRFEngine, RRFResult, SearchResult as RRFSearchResult, reciprocal_rank_fusion};

/// Search result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub highlights: HashMap<String, Vec<String>>,
    pub doc: HashMap<String, String>,
}

/// Search query options
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchOptions {
    pub limit: usize,
    pub offset: usize,
    pub fuzzy: bool,
    pub highlight: bool,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            limit: 10,
            offset: 0,
            fuzzy: false,
            highlight: true,
        }
    }
}

/// Search index configuration
#[derive(Clone)]
pub struct SearchIndex {
    name: String,
    index: Index,
    writer: std::sync::Arc<RwLock<IndexWriter>>,
    reader: IndexReader,
    schema: Schema,
    id_field: Field,
    text_fields: Vec<Field>,
}

/// Search engine
pub struct SearchEngine {
    indices: RwLock<HashMap<String, SearchIndex>>,
    base_path: String,
}

impl SearchEngine {
    pub fn new(base_path: &str) -> Self {
        Self {
            indices: RwLock::new(HashMap::new()),
            base_path: base_path.to_string(),
        }
    }

    /// Create a new search index
    pub async fn create_index(&self, name: &str, text_field_names: &[&str]) -> Result<(), String> {
        let index_path = format!("{}/{}", self.base_path, name);
        
        // Build schema
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("_id", STRING | STORED);
        
        let mut text_fields = Vec::new();
        for field_name in text_field_names {
            let field = schema_builder.add_text_field(field_name, TEXT | STORED);
            text_fields.push(field);
        }
        
        let schema = schema_builder.build();

        // Create index
        std::fs::create_dir_all(&index_path)
            .map_err(|e| format!("Failed to create index directory: {}", e))?;
        
        let index = Index::create_in_dir(&index_path, schema.clone())
            .map_err(|e| format!("Failed to create index: {}", e))?;

        let writer = index.writer(50_000_000)
            .map_err(|e| format!("Failed to create writer: {}", e))?;
        
        let reader = index.reader()
            .map_err(|e| format!("Failed to create reader: {}", e))?;

        let search_index = SearchIndex {
            name: name.to_string(),
            index,
            writer: std::sync::Arc::new(RwLock::new(writer)),
            reader,
            schema,
            id_field,
            text_fields,
        };

        let mut indices = self.indices.write().await;
        indices.insert(name.to_string(), search_index);

        Ok(())
    }

    /// Index a document
    pub async fn index_doc(&self, index_name: &str, id: &str, fields: HashMap<String, String>) -> Result<(), String> {
        let indices = self.indices.read().await;
        let search_index = indices.get(index_name)
            .ok_or("Index not found")?;

        let mut doc = TantivyDocument::new();
        doc.add_text(search_index.id_field, id);

        for (i, field) in search_index.text_fields.iter().enumerate() {
            if let Some(value) = fields.values().nth(i) {
                doc.add_text(*field, value);
            }
        }

        let mut writer = search_index.writer.write().await;
        writer.add_document(doc)
            .map_err(|e| format!("Failed to index document: {}", e))?;

        Ok(())
    }

    /// Commit changes
    pub async fn commit(&self, index_name: &str) -> Result<(), String> {
        let indices = self.indices.read().await;
        let search_index = indices.get(index_name)
            .ok_or("Index not found")?;

        let mut writer = search_index.writer.write().await;
        writer.commit()
            .map_err(|e| format!("Failed to commit: {}", e))?;

        search_index.reader.reload()
            .map_err(|e| format!("Failed to reload: {}", e))?;

        Ok(())
    }

    /// Search documents
    pub async fn search(&self, index_name: &str, query_str: &str, options: SearchOptions) -> Result<Vec<SearchResult>, String> {
        let indices = self.indices.read().await;
        let search_index = indices.get(index_name)
            .ok_or("Index not found")?;

        let searcher = search_index.reader.searcher();
        let query_parser = QueryParser::for_index(&search_index.index, search_index.text_fields.clone());
        
        let query = query_parser.parse_query(query_str)
            .map_err(|e| format!("Failed to parse query: {}", e))?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(options.limit))
            .map_err(|e| format!("Search failed: {}", e))?;

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)
                .map_err(|e| format!("Failed to retrieve document: {}", e))?;

            let id = retrieved_doc.get_first(search_index.id_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut doc_fields = HashMap::new();
            for (i, field) in search_index.text_fields.iter().enumerate() {
                if let Some(value) = retrieved_doc.get_first(*field) {
                    if let Some(text) = value.as_str() {
                        doc_fields.insert(format!("field_{}", i), text.to_string());
                    }
                }
            }

            results.push(SearchResult {
                id,
                score,
                highlights: HashMap::new(),
                doc: doc_fields,
            });
        }

        Ok(results)
    }

    /// Delete a document
    pub async fn delete_doc(&self, index_name: &str, id: &str) -> Result<(), String> {
        let indices = self.indices.read().await;
        let search_index = indices.get(index_name)
            .ok_or("Index not found")?;

        let term = tantivy::Term::from_field_text(search_index.id_field, id);
        
        let mut writer = search_index.writer.write().await;
        writer.delete_term(term);

        Ok(())
    }

    /// Count documents in index
    pub async fn count(&self, index_name: &str) -> Result<usize, String> {
        let indices = self.indices.read().await;
        let search_index = indices.get(index_name)
            .ok_or("Index not found")?;

        let searcher = search_index.reader.searcher();
        let count = searcher.search(&tantivy::query::AllQuery, &Count)
            .map_err(|e| format!("Count failed: {}", e))?;

        Ok(count)
    }
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self::new("./search_indices")
    }
}
