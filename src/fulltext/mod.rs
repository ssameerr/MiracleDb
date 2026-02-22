//! Full-Text Search Module
//!
//! Provides full-text search capabilities using Tantivy.
//! Supports indexing, querying, and managing text search indexes.

use tantivy::{
    schema::{Schema, Field, STORED, TEXT, IndexRecordOption, Value},
    Index, IndexWriter, IndexReader, ReloadPolicy,
    query::{QueryParser, Query, PhraseQuery, RegexQuery},
    collector::TopDocs,
    snippet::SnippetGenerator,
    doc,
    Document,
    TantivyDocument,
    TantivyError,
    Term,
};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};

/// Configuration for full-text index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FullTextIndexConfig {
    /// Fields to index for full-text search
    pub fields: Vec<String>,
    /// Enable fuzzy search (Levenshtein distance)
    pub fuzzy_search: bool,
    /// Default operator (AND or OR)
    pub default_operator: String,
    /// Enable position indexing (for phrase queries)
    pub position_indexing: bool,
}

impl Default for FullTextIndexConfig {
    fn default() -> Self {
        Self {
            fields: vec!["text".to_string()],
            fuzzy_search: false,
            default_operator: "OR".to_string(),
            position_indexing: true,
        }
    }
}

/// Full-text search index manager
pub struct FullTextIndexManager {
    base_path: PathBuf,
    indexes: Arc<RwLock<HashMap<String, FullTextIndex>>>,
}

impl FullTextIndexManager {
    /// Create a new full-text index manager
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: PathBuf::from(base_path),
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create or open a full-text index for a table
    pub fn create_index(
        &self,
        table_name: &str,
        config: FullTextIndexConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let index_path = self.base_path.join(table_name);

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&index_path)?;

        // Build Tantivy schema
        let mut schema_builder = Schema::builder();

        // Add ID field (stored, not indexed)
        let id_field = schema_builder.add_text_field("id", STORED);

        // Add text fields for full-text search
        let mut text_fields = Vec::new();
        for field_name in &config.fields {
            let index_option = if config.position_indexing {
                IndexRecordOption::WithFreqsAndPositions
            } else {
                IndexRecordOption::WithFreqs
            };

            let field = schema_builder.add_text_field(
                field_name,
                TEXT | STORED,
            );
            text_fields.push((field_name.clone(), field));
        }

        let schema = schema_builder.build();

        // Create or open the index
        let index = if index_path.exists() && index_path.join("meta.json").exists() {
            tracing::info!("Opening existing Tantivy index at {:?}", index_path);
            Index::open_in_dir(&index_path)?
        } else {
            tracing::info!("Creating new Tantivy index at {:?}", index_path);
            Index::create_in_dir(&index_path, schema.clone())?
        };

        // Create index writer (50 MB heap)
        let writer = index.writer(50_000_000)?;

        // Create index reader with manual reload
        // Note: In Tantivy 0.22+, use manual reload and call reader.reload() as needed
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        // Store the index
        let num_text_fields = text_fields.len();
        let full_text_index = FullTextIndex {
            index,
            writer: Arc::new(RwLock::new(writer)),
            reader,
            id_field,
            text_fields,
            config,
        };

        let mut indexes = self.indexes.write().unwrap();
        indexes.insert(table_name.to_string(), full_text_index);

        tracing::info!(
            "Created full-text index for table '{}' with {} fields",
            table_name,
            num_text_fields
        );

        Ok(())
    }

    /// Add a document to the index
    pub fn add_document(
        &self,
        table_name: &str,
        id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let indexes = self.indexes.read().unwrap();
        let index = indexes.get(table_name)
            .ok_or_else(|| format!("No full-text index found for table '{}'", table_name))?;

        index.add_document(id, fields)?;

        Ok(())
    }

    /// Update a document in the index (delete + add)
    pub fn update_document(
        &self,
        table_name: &str,
        id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let indexes = self.indexes.read().unwrap();
        let index = indexes.get(table_name)
            .ok_or_else(|| format!("No full-text index found for table '{}'", table_name))?;

        index.update_document(id, fields)?;

        Ok(())
    }

    /// Delete a document from the index
    pub fn delete_document(
        &self,
        table_name: &str,
        id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let indexes = self.indexes.read().unwrap();
        let index = indexes.get(table_name)
            .ok_or_else(|| format!("No full-text index found for table '{}'", table_name))?;

        index.delete_document(id)?;

        Ok(())
    }

    /// Search the full-text index
    pub fn search(
        &self,
        table_name: &str,
        query: &str,
        limit: usize,
        options: &TextSearchOptions,
    ) -> Result<Vec<FullTextSearchResult>, Box<dyn std::error::Error>> {
        let indexes = self.indexes.read().unwrap();
        let index = indexes.get(table_name)
            .ok_or_else(|| format!("No full-text index found for table '{}'", table_name))?;

        index.search(query, limit, options)
    }

    /// Commit pending changes
    pub fn commit(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let indexes = self.indexes.read().unwrap();
        let index = indexes.get(table_name)
            .ok_or_else(|| format!("No full-text index found for table '{}'", table_name))?;

        index.commit()?;

        Ok(())
    }
}

/// Internal full-text index structure
struct FullTextIndex {
    index: Index,
    writer: Arc<RwLock<IndexWriter>>,
    reader: IndexReader,
    id_field: Field,
    text_fields: Vec<(String, Field)>,
    config: FullTextIndexConfig,
}

impl FullTextIndex {
    /// Add a document to the index
    fn add_document(
        &self,
        id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut doc = TantivyDocument::default();

        // Add ID field
        doc.add_text(self.id_field, id);

        // Add text fields
        for (field_name, field) in &self.text_fields {
            if let Some(value) = fields.get(field_name) {
                doc.add_text(*field, value);
            }
        }

        // Add to index
        let mut writer = self.writer.write().unwrap();
        writer.add_document(doc)?;

        tracing::debug!("Added document with id '{}' to full-text index", id);

        Ok(())
    }

    /// Update a document (delete old, add new)
    fn update_document(
        &self,
        id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Delete existing document
        self.delete_document(id)?;

        // Add new version
        self.add_document(id, fields)?;

        Ok(())
    }

    /// Delete a document by ID
    fn delete_document(&self, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = self.writer.write().unwrap();

        // Create term for ID field
        let term = tantivy::Term::from_field_text(self.id_field, id);

        // Delete document
        writer.delete_term(term);

        tracing::debug!("Deleted document with id '{}' from full-text index", id);

        Ok(())
    }

    /// Parse a search query string into a Tantivy query.
    ///
    /// Supports three syntaxes:
    /// - `"exact phrase"` – wrapped in double quotes → `PhraseQuery`
    /// - `prefix*`        – trailing asterisk       → `RegexQuery` (prefix match)
    /// - anything else    – standard Tantivy query parser syntax
    fn parse_query(
        &self,
        query_str: &str,
        options: &TextSearchOptions,
    ) -> Result<Box<dyn Query>, Box<dyn std::error::Error>> {
        let fields: Vec<Field> = self.text_fields.iter().map(|(_, f)| *f).collect();

        // --- Phrase query: "some exact phrase" ---
        if query_str.starts_with('"') && query_str.ends_with('"') && query_str.len() >= 2 {
            let inner = &query_str[1..query_str.len() - 1];
            let words: Vec<&str> = inner.split_whitespace().collect();

            if words.len() > 1 {
                // Use the first text field for the phrase query (Tantivy PhraseQuery
                // operates on a single field; all terms must share the same field).
                let field = fields
                    .first()
                    .copied()
                    .ok_or("No text fields configured for full-text index")?;
                let terms: Vec<Term> = words
                    .iter()
                    .map(|w| Term::from_field_text(field, &w.to_lowercase()))
                    .collect();
                tracing::debug!("Using PhraseQuery for {:?}", query_str);
                return Ok(Box::new(PhraseQuery::new(terms)));
            }
            // Single-word phrase – fall through to normal parsing (no quotes needed).
        }

        // --- Wildcard / prefix query: word* ---
        if query_str.ends_with('*') && query_str.len() > 1 {
            let prefix_raw = &query_str[..query_str.len() - 1];
            // Only treat as prefix if the prefix itself contains no spaces or
            // special characters (i.e. it is a single-token pattern).
            if !prefix_raw.contains(char::is_whitespace) {
                let field = fields
                    .first()
                    .copied()
                    .ok_or("No text fields configured for full-text index")?;
                let prefix_lower = prefix_raw.to_lowercase();
                // Escape any regex meta-chars inside the literal prefix, then
                // append `.*` so every token that starts with it will match.
                let escaped = regex::escape(&prefix_lower);
                let pattern = format!("{}.*", escaped);
                match RegexQuery::from_pattern(&pattern, field) {
                    Ok(q) => {
                        tracing::debug!(
                            "Using RegexQuery with pattern {:?} for {:?}",
                            pattern,
                            query_str
                        );
                        return Ok(Box::new(q));
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to build RegexQuery for pattern {:?}: {}; \
                             falling back to standard parser",
                            pattern,
                            e
                        );
                    }
                }
            }
        }

        // --- Default: standard Tantivy QueryParser ---
        let mut query_parser = QueryParser::for_index(&self.index, fields);
        match options.operator.to_uppercase().as_str() {
            "AND" => query_parser.set_conjunction_by_default(),
            _ => {} // OR is the default
        }
        let query = query_parser
            .parse_query(query_str)
            .unwrap_or_else(|e| {
                tracing::warn!("Query parse error for {:?}: {}; returning AllQuery", query_str, e);
                Box::new(tantivy::query::AllQuery)
            });
        Ok(query)
    }

    /// Search the index
    fn search(
        &self,
        query_str: &str,
        limit: usize,
        options: &TextSearchOptions,
    ) -> Result<Vec<FullTextSearchResult>, Box<dyn std::error::Error>> {
        let searcher = self.reader.searcher();

        // Build and parse the query (phrase / wildcard / standard).
        let query = self.parse_query(query_str, options)?;

        tracing::debug!("Parsed full-text query: {:?}", query);

        // Execute search
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        // Build one SnippetGenerator per text field so we can try each field
        // and return the first non-empty highlighted snippet.
        let snippet_generators: Vec<(usize, SnippetGenerator)> = self
            .text_fields
            .iter()
            .enumerate()
            .filter_map(|(i, (_, field))| {
                match SnippetGenerator::create(&searcher, &*query, *field) {
                    Ok(gen) => Some((i, gen)),
                    Err(e) => {
                        tracing::warn!("Could not create SnippetGenerator for field {}: {}", i, e);
                        None
                    }
                }
            })
            .collect();

        // Extract results
        let mut results = Vec::new();

        for (score, doc_address) in top_docs {
            // Retrieve document
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;

            // Extract ID
            let id = retrieved_doc
                .get_first(self.id_field)
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            // Apply minimum score filter
            if score < options.min_score {
                continue;
            }

            // Generate a highlight snippet from the first field that yields a
            // non-empty result.  If no field contains a matching fragment,
            // `highlight` remains `None`.
            let highlight: Option<String> = snippet_generators
                .iter()
                .find_map(|(_, gen)| {
                    let snippet = gen.snippet_from_doc(&retrieved_doc);
                    if snippet.is_empty() {
                        None
                    } else {
                        Some(snippet.to_html())
                    }
                });

            results.push(FullTextSearchResult {
                id,
                score,
                highlight,
            });
        }

        tracing::debug!("Full-text search returned {} results", results.len());

        Ok(results)
    }

    /// Commit pending changes and reload the reader so searches see new docs
    fn commit(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = self.writer.write().unwrap();
        writer.commit()?;
        drop(writer); // release the write lock before reloading

        // ReloadPolicy::Manual requires an explicit reload after each commit
        self.reader.reload()?;

        tracing::debug!("Committed and reloaded full-text index");

        Ok(())
    }
}

/// Text search options
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextSearchOptions {
    pub fuzzy: bool,
    pub operator: String,  // "AND" or "OR"
    pub min_score: f32,
    pub boost: HashMap<String, f32>,
}

impl Default for TextSearchOptions {
    fn default() -> Self {
        Self {
            fuzzy: false,
            operator: "OR".to_string(),
            min_score: 0.0,
            boost: HashMap::new(),
        }
    }
}

/// Full-text search result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FullTextSearchResult {
    pub id: String,
    pub score: f32,
    /// HTML snippet with matched terms wrapped in `<b>` tags.
    /// `None` when no matching fragment could be extracted from the document.
    pub highlight: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_index() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FullTextIndexManager::new(temp_dir.path().to_str().unwrap());

        let config = FullTextIndexConfig {
            fields: vec!["title".to_string(), "body".to_string()],
            ..Default::default()
        };

        let result = manager.create_index("test_table", config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_and_search() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FullTextIndexManager::new(temp_dir.path().to_str().unwrap());

        // Create index
        let config = FullTextIndexConfig {
            fields: vec!["text".to_string()],
            ..Default::default()
        };
        manager.create_index("test_table", config).unwrap();

        // Add documents
        let mut fields1 = HashMap::new();
        fields1.insert("text".to_string(), "The quick brown fox".to_string());
        manager.add_document("test_table", "1", &fields1).unwrap();

        let mut fields2 = HashMap::new();
        fields2.insert("text".to_string(), "jumps over the lazy dog".to_string());
        manager.add_document("test_table", "2", &fields2).unwrap();

        // Commit
        manager.commit("test_table").unwrap();

        // Search
        let options = TextSearchOptions::default();
        let results = manager.search("test_table", "fox", 10, &options).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "1");
    }

    /// Helper: build a manager with two indexed documents.
    fn build_test_manager() -> (TempDir, FullTextIndexManager) {
        let temp_dir = TempDir::new().unwrap();
        let manager = FullTextIndexManager::new(temp_dir.path().to_str().unwrap());
        let config = FullTextIndexConfig {
            fields: vec!["text".to_string()],
            ..Default::default()
        };
        manager.create_index("tbl", config).unwrap();

        let mut f1 = HashMap::new();
        f1.insert("text".to_string(), "machine learning is powerful".to_string());
        manager.add_document("tbl", "1", &f1).unwrap();

        let mut f2 = HashMap::new();
        f2.insert("text".to_string(), "deep learning transforms machine vision".to_string());
        manager.add_document("tbl", "2", &f2).unwrap();

        let mut f3 = HashMap::new();
        f3.insert("text".to_string(), "natural language processing advances".to_string());
        manager.add_document("tbl", "3", &f3).unwrap();

        manager.commit("tbl").unwrap();
        (temp_dir, manager)
    }

    #[test]
    fn test_phrase_query_exact_match() {
        let (_dir, manager) = build_test_manager();
        let options = TextSearchOptions::default();

        // "machine learning" should match doc 1 (exact adjacent phrase) but
        // NOT doc 2 (contains both words, but not adjacent in that order).
        let results = manager
            .search("tbl", "\"machine learning\"", 10, &options)
            .unwrap();

        assert!(
            results.iter().any(|r| r.id == "1"),
            "expected doc 1 in phrase results"
        );
        // Doc 2 has "machine" and "learning" but they are NOT adjacent:
        // "deep learning transforms machine vision" – so it must NOT appear.
        assert!(
            !results.iter().any(|r| r.id == "2"),
            "doc 2 must not appear in phrase-only results"
        );
    }

    #[test]
    fn test_wildcard_prefix_query() {
        let (_dir, manager) = build_test_manager();
        let options = TextSearchOptions::default();

        // "mac*" should match "machine" in docs 1 and 2.
        let results = manager.search("tbl", "mac*", 10, &options).unwrap();

        assert!(
            results.iter().any(|r| r.id == "1"),
            "expected doc 1 in wildcard results"
        );
        assert!(
            results.iter().any(|r| r.id == "2"),
            "expected doc 2 in wildcard results"
        );
        // Doc 3 has no "mac…" token.
        assert!(
            !results.iter().any(|r| r.id == "3"),
            "doc 3 must not appear in wildcard results"
        );
    }

    #[test]
    fn test_standard_query_unchanged() {
        let (_dir, manager) = build_test_manager();
        let options = TextSearchOptions::default();

        // Plain keyword search must still work normally.
        let results = manager.search("tbl", "language", 10, &options).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "3");
    }
}
