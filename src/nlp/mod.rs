//! NLP Module - Text processing and semantic search

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// NLP processing result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NLPResult {
    pub tokens: Vec<String>,
    pub entities: Vec<Entity>,
    pub sentiment: Option<Sentiment>,
    pub language: Option<String>,
}

/// Named entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entity {
    pub text: String,
    pub entity_type: EntityType,
    pub start: usize,
    pub end: usize,
    pub confidence: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EntityType {
    Person,
    Organization,
    Location,
    Date,
    Money,
    Email,
    Phone,
    Custom(String),
}

/// Sentiment analysis result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sentiment {
    pub score: f32, // -1.0 to 1.0
    pub label: SentimentLabel,
    pub confidence: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SentimentLabel {
    Positive,
    Negative,
    Neutral,
}

/// Text chunk for RAG
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextChunk {
    pub id: String,
    pub text: String,
    pub embedding: Vec<f32>,
    pub metadata: HashMap<String, String>,
}

/// NLP Engine
pub struct NLPEngine {
    stop_words: Vec<String>,
}

impl NLPEngine {
    pub fn new() -> Self {
        let stop_words = vec![
            "the", "a", "an", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "do", "does", "did", "will",
            "would", "could", "should", "may", "might", "must", "shall",
            "can", "need", "dare", "ought", "used", "to", "of", "in",
            "for", "on", "with", "at", "by", "from", "as", "into",
            "through", "during", "before", "after", "above", "below",
        ].into_iter().map(String::from).collect();
        
        Self { stop_words }
    }

    /// Tokenize text
    pub fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }

    /// Remove stop words
    pub fn remove_stop_words(&self, tokens: &[String]) -> Vec<String> {
        tokens.iter()
            .filter(|t| !self.stop_words.contains(t))
            .cloned()
            .collect()
    }

    /// Stem words (simplified Porter stemmer)
    pub fn stem(&self, word: &str) -> String {
        let mut result = word.to_string();
        
        // Simple suffix removal
        for suffix in &["ing", "ed", "ly", "tion", "ness", "ment", "er", "est"] {
            if result.ends_with(suffix) && result.len() > suffix.len() + 2 {
                result = result[..result.len() - suffix.len()].to_string();
                break;
            }
        }
        
        result
    }

    /// Extract named entities (rule-based)
    pub fn extract_entities(&self, text: &str) -> Vec<Entity> {
        let mut entities = Vec::new();
        
        // Email detection
        let email_re = regex::Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap();
        for mat in email_re.find_iter(text) {
            entities.push(Entity {
                text: mat.as_str().to_string(),
                entity_type: EntityType::Email,
                start: mat.start(),
                end: mat.end(),
                confidence: 0.95,
            });
        }

        // Phone detection
        let phone_re = regex::Regex::new(r"\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}").unwrap();
        for mat in phone_re.find_iter(text) {
            entities.push(Entity {
                text: mat.as_str().to_string(),
                entity_type: EntityType::Phone,
                start: mat.start(),
                end: mat.end(),
                confidence: 0.90,
            });
        }

        // Money detection
        let money_re = regex::Regex::new(r"\$[\d,]+\.?\d*").unwrap();
        for mat in money_re.find_iter(text) {
            entities.push(Entity {
                text: mat.as_str().to_string(),
                entity_type: EntityType::Money,
                start: mat.start(),
                end: mat.end(),
                confidence: 0.95,
            });
        }

        entities
    }

    /// Analyze sentiment (lexicon-based)
    pub fn analyze_sentiment(&self, text: &str) -> Sentiment {
        let positive_words = vec!["good", "great", "excellent", "amazing", "wonderful", 
                                   "fantastic", "love", "happy", "joy", "best"];
        let negative_words = vec!["bad", "terrible", "awful", "horrible", "hate",
                                   "sad", "angry", "worst", "poor", "disappointing"];

        let tokens = self.tokenize(text);
        let mut score = 0.0f32;

        for token in &tokens {
            if positive_words.contains(&token.as_str()) {
                score += 1.0;
            } else if negative_words.contains(&token.as_str()) {
                score -= 1.0;
            }
        }

        // Normalize score
        let normalized = (score / (tokens.len() as f32 + 1.0)).clamp(-1.0, 1.0);

        let label = if normalized > 0.1 {
            SentimentLabel::Positive
        } else if normalized < -0.1 {
            SentimentLabel::Negative
        } else {
            SentimentLabel::Neutral
        };

        Sentiment {
            score: normalized,
            label,
            confidence: 0.7,
        }
    }

    /// Chunk text for RAG (Retrieval Augmented Generation)
    pub fn chunk_text(&self, text: &str, chunk_size: usize, overlap: usize) -> Vec<String> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let mut chunks = Vec::new();
        let mut i = 0;

        while i < words.len() {
            let end = (i + chunk_size).min(words.len());
            let chunk = words[i..end].join(" ");
            chunks.push(chunk);
            i += chunk_size - overlap;
        }

        chunks
    }

    /// Calculate TF-IDF scores
    pub fn tfidf(&self, documents: &[Vec<String>]) -> Vec<HashMap<String, f32>> {
        let n_docs = documents.len() as f32;
        
        // Calculate document frequency for each term
        let mut df: HashMap<String, u32> = HashMap::new();
        for doc in documents {
            let unique: std::collections::HashSet<_> = doc.iter().collect();
            for term in unique {
                *df.entry(term.clone()).or_insert(0) += 1;
            }
        }

        // Calculate TF-IDF for each document
        documents.iter().map(|doc| {
            let mut tfidf_scores = HashMap::new();
            let doc_len = doc.len() as f32;
            
            // Count term frequency
            let mut tf: HashMap<String, u32> = HashMap::new();
            for term in doc {
                *tf.entry(term.clone()).or_insert(0) += 1;
            }

            for (term, count) in tf {
                let tf_score = count as f32 / doc_len;
                let idf_score = (n_docs / *df.get(&term).unwrap_or(&1) as f32).ln() + 1.0;
                tfidf_scores.insert(term, tf_score * idf_score);
            }

            tfidf_scores
        }).collect()
    }

    /// Full NLP processing
    pub fn process(&self, text: &str) -> NLPResult {
        let tokens = self.tokenize(text);
        let entities = self.extract_entities(text);
        let sentiment = Some(self.analyze_sentiment(text));

        NLPResult {
            tokens,
            entities,
            sentiment,
            language: Some("en".to_string()),
        }
    }
}

impl Default for NLPEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        let engine = NLPEngine::new();
        let tokens = engine.tokenize("Hello, World! How are you?");
        assert_eq!(tokens, vec!["hello", "world", "how", "are", "you"]);
    }

    #[test]
    fn test_sentiment() {
        let engine = NLPEngine::new();
        let sentiment = engine.analyze_sentiment("This is great and amazing!");
        assert!(matches!(sentiment.label, SentimentLabel::Positive));
    }

    #[test]
    fn test_entities() {
        let engine = NLPEngine::new();
        let entities = engine.extract_entities("Contact us at test@example.com or $500");
        assert!(entities.iter().any(|e| matches!(e.entity_type, EntityType::Email)));
        assert!(entities.iter().any(|e| matches!(e.entity_type, EntityType::Money)));
    }

    #[test]
    fn test_text_to_sql_users() {
        let sql = TextToSql::translate("show all users");
        assert_eq!(sql, "SELECT * FROM users");
    }

    #[test]
    fn test_text_to_sql_count_products() {
        let sql = TextToSql::translate("count all products");
        assert_eq!(sql, "SELECT COUNT(*) FROM products");
    }

    #[test]
    fn test_analyze_sentiment_negative() {
        let engine = NLPEngine::new();
        let sentiment = engine.analyze_sentiment("This is terrible and awful, the worst experience!");
        assert!(matches!(sentiment.label, SentimentLabel::Negative));
    }

    #[test]
    fn test_detect_language_from_process() {
        let engine = NLPEngine::new();
        let result = engine.process("Hello world");
        assert_eq!(result.language, Some("en".to_string()));
    }
}

/// Text-to-SQL Converter (Simulated LLM)
pub struct TextToSql {
    // In production, would hold LLM client (Candle, OpenAI)
}

impl TextToSql {
    pub fn translate(natural_query: &str) -> String {
        // Stub: Simple heuristic
        let lower = natural_query.to_lowercase();
        if lower.contains("show") && lower.contains("users") {
            "SELECT * FROM users".to_string()
        } else if lower.contains("count") && lower.contains("products") {
            "SELECT COUNT(*) FROM products".to_string()
        } else {
            // Default fallback or LLM call
            format!("-- Could not translate: {}", natural_query)
        }
    }
}

/// Semantic Search (Vector Embedding Stub)
pub struct SemanticSearch;

impl SemanticSearch {
    pub fn embed(text: &str) -> Vec<f32> {
        // In production: Use BERT/All-MiniLM via `candle-transformers`
        // Stub: Random 384-d vector based on hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let seed = hasher.finish();
        
        (0..384).map(|i| (seed.wrapping_add(i) % 100) as f32 / 100.0).collect()
    }
}
