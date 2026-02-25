//! Collation Module - String collation support

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Collation definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Collation {
    pub name: String,
    pub schema: String,
    pub provider: CollationProvider,
    pub locale: Option<String>,
    pub lc_collate: Option<String>,
    pub lc_ctype: Option<String>,
    pub deterministic: bool,
    pub version: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum CollationProvider {
    Libc,
    Icu,
    Default,
}

impl Collation {
    pub fn new(schema: &str, name: &str, provider: CollationProvider) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            provider,
            locale: None,
            lc_collate: None,
            lc_ctype: None,
            deterministic: true,
            version: None,
        }
    }

    pub fn with_locale(mut self, locale: &str) -> Self {
        self.locale = Some(locale.to_string());
        self
    }

    pub fn non_deterministic(mut self) -> Self {
        self.deterministic = false;
        self
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Compare two strings using this collation
    pub fn compare(&self, a: &str, b: &str) -> std::cmp::Ordering {
        match &self.locale {
            Some(locale) if locale.contains("_ci") => {
                // Case-insensitive comparison
                a.to_lowercase().cmp(&b.to_lowercase())
            }
            Some(locale) if locale.contains("_ai") => {
                // Accent-insensitive (simplified)
                self.normalize(a).cmp(&self.normalize(b))
            }
            _ => {
                // Default comparison
                a.cmp(b)
            }
        }
    }

    fn normalize(&self, s: &str) -> String {
        // Simplified accent normalization
        s.chars()
            .map(|c| match c {
                'á' | 'à' | 'â' | 'ä' => 'a',
                'é' | 'è' | 'ê' | 'ë' => 'e',
                'í' | 'ì' | 'î' | 'ï' => 'i',
                'ó' | 'ò' | 'ô' | 'ö' => 'o',
                'ú' | 'ù' | 'û' | 'ü' => 'u',
                _ => c,
            })
            .collect()
    }

    /// Check equality using this collation
    pub fn equals(&self, a: &str, b: &str) -> bool {
        if self.deterministic {
            a == b
        } else {
            self.compare(a, b) == std::cmp::Ordering::Equal
        }
    }
}

/// Collation manager
pub struct CollationManager {
    collations: RwLock<HashMap<String, Collation>>,
}

impl CollationManager {
    pub fn new() -> Self {
        let mut manager = Self {
            collations: RwLock::new(HashMap::new()),
        };

        // Register built-in collations
        manager.register_builtins();
        manager
    }

    fn register_builtins(&mut self) {
        let builtins = vec![
            Collation::new("pg_catalog", "default", CollationProvider::Default),
            Collation::new("pg_catalog", "C", CollationProvider::Libc)
                .with_locale("C"),
            Collation::new("pg_catalog", "POSIX", CollationProvider::Libc)
                .with_locale("POSIX"),
            Collation::new("pg_catalog", "ucs_basic", CollationProvider::Libc)
                .with_locale("C"),
            Collation::new("public", "en_US", CollationProvider::Icu)
                .with_locale("en-US"),
            Collation::new("public", "en_US_ci", CollationProvider::Icu)
                .with_locale("en-US_ci")
                .non_deterministic(),
        ];

        // Would be populated async in actual use
    }

    /// Create a collation
    pub async fn create(&self, collation: Collation) -> Result<(), String> {
        let mut collations = self.collations.write().await;
        let key = collation.full_name();

        if collations.contains_key(&key) {
            return Err(format!("Collation {} already exists", key));
        }

        collations.insert(key, collation);
        Ok(())
    }

    /// Drop a collation
    pub async fn drop(&self, schema: &str, name: &str) -> Result<(), String> {
        let mut collations = self.collations.write().await;
        let key = format!("{}.{}", schema, name);

        collations.remove(&key)
            .ok_or_else(|| format!("Collation {} not found", key))?;
        Ok(())
    }

    /// Get a collation
    pub async fn get(&self, schema: &str, name: &str) -> Option<Collation> {
        let collations = self.collations.read().await;
        let key = format!("{}.{}", schema, name);
        collations.get(&key).cloned()
    }

    /// Get collation by name (searches common schemas)
    pub async fn resolve(&self, name: &str) -> Option<Collation> {
        let collations = self.collations.read().await;

        // Try exact match first
        if let Some(collation) = collations.get(name) {
            return Some(collation.clone());
        }

        // Search in common schemas
        for schema in &["pg_catalog", "public"] {
            let key = format!("{}.{}", schema, name);
            if let Some(collation) = collations.get(&key) {
                return Some(collation.clone());
            }
        }

        None
    }

    /// List all collations
    pub async fn list(&self, schema: Option<&str>) -> Vec<Collation> {
        let collations = self.collations.read().await;
        collations.values()
            .filter(|c| schema.map(|s| c.schema == s).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// Compare strings using specified collation
    pub async fn compare(&self, collation_name: &str, a: &str, b: &str) -> std::cmp::Ordering {
        if let Some(collation) = self.resolve(collation_name).await {
            collation.compare(a, b)
        } else {
            a.cmp(b)
        }
    }
}

impl Default for CollationManager {
    fn default() -> Self {
        Self::new()
    }
}
