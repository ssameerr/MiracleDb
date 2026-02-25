//! Comment Module - Database object comments and documentation

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Object type for comments
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ObjectType {
    Database,
    Schema,
    Table,
    Column,
    Index,
    Constraint,
    Function,
    Procedure,
    Trigger,
    View,
    Sequence,
    Type,
    Extension,
}

/// Object identifier
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectId {
    pub object_type: ObjectType,
    pub schema: Option<String>,
    pub name: String,
    pub parent: Option<String>, // For columns: table name
}

impl ObjectId {
    pub fn table(name: &str) -> Self {
        Self {
            object_type: ObjectType::Table,
            schema: None,
            name: name.to_string(),
            parent: None,
        }
    }

    pub fn column(table: &str, column: &str) -> Self {
        Self {
            object_type: ObjectType::Column,
            schema: None,
            name: column.to_string(),
            parent: Some(table.to_string()),
        }
    }

    pub fn index(name: &str) -> Self {
        Self {
            object_type: ObjectType::Index,
            schema: None,
            name: name.to_string(),
            parent: None,
        }
    }

    pub fn function(name: &str) -> Self {
        Self {
            object_type: ObjectType::Function,
            schema: None,
            name: name.to_string(),
            parent: None,
        }
    }

    pub fn with_schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }
}

/// Comment entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Comment {
    pub object_id: ObjectId,
    pub text: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub created_by: Option<String>,
}

/// Comment manager
pub struct CommentManager {
    comments: RwLock<HashMap<ObjectId, Comment>>,
}

impl CommentManager {
    pub fn new() -> Self {
        Self {
            comments: RwLock::new(HashMap::new()),
        }
    }

    /// Set comment on object
    pub async fn set(&self, object_id: ObjectId, text: &str, user: Option<&str>) {
        let now = chrono::Utc::now().timestamp();
        let mut comments = self.comments.write().await;

        if let Some(existing) = comments.get_mut(&object_id) {
            existing.text = text.to_string();
            existing.updated_at = now;
        } else {
            comments.insert(object_id.clone(), Comment {
                object_id,
                text: text.to_string(),
                created_at: now,
                updated_at: now,
                created_by: user.map(|s| s.to_string()),
            });
        }
    }

    /// Get comment
    pub async fn get(&self, object_id: &ObjectId) -> Option<String> {
        let comments = self.comments.read().await;
        comments.get(object_id).map(|c| c.text.clone())
    }

    /// Remove comment
    pub async fn remove(&self, object_id: &ObjectId) {
        let mut comments = self.comments.write().await;
        comments.remove(object_id);
    }

    /// Get all comments for table
    pub async fn get_table_comments(&self, table: &str) -> HashMap<String, String> {
        let comments = self.comments.read().await;
        let mut result = HashMap::new();

        // Table comment
        let table_id = ObjectId::table(table);
        if let Some(comment) = comments.get(&table_id) {
            result.insert("_table".to_string(), comment.text.clone());
        }

        // Column comments
        for (id, comment) in comments.iter() {
            if id.object_type == ObjectType::Column && id.parent.as_deref() == Some(table) {
                result.insert(id.name.clone(), comment.text.clone());
            }
        }

        result
    }

    /// Get all comments
    pub async fn get_all(&self) -> Vec<Comment> {
        let comments = self.comments.read().await;
        comments.values().cloned().collect()
    }

    /// Search comments
    pub async fn search(&self, query: &str) -> Vec<Comment> {
        let query_lower = query.to_lowercase();
        let comments = self.comments.read().await;
        comments.values()
            .filter(|c| c.text.to_lowercase().contains(&query_lower))
            .cloned()
            .collect()
    }

    /// Generate documentation
    pub async fn generate_docs(&self, table: &str) -> String {
        let table_comments = self.get_table_comments(table).await;
        let mut doc = format!("# Table: {}\n\n", table);

        if let Some(desc) = table_comments.get("_table") {
            doc.push_str(&format!("{}\n\n", desc));
        }

        doc.push_str("## Columns\n\n");
        doc.push_str("| Column | Description |\n");
        doc.push_str("|--------|-------------|\n");

        for (column, description) in table_comments.iter() {
            if column != "_table" {
                doc.push_str(&format!("| {} | {} |\n", column, description));
            }
        }

        doc
    }
}

impl Default for CommentManager {
    fn default() -> Self {
        Self::new()
    }
}
