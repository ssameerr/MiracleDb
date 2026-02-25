//! View Module - View management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// View definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct View {
    pub name: String,
    pub schema: String,
    pub query: String,
    pub columns: Vec<ViewColumn>,
    pub owner: String,
    pub security_barrier: bool,
    pub check_option: CheckOption,
    pub created_at: i64,
    pub dependencies: Vec<String>,
}

/// View column
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewColumn {
    pub name: String,
    pub data_type: String,
    pub position: u32,
}

/// Check option for updatable views
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum CheckOption {
    #[default]
    None,
    Local,
    Cascaded,
}

impl View {
    pub fn new(schema: &str, name: &str, query: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            query: query.to_string(),
            columns: vec![],
            owner: "postgres".to_string(),
            security_barrier: false,
            check_option: CheckOption::default(),
            created_at: chrono::Utc::now().timestamp(),
            dependencies: vec![],
        }
    }

    pub fn with_columns(mut self, columns: Vec<ViewColumn>) -> Self {
        self.columns = columns;
        self
    }

    pub fn with_security_barrier(mut self) -> Self {
        self.security_barrier = true;
        self
    }

    pub fn with_check_option(mut self, option: CheckOption) -> Self {
        self.check_option = option;
        self
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Check if view is updatable
    pub fn is_updatable(&self) -> bool {
        // Simplified check - real implementation would analyze the query
        let query_lower = self.query.to_lowercase();
        !query_lower.contains(" join ") &&
        !query_lower.contains(" group by ") &&
        !query_lower.contains(" union ") &&
        !query_lower.contains(" distinct ") &&
        !query_lower.contains(" having ")
    }

    /// Extract table dependencies from query
    pub fn extract_dependencies(query: &str) -> Vec<String> {
        let mut deps = Vec::new();
        let query_lower = query.to_lowercase();

        // Simple parsing - look for FROM and JOIN clauses
        let words: Vec<&str> = query_lower.split_whitespace().collect();
        for (i, word) in words.iter().enumerate() {
            if (*word == "from" || *word == "join") && i + 1 < words.len() {
                let table = words[i + 1].trim_matches(|c| c == ',' || c == '(' || c == ')');
                if !table.is_empty() {
                    deps.push(table.to_string());
                }
            }
        }

        deps
    }
}

/// View manager
pub struct ViewManager {
    views: RwLock<HashMap<String, View>>,
}

impl ViewManager {
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
        }
    }

    /// Create a view
    pub async fn create(&self, view: View, replace: bool) -> Result<(), String> {
        let mut views = self.views.write().await;
        let key = view.full_name();

        match (views.contains_key(&key), replace) {
            (true, false) => {
                return Err(format!("View {} already exists", key));
            }
            _ => {
                views.insert(key, view);
            }
        }

        Ok(())
    }

    /// Drop a view
    pub async fn drop(&self, schema: &str, name: &str, cascade: bool) -> Result<Vec<String>, String> {
        let key = format!("{}.{}", schema, name);
        let mut dropped = vec![key.clone()];

        // Check for dependent views
        if !cascade {
            let views = self.views.read().await;
            for view in views.values() {
                if view.dependencies.contains(&key) {
                    return Err(format!("View {} depends on {}", view.full_name(), key));
                }
            }
        }

        let mut views = self.views.write().await;
        views.remove(&key)
            .ok_or_else(|| format!("View {} not found", key))?;

        // Cascade drop dependent views
        if cascade {
            let dependents: Vec<String> = views.values()
                .filter(|v| v.dependencies.contains(&key))
                .map(|v| v.full_name())
                .collect();

            for dep in dependents {
                views.remove(&dep);
                dropped.push(dep);
            }
        }

        Ok(dropped)
    }

    /// Get a view
    pub async fn get(&self, schema: &str, name: &str) -> Option<View> {
        let views = self.views.read().await;
        let key = format!("{}.{}", schema, name);
        views.get(&key).cloned()
    }

    /// List views
    pub async fn list(&self, schema: Option<&str>) -> Vec<View> {
        let views = self.views.read().await;
        views.values()
            .filter(|v| schema.map(|s| v.schema == s).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// Rename a view
    pub async fn rename(&self, schema: &str, old_name: &str, new_name: &str) -> Result<(), String> {
        let mut views = self.views.write().await;
        let old_key = format!("{}.{}", schema, old_name);
        let new_key = format!("{}.{}", schema, new_name);

        if views.contains_key(&new_key) {
            return Err(format!("View {} already exists", new_key));
        }

        let mut view = views.remove(&old_key)
            .ok_or_else(|| format!("View {} not found", old_key))?;

        view.name = new_name.to_string();
        views.insert(new_key, view);

        Ok(())
    }
}

impl Default for ViewManager {
    fn default() -> Self {
        Self::new()
    }
}
