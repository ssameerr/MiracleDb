//! Namespace Module - Logical namespace management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Namespace
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Namespace {
    pub name: String,
    pub parent: Option<String>,
    pub description: String,
    pub created_at: i64,
    pub properties: HashMap<String, String>,
}

impl Namespace {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            parent: None,
            description: String::new(),
            created_at: chrono::Utc::now().timestamp(),
            properties: HashMap::new(),
        }
    }

    pub fn with_parent(mut self, parent: &str) -> Self {
        self.parent = Some(parent.to_string());
        self
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.description = description.to_string();
        self
    }

    pub fn full_name(&self) -> String {
        match &self.parent {
            Some(parent) => format!("{}.{}", parent, self.name),
            None => self.name.clone(),
        }
    }
}

/// Named object in a namespace
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NamespacedObject {
    pub namespace: String,
    pub name: String,
    pub object_type: ObjectType,
    pub created_at: i64,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectType {
    Table,
    View,
    Function,
    Procedure,
    Index,
    Sequence,
    Type,
    Extension,
}

/// Namespace manager
pub struct NamespaceManager {
    namespaces: RwLock<HashMap<String, Namespace>>,
    objects: RwLock<HashMap<String, Vec<NamespacedObject>>>,
    default_namespace: String,
}

impl NamespaceManager {
    pub fn new(default_namespace: &str) -> Self {
        let manager = Self {
            namespaces: RwLock::new(HashMap::new()),
            objects: RwLock::new(HashMap::new()),
            default_namespace: default_namespace.to_string(),
        };
        manager
    }

    /// Create a namespace
    pub async fn create(&self, namespace: Namespace) -> Result<(), String> {
        let mut namespaces = self.namespaces.write().await;
        
        if namespaces.contains_key(&namespace.full_name()) {
            return Err(format!("Namespace {} already exists", namespace.full_name()));
        }

        // Check parent exists
        if let Some(parent) = &namespace.parent {
            if !namespaces.contains_key(parent) {
                return Err(format!("Parent namespace {} does not exist", parent));
            }
        }

        namespaces.insert(namespace.full_name(), namespace);
        Ok(())
    }

    /// Drop a namespace
    pub async fn drop(&self, name: &str, cascade: bool) -> Result<(), String> {
        let mut namespaces = self.namespaces.write().await;
        let objects = self.objects.read().await;

        // Check for child namespaces
        let has_children = namespaces.values()
            .any(|ns| ns.parent.as_deref() == Some(name));

        if has_children && !cascade {
            return Err("Namespace has child namespaces".to_string());
        }

        // Check for objects
        if let Some(objs) = objects.get(name) {
            if !objs.is_empty() && !cascade {
                return Err("Namespace contains objects".to_string());
            }
        }

        namespaces.remove(name);
        
        // Remove children if cascade
        if cascade {
            let children: Vec<String> = namespaces.values()
                .filter(|ns| ns.parent.as_deref() == Some(name))
                .map(|ns| ns.full_name())
                .collect();
            
            for child in children {
                namespaces.remove(&child);
            }
        }

        Ok(())
    }

    /// Get a namespace
    pub async fn get(&self, name: &str) -> Option<Namespace> {
        let namespaces = self.namespaces.read().await;
        namespaces.get(name).cloned()
    }

    /// List namespaces
    pub async fn list(&self) -> Vec<Namespace> {
        let namespaces = self.namespaces.read().await;
        namespaces.values().cloned().collect()
    }

    /// List child namespaces
    pub async fn list_children(&self, parent: &str) -> Vec<Namespace> {
        let namespaces = self.namespaces.read().await;
        namespaces.values()
            .filter(|ns| ns.parent.as_deref() == Some(parent))
            .cloned()
            .collect()
    }

    /// Register an object in a namespace
    pub async fn register_object(&self, object: NamespacedObject) -> Result<(), String> {
        let namespaces = self.namespaces.read().await;
        
        if !namespaces.contains_key(&object.namespace) && object.namespace != self.default_namespace {
            return Err(format!("Namespace {} does not exist", object.namespace));
        }
        drop(namespaces);

        let mut objects = self.objects.write().await;
        let ns_objects = objects.entry(object.namespace.clone()).or_insert_with(Vec::new);
        ns_objects.push(object);

        Ok(())
    }

    /// Unregister an object
    pub async fn unregister_object(&self, namespace: &str, name: &str) {
        let mut objects = self.objects.write().await;
        if let Some(ns_objects) = objects.get_mut(namespace) {
            ns_objects.retain(|o| o.name != name);
        }
    }

    /// Get objects in namespace
    pub async fn get_objects(&self, namespace: &str) -> Vec<NamespacedObject> {
        let objects = self.objects.read().await;
        objects.get(namespace).cloned().unwrap_or_default()
    }

    /// Resolve a name to full qualified name
    pub fn resolve(&self, name: &str, current_namespace: &str) -> String {
        if name.contains('.') {
            name.to_string()
        } else {
            format!("{}.{}", current_namespace, name)
        }
    }

    /// Parse a qualified name
    pub fn parse(qualified_name: &str) -> (Option<String>, String) {
        if let Some(pos) = qualified_name.rfind('.') {
            let namespace = qualified_name[..pos].to_string();
            let name = qualified_name[pos + 1..].to_string();
            (Some(namespace), name)
        } else {
            (None, qualified_name.to_string())
        }
    }

    /// Search objects by name
    pub async fn search(&self, pattern: &str) -> Vec<NamespacedObject> {
        let objects = self.objects.read().await;
        let pattern_lower = pattern.to_lowercase();

        objects.values()
            .flat_map(|ns_objects| ns_objects.iter())
            .filter(|o| o.name.to_lowercase().contains(&pattern_lower))
            .cloned()
            .collect()
    }

    /// Get default namespace
    pub fn default_namespace(&self) -> &str {
        &self.default_namespace
    }
}

impl Default for NamespaceManager {
    fn default() -> Self {
        Self::new("public")
    }
}
