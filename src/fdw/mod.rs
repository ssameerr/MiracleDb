//! FDW Module - Foreign Data Wrappers

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Foreign data wrapper
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForeignDataWrapper {
    pub name: String,
    pub handler: Option<String>,
    pub validator: Option<String>,
    pub options: HashMap<String, String>,
    pub owner: String,
    pub created_at: i64,
}

/// Foreign server
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForeignServer {
    pub name: String,
    pub fdw_name: String,
    pub server_type: Option<String>,
    pub server_version: Option<String>,
    pub options: HashMap<String, String>,
    pub owner: String,
    pub created_at: i64,
}

/// User mapping
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserMapping {
    pub server_name: String,
    pub user_name: String,
    pub options: HashMap<String, String>,
}

/// Foreign table
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForeignTable {
    pub name: String,
    pub schema: String,
    pub server_name: String,
    pub columns: Vec<ForeignColumn>,
    pub options: HashMap<String, String>,
    pub owner: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForeignColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub options: HashMap<String, String>,
}

impl ForeignDataWrapper {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            handler: None,
            validator: None,
            options: HashMap::new(),
            owner: "postgres".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn with_handler(mut self, handler: &str) -> Self {
        self.handler = Some(handler.to_string());
        self
    }

    pub fn with_option(mut self, key: &str, value: &str) -> Self {
        self.options.insert(key.to_string(), value.to_string());
        self
    }
}

impl ForeignServer {
    pub fn new(name: &str, fdw_name: &str) -> Self {
        Self {
            name: name.to_string(),
            fdw_name: fdw_name.to_string(),
            server_type: None,
            server_version: None,
            options: HashMap::new(),
            owner: "postgres".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn with_type(mut self, server_type: &str) -> Self {
        self.server_type = Some(server_type.to_string());
        self
    }

    pub fn with_option(mut self, key: &str, value: &str) -> Self {
        self.options.insert(key.to_string(), value.to_string());
        self
    }
}

impl ForeignTable {
    pub fn new(schema: &str, name: &str, server: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            server_name: server.to_string(),
            columns: vec![],
            options: HashMap::new(),
            owner: "postgres".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn add_column(mut self, name: &str, data_type: &str) -> Self {
        self.columns.push(ForeignColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
            options: HashMap::new(),
        });
        self
    }

    pub fn with_option(mut self, key: &str, value: &str) -> Self {
        self.options.insert(key.to_string(), value.to_string());
        self
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

/// FDW manager
pub struct FdwManager {
    wrappers: RwLock<HashMap<String, ForeignDataWrapper>>,
    servers: RwLock<HashMap<String, ForeignServer>>,
    user_mappings: RwLock<HashMap<String, UserMapping>>,
    foreign_tables: RwLock<HashMap<String, ForeignTable>>,
}

impl FdwManager {
    pub fn new() -> Self {
        Self {
            wrappers: RwLock::new(HashMap::new()),
            servers: RwLock::new(HashMap::new()),
            user_mappings: RwLock::new(HashMap::new()),
            foreign_tables: RwLock::new(HashMap::new()),
        }
    }

    /// Create FDW
    pub async fn create_fdw(&self, fdw: ForeignDataWrapper) -> Result<(), String> {
        let mut wrappers = self.wrappers.write().await;
        if wrappers.contains_key(&fdw.name) {
            return Err(format!("FDW {} already exists", fdw.name));
        }
        wrappers.insert(fdw.name.clone(), fdw);
        Ok(())
    }

    /// Create server
    pub async fn create_server(&self, server: ForeignServer) -> Result<(), String> {
        // Verify FDW exists
        let wrappers = self.wrappers.read().await;
        if !wrappers.contains_key(&server.fdw_name) {
            return Err(format!("FDW {} not found", server.fdw_name));
        }
        drop(wrappers);

        let mut servers = self.servers.write().await;
        if servers.contains_key(&server.name) {
            return Err(format!("Server {} already exists", server.name));
        }
        servers.insert(server.name.clone(), server);
        Ok(())
    }

    /// Create user mapping
    pub async fn create_user_mapping(&self, mapping: UserMapping) -> Result<(), String> {
        let servers = self.servers.read().await;
        if !servers.contains_key(&mapping.server_name) {
            return Err(format!("Server {} not found", mapping.server_name));
        }
        drop(servers);

        let key = format!("{}:{}", mapping.server_name, mapping.user_name);
        let mut mappings = self.user_mappings.write().await;
        mappings.insert(key, mapping);
        Ok(())
    }

    /// Create foreign table
    pub async fn create_foreign_table(&self, table: ForeignTable) -> Result<(), String> {
        let servers = self.servers.read().await;
        if !servers.contains_key(&table.server_name) {
            return Err(format!("Server {} not found", table.server_name));
        }
        drop(servers);

        let key = table.full_name();
        let mut tables = self.foreign_tables.write().await;
        if tables.contains_key(&key) {
            return Err(format!("Foreign table {} already exists", key));
        }
        tables.insert(key, table);
        Ok(())
    }

    /// Get foreign table
    pub async fn get_foreign_table(&self, schema: &str, name: &str) -> Option<ForeignTable> {
        let key = format!("{}.{}", schema, name);
        let tables = self.foreign_tables.read().await;
        tables.get(&key).cloned()
    }

    /// List FDWs
    pub async fn list_fdw(&self) -> Vec<ForeignDataWrapper> {
        let wrappers = self.wrappers.read().await;
        wrappers.values().cloned().collect()
    }

    /// List servers
    pub async fn list_servers(&self) -> Vec<ForeignServer> {
        let servers = self.servers.read().await;
        servers.values().cloned().collect()
    }

    /// List foreign tables
    pub async fn list_foreign_tables(&self, schema: Option<&str>) -> Vec<ForeignTable> {
        let tables = self.foreign_tables.read().await;
        tables.values()
            .filter(|t| schema.map(|s| t.schema == s).unwrap_or(true))
            .cloned()
            .collect()
    }
}

impl Default for FdwManager {
    fn default() -> Self {
        Self::new()
    }
}
