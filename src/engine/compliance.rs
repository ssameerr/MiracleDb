//! Compliance Manager - Query hooks for security and audit
//!
//! Provides before/after query hooks for data masking and audit logging

use std::sync::Arc;
use tokio::sync::RwLock;
use datafusion::error::Result;
use tracing::{info, warn};

/// Compliance policy for data masking
#[derive(Clone, Debug)]
pub struct MaskingPolicy {
    /// Policy name
    pub name: String,
    /// Column to mask
    pub column: String,
    /// Table the column belongs to
    pub table: String,
    /// Masking function
    pub mask_type: MaskType,
    /// Roles that see unmasked data
    pub exempt_roles: Vec<String>,
}

/// Types of data masking
#[derive(Clone, Debug)]
pub enum MaskType {
    /// Full redaction (replace with ***)
    Full,
    /// Partial (e.g., ***-**-1234 for SSN)
    Partial { visible_chars: usize, from_end: bool },
    /// Email masking (j***@example.com)
    Email,
    /// Hash the value
    Hash,
    /// Replace with NULL
    Nullify,
    /// Custom pattern
    Pattern(String),
}

/// Audit log entry
#[derive(Clone, Debug)]
pub struct AuditEntry {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub user_id: Option<String>,
    pub query: String,
    pub query_type: QueryType,
    pub tables_accessed: Vec<String>,
    pub success: bool,
    pub execution_time_ms: Option<u64>,
    pub rows_affected: Option<u64>,
}

/// Type of query for audit purposes
#[derive(Clone, Debug)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    DDL,
    Other,
}

/// Manages compliance features
pub struct ComplianceManager {
    /// Active masking policies
    masking_policies: RwLock<Vec<MaskingPolicy>>,
    /// Audit log buffer (would be persisted to Tantivy in production)
    audit_log: RwLock<Vec<AuditEntry>>,
    /// Current user context
    current_user: RwLock<Option<UserContext>>,
}

/// User context for access control
#[derive(Clone, Debug)]
pub struct UserContext {
    pub user_id: String,
    pub roles: Vec<String>,
    pub session_id: String,
}

impl ComplianceManager {
    pub fn new() -> Self {
        Self {
            masking_policies: RwLock::new(Vec::new()),
            audit_log: RwLock::new(Vec::new()),
            current_user: RwLock::new(None),
        }
    }

    /// Set the current user context
    pub async fn set_user_context(&self, context: UserContext) {
        let mut user = self.current_user.write().await;
        *user = Some(context);
    }

    /// Clear the current user context
    pub async fn clear_user_context(&self) {
        let mut user = self.current_user.write().await;
        *user = None;
    }

    /// Add a masking policy
    pub async fn add_masking_policy(&self, policy: MaskingPolicy) {
        let mut policies = self.masking_policies.write().await;
        policies.push(policy);
    }

    /// Remove a masking policy by name
    pub async fn remove_masking_policy(&self, name: &str) {
        let mut policies = self.masking_policies.write().await;
        policies.retain(|p| p.name != name);
    }

    /// Hook called before query execution
    pub async fn before_query(&self, sql: &str) -> Result<String> {
        let user = self.current_user.read().await;
        let policies = self.masking_policies.read().await;
        
        info!(
            "Before query hook - User: {:?}, Query: {}",
            user.as_ref().map(|u| &u.user_id),
            truncate_sql(sql, 100)
        );
        
        // Check access control - deny if user has no context for sensitive tables
        let tables = extract_tables(sql);
        for table in &tables {
            let table_policies: Vec<_> = policies.iter().filter(|p| &p.table == table).collect();
            if !table_policies.is_empty() && user.is_none() {
                warn!("Anonymous access denied to table with masking policies: {}", table);
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Access denied to table: {}", table)
                ));
            }
        }
        
        // Apply query rewriting for column masking
        let mut rewritten_sql = sql.to_string();
        
        for policy in policies.iter() {
            // Check if user is exempt
            let is_exempt = if let Some(ref ctx) = *user {
                policy.exempt_roles.iter().any(|r| ctx.roles.contains(r))
            } else {
                false
            };
            
            if !is_exempt && tables.contains(&policy.table) {
                // Rewrite column references to apply masking
                // Simple approach: wrap column in CASE expression
                let mask_expr = match &policy.mask_type {
                    MaskType::Full => "'***'".to_string(),
                    MaskType::Nullify => "NULL".to_string(),
                    MaskType::Partial { visible_chars, from_end } => {
                        if *from_end {
                            format!("CONCAT('***', RIGHT({}, {}))", policy.column, visible_chars)
                        } else {
                            format!("CONCAT(LEFT({}, {}), '***')", policy.column, visible_chars)
                        }
                    }
                    MaskType::Email => {
                        format!("CONCAT(LEFT({}, 1), '***', SUBSTR({}, INSTR({}, '@')))", 
                            policy.column, policy.column, policy.column)
                    }
                    _ => format!("'***'"), // Default fallback
                };
                
                // Replace column with masked version in SELECT clause
                // This is a simplified approach - production would use SQL parser
                let pattern = format!("SELECT {}", policy.column);
                let replacement = format!("SELECT {} AS {}", mask_expr, policy.column);
                if rewritten_sql.to_uppercase().contains(&pattern.to_uppercase()) {
                    rewritten_sql = rewritten_sql.replace(&policy.column, &format!("({} AS {})", mask_expr, policy.column));
                    info!("Applied masking to column {} in query", policy.column);
                }
            }
        }
        
        Ok(rewritten_sql)
    }

    /// Hook called after query execution
    pub async fn after_query(&self, sql: &str, success: bool) -> Result<()> {
        let user = self.current_user.read().await;
        
        let entry = AuditEntry {
            timestamp: chrono::Utc::now(),
            user_id: user.as_ref().map(|u| u.user_id.clone()),
            query: sql.to_string(),
            query_type: detect_query_type(sql),
            tables_accessed: extract_tables(sql),
            success,
            execution_time_ms: None, // Would be tracked separately
            rows_affected: None,
        };
        
        // Add to audit log
        let mut log = self.audit_log.write().await;
        log.push(entry);
        
        // Keep audit log bounded (in production would persist to storage)
        if log.len() > 10000 {
            log.drain(0..5000);
        }
        
        info!(
            "After query hook - Success: {}, Query: {}",
            success,
            truncate_sql(sql, 100)
        );
        
        Ok(())
    }

    /// Apply masking to a value based on policies
    pub async fn mask_value(
        &self,
        table: &str,
        column: &str,
        value: &str,
    ) -> String {
        let policies = self.masking_policies.read().await;
        let user = self.current_user.read().await;
        
        for policy in policies.iter() {
            if policy.table == table && policy.column == column {
                // Check if user is exempt
                if let Some(ref ctx) = *user {
                    if policy.exempt_roles.iter().any(|r| ctx.roles.contains(r)) {
                        return value.to_string();
                    }
                }
                
                return apply_mask(&policy.mask_type, value);
            }
        }
        
        value.to_string()
    }

    /// Get recent audit entries
    pub async fn get_audit_log(&self, limit: usize) -> Vec<AuditEntry> {
        let log = self.audit_log.read().await;
        log.iter().rev().take(limit).cloned().collect()
    }

    /// Get masking policies for a table
    pub async fn get_policies_for_table(&self, table: &str) -> Vec<MaskingPolicy> {
        let policies = self.masking_policies.read().await;
        policies.iter().filter(|p| p.table == table).cloned().collect()
    }
}

impl Default for ComplianceManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply a mask to a value
fn apply_mask(mask_type: &MaskType, value: &str) -> String {
    match mask_type {
        MaskType::Full => "***".to_string(),
        MaskType::Partial { visible_chars, from_end } => {
            let len = value.len();
            if len <= *visible_chars {
                return "***".to_string();
            }
            
            if *from_end {
                format!("***{}", &value[len - visible_chars..])
            } else {
                format!("{}***", &value[..*visible_chars])
            }
        }
        MaskType::Email => {
            if let Some(at_pos) = value.find('@') {
                let local = &value[..at_pos];
                let domain = &value[at_pos..];
                if local.len() > 1 {
                    format!("{}***{}", &local[..1], domain)
                } else {
                    format!("***{}", domain)
                }
            } else {
                "***".to_string()
            }
        }
        MaskType::Hash => {
            use sha2::{Sha256, Digest};
            let mut hasher = Sha256::new();
            hasher.update(value.as_bytes());
            format!("{:x}", hasher.finalize())[..16].to_string()
        }
        MaskType::Nullify => "NULL".to_string(),
        MaskType::Pattern(pattern) => pattern.clone(),
    }
}

/// Detect the type of SQL query
fn detect_query_type(sql: &str) -> QueryType {
    let sql_upper = sql.trim().to_uppercase();
    if sql_upper.starts_with("SELECT") {
        QueryType::Select
    } else if sql_upper.starts_with("INSERT") {
        QueryType::Insert
    } else if sql_upper.starts_with("UPDATE") {
        QueryType::Update
    } else if sql_upper.starts_with("DELETE") {
        QueryType::Delete
    } else if sql_upper.starts_with("CREATE") || sql_upper.starts_with("ALTER") || sql_upper.starts_with("DROP") {
        QueryType::DDL
    } else {
        QueryType::Other
    }
}

/// Extract table names from SQL (simplified)
fn extract_tables(sql: &str) -> Vec<String> {
    // Simplified extraction - in production would use SQL parser
    let mut tables = Vec::new();
    let sql_upper = sql.to_uppercase();
    
    for keyword in &["FROM", "JOIN", "INTO", "UPDATE", "TABLE"] {
        if let Some(pos) = sql_upper.find(keyword) {
            let after = &sql[pos + keyword.len()..];
            if let Some(table) = after.split_whitespace().next() {
                let table = table.trim_matches(|c| c == '(' || c == ')' || c == ';' || c == ',');
                if !table.is_empty() && !["WHERE", "ON", "AND", "OR", "SET"].contains(&table.to_uppercase().as_str()) {
                    tables.push(table.to_string());
                }
            }
        }
    }
    
    tables
}

/// Truncate SQL for logging
fn truncate_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        sql.to_string()
    } else {
        format!("{}...", &sql[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_full() {
        let result = apply_mask(&MaskType::Full, "secret");
        assert_eq!(result, "***");
    }

    #[test]
    fn test_mask_partial() {
        let result = apply_mask(
            &MaskType::Partial { visible_chars: 4, from_end: true },
            "123-45-6789"
        );
        assert_eq!(result, "***6789");
    }

    #[test]
    fn test_mask_email() {
        let result = apply_mask(&MaskType::Email, "john.doe@example.com");
        assert_eq!(result, "j***@example.com");
    }

    #[test]
    fn test_detect_query_type() {
        assert!(matches!(detect_query_type("SELECT * FROM users"), QueryType::Select));
        assert!(matches!(detect_query_type("INSERT INTO users VALUES (1)"), QueryType::Insert));
        assert!(matches!(detect_query_type("UPDATE users SET name = 'x'"), QueryType::Update));
    }
}
