//! Role-Based Access Control (RBAC) and Attribute-Based Access Control (ABAC)
//!
//! Implements fine-grained access control for database resources

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Actions that can be performed on resources
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Grant,
    Execute,
    Admin,
}

/// A role with associated permissions
#[derive(Clone, Debug)]
pub struct Role {
    pub name: String,
    pub description: String,
    /// Permissions: resource -> set of allowed actions
    pub permissions: HashMap<String, HashSet<Action>>,
    /// Roles this role inherits from
    pub inherits: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// A user with assigned roles
#[derive(Clone, Debug)]
pub struct User {
    pub id: String,
    pub username: String,
    pub roles: Vec<String>,
    pub attributes: HashMap<String, String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_login: Option<chrono::DateTime<chrono::Utc>>,
}

/// ABAC policy for fine-grained control
#[derive(Clone, Debug)]
pub struct AbacPolicy {
    pub name: String,
    pub resource_pattern: String,
    pub action: Action,
    /// Condition expression (simplified - would use a proper expression language)
    pub condition: String,
    pub effect: PolicyEffect,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

/// Manages RBAC and ABAC
pub struct RbacManager {
    roles: RwLock<HashMap<String, Role>>,
    users: RwLock<HashMap<String, User>>,
    abac_policies: RwLock<Vec<AbacPolicy>>,
}

impl RbacManager {
    pub fn new() -> Self {
        let mut manager = Self {
            roles: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
            abac_policies: RwLock::new(Vec::new()),
        };
        
        // Create default roles
        tokio::runtime::Handle::try_current().map(|_| {
            // We're in an async context, but new() is sync
            // Default roles will be added on first use
        });
        
        manager
    }

    /// Initialize default roles
    pub async fn init_default_roles(&self) {
        // Admin role with all permissions
        let admin = Role {
            name: "admin".to_string(),
            description: "Full administrative access".to_string(),
            permissions: {
                let mut perms = HashMap::new();
                perms.insert("*".to_string(), {
                    let mut actions = HashSet::new();
                    actions.insert(Action::Select);
                    actions.insert(Action::Insert);
                    actions.insert(Action::Update);
                    actions.insert(Action::Delete);
                    actions.insert(Action::Create);
                    actions.insert(Action::Drop);
                    actions.insert(Action::Alter);
                    actions.insert(Action::Grant);
                    actions.insert(Action::Execute);
                    actions.insert(Action::Admin);
                    actions
                });
                perms
            },
            inherits: vec![],
            created_at: chrono::Utc::now(),
        };
        
        // Read-only role
        let readonly = Role {
            name: "readonly".to_string(),
            description: "Read-only access to all tables".to_string(),
            permissions: {
                let mut perms = HashMap::new();
                perms.insert("*".to_string(), {
                    let mut actions = HashSet::new();
                    actions.insert(Action::Select);
                    actions
                });
                perms
            },
            inherits: vec![],
            created_at: chrono::Utc::now(),
        };
        
        let mut roles = self.roles.write().await;
        roles.insert("admin".to_string(), admin);
        roles.insert("readonly".to_string(), readonly);
        
        info!("Initialized default roles: admin, readonly");
    }

    /// Create a new role
    pub async fn create_role(&self, role: Role) -> Result<(), String> {
        let mut roles = self.roles.write().await;
        if roles.contains_key(&role.name) {
            return Err(format!("Role {} already exists", role.name));
        }
        info!("Created role: {}", role.name);
        roles.insert(role.name.clone(), role);
        Ok(())
    }

    /// Delete a role
    pub async fn delete_role(&self, name: &str) -> Result<(), String> {
        let mut roles = self.roles.write().await;
        if roles.remove(name).is_some() {
            info!("Deleted role: {}", name);
            Ok(())
        } else {
            Err(format!("Role {} not found", name))
        }
    }

    /// Create a new user
    pub async fn create_user(&self, user: User) -> Result<(), String> {
        let mut users = self.users.write().await;
        if users.contains_key(&user.id) {
            return Err(format!("User {} already exists", user.id));
        }
        info!("Created user: {} ({})", user.username, user.id);
        users.insert(user.id.clone(), user);
        Ok(())
    }

    /// Assign a role to a user
    pub async fn assign_role(&self, user_id: &str, role_name: &str) -> Result<(), String> {
        let roles = self.roles.read().await;
        if !roles.contains_key(role_name) {
            return Err(format!("Role {} not found", role_name));
        }
        drop(roles);
        
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(user_id) {
            if !user.roles.contains(&role_name.to_string()) {
                user.roles.push(role_name.to_string());
                info!("Assigned role {} to user {}", role_name, user_id);
            }
            Ok(())
        } else {
            Err(format!("User {} not found", user_id))
        }
    }

    /// Revoke a role from a user
    pub async fn revoke_role(&self, user_id: &str, role_name: &str) -> Result<(), String> {
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(user_id) {
            user.roles.retain(|r| r != role_name);
            info!("Revoked role {} from user {}", role_name, user_id);
            Ok(())
        } else {
            Err(format!("User {} not found", user_id))
        }
    }

    /// Check if a user has permission for an action on a resource
    pub async fn check_permission(
        &self,
        user_id: &str,
        resource: &str,
        action: Action,
    ) -> bool {
        let users = self.users.read().await;
        let user = match users.get(user_id) {
            Some(u) => u.clone(),
            None => {
                warn!("User {} not found for permission check", user_id);
                return false;
            }
        };
        drop(users);
        
        // Check ABAC policies first (deny takes precedence)
        let policies = self.abac_policies.read().await;
        for policy in policies.iter() {
            if self.matches_resource(&policy.resource_pattern, resource) && policy.action == action {
                if self.evaluate_condition(&policy.condition, &user) {
                    if policy.effect == PolicyEffect::Deny {
                        warn!("ABAC policy {} denied access for user {}", policy.name, user_id);
                        return false;
                    }
                }
            }
        }
        drop(policies);
        
        // Check RBAC permissions
        let roles = self.roles.read().await;
        for role_name in &user.roles {
            if self.has_permission_recursive(&roles, role_name, resource, action, &mut HashSet::new()) {
                return true;
            }
        }
        
        warn!(
            "Permission denied: user={}, resource={}, action={:?}",
            user_id, resource, action
        );
        false
    }

    /// Recursively check permissions including inherited roles
    fn has_permission_recursive(
        &self,
        roles: &HashMap<String, Role>,
        role_name: &str,
        resource: &str,
        action: Action,
        visited: &mut HashSet<String>,
    ) -> bool {
        if visited.contains(role_name) {
            return false; // Prevent infinite loops
        }
        visited.insert(role_name.to_string());
        
        if let Some(role) = roles.get(role_name) {
            // Check direct permissions
            if let Some(actions) = role.permissions.get(resource) {
                if actions.contains(&action) {
                    return true;
                }
            }
            
            // Check wildcard permissions
            if let Some(actions) = role.permissions.get("*") {
                if actions.contains(&action) {
                    return true;
                }
            }
            
            // Check inherited roles
            for inherited in &role.inherits {
                if self.has_permission_recursive(roles, inherited, resource, action, visited) {
                    return true;
                }
            }
        }
        
        false
    }

    /// Check if a resource matches a pattern
    fn matches_resource(&self, pattern: &str, resource: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if pattern.ends_with('*') {
            resource.starts_with(&pattern[..pattern.len() - 1])
        } else {
            pattern == resource
        }
    }

    /// Evaluate ABAC condition (simplified)
    fn evaluate_condition(&self, condition: &str, user: &User) -> bool {
        // Simplified condition evaluation
        // In production, would use a proper expression language
        if condition.is_empty() || condition == "true" {
            return true;
        }
        
        // Check attribute-based conditions like "department=engineering"
        if let Some((key, value)) = condition.split_once('=') {
            if let Some(attr_value) = user.attributes.get(key.trim()) {
                return attr_value == value.trim();
            }
        }
        
        false
    }

    /// Add ABAC policy
    pub async fn add_abac_policy(&self, policy: AbacPolicy) {
        let mut policies = self.abac_policies.write().await;
        info!("Added ABAC policy: {}", policy.name);
        policies.push(policy);
    }

    /// Get user by ID
    pub async fn get_user(&self, user_id: &str) -> Option<User> {
        let users = self.users.read().await;
        users.get(user_id).cloned()
    }

    /// List all roles
    pub async fn list_roles(&self) -> Vec<String> {
        let roles = self.roles.read().await;
        roles.keys().cloned().collect()
    }

    /// Get permissions for a specific role
    pub async fn get_role_permissions(&self, role_name: &str) -> Option<Vec<String>> {
        let roles = self.roles.read().await;
        roles.get(role_name).map(|role| {
            let mut perms: Vec<String> = Vec::new();
            for (resource, actions) in &role.permissions {
                for action in actions {
                    perms.push(format!("{:?} on {}", action, resource));
                }
            }
            if perms.is_empty() {
                perms.push("none".to_string());
            }
            perms
        })
    }
}

impl Default for RbacManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rbac_basic() {
        let mgr = RbacManager::new();
        mgr.init_default_roles().await;
        
        // Create user with admin role
        let user = User {
            id: "user1".to_string(),
            username: "admin_user".to_string(),
            roles: vec!["admin".to_string()],
            attributes: HashMap::new(),
            created_at: chrono::Utc::now(),
            last_login: None,
        };
        
        mgr.create_user(user).await.unwrap();
        
        // Admin should have all permissions
        assert!(mgr.check_permission("user1", "users", Action::Select).await);
        assert!(mgr.check_permission("user1", "users", Action::Delete).await);
    }

    #[tokio::test]
    async fn test_readonly_role() {
        let mgr = RbacManager::new();
        mgr.init_default_roles().await;
        
        let user = User {
            id: "user2".to_string(),
            username: "readonly_user".to_string(),
            roles: vec!["readonly".to_string()],
            attributes: HashMap::new(),
            created_at: chrono::Utc::now(),
            last_login: None,
        };
        
        mgr.create_user(user).await.unwrap();
        
        // Readonly can select but not delete
        assert!(mgr.check_permission("user2", "users", Action::Select).await);
        assert!(!mgr.check_permission("user2", "users", Action::Delete).await);
    }
}
