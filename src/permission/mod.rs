//! Permission Module - Fine-grained permissions management

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Permission
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct Permission {
    pub resource_type: ResourceType,
    pub resource_id: Option<String>,
    pub action: PermissionAction,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ResourceType {
    Database,
    Schema,
    Table,
    Column,
    View,
    Function,
    Procedure,
    Index,
    Sequence,
    Type,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum PermissionAction {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Execute,
    Grant,
    Truncate,
    References,
    Trigger,
    All,
}

/// User permissions
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UserPermissions {
    pub user_id: String,
    pub permissions: HashSet<Permission>,
    pub denied: HashSet<Permission>,
    pub groups: Vec<String>,
}

/// Permission grant
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionGrant {
    pub grantee: String,
    pub permission: Permission,
    pub granted_by: String,
    pub granted_at: i64,
    pub with_grant_option: bool,
}

/// Permission manager
pub struct PermissionManager {
    users: RwLock<HashMap<String, UserPermissions>>,
    groups: RwLock<HashMap<String, HashSet<Permission>>>,
    grants: RwLock<Vec<PermissionGrant>>,
}

impl PermissionManager {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            grants: RwLock::new(Vec::new()),
        }
    }

    /// Grant permission to user
    pub async fn grant(&self, grantee: &str, permission: Permission, granted_by: &str, with_grant_option: bool) {
        let mut users = self.users.write().await;
        let user = users.entry(grantee.to_string())
            .or_insert_with(|| UserPermissions {
                user_id: grantee.to_string(),
                ..Default::default()
            });
        
        user.permissions.insert(permission.clone());
        user.denied.remove(&permission);
        drop(users);

        let mut grants = self.grants.write().await;
        grants.push(PermissionGrant {
            grantee: grantee.to_string(),
            permission,
            granted_by: granted_by.to_string(),
            granted_at: chrono::Utc::now().timestamp(),
            with_grant_option,
        });
    }

    /// Revoke permission from user
    pub async fn revoke(&self, grantee: &str, permission: &Permission) {
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(grantee) {
            user.permissions.remove(permission);
        }
    }

    /// Deny permission (explicit deny)
    pub async fn deny(&self, grantee: &str, permission: Permission) {
        let mut users = self.users.write().await;
        let user = users.entry(grantee.to_string())
            .or_insert_with(|| UserPermissions {
                user_id: grantee.to_string(),
                ..Default::default()
            });
        
        user.denied.insert(permission.clone());
        user.permissions.remove(&permission);
    }

    /// Check if user has permission
    pub async fn check(&self, user_id: &str, permission: &Permission) -> bool {
        let users = self.users.read().await;
        
        if let Some(user) = users.get(user_id) {
            // Check explicit deny first
            if user.denied.contains(permission) {
                return false;
            }
            
            // Check explicit grant
            if user.permissions.contains(permission) {
                return true;
            }
            
            // Check ALL permission
            let all_perm = Permission {
                resource_type: permission.resource_type.clone(),
                resource_id: permission.resource_id.clone(),
                action: PermissionAction::All,
            };
            if user.permissions.contains(&all_perm) {
                return true;
            }
            
            // Check group permissions
            let groups = self.groups.read().await;
            for group_name in &user.groups {
                if let Some(group_perms) = groups.get(group_name) {
                    if group_perms.contains(permission) || group_perms.contains(&all_perm) {
                        return true;
                    }
                }
            }
        }
        
        false
    }

    /// Add user to group
    pub async fn add_to_group(&self, user_id: &str, group: &str) {
        let mut users = self.users.write().await;
        let user = users.entry(user_id.to_string())
            .or_insert_with(|| UserPermissions {
                user_id: user_id.to_string(),
                ..Default::default()
            });
        
        if !user.groups.contains(&group.to_string()) {
            user.groups.push(group.to_string());
        }
    }

    /// Remove user from group
    pub async fn remove_from_group(&self, user_id: &str, group: &str) {
        let mut users = self.users.write().await;
        if let Some(user) = users.get_mut(user_id) {
            user.groups.retain(|g| g != group);
        }
    }

    /// Create a group
    pub async fn create_group(&self, name: &str) {
        let mut groups = self.groups.write().await;
        groups.entry(name.to_string()).or_insert_with(HashSet::new);
    }

    /// Grant permission to group
    pub async fn grant_to_group(&self, group: &str, permission: Permission) {
        let mut groups = self.groups.write().await;
        let group_perms = groups.entry(group.to_string()).or_insert_with(HashSet::new);
        group_perms.insert(permission);
    }

    /// Get user permissions
    pub async fn get_user_permissions(&self, user_id: &str) -> Option<UserPermissions> {
        let users = self.users.read().await;
        users.get(user_id).cloned()
    }

    /// List grants
    pub async fn list_grants(&self, grantee: Option<&str>) -> Vec<PermissionGrant> {
        let grants = self.grants.read().await;
        match grantee {
            Some(g) => grants.iter().filter(|gr| gr.grantee == g).cloned().collect(),
            None => grants.clone(),
        }
    }
}

impl Default for PermissionManager {
    fn default() -> Self {
        Self::new()
    }
}
