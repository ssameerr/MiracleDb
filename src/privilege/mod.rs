//! Privilege Module - Object-level privileges

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Privilege type
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    References,
    Trigger,
    Create,
    Connect,
    Temporary,
    Execute,
    Usage,
    All,
}

impl Privilege {
    pub fn all_table_privileges() -> Vec<Self> {
        vec![
            Privilege::Select,
            Privilege::Insert,
            Privilege::Update,
            Privilege::Delete,
            Privilege::Truncate,
            Privilege::References,
            Privilege::Trigger,
        ]
    }

    pub fn all_database_privileges() -> Vec<Self> {
        vec![
            Privilege::Create,
            Privilege::Connect,
            Privilege::Temporary,
        ]
    }

    pub fn all_function_privileges() -> Vec<Self> {
        vec![Privilege::Execute]
    }
}

/// Privilege grant
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrivilegeGrant {
    pub grantee: String,
    pub grantor: String,
    pub object_type: String,
    pub object_name: String,
    pub privilege: Privilege,
    pub with_grant_option: bool,
    pub granted_at: i64,
}

/// Privilege key
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct PrivilegeKey {
    grantee: String,
    object_type: String,
    object_name: String,
    privilege: Privilege,
}

/// Privilege manager
pub struct PrivilegeManager {
    grants: RwLock<HashMap<PrivilegeKey, PrivilegeGrant>>,
    role_members: RwLock<HashMap<String, HashSet<String>>>,
}

impl PrivilegeManager {
    pub fn new() -> Self {
        Self {
            grants: RwLock::new(HashMap::new()),
            role_members: RwLock::new(HashMap::new()),
        }
    }

    /// Grant privilege
    pub async fn grant(
        &self,
        grantor: &str,
        grantee: &str,
        object_type: &str,
        object_name: &str,
        privilege: Privilege,
        with_grant_option: bool,
    ) {
        let key = PrivilegeKey {
            grantee: grantee.to_string(),
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            privilege,
        };

        let grant = PrivilegeGrant {
            grantee: grantee.to_string(),
            grantor: grantor.to_string(),
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            privilege,
            with_grant_option,
            granted_at: chrono::Utc::now().timestamp(),
        };

        let mut grants = self.grants.write().await;
        grants.insert(key, grant);
    }

    /// Revoke privilege
    pub async fn revoke(
        &self,
        grantee: &str,
        object_type: &str,
        object_name: &str,
        privilege: Privilege,
    ) {
        let key = PrivilegeKey {
            grantee: grantee.to_string(),
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            privilege,
        };

        let mut grants = self.grants.write().await;
        grants.remove(&key);
    }

    /// Check if user has privilege
    pub async fn has_privilege(
        &self,
        user: &str,
        object_type: &str,
        object_name: &str,
        privilege: Privilege,
    ) -> bool {
        let grants = self.grants.read().await;
        let role_members = self.role_members.read().await;

        // Check direct grant
        let key = PrivilegeKey {
            grantee: user.to_string(),
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            privilege,
        };

        if grants.contains_key(&key) {
            return true;
        }

        // Check ALL privilege
        let all_key = PrivilegeKey {
            grantee: user.to_string(),
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            privilege: Privilege::All,
        };

        if grants.contains_key(&all_key) {
            return true;
        }

        // Check role memberships
        for (role, members) in role_members.iter() {
            if members.contains(user) {
                let role_key = PrivilegeKey {
                    grantee: role.clone(),
                    object_type: object_type.to_string(),
                    object_name: object_name.to_string(),
                    privilege,
                };
                if grants.contains_key(&role_key) {
                    return true;
                }
            }
        }

        // Check PUBLIC
        let public_key = PrivilegeKey {
            grantee: "PUBLIC".to_string(),
            object_type: object_type.to_string(),
            object_name: object_name.to_string(),
            privilege,
        };

        grants.contains_key(&public_key)
    }

    /// Add user to role
    pub async fn grant_role(&self, role: &str, member: &str) {
        let mut role_members = self.role_members.write().await;
        role_members.entry(role.to_string())
            .or_insert_with(HashSet::new)
            .insert(member.to_string());
    }

    /// Remove user from role
    pub async fn revoke_role(&self, role: &str, member: &str) {
        let mut role_members = self.role_members.write().await;
        if let Some(members) = role_members.get_mut(role) {
            members.remove(member);
        }
    }

    /// Get all privileges for object
    pub async fn get_object_privileges(&self, object_type: &str, object_name: &str) -> Vec<PrivilegeGrant> {
        let grants = self.grants.read().await;
        grants.values()
            .filter(|g| g.object_type == object_type && g.object_name == object_name)
            .cloned()
            .collect()
    }

    /// Get all privileges for user
    pub async fn get_user_privileges(&self, user: &str) -> Vec<PrivilegeGrant> {
        let grants = self.grants.read().await;
        grants.values()
            .filter(|g| g.grantee == user)
            .cloned()
            .collect()
    }

    /// Revoke all privileges on object
    pub async fn revoke_all_on_object(&self, object_type: &str, object_name: &str) {
        let mut grants = self.grants.write().await;
        grants.retain(|_, g| g.object_type != object_type || g.object_name != object_name);
    }
}

impl Default for PrivilegeManager {
    fn default() -> Self {
        Self::new()
    }
}
