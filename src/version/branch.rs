use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    pub name: String,
    pub base_snapshot_id: String,
    pub commits: Vec<String>, // snapshot IDs on this branch
}

pub struct BranchManager {
    branches: HashMap<String, Branch>,
    current: String,
}

impl BranchManager {
    pub fn new() -> Self {
        let main = Branch { name: "main".to_string(), base_snapshot_id: String::new(), commits: vec![] };
        let mut branches = HashMap::new();
        branches.insert("main".to_string(), main);
        Self { branches, current: "main".to_string() }
    }

    pub fn create_branch(&mut self, name: &str, from_snapshot_id: &str) -> Result<(), String> {
        if self.branches.contains_key(name) {
            return Err(format!("Branch '{}' already exists", name));
        }
        self.branches.insert(name.to_string(), Branch {
            name: name.to_string(),
            base_snapshot_id: from_snapshot_id.to_string(),
            commits: vec![],
        });
        Ok(())
    }

    pub fn checkout(&mut self, name: &str) -> Result<(), String> {
        if !self.branches.contains_key(name) {
            return Err(format!("Branch '{}' not found", name));
        }
        self.current = name.to_string();
        Ok(())
    }

    pub fn current_branch(&self) -> &str { &self.current }
}
