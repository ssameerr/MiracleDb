//! Rule Module - Query rewrite rules

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Rule event
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum RuleEvent {
    Select,
    Insert,
    Update,
    Delete,
}

/// Rule definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rule {
    pub name: String,
    pub schema: String,
    pub table: String,
    pub event: RuleEvent,
    pub condition: Option<String>,
    pub instead: bool,
    pub commands: Vec<String>,
    pub enabled: bool,
    pub created_at: i64,
}

impl Rule {
    pub fn new(name: &str, schema: &str, table: &str, event: RuleEvent) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            event,
            condition: None,
            instead: false,
            commands: vec![],
            enabled: true,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn with_condition(mut self, condition: &str) -> Self {
        self.condition = Some(condition.to_string());
        self
    }

    pub fn instead_of(mut self) -> Self {
        self.instead = true;
        self
    }

    pub fn also(mut self) -> Self {
        self.instead = false;
        self
    }

    pub fn do_command(mut self, command: &str) -> Self {
        self.commands.push(command.to_string());
        self
    }

    pub fn do_nothing(mut self) -> Self {
        self.commands.clear();
        self
    }

    /// Get full qualified name
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.schema, self.table, self.name)
    }
}

/// Rule rewriter
pub struct RuleRewriter;

impl RuleRewriter {
    /// Apply rules to a query
    pub fn rewrite(query: &str, rules: &[Rule], event: RuleEvent) -> Vec<String> {
        let applicable: Vec<&Rule> = rules.iter()
            .filter(|r| r.enabled && r.event == event)
            .collect();

        if applicable.is_empty() {
            return vec![query.to_string()];
        }

        let mut result = Vec::new();

        // Check for INSTEAD rules
        let instead_rules: Vec<&&Rule> = applicable.iter()
            .filter(|r| r.instead)
            .collect();

        if !instead_rules.is_empty() {
            // INSTEAD rules replace the original query
            for rule in instead_rules {
                if rule.commands.is_empty() {
                    // DO NOTHING
                    return vec![];
                }
                for cmd in &rule.commands {
                    result.push(Self::substitute(cmd, query));
                }
            }
        } else {
            // ALSO rules add to the original query
            result.push(query.to_string());
            
            for rule in applicable.iter().filter(|r| !r.instead) {
                for cmd in &rule.commands {
                    result.push(Self::substitute(cmd, query));
                }
            }
        }

        result
    }

    fn substitute(template: &str, _original: &str) -> String {
        // In production: replace NEW, OLD references
        template.to_string()
    }
}

/// Rule manager
pub struct RuleManager {
    rules: RwLock<HashMap<String, Vec<Rule>>>,
}

impl RuleManager {
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
        }
    }

    /// Create a rule
    pub async fn create(&self, rule: Rule, replace: bool) -> Result<(), String> {
        let mut rules = self.rules.write().await;
        let table_key = format!("{}.{}", rule.schema, rule.table);

        let table_rules = rules.entry(table_key).or_insert_with(Vec::new);

        let existing = table_rules.iter().position(|r| r.name == rule.name);

        match (existing, replace) {
            (Some(idx), true) => {
                table_rules[idx] = rule;
            }
            (Some(_), false) => {
                return Err(format!("Rule {} already exists", rule.name));
            }
            (None, _) => {
                table_rules.push(rule);
            }
        }

        Ok(())
    }

    /// Drop a rule
    pub async fn drop(&self, schema: &str, table: &str, name: &str) -> Result<(), String> {
        let mut rules = self.rules.write().await;
        let table_key = format!("{}.{}", schema, table);

        let table_rules = rules.get_mut(&table_key)
            .ok_or_else(|| format!("No rules for table {}", table_key))?;

        let pos = table_rules.iter().position(|r| r.name == name)
            .ok_or_else(|| format!("Rule {} not found", name))?;

        table_rules.remove(pos);
        Ok(())
    }

    /// Get rules for table
    pub async fn get_rules(&self, schema: &str, table: &str, event: Option<RuleEvent>) -> Vec<Rule> {
        let rules = self.rules.read().await;
        let table_key = format!("{}.{}", schema, table);

        rules.get(&table_key)
            .map(|table_rules| {
                table_rules.iter()
                    .filter(|r| event.map(|e| r.event == e).unwrap_or(true))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Enable/disable rule
    pub async fn set_enabled(&self, schema: &str, table: &str, name: &str, enabled: bool) -> Result<(), String> {
        let mut rules = self.rules.write().await;
        let table_key = format!("{}.{}", schema, table);

        let table_rules = rules.get_mut(&table_key)
            .ok_or_else(|| format!("No rules for table {}", table_key))?;

        let rule = table_rules.iter_mut().find(|r| r.name == name)
            .ok_or_else(|| format!("Rule {} not found", name))?;

        rule.enabled = enabled;
        Ok(())
    }

    /// Rewrite query using applicable rules
    pub async fn apply(&self, schema: &str, table: &str, query: &str, event: RuleEvent) -> Vec<String> {
        let rules = self.get_rules(schema, table, Some(event)).await;
        RuleRewriter::rewrite(query, &rules, event)
    }
}

impl Default for RuleManager {
    fn default() -> Self {
        Self::new()
    }
}
