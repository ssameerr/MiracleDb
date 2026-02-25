//! Query Plan Module - Query plan analysis and visualization

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Query plan node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanNode {
    pub id: String,
    pub node_type: PlanNodeType,
    pub description: String,
    pub estimated_rows: u64,
    pub estimated_cost: f64,
    pub actual_rows: Option<u64>,
    pub actual_time_ms: Option<f64>,
    pub children: Vec<PlanNode>,
    pub properties: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PlanNodeType {
    TableScan,
    IndexScan,
    IndexSeek,
    Filter,
    Project,
    HashJoin,
    MergeJoin,
    NestedLoopJoin,
    Sort,
    Aggregate,
    Limit,
    Union,
    Distinct,
    Subquery,
}

/// Query plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPlan {
    pub sql: String,
    pub root: PlanNode,
    pub total_estimated_cost: f64,
    pub warnings: Vec<String>,
}

impl QueryPlan {
    /// Create a plan from SQL (simplified)
    pub fn from_sql(sql: &str) -> Self {
        let sql_lower = sql.to_lowercase();
        
        let mut root = PlanNode {
            id: "1".to_string(),
            node_type: PlanNodeType::Project,
            description: "Project".to_string(),
            estimated_rows: 100,
            estimated_cost: 10.0,
            actual_rows: None,
            actual_time_ms: None,
            children: vec![],
            properties: HashMap::new(),
        };

        let mut warnings = Vec::new();
        let mut total_cost = 10.0;

        // Build plan based on SQL keywords
        if sql_lower.contains("join") {
            let join_type = if sql_lower.contains("hash") {
                PlanNodeType::HashJoin
            } else if sql_lower.contains("merge") {
                PlanNodeType::MergeJoin
            } else {
                PlanNodeType::HashJoin
            };

            root.children.push(PlanNode {
                id: "2".to_string(),
                node_type: join_type,
                description: "Join".to_string(),
                estimated_rows: 500,
                estimated_cost: 50.0,
                actual_rows: None,
                actual_time_ms: None,
                children: vec![
                    PlanNode {
                        id: "3".to_string(),
                        node_type: PlanNodeType::TableScan,
                        description: "Table Scan (left)".to_string(),
                        estimated_rows: 1000,
                        estimated_cost: 20.0,
                        actual_rows: None,
                        actual_time_ms: None,
                        children: vec![],
                        properties: HashMap::new(),
                    },
                    PlanNode {
                        id: "4".to_string(),
                        node_type: PlanNodeType::IndexScan,
                        description: "Index Scan (right)".to_string(),
                        estimated_rows: 500,
                        estimated_cost: 10.0,
                        actual_rows: None,
                        actual_time_ms: None,
                        children: vec![],
                        properties: HashMap::new(),
                    },
                ],
                properties: HashMap::new(),
            });
            total_cost += 80.0;
        } else if sql_lower.contains("where") {
            root.children.push(PlanNode {
                id: "2".to_string(),
                node_type: PlanNodeType::Filter,
                description: "Filter".to_string(),
                estimated_rows: 100,
                estimated_cost: 5.0,
                actual_rows: None,
                actual_time_ms: None,
                children: vec![
                    PlanNode {
                        id: "3".to_string(),
                        node_type: PlanNodeType::TableScan,
                        description: "Table Scan".to_string(),
                        estimated_rows: 1000,
                        estimated_cost: 20.0,
                        actual_rows: None,
                        actual_time_ms: None,
                        children: vec![],
                        properties: HashMap::new(),
                    },
                ],
                properties: HashMap::new(),
            });
            total_cost += 25.0;

            if sql_lower.contains("like '%") {
                warnings.push("Leading wildcard in LIKE prevents index usage".to_string());
            }
        } else {
            root.children.push(PlanNode {
                id: "2".to_string(),
                node_type: PlanNodeType::TableScan,
                description: "Table Scan".to_string(),
                estimated_rows: 1000,
                estimated_cost: 20.0,
                actual_rows: None,
                actual_time_ms: None,
                children: vec![],
                properties: HashMap::new(),
            });
            total_cost += 20.0;
        }

        if sql_lower.contains("order by") {
            let sort_node = PlanNode {
                id: uuid::Uuid::new_v4().to_string(),
                node_type: PlanNodeType::Sort,
                description: "Sort".to_string(),
                estimated_rows: 100,
                estimated_cost: 15.0,
                actual_rows: None,
                actual_time_ms: None,
                children: vec![],
                properties: HashMap::new(),
            };
            root.children.push(sort_node);
            total_cost += 15.0;
        }

        if sql_lower.contains("select *") {
            warnings.push("SELECT * fetches all columns; consider selecting only needed columns".to_string());
        }

        Self {
            sql: sql.to_string(),
            root,
            total_estimated_cost: total_cost,
            warnings,
        }
    }

    /// Convert plan to visual tree format
    pub fn to_tree_string(&self) -> String {
        let mut output = String::new();
        self.format_node(&self.root, "", true, &mut output);
        output
    }

    fn format_node(&self, node: &PlanNode, prefix: &str, is_last: bool, output: &mut String) {
        let connector = if is_last { "└── " } else { "├── " };
        output.push_str(&format!(
            "{}{}{} (rows: {}, cost: {:.1})\n",
            prefix,
            connector,
            node.description,
            node.estimated_rows,
            node.estimated_cost
        ));

        let new_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
        for (i, child) in node.children.iter().enumerate() {
            self.format_node(child, &new_prefix, i == node.children.len() - 1, output);
        }
    }

    /// Convert to DOT format for GraphViz
    pub fn to_dot(&self) -> String {
        let mut output = String::from("digraph QueryPlan {\n");
        output.push_str("  rankdir=TB;\n");
        output.push_str("  node [shape=box];\n");
        self.node_to_dot(&self.root, &mut output);
        output.push_str("}\n");
        output
    }

    fn node_to_dot(&self, node: &PlanNode, output: &mut String) {
        output.push_str(&format!(
            "  \"{}\" [label=\"{}\\nRows: {}\\nCost: {:.1}\"];\n",
            node.id, node.description, node.estimated_rows, node.estimated_cost
        ));

        for child in &node.children {
            output.push_str(&format!("  \"{}\" -> \"{}\";\n", node.id, child.id));
            self.node_to_dot(child, output);
        }
    }

    /// Get optimization suggestions
    pub fn get_suggestions(&self) -> Vec<String> {
        let mut suggestions = self.warnings.clone();
        
        // Add more suggestions based on plan analysis
        self.analyze_node(&self.root, &mut suggestions);
        
        suggestions
    }

    fn analyze_node(&self, node: &PlanNode, suggestions: &mut Vec<String>) {
        match node.node_type {
            PlanNodeType::TableScan if node.estimated_rows > 10000 => {
                suggestions.push(format!(
                    "Consider adding an index to reduce {} row table scan",
                    node.estimated_rows
                ));
            }
            PlanNodeType::NestedLoopJoin if node.estimated_rows > 1000 => {
                suggestions.push("Nested loop join with large dataset; consider hash join".to_string());
            }
            PlanNodeType::Sort if node.estimated_rows > 10000 => {
                suggestions.push("Large sort operation; consider adding index for ORDER BY columns".to_string());
            }
            _ => {}
        }

        for child in &node.children {
            self.analyze_node(child, suggestions);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_from_sql() {
        let plan = QueryPlan::from_sql("SELECT * FROM users WHERE id = 1");
        assert!(plan.warnings.iter().any(|w| w.contains("SELECT *")));
    }

    #[test]
    fn test_plan_to_tree() {
        let plan = QueryPlan::from_sql("SELECT name FROM users");
        let tree = plan.to_tree_string();
        assert!(tree.contains("Project"));
    }
}
