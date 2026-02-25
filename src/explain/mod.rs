//! Explain Module - Query plan explanation

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Explain options
#[derive(Clone, Debug, Default)]
pub struct ExplainOptions {
    pub analyze: bool,
    pub verbose: bool,
    pub costs: bool,
    pub buffers: bool,
    pub timing: bool,
    pub summary: bool,
    pub format: ExplainFormat,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum ExplainFormat {
    #[default]
    Text,
    Json,
    Xml,
    Yaml,
}

/// Plan node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanNode {
    pub node_type: String,
    pub relation_name: Option<String>,
    pub alias: Option<String>,
    pub startup_cost: f64,
    pub total_cost: f64,
    pub plan_rows: u64,
    pub plan_width: u32,
    pub actual_startup_time: Option<f64>,
    pub actual_total_time: Option<f64>,
    pub actual_rows: Option<u64>,
    pub actual_loops: Option<u64>,
    pub output: Vec<String>,
    pub filter: Option<String>,
    pub rows_removed_by_filter: Option<u64>,
    pub join_type: Option<String>,
    pub hash_cond: Option<String>,
    pub index_name: Option<String>,
    pub index_cond: Option<String>,
    pub sort_key: Option<Vec<String>>,
    pub sort_method: Option<String>,
    pub sort_space_used: Option<u64>,
    pub sort_space_type: Option<String>,
    pub shared_hit_blocks: Option<u64>,
    pub shared_read_blocks: Option<u64>,
    pub shared_dirtied_blocks: Option<u64>,
    pub shared_written_blocks: Option<u64>,
    pub plans: Vec<PlanNode>,
}

impl PlanNode {
    pub fn new(node_type: &str) -> Self {
        Self {
            node_type: node_type.to_string(),
            relation_name: None,
            alias: None,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0,
            plan_width: 0,
            actual_startup_time: None,
            actual_total_time: None,
            actual_rows: None,
            actual_loops: None,
            output: vec![],
            filter: None,
            rows_removed_by_filter: None,
            join_type: None,
            hash_cond: None,
            index_name: None,
            index_cond: None,
            sort_key: None,
            sort_method: None,
            sort_space_used: None,
            sort_space_type: None,
            shared_hit_blocks: None,
            shared_read_blocks: None,
            shared_dirtied_blocks: None,
            shared_written_blocks: None,
            plans: vec![],
        }
    }

    pub fn with_costs(mut self, startup: f64, total: f64, rows: u64, width: u32) -> Self {
        self.startup_cost = startup;
        self.total_cost = total;
        self.plan_rows = rows;
        self.plan_width = width;
        self
    }

    pub fn with_actuals(mut self, startup_time: f64, total_time: f64, rows: u64, loops: u64) -> Self {
        self.actual_startup_time = Some(startup_time);
        self.actual_total_time = Some(total_time);
        self.actual_rows = Some(rows);
        self.actual_loops = Some(loops);
        self
    }

    pub fn with_table(mut self, table: &str, alias: Option<&str>) -> Self {
        self.relation_name = Some(table.to_string());
        self.alias = alias.map(|s| s.to_string());
        self
    }

    pub fn add_child(&mut self, child: PlanNode) {
        self.plans.push(child);
    }
}

/// Explain result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExplainResult {
    pub plan: PlanNode,
    pub planning_time: Option<f64>,
    pub execution_time: Option<f64>,
    pub triggers: Vec<TriggerExplain>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriggerExplain {
    pub trigger_name: String,
    pub relation: String,
    pub time: f64,
    pub calls: u64,
}

/// Explain formatter
pub struct ExplainFormatter;

impl ExplainFormatter {
    /// Format as text
    pub fn to_text(result: &ExplainResult, options: &ExplainOptions) -> String {
        let mut output = String::new();
        Self::format_node(&mut output, &result.plan, 0, options);

        if let (Some(planning), Some(execution)) = (result.planning_time, result.execution_time) {
            if options.summary {
                output.push_str(&format!("\nPlanning Time: {:.3} ms\n", planning));
                output.push_str(&format!("Execution Time: {:.3} ms\n", execution));
            }
        }

        output
    }

    fn format_node(output: &mut String, node: &PlanNode, indent: usize, options: &ExplainOptions) {
        let prefix = "  ".repeat(indent);

        // Node type and table
        let mut line = format!("{}{}", prefix, node.node_type);
        if let Some(ref table) = node.relation_name {
            line.push_str(&format!(" on {}", table));
            if let Some(ref alias) = node.alias {
                if alias != table {
                    line.push_str(&format!(" {}", alias));
                }
            }
        }

        // Costs
        if options.costs {
            line.push_str(&format!(
                "  (cost={:.2}..{:.2} rows={} width={})",
                node.startup_cost, node.total_cost, node.plan_rows, node.plan_width
            ));
        }

        // Actuals
        if options.analyze {
            if let (Some(time), Some(rows), Some(loops)) = 
                (node.actual_total_time, node.actual_rows, node.actual_loops) {
                line.push_str(&format!(
                    " (actual time={:.3}..{:.3} rows={} loops={})",
                    node.actual_startup_time.unwrap_or(0.0), time, rows, loops
                ));
            }
        }

        output.push_str(&line);
        output.push('\n');

        // Filter
        if let Some(ref filter) = node.filter {
            output.push_str(&format!("{}  Filter: {}\n", prefix, filter));
            if let Some(removed) = node.rows_removed_by_filter {
                output.push_str(&format!("{}  Rows Removed by Filter: {}\n", prefix, removed));
            }
        }

        // Index
        if let Some(ref index) = node.index_name {
            output.push_str(&format!("{}  Index Name: {}\n", prefix, index));
        }
        if let Some(ref cond) = node.index_cond {
            output.push_str(&format!("{}  Index Cond: {}\n", prefix, cond));
        }

        // Buffers
        if options.buffers {
            if let Some(hit) = node.shared_hit_blocks {
                output.push_str(&format!(
                    "{}  Buffers: shared hit={}\n", prefix, hit
                ));
            }
        }

        // Children
        for child in &node.plans {
            Self::format_node(output, child, indent + 1, options);
        }
    }

    /// Format as JSON
    pub fn to_json(result: &ExplainResult) -> String {
        serde_json::to_string_pretty(result).unwrap_or_default()
    }

    /// Format according to options
    pub fn format(result: &ExplainResult, options: &ExplainOptions) -> String {
        match options.format {
            ExplainFormat::Text => Self::to_text(result, options),
            ExplainFormat::Json => Self::to_json(result),
            _ => Self::to_text(result, options),
        }
    }
}
