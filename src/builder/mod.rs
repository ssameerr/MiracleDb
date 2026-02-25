//! Query Builder Module - Fluent query construction

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Query builder for constructing SQL queries programmatically
pub struct QueryBuilder {
    query_type: QueryType,
    table: String,
    select_columns: Vec<String>,
    where_clauses: Vec<WhereClause>,
    joins: Vec<JoinClause>,
    order_by: Vec<OrderByClause>,
    group_by: Vec<String>,
    having: Option<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    values: Vec<HashMap<String, serde_json::Value>>,
    set_values: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug)]
struct WhereClause {
    column: String,
    operator: String,
    value: serde_json::Value,
    conjunction: String,
}

#[derive(Clone, Debug)]
struct JoinClause {
    join_type: String,
    table: String,
    on_left: String,
    on_right: String,
}

#[derive(Clone, Debug)]
struct OrderByClause {
    column: String,
    direction: String,
}

impl QueryBuilder {
    /// Start a SELECT query
    pub fn select(columns: &[&str]) -> Self {
        Self {
            query_type: QueryType::Select,
            table: String::new(),
            select_columns: columns.iter().map(|s| s.to_string()).collect(),
            where_clauses: vec![],
            joins: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: None,
            offset: None,
            values: vec![],
            set_values: HashMap::new(),
        }
    }

    /// Start an INSERT query
    pub fn insert_into(table: &str) -> Self {
        Self {
            query_type: QueryType::Insert,
            table: table.to_string(),
            select_columns: vec![],
            where_clauses: vec![],
            joins: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: None,
            offset: None,
            values: vec![],
            set_values: HashMap::new(),
        }
    }

    /// Start an UPDATE query
    pub fn update(table: &str) -> Self {
        Self {
            query_type: QueryType::Update,
            table: table.to_string(),
            select_columns: vec![],
            where_clauses: vec![],
            joins: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: None,
            offset: None,
            values: vec![],
            set_values: HashMap::new(),
        }
    }

    /// Start a DELETE query
    pub fn delete() -> Self {
        Self {
            query_type: QueryType::Delete,
            table: String::new(),
            select_columns: vec![],
            where_clauses: vec![],
            joins: vec![],
            order_by: vec![],
            group_by: vec![],
            having: None,
            limit: None,
            offset: None,
            values: vec![],
            set_values: HashMap::new(),
        }
    }

    pub fn from(mut self, table: &str) -> Self {
        self.table = table.to_string();
        self
    }

    pub fn where_eq(mut self, column: &str, value: serde_json::Value) -> Self {
        self.where_clauses.push(WhereClause {
            column: column.to_string(),
            operator: "=".to_string(),
            value,
            conjunction: "AND".to_string(),
        });
        self
    }

    pub fn where_ne(mut self, column: &str, value: serde_json::Value) -> Self {
        self.where_clauses.push(WhereClause {
            column: column.to_string(),
            operator: "<>".to_string(),
            value,
            conjunction: "AND".to_string(),
        });
        self
    }

    pub fn where_gt(mut self, column: &str, value: serde_json::Value) -> Self {
        self.where_clauses.push(WhereClause {
            column: column.to_string(),
            operator: ">".to_string(),
            value,
            conjunction: "AND".to_string(),
        });
        self
    }

    pub fn where_lt(mut self, column: &str, value: serde_json::Value) -> Self {
        self.where_clauses.push(WhereClause {
            column: column.to_string(),
            operator: "<".to_string(),
            value,
            conjunction: "AND".to_string(),
        });
        self
    }

    pub fn or_where(mut self, column: &str, operator: &str, value: serde_json::Value) -> Self {
        self.where_clauses.push(WhereClause {
            column: column.to_string(),
            operator: operator.to_string(),
            value,
            conjunction: "OR".to_string(),
        });
        self
    }

    pub fn join(mut self, table: &str, left: &str, right: &str) -> Self {
        self.joins.push(JoinClause {
            join_type: "INNER".to_string(),
            table: table.to_string(),
            on_left: left.to_string(),
            on_right: right.to_string(),
        });
        self
    }

    pub fn left_join(mut self, table: &str, left: &str, right: &str) -> Self {
        self.joins.push(JoinClause {
            join_type: "LEFT".to_string(),
            table: table.to_string(),
            on_left: left.to_string(),
            on_right: right.to_string(),
        });
        self
    }

    pub fn order_by(mut self, column: &str, direction: &str) -> Self {
        self.order_by.push(OrderByClause {
            column: column.to_string(),
            direction: direction.to_uppercase(),
        });
        self
    }

    pub fn group_by(mut self, columns: &[&str]) -> Self {
        self.group_by = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn having(mut self, clause: &str) -> Self {
        self.having = Some(clause.to_string());
        self
    }

    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn values(mut self, row: HashMap<String, serde_json::Value>) -> Self {
        self.values.push(row);
        self
    }

    pub fn set(mut self, column: &str, value: serde_json::Value) -> Self {
        self.set_values.insert(column.to_string(), value);
        self
    }

    /// Build the SQL query
    pub fn build(&self) -> String {
        match self.query_type {
            QueryType::Select => self.build_select(),
            QueryType::Insert => self.build_insert(),
            QueryType::Update => self.build_update(),
            QueryType::Delete => self.build_delete(),
        }
    }

    fn build_select(&self) -> String {
        let mut sql = format!("SELECT {} FROM {}", 
            self.select_columns.join(", "),
            self.table
        );

        for join in &self.joins {
            sql.push_str(&format!(" {} JOIN {} ON {} = {}", 
                join.join_type, join.table, join.on_left, join.on_right
            ));
        }

        if !self.where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            for (i, clause) in self.where_clauses.iter().enumerate() {
                if i > 0 {
                    sql.push_str(&format!(" {} ", clause.conjunction));
                }
                sql.push_str(&format!("{} {} {}", clause.column, clause.operator, 
                    Self::format_value(&clause.value)));
            }
        }

        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }

        if let Some(having) = &self.having {
            sql.push_str(&format!(" HAVING {}", having));
        }

        if !self.order_by.is_empty() {
            let order: Vec<_> = self.order_by.iter()
                .map(|o| format!("{} {}", o.column, o.direction))
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order.join(", ")));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        sql
    }

    fn build_insert(&self) -> String {
        if self.values.is_empty() {
            return format!("INSERT INTO {} DEFAULT VALUES", self.table);
        }

        let columns: Vec<_> = self.values[0].keys().cloned().collect();
        let mut sql = format!("INSERT INTO {} ({}) VALUES ", self.table, columns.join(", "));

        let value_rows: Vec<_> = self.values.iter().map(|row| {
            let vals: Vec<_> = columns.iter()
                .map(|c| row.get(c).map(Self::format_value).unwrap_or("NULL".to_string()))
                .collect();
            format!("({})", vals.join(", "))
        }).collect();

        sql.push_str(&value_rows.join(", "));
        sql
    }

    fn build_update(&self) -> String {
        let sets: Vec<_> = self.set_values.iter()
            .map(|(k, v)| format!("{} = {}", k, Self::format_value(v)))
            .collect();

        let mut sql = format!("UPDATE {} SET {}", self.table, sets.join(", "));

        if !self.where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            for (i, clause) in self.where_clauses.iter().enumerate() {
                if i > 0 {
                    sql.push_str(&format!(" {} ", clause.conjunction));
                }
                sql.push_str(&format!("{} {} {}", clause.column, clause.operator, 
                    Self::format_value(&clause.value)));
            }
        }

        sql
    }

    fn build_delete(&self) -> String {
        let mut sql = format!("DELETE FROM {}", self.table);

        if !self.where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            for (i, clause) in self.where_clauses.iter().enumerate() {
                if i > 0 {
                    sql.push_str(&format!(" {} ", clause.conjunction));
                }
                sql.push_str(&format!("{} {} {}", clause.column, clause.operator, 
                    Self::format_value(&clause.value)));
            }
        }

        sql
    }

    fn format_value(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            _ => value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select() {
        let sql = QueryBuilder::select(&["id", "name"])
            .from("users")
            .where_eq("active", serde_json::json!(true))
            .order_by("name", "ASC")
            .limit(10)
            .build();

        assert!(sql.contains("SELECT id, name FROM users"));
        assert!(sql.contains("WHERE active = TRUE"));
    }

    #[test]
    fn test_insert() {
        let mut row = HashMap::new();
        row.insert("name".to_string(), serde_json::json!("John"));
        row.insert("age".to_string(), serde_json::json!(30));

        let sql = QueryBuilder::insert_into("users")
            .values(row)
            .build();

        assert!(sql.contains("INSERT INTO users"));
    }
}
