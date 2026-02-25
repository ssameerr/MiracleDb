//! Planner Module - Query planner
//! Transforms AST into a Logical Plan

use std::sync::Arc;
use sqlparser::ast::{Statement, Query, SetExpr, TableFactor, Expr, SelectItem, Ident};
use crate::catalog::Catalog;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        table_oid: u64,
        columns: Vec<String>, // Projections handled here for simple Scan
        filter: Option<Expr>,
    },
    CreateTable {
        name: String,
        columns: Vec<(String, String)>, // Name, Type
    },
    Insert {
        table_name: String,
        table_oid: u64,
        values: Vec<Vec<String>>, // Simple string values for MVP
    },
}

pub struct Planner {
    catalog: Arc<Catalog>,
}

impl Planner {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub async fn plan(&self, stmt: Statement) -> Result<LogicalPlan, String> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                let table_name = name.to_string();
                let cols = columns.iter()
                    .map(|c| (c.name.to_string(), c.data_type.to_string()))
                    .collect();
                Ok(LogicalPlan::CreateTable { name: table_name, columns: cols })
            }
            Statement::Insert { table_name, source, .. } => {
                let name = table_name.to_string();
                let oid = self.catalog.get_table_oid(&name).await
                    .ok_or(format!("Table {} not found", name))?;
                
                // Extract values from source (Box<Query>)
                let values = if let Some(query) = source {
                    self.extract_values(*query)?
                } else {
                    return Err("INSERT without values".to_string());
                };

                Ok(LogicalPlan::Insert { 
                    table_name: name, 
                    table_oid: oid, 
                    values 
                })
            }
            Statement::Query(query) => {
                self.plan_query(*query).await
            }
            _ => Err("Unsupported statement".to_string()),
        }
    }

    async fn plan_query(&self, query: Query) -> Result<LogicalPlan, String> {
        match *query.body {
            SetExpr::Select(select) => {
                // FROM
                if select.from.is_empty() {
                    return Err("SELECT without FROM not supported".to_string());
                }
                let table = &select.from[0].relation;
                let table_name = match table {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err("Only simple table scan supported".to_string()),
                };

                let oid = self.catalog.get_table_oid(&table_name).await
                    .ok_or(format!("Table {} not found", table_name))?;

                // PROJECTION
                let mut columns = Vec::new();
                for item in select.projection {
                    match item {
                        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => columns.push(ident.value),
                        SelectItem::Wildcard(_) => {
                            // Expand wildcards (fetch schema from catalog)
                            if let Some(schema) = self.catalog.get_table_schema(&table_name).await {
                                for col in schema.columns {
                                    columns.push(col.name);
                                }
                            }
                        }
                        _ => return Err("Unsupported projection".to_string()),
                    }
                }

                // FILTER
                let filter = select.selection;

                Ok(LogicalPlan::Scan {
                    table_name,
                    table_oid: oid,
                    columns,
                    filter,
                })
            }
            _ => Err("Unsupported query type".to_string()),
        }
    }

    fn extract_values(&self, query: Query) -> Result<Vec<Vec<String>>, String> {
        match *query.body {
            SetExpr::Values(values) => {
                let mut rows = Vec::new();
                for row in values.rows {
                    let mut row_vals = Vec::new();
                    for expr in row {
                        match expr {
                            Expr::Value(v) => row_vals.push(v.to_string()),
                            Expr::Identifier(i) => row_vals.push(i.value), // Treat idents as strings for now? or error
                            _ => return Err("Unsupported value expression".to_string()),
                        }
                    }
                    rows.push(row_vals);
                }
                Ok(rows)
            }
            _ => Err("INSERT source must be VALUES".to_string()),
        }
    }
}
