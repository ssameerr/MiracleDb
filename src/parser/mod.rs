//! Parser Module - SQL Parser Integration
//! Wraps `sqlparser` to provide AST generation for MiracleDb.

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser as SqlParser, ParserError};
use sqlparser::ast::{Statement, VisitMut, VisitorMut};
use std::result::Result;

/// Parse SQL string into AST statements
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, String> {
    let dialect = PostgreSqlDialect {};
    SqlParser::parse_sql(&dialect, sql).map_err(|e| e.to_string())
}

/// Parser wrapper for compatibility
pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, String> {
        parse_sql(sql)
    }
}

/// Helper to ensure the query is valid (basic syntax check)
pub fn validate_sql(sql: &str) -> Result<(), String> {
    parse_sql(sql).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_parse() {
        let sql = "SELECT id, name FROM users WHERE id > 10";
        let ast = parse_sql(sql).unwrap();
        assert_eq!(ast.len(), 1);
        match &ast[0] {
            Statement::Query(_) => {},
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_insert_parse() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let ast = parse_sql(sql).unwrap();
        assert_eq!(ast.len(), 1);
        match &ast[0] {
            Statement::Insert { .. } => {},
            _ => panic!("Expected Insert"),
        }
    }
    
    #[test]
    fn test_transaction_parse() {
        let sql = "BEGIN; INSERT INTO foo VALUES (1); COMMIT;";
        let ast = parse_sql(sql).unwrap();
        assert_eq!(ast.len(), 3);
    }
}
