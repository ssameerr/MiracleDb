//! Tcop Module - Traffic Cop (Request Processing Loop)

use std::sync::Arc;
use crate::parser::Parser;
use crate::rewrite::Rewriter;
// use crate::planner::PlannerInfo; // Removed
// use crate::executor::ExecutorState; // Removed unused import
use crate::portal::PortalManager;

/// Request types
pub enum Request {
    Parse(String, String), // Name, SQL
    Bind(String, String, Vec<serde_json::Value>), // Portal, Stmt, Params
    Describe(char, String), // Type ('S'tmt / 'P'ortal), Name
    Execute(String, i32), // Portal, MaxRows
    Sync,
    Close(char, String),
}

/// Traffic Cop
pub struct TrafficCop {
    parser: Arc<Parser>,
    rewriter: Arc<Rewriter>,
    portal_manager: Arc<PortalManager>,
}

impl TrafficCop {
    pub fn new(
        parser: Arc<Parser>,
        rewriter: Arc<Rewriter>,
        portal_manager: Arc<PortalManager>
    ) -> Self {
        Self {
            parser,
            rewriter,
            portal_manager,
        }
    }

    /// Process a simple query (exec_simple_query)
    pub async fn exec_simple_query(&self, sql: &str) -> Result<String, String> {
        // 1. Parse
        let stmts = self.parser.parse(sql)?;

        // 2. Rewrite / Analyze (Skipped for MVP - directly plan)
        // let queries = self.rewriter.rewrite(stmts).await?;

        // 3. Plan & Execute loop
        for stmt in stmts {
             // let plan = planner.plan(stmt).await?; // Assuming we had planner access
             // let result = executor.exec(plan).await?;
        }

        Ok("COMMAND OK (Parsed)".to_string())
    }

    /// Handle extended protocol request
    pub async fn handle_request(&self, request: Request) -> Result<String, String> {
        match request {
            Request::Parse(name, sql) => {
                let _tree = self.parser.parse(&sql)?;
                // Store prepared statement
                Ok("PARSE_COMPLETE".to_string())
            }
            Request::Bind(portal_name, _stmt_name, params) => {
                self.portal_manager.create(&portal_name, "").await?;
                self.portal_manager.define(&portal_name, params).await?;
                Ok("BIND_COMPLETE".to_string())
            }
            Request::Execute(portal_name, _max_rows) => {
                let portal = self.portal_manager.get(&portal_name).await
                    .ok_or("Portal not found")?;
                // executor.exec_portal(portal)
                Ok("EXECUTE_COMPLETE".to_string())
            }
            Request::Sync => {
                Ok("READY_FOR_QUERY".to_string())
            }
            _ => Ok("OK".to_string()),
        }
    }
}
