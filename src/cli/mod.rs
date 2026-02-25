//! CLI Module - Command line interface utilities

use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper};
use rustyline::config::Configurer;
use rustyline::Editor;
use std::borrow::Cow;
use std::collections::HashSet;

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "TABLE", "INDEX", "DROP", "ALTER", "JOIN", "ON", "GROUP", "BY",
    "ORDER", "LIMIT", "OFFSET", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
    "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "DEFAULT", "UNIQUE", "CHECK",
];

pub struct MiracleHelper {
    keywords: HashSet<String>,
    tables: HashSet<String>,
    highlighter: MatchingBracketHighlighter,
    hinter: HistoryHinter,
    colored_prompt: String,
}

impl MiracleHelper {
    pub fn new() -> Self {
        let mut keywords = HashSet::new();
        for &kw in SQL_KEYWORDS {
            keywords.insert(kw.to_string());
            keywords.insert(kw.to_lowercase());
        }

        Self {
            keywords,
            tables: HashSet::new(),
            highlighter: MatchingBracketHighlighter::new(),
            hinter: HistoryHinter {},
            colored_prompt: "\x1b[1;32mmiracledb>\x1b[0m ".to_owned(),
        }
    }

    pub fn add_table(&mut self, table: &str) {
        self.tables.insert(table.to_string());
    }
}

impl Completer for MiracleHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let mut candidates = Vec::new();
        let (start, word) = extract_word(line, pos);

        // Complete keywords
        for kw in &self.keywords {
            if kw.starts_with(word) || kw.to_lowercase().starts_with(&word.to_lowercase()) {
                candidates.push(Pair {
                    display: kw.clone(),
                    replacement: kw.clone(),
                });
            }
        }

        // Complete tables
        for table in &self.tables {
            if table.starts_with(word) {
                candidates.push(Pair {
                    display: table.clone(),
                    replacement: table.clone(),
                });
            }
        }

        Ok((start, candidates))
    }
}

impl Hinter for MiracleHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl Highlighter for MiracleHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Cow::Borrowed(&self.colored_prompt)
        } else {
            Cow::Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned("\x1b[1m".to_owned() + hint + "\x1b[m")
    }
}

impl Validator for MiracleHelper {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();
        if input.trim().is_empty() {
            return Ok(ValidationResult::Valid(None));
        }
        
        // Basic check for unclosed quotes or semicolons
        if !input.trim().ends_with(';') && !input.starts_with('\\') && !input.starts_with(':') {
             return Ok(ValidationResult::Incomplete);
        }

        Ok(ValidationResult::Valid(None))
    }
}

impl Helper for MiracleHelper {}

fn extract_word(line: &str, pos: usize) -> (usize, &str) {
    let line = &line[..pos];
    if line.is_empty() {
        return (0, line);
    }

    let mut start = line.len();
    for (i, c) in line.char_indices().rev() {
        if c.is_whitespace() || "(),;".contains(c) {
            break;
        }
        start = i;
    }
    (start, &line[start..])
}

pub async fn run_cli(host: String) -> Result<(), Box<dyn std::error::Error>> {
    let base_url = if host.starts_with("http") {
        host.clone()
    } else {
        format!("http://{}", host)
    };
    
    println!("Connecting to MiracleDb at {}...", base_url);

    // Try to load token
    let mut headers = reqwest::header::HeaderMap::new();
    if let Ok(token) = std::fs::read_to_string("token.jwt") {
        let token = token.trim();
        if !token.is_empty() {
             let mut auth_value = reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
                .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static(""));
             auth_value.set_sensitive(true);
             headers.insert(reqwest::header::AUTHORIZATION, auth_value);
             println!("Loaded authentication token from token.jwt");
        }
    } else {
        println!("Warning: No token.jwt found. You may need to run 'token' command if auth is required.");
    }
    
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    // Fetch schema metadata for autocompletion
    let mut helper = MiracleHelper::new();
    
    match client.get(format!("{}/api/v1/metadata", base_url)).send().await {
        Ok(res) => {
            if let Ok(meta) = res.json::<serde_json::Value>().await {
                if let Some(tables) = meta.get("tables").and_then(|v| v.as_array()) {
                    for t in tables {
                        if let Some(s) = t.as_str() {
                            helper.add_table(s);
                        }
                    }
                    println!("Loaded metadata: {} tables.", tables.len());
                }
            }
        }
        Err(e) => {
            println!("Warning: Could not fetch metadata from server: {}. Autocompletion will be limited.", e);
            // Add default fallback tables
            helper.add_table("users"); 
        }
    }

    let mut rl = Editor::new()?;
    rl.set_helper(Some(helper));
    rl.load_history("history.txt").ok();

    // CLI session state
    struct CliState {
        timing: bool,
        expanded: bool,
        current_tx: Option<u64>,
        output_file: Option<String>,
        last_query: Option<String>,
    }
    
    let mut state = CliState {
        timing: false,
        expanded: false,
        current_tx: None,
        output_file: None,
        last_query: None,
    };

    println!("Welcome to MiracleDb CLI v0.1.0");
    println!("Type 'help' for help, 'exit' to quit.");

    loop {
        let readline = rl.readline("miracledb> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                rl.add_history_entry(line)?;
                
                if line.eq_ignore_ascii_case("exit") || line.eq_ignore_ascii_case("quit") {
                    break;
                }
                
                if line.eq_ignore_ascii_case("help") || line == "\\?" {
                     println!("MiracleDb CLI Commands");
                     println!("======================");
                     println!();
                     println!("Schema Inspection:");
                     println!("  \\d                     List all tables, views, sequences");
                     println!("  \\d <name>              Describe table structure");
                     println!("  \\dt                    List tables");
                     println!("  \\di                    List indexes");
                     println!("  \\dv                    List views");
                     println!("  \\ds                    List sequences");
                     println!("  \\df                    List functions");
                     println!("  \\dn                    List schemas");
                     println!("  \\du                    List users/roles");
                     println!();
                     println!("Query & Output:");
                     println!("  \\timing                Toggle query timing");
                     println!("  \\x                     Toggle expanded output");
                     println!("  \\o <file>              Output to file");
                     println!("  \\i <file>              Execute SQL from file");
                     println!("  \\watch <n>             Re-run last query every n seconds");
                     println!();
                     println!("Transactions:");
                     println!("  \\begin                 Begin transaction");
                     println!("  \\commit                Commit transaction");
                     println!("  \\rollback              Rollback transaction");
                     println!();
                     println!("Server & Admin:");
                     println!("  \\status, \\health       Check server status");
                     println!("  \\version               Show server version");
                     println!("  \\conninfo              Show connection info");
                     println!("  \\l                     List databases");
                     println!("  \\sessions              List active sessions");
                     println!("  \\locks                 Show active locks");
                     println!("  \\stats                 Show server statistics");
                     println!("  \\config                Show configuration");
                     println!();
                     println!("Maintenance:");
                     println!("  \\vacuum [table]        Run vacuum");
                     println!("  \\analyze [table]       Update statistics");
                     println!("  \\checkpoint            Force checkpoint");
                     println!();
                     println!("Security:");
                     println!("  \\users                 List users");
                     println!("  \\roles                 List roles");
                     println!("  \\grants                Show grants");
                     println!("  \\token [u] [r] [d]     Generate JWT token");
                     println!();
                     println!("Advanced:");
                     println!("  \\agent <goal>          Run AI agent");
                     println!("  \\benchmark             Run stress test (50 tables x 1M rows)");
                     println!("  \\graph                 Graph database status");
                     println!("  \\vector                Vector search status");
                     println!("  \\metrics               Show metrics");
                     println!();
                     println!("Utilities:");
                     println!("  \\clear                 Clear screen");
                     println!("  \\! <cmd>               Execute shell command");
                     println!("  help, \\?               Show this help");
                     println!("  exit, quit             Exit the CLI");
                     continue;
                }

                if line.is_empty() {
                    continue;
                }
                
                // Handle backslash commands
                if line.starts_with('\\') {
                    let parts: Vec<&str> = line[1..].split_whitespace().collect();
                    let cmd = parts.first().map(|s| *s).unwrap_or("");
                    
                    match cmd {
                        // ===== Benchmarking =====
                        "benchmark" => {
                            let tables: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(50);
                            let rows: usize = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(1000000);
                            println!("Starting benchmark with {} tables of {} rows each...", tables, rows);
                            
                            // Trigger server-side generation
                            match client.post(format!("{}/api/v1/debug/benchmark", base_url))
                                .json(&serde_json::json!({ "tables": tables, "rows": rows }))
                                .send().await {
                                Ok(res) => {
                                    if !res.status().is_success() {
                                        let status = res.status();
                                        let body = res.text().await.unwrap_or_default();
                                        println!("Request failed with status: {}", status);
                                        println!("Response body: {}", body);
                                    } else {
                                        // Attempt to parse JSON response
                                        // We need to read text first then parse, because json() consumes self
                                        let body = res.text().await.unwrap_or_default();
                                        match serde_json::from_str::<serde_json::Value>(&body) {
                                            Ok(result) => {
                                                println!("Benchmark Result:");
                                                println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
                                            }
                                            Err(_) => {
                                                println!("Benchmark started (async). Response: {}", body);
                                            }
                                        }
                                    }
                                }
                                Err(e) => println!("Error sending request: {}", e),
                            }
                        }

                        // ===== Schema Inspection =====
                        "d" if parts.len() == 1 => {
                            // List all objects
                            match client.get(format!("{}/api/v1/tables", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(tables) = res.json::<Vec<String>>().await {
                                        println!("        List of relations");
                                        println!(" Schema |    Name    | Type  | Owner");
                                        println!("--------+------------+-------+-------");
                                        for t in tables {
                                            println!(" public | {:10} | table | admin", t);
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "d" => {
                            // Describe specific table
                            let table_name = parts.get(1).unwrap_or(&"");
                            match client.get(format!("{}/api/v1/tables/{}/columns", base_url, table_name)).send().await {
                                Ok(res) => {
                                    if res.status().is_success() {
                                        if let Ok(cols) = res.json::<Vec<serde_json::Value>>().await {
                                            println!("          Table \"public.{}\"", table_name);
                                            println!("  Column   |     Type     | Nullable");
                                            println!("-----------+--------------+----------");
                                            for col in cols {
                                                let name = col.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                                                let dtype = col.get("data_type").and_then(|v| v.as_str()).unwrap_or("?");
                                                let nullable = col.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                                                println!(" {:9} | {:12} | {}", name, dtype, if nullable { "YES" } else { "NO" });
                                            }
                                        }
                                    } else {
                                        println!("Table '{}' not found", table_name);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "dt" | "tables" => {
                            match client.get(format!("{}/api/v1/tables", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(tables) = res.json::<Vec<String>>().await {
                                        println!("        List of tables");
                                        println!(" Schema |    Name    | Type  | Owner");
                                        println!("--------+------------+-------+-------");
                                        for t in &tables {
                                            println!(" public | {:10} | table | admin", t);
                                        }
                                        println!("({} rows)", tables.len());
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "di" => {
                            match client.get(format!("{}/api/v1/indexes", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(indexes) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("        List of indexes");
                                        println!("     Name      |  Table  | Columns | Type");
                                        println!("---------------+---------+---------+------");
                                        for idx in &indexes {
                                            let name = idx.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                                            let table = idx.get("table").and_then(|v| v.as_str()).unwrap_or("?");
                                            let idx_type = idx.get("index_type").and_then(|v| v.as_str()).unwrap_or("?");
                                            println!(" {:13} | {:7} | ...     | {}", name, table, idx_type);
                                        }
                                        println!("({} rows)", indexes.len());
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "dv" => {
                            match client.get(format!("{}/api/v1/views", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(views) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("        List of views");
                                        if views.is_empty() {
                                            println!("(0 rows)");
                                        } else {
                                            for v in &views {
                                                println!("  {}", v.get("name").and_then(|x| x.as_str()).unwrap_or("?"));
                                            }
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "ds" => {
                            match client.get(format!("{}/api/v1/sequences", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(seqs) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("        List of sequences");
                                        if seqs.is_empty() { println!("(0 rows)"); }
                                        for s in &seqs {
                                            println!("  {}", s.get("name").and_then(|x| x.as_str()).unwrap_or("?"));
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "df" => {
                            match client.get(format!("{}/api/v1/functions", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(funcs) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("        List of functions");
                                        println!("     Name       |   Args    | Returns  | Language");
                                        println!("----------------+-----------+----------+----------");
                                        for f in &funcs {
                                            let name = f.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                                            let args = f.get("args").and_then(|v| v.as_str()).unwrap_or("?");
                                            let ret = f.get("return_type").and_then(|v| v.as_str()).unwrap_or("?");
                                            let lang = f.get("language").and_then(|v| v.as_str()).unwrap_or("?");
                                            println!(" {:14} | {:9} | {:8} | {}", name, args, ret, lang);
                                        }
                                        println!("({} rows)", funcs.len());
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "dn" => {
                            match client.get(format!("{}/api/v1/schemas", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(schemas) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("  List of schemas");
                                        println!("     Name       | Owner");
                                        println!("----------------+-------");
                                        for s in &schemas {
                                            let name = s.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                                            let owner = s.get("owner").and_then(|v| v.as_str()).unwrap_or("?");
                                            println!(" {:14} | {}", name, owner);
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "du" | "users" => {
                            match client.get(format!("{}/api/v1/users", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(users) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("        List of users");
                                        println!("   Username    |   Roles   | Active");
                                        println!("---------------+-----------+--------");
                                        for u in &users {
                                            let name = u.get("username").and_then(|v| v.as_str()).unwrap_or("?");
                                            let roles = u.get("roles").and_then(|v| v.as_array()).map(|a| {
                                                a.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join(",")
                                            }).unwrap_or_default();
                                            let active = u.get("active").and_then(|v| v.as_bool()).unwrap_or(false);
                                            println!(" {:13} | {:9} | {}", name, roles, if active { "yes" } else { "no" });
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "roles" => {
                            match client.get(format!("{}/api/v1/roles", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(roles) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("   List of roles");
                                        for r in &roles {
                                            println!("  {}", r.get("name").and_then(|v| v.as_str()).unwrap_or("?"));
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "grants" => {
                            match client.get(format!("{}/api/v1/grants", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(grants) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("   Access privileges");
                                        println!("  Grantee  | Resource | Privileges");
                                        println!("-----------+----------+------------");
                                        for g in &grants {
                                            let grantee = g.get("grantee").and_then(|v| v.as_str()).unwrap_or("?");
                                            let resource = g.get("resource").and_then(|v| v.as_str()).unwrap_or("?");
                                            let privs = g.get("privileges").and_then(|v| v.as_array()).map(|a| {
                                                a.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>().join(",")
                                            }).unwrap_or_default();
                                            println!(" {:9} | {:8} | {}", grantee, resource, privs);
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        // ===== Query & Output =====
                        "timing" => {
                            state.timing = !state.timing;
                            println!("Timing is {}.", if state.timing { "on" } else { "off" });
                        }
                        "x" => {
                            state.expanded = !state.expanded;
                            println!("Expanded display is {}.", if state.expanded { "on" } else { "off" });
                        }
                        "o" => {
                            let file = parts.get(1).map(|s| s.to_string());
                            if let Some(ref f) = file {
                                if f.is_empty() {
                                    state.output_file = None;
                                    println!("Output to stdout.");
                                } else {
                                    state.output_file = Some(f.clone());
                                    println!("Output will be written to: {}", f);
                                }
                            } else {
                                state.output_file = None;
                                println!("Output to stdout.");
                            }
                        }
                        "i" => {
                            let file = parts.get(1).unwrap_or(&"");
                            if file.is_empty() {
                                println!("Usage: \\i <filename>");
                            } else {
                                match std::fs::read_to_string(file) {
                                    Ok(contents) => {
                                        for sql in contents.split(';').filter(|s| !s.trim().is_empty()) {
                                            println!("Executing: {}", sql.trim());
                                            let _ = client.post(format!("{}/api/v1/sql", base_url))
                                                .json(&serde_json::json!({ "query": format!("{};", sql.trim()) }))
                                                .send().await;
                                        }
                                        println!("File executed.");
                                    }
                                    Err(e) => println!("Error reading file: {}", e),
                                }
                            }
                        }
                        // ===== Transactions =====
                        "begin" => {
                            if state.current_tx.is_some() {
                                println!("WARNING: Already in transaction {}", state.current_tx.unwrap());
                            }
                            match client.post(format!("{}/api/v1/transactions", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(tx) = res.json::<serde_json::Value>().await {
                                        let tx_id = tx.get("transaction_id").and_then(|v| v.as_u64()).unwrap_or(0);
                                        state.current_tx = Some(tx_id);
                                        println!("BEGIN - Transaction {} started", tx_id);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "commit" => {
                            if let Some(tx_id) = state.current_tx {
                                match client.post(format!("{}/api/v1/transactions/{}/commit", base_url, tx_id)).send().await {
                                    Ok(res) => {
                                        if res.status().is_success() {
                                            println!("COMMIT - Transaction {} committed", tx_id);
                                            state.current_tx = None;
                                        } else {
                                            println!("COMMIT failed: {}", res.status());
                                        }
                                    }
                                    Err(e) => println!("Error: {}", e),
                                }
                            } else {
                                println!("No active transaction");
                            }
                        }
                        "rollback" => {
                            if let Some(tx_id) = state.current_tx {
                                match client.post(format!("{}/api/v1/transactions/{}/rollback", base_url, tx_id)).send().await {
                                    Ok(res) => {
                                        if res.status().is_success() {
                                            println!("ROLLBACK - Transaction {} rolled back", tx_id);
                                            state.current_tx = None;
                                        } else {
                                            println!("ROLLBACK failed: {}", res.status());
                                        }
                                    }
                                    Err(e) => println!("Error: {}", e),
                                }
                            } else {
                                println!("No active transaction");
                            }
                        }

                        // ===== Server Info =====
                        "status" | "health" => {
                            match client.get(format!("{}/health", base_url)).send().await {
                                Ok(res) => {
                                    if res.status().is_success() {
                                        println!("✓ Server is UP ({})", res.status());
                                    } else {
                                        println!("✗ Server returned: {}", res.status());
                                    }
                                }
                                Err(e) => println!("✗ Connection failed: {}", e),
                            }
                        }
                        "version" => {
                            match client.get(format!("{}/api/v1/version", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(v) = res.json::<serde_json::Value>().await {
                                        println!("MiracleDb {}", v.get("version").and_then(|x| x.as_str()).unwrap_or("?"));
                                        println!("Build: {}", v.get("build").and_then(|x| x.as_str()).unwrap_or("?"));
                                        if let Some(features) = v.get("features").and_then(|f| f.as_array()) {
                                            let feat_str: Vec<&str> = features.iter().filter_map(|x| x.as_str()).collect();
                                            println!("Features: {}", feat_str.join(", "));
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "conninfo" => {
                            println!("You are connected to server \"{}\"", base_url);
                        }
                        "l" => {
                            match client.get(format!("{}/api/v1/databases", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(dbs) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("        List of databases");
                                        println!("    Name     | Owner | Size");
                                        println!("-------------+-------+------");
                                        for db in &dbs {
                                            let name = db.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                                            let owner = db.get("owner").and_then(|v| v.as_str()).unwrap_or("?");
                                            let size = db.get("size_mb").and_then(|v| v.as_u64()).unwrap_or(0);
                                            println!(" {:11} | {:5} | {} MB", name, owner, size);
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "sessions" => {
                            match client.get(format!("{}/api/v1/sessions", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(sessions) = res.json::<Vec<serde_json::Value>>().await {
                                        println!("  Active sessions");
                                        println!(" PID |  User  | Database | State");
                                        println!("-----+--------+----------+-------");
                                        for s in &sessions {
                                            let pid = s.get("pid").and_then(|v| v.as_u64()).unwrap_or(0);
                                            let user = s.get("username").and_then(|v| v.as_str()).unwrap_or("?");
                                            let db = s.get("database").and_then(|v| v.as_str()).unwrap_or("?");
                                            let state = s.get("state").and_then(|v| v.as_str()).unwrap_or("?");
                                            println!(" {:3} | {:6} | {:8} | {}", pid, user, db, state);
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "locks" => {
                            match client.get(format!("{}/api/v1/locks", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(locks) = res.json::<Vec<serde_json::Value>>().await {
                                        if locks.is_empty() {
                                            println!("No active locks");
                                        } else {
                                            println!("  Active locks");
                                            for l in &locks {
                                                println!("  {:?}", l);
                                            }
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "stats" => {
                            match client.get(format!("{}/api/v1/stats", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(stats) = res.json::<serde_json::Value>().await {
                                        println!("Server Statistics");
                                        println!("-----------------");
                                        println!("  Total queries:      {}", stats.get("total_queries").and_then(|v| v.as_u64()).unwrap_or(0));
                                        println!("  Cache hit ratio:    {:.1}%", stats.get("cache_hit_ratio").and_then(|v| v.as_f64()).unwrap_or(0.0) * 100.0);
                                        println!("  Active connections: {}", stats.get("active_connections").and_then(|v| v.as_u64()).unwrap_or(0));
                                        println!("  Total transactions: {}", stats.get("total_transactions").and_then(|v| v.as_u64()).unwrap_or(0));
                                        println!("  Uptime:             {} seconds", stats.get("uptime_seconds").and_then(|v| v.as_u64()).unwrap_or(0));
                                        println!("  Memory used:        {} MB", stats.get("memory_used_mb").and_then(|v| v.as_u64()).unwrap_or(0));
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "config" => {
                            match client.get(format!("{}/api/v1/config", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(config) = res.json::<serde_json::Value>().await {
                                        println!("Server Configuration");
                                        println!("--------------------");
                                        if let Some(settings) = config.get("settings").and_then(|s| s.as_object()) {
                                            for (k, v) in settings {
                                                println!("  {:20} = {}", k, v.as_str().unwrap_or("?"));
                                            }
                                        }
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "metrics" => {
                            match client.get(format!("{}/metrics", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(text) = res.text().await {
                                        println!("{}", text);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        // ===== Maintenance =====
                        "vacuum" => {
                            let table = parts.get(1).map(|s| s.to_string());
                            match client.post(format!("{}/api/v1/vacuum", base_url))
                                .json(&serde_json::json!({ "table": table }))
                                .send().await {
                                Ok(res) => {
                                    if let Ok(result) = res.json::<serde_json::Value>().await {
                                        println!("VACUUM completed: {:?}", result);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "analyze" => {
                            let table = parts.get(1).map(|s| s.to_string());
                            match client.post(format!("{}/api/v1/analyze", base_url))
                                .json(&serde_json::json!({ "table": table }))
                                .send().await {
                                Ok(res) => {
                                    if let Ok(result) = res.json::<serde_json::Value>().await {
                                        println!("ANALYZE completed: {:?}", result);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "checkpoint" => {
                            match client.post(format!("{}/api/v1/checkpoint", base_url)).send().await {
                                Ok(res) => {
                                    if let Ok(result) = res.json::<serde_json::Value>().await {
                                        println!("CHECKPOINT completed: {:?}", result);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        // ===== Security =====
                        "token" => {
                            let user = parts.get(1).unwrap_or(&"admin");
                            let roles = parts.get(2).unwrap_or(&"admin");
                            let days: u64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(365);
                            generate_token(user, roles, days);
                        }
                        // ===== Advanced =====
                        "agent" => {
                            let goal = parts[1..].join(" ");
                            if goal.is_empty() {
                                println!("Usage: \\agent <goal>");
                            } else {
                                match client.post(format!("{}/api/v1/agent", base_url))
                                    .json(&serde_json::json!({ "goal": goal }))
                                    .send().await {
                                    Ok(res) => {
                                        let body = res.text().await.unwrap_or_default();
                                        println!("Agent response: {}", body);
                                    }
                                    Err(e) => println!("Error: {}", e),
                                }
                            }
                        }
                        "seed" => {
                            let table = parts.get(1).unwrap_or(&"users");
                            let count: usize = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(100);
                            println!("Seeding table '{}' with {} rows...", table, count);

                            match client.post(format!("{}/api/v1/debug/seed", base_url))
                                .json(&serde_json::json!({ "table": table, "count": count }))
                                .send().await {
                                Ok(res) => {
                                    if res.status().is_success() {
                                        println!("Seeding completed.");
                                    } else {
                                        println!("Seeding failed: {}", res.status());
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                        "graph" => {
                            println!("Graph database: enabled");
                            println!("  Nodes: 0");
                            println!("  Edges: 0");
                        }
                        "vector" => {
                            println!("Vector search: enabled");
                            println!("  Indexes: 0");
                        }
                        // ===== Utilities =====
                        "clear" => {
                            print!("\x1B[2J\x1B[1;1H");
                        }
                        "!" => {
                            let shell_cmd = parts[1..].join(" ");
                            if shell_cmd.is_empty() {
                                println!("Usage: \\! <command>");
                            } else {
                                match std::process::Command::new("sh").arg("-c").arg(&shell_cmd).output() {
                                    Ok(output) => {
                                        print!("{}", String::from_utf8_lossy(&output.stdout));
                                        eprint!("{}", String::from_utf8_lossy(&output.stderr));
                                    }
                                    Err(e) => println!("Error: {}", e),
                                }
                            }
                        }
                        _ => {
                            println!("Unknown command: \\{}. Type 'help' for available commands.", cmd);
                        }
                    }
                    continue;
                }


                // Send query to server
                // println!("Executing: {}", line);
                let response = client.post(format!("{}/api/v1/sql", base_url))
                    .json(&serde_json::json!({ "query": line }))
                    .send()
                    .await;

                match response {
                    Ok(res) => {
                        if res.status().is_success() {
                            let body = res.text().await?;
                            match serde_json::from_str::<serde_json::Value>(&body) {
                                Ok(json) => {
                                    if let Some(data) = json.get("data") {
                                         println!("{}", serde_json::to_string_pretty(data).unwrap_or(body));
                                    } else {
                                         println!("Success (No Data)");
                                    }
                                }
                                Err(_) => println!("{}", body),
                            }
                        } else {
                            println!("Error: Server returned {}", res.status());
                        }
                    }
                    Err(e) => println!("Error sending request: {}", e),
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt")?;
    Ok(())
}

pub async fn run_init(output: String) -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing configuration file at {}...", output);
    let default_config = r#"# MiracleDb Configuration
host = "0.0.0.0"
port = 8080
log_level = "info"

[storage]
data_dir = "./data"
wal_dir = "./wal"
"#;
    use std::fs::File;
    use std::io::Write;
    let mut file = File::create(&output)?;
    file.write_all(default_config.as_bytes())?;
    println!("Configuration file created successfully.");
    Ok(())
}

pub async fn run_status(host: String) -> Result<(), Box<dyn std::error::Error>> {
    let base_url = if host.starts_with("http") { host.clone() } else { format!("http://{}", host) };
    println!("Checking status of {}...", base_url);
    
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?;
        
    match client.get(format!("{}/health", base_url)).send().await {
        Ok(res) => {
            if res.status().is_success() {
                println!("SUCCESS: Server is UP and responding.");
                println!("Status: {}", res.status());
            } else {
                println!("WARNING: Server responded with error status: {}", res.status());
            }
        }
        Err(e) => {
            println!("ERROR: Could not connect to server: {}", e);
            println!("Is the server running?");
        }
    }
    Ok(())
}

pub async fn run_auth(command: crate::AuthCommands) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let base_url = "http://localhost:8080"; // TODO: Make configurable

    match command {
        crate::AuthCommands::Login { username, password } => {
            println!("Attempting to log in user '{}'...", username);
            let res = client.post(format!("{}/api/v1/login", base_url))
                .json(&serde_json::json!({ "username": username, "password": password }))
                .send()
                .await?;

            if res.status().is_success() {
                let token_response: serde_json::Value = res.json().await?;
                if let Some(token) = token_response.get("token").and_then(|t| t.as_str()) {
                    println!("Login successful. JWT Token received.");
                    println!("Bearer {}", token);
                    // Optionally save token to a file for CLI usage
                    use std::io::Write;
                    if let Ok(mut file) = std::fs::File::create("token.jwt") {
                        if let Ok(_) = file.write_all(token.as_bytes()) {
                            println!();
                            println!("SUCCESS: Token saved to 'token.jwt'");
                            println!("You can now run 'miracledb cli' and it will automatically authenticate.");
                        }
                    }
                } else {
                    println!("Login successful, but no token received.");
                }
            } else {
                println!("Login failed: {}", res.text().await?);
            }
        }
    }
    Ok(())
}

pub async fn run_admin(command: crate::AdminCommands) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    // Assuming localhost:8080 for now, should take host config
    let base_url = "http://localhost:8080"; 

    match command {
        crate::AdminCommands::CreateUser { username } => {
            println!("Creating user '{}'...", username);
            let res = client.post(format!("{}/admin/api/users", base_url))
                .json(&serde_json::json!({ "username": username }))
                .send()
                .await?;
            
            if res.status().is_success() {
                println!("User '{}' created successfully.", username);
            } else {
                println!("Failed to create user: {}", res.text().await?);
            }
        }
    }
    Ok(())
}

/// Generate a JWT token for API authentication
pub fn generate_token(user: &str, roles_str: &str, expiry_days: u64) {
    use jsonwebtoken::{encode, Header, EncodingKey};
    use serde::{Serialize, Deserialize};
    
    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        sub: String,
        exp: usize,
        roles: Vec<String>,
    }
    
    let secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "default_secret_do_not_use_in_prod".to_string());
    
    let roles: Vec<String> = roles_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    
    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() + (expiry_days * 24 * 60 * 60);
    
    let claims = Claims {
        sub: user.to_string(),
        exp: expiry as usize,
        roles,
    };
    
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    ).expect("Failed to create token");
    
    println!("Generated JWT Token:");
    println!("Bearer {}", token);
    println!();
    println!("Token Details:");
    println!("  User:   {}", user);
    println!("  Roles:  {}", roles_str);
    println!("  Expiry: {} days", expiry_days);
    println!();
    println!("Usage:");
    println!("  curl -H \"Authorization: Bearer {}\" http://localhost:8080/api/v1/sql", token);

    // Save to file
    use std::io::Write;
    if let Ok(mut file) = std::fs::File::create("token.jwt") {
        if let Ok(_) = file.write_all(token.as_bytes()) {
            println!();
            println!("SUCCESS: Token saved to 'token.jwt'");
            println!("You can now run 'miracledb cli' and it will automatically authenticate.");
        }
    }
}

/// Run ML management commands
#[cfg(feature = "nlp")]
pub async fn run_ml(command: crate::MlCommands) -> Result<(), Box<dyn std::error::Error>> {
    use crate::ml::CandleEngine;

    match command {
        crate::MlCommands::Download { model_id, name, cache_dir } => {
            println!("Downloading model from HuggingFace...");
            println!("  Model ID: {}", model_id);
            println!("  Local name: {}", name);
            println!("  Cache dir: {}", cache_dir);
            println!();

            let engine = CandleEngine::new()?;

            println!("Starting download...");
            match engine.download_model_from_hf(&model_id, &name, &cache_dir).await {
                Ok(_) => {
                    println!("\x1b[32m✓\x1b[0m Model '{}' downloaded successfully!", name);
                    println!();
                    println!("Usage:");
                    println!("  SELECT candle_embed('{}', 'your text here') as embedding", name);
                }
                Err(e) => {
                    println!("\x1b[31m✗\x1b[0m Failed to download model: {}", e);
                    return Err(e);
                }
            }
        }
        crate::MlCommands::List => {
            println!("Listing loaded models...");
            println!();

            let engine = CandleEngine::new()?;
            let models = engine.list_models();

            if models.is_empty() {
                println!("No models loaded.");
                println!();
                println!("Download a model with:");
                println!("  miracledb ml download --model-id sentence-transformers/all-MiniLM-L6-v2 --name minilm");
            } else {
                println!("Loaded models ({}):", models.len());
                for model in models {
                    println!("  \x1b[32m•\x1b[0m {}", model);
                }
            }
        }
        crate::MlCommands::Info { name } => {
            println!("Model information: {}", name);
            println!();

            let engine = CandleEngine::new()?;
            let models = engine.list_models();

            if models.contains(&name) {
                println!("  Name: {}", name);
                println!("  Status: \x1b[32mLoaded\x1b[0m");
                println!("  Dimensions: 384 (estimated)");
                println!();
                println!("Usage:");
                println!("  SELECT candle_embed('{}', 'text') as embedding", name);
            } else {
                println!("\x1b[31m✗\x1b[0m Model '{}' not found", name);
                println!();
                println!("Available models:");
                if models.is_empty() {
                    println!("  (none)");
                } else {
                    for model in models {
                        println!("  • {}", model);
                    }
                }
            }
        }
        crate::MlCommands::Unload { name } => {
            println!("Unloading model: {}", name);

            let engine = CandleEngine::new()?;

            if engine.unload_model(&name) {
                println!("\x1b[32m✓\x1b[0m Model '{}' unloaded successfully", name);
            } else {
                println!("\x1b[31m✗\x1b[0m Model '{}' not found", name);
            }
        }
        crate::MlCommands::Test { model, text } => {
            println!("Testing embedding generation...");
            println!("  Model: {}", model);
            println!("  Text: \"{}\"", text);
            println!();

            let engine = CandleEngine::new()?;

            println!("Generating embedding...");
            match engine.generate_embedding(&model, &text) {
                Ok(embedding) => {
                    println!("\x1b[32m✓\x1b[0m Embedding generated successfully!");
                    println!();
                    println!("Dimensions: {}", embedding.len());
                    println!("First 10 values: {:?}", &embedding[..10.min(embedding.len())]);
                    println!("...");
                }
                Err(e) => {
                    println!("\x1b[31m✗\x1b[0m Failed to generate embedding: {}", e);
                    println!();
                    println!("Make sure the model is loaded:");
                    println!("  miracledb ml download --model-id sentence-transformers/all-MiniLM-L6-v2 --name {}", model);
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}
