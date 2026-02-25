import subprocess
import time
import sys
import requests
import json
import os
import argparse
import random
import string
import platform

# Configuration
# Path to the executable
SERVER_BIN = "./miracledb.exe" if platform.system() == "Windows" or os.path.exists("./miracledb.exe") else "./miracledb"
  # Modify if executable is elsewhere
BASE_URL = "http://localhost:8080"
COLORS = {
    "HEADER": "\033[95m",
    "OKBLUE": "\033[94m",
    "OKCYAN": "\033[96m",
    "OKGREEN": "\033[92m",
    "WARNING": "\033[93m",
    "FAIL": "\033[91m",
    "ENDC": "\033[0m",
    "BOLD": "\033[1m",
}

def log(msg, type="INFO"):
    color = COLORS.get(type, COLORS["ENDC"])
    print(f"{color}[{type}] {msg}{COLORS['ENDC']}")

def check_server_health(retries=10, delay=2):
    log(f"Waiting for server at {BASE_URL}...", "HEADER")
    for i in range(retries):
        try:
            resp = requests.get(f"{BASE_URL}/health", timeout=1)
            if resp.status_code == 200:
                log("Server is UP!", "OKGREEN")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(delay)
        print(".", end="", flush=True)
    print()
    log("Server failed to allow connections.", "FAIL")
    return False

def test_endpoint(name, method, endpoint, data=None, expected_status=200):
    log(f"Testing {name}...", "OKCYAN")
    url = f"{BASE_URL}{endpoint}"
    try:
        if method == "GET":
            resp = requests.get(url)
        elif method == "POST":
            resp = requests.post(url, json=data)
        elif method == "PUT":
            resp = requests.put(url, json=data)
        elif method == "DELETE":
            resp = requests.delete(url)
        else:
            raise ValueError(f"Unknown method {method}")
        
        if resp.status_code == expected_status:
            log(f"PASS: {name}", "OKGREEN")
            return True
        else:
            log(f"FAIL: {name} (Expected {expected_status}, got {resp.status_code})", "FAIL")
            print(resp.text)
            return False
    except Exception as e:
        log(f"ERROR: {name} - {str(e)}", "FAIL")
        return False

def run_standard_tests():
    log("Running Standard Functional Tests...", "HEADER")
    # 1. Health
    if not test_endpoint("Health Check", "GET", "/health"): return

    # 2. Metrics
    test_endpoint("Metrics", "GET", "/metrics")

    # 3. Swagger UI
    test_endpoint("Swagger UI", "GET", "/api/docs")

    # 4. Create Table
    create_sql = {
        "query": "CREATE TABLE test_users (id INT, name TEXT)"
    }
    test_endpoint("SQL Create Table", "POST", "/api/v1/sql", create_sql)

    # 5. Insert Data
    insert_sql = {
        "query": "INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob')"
    }
    test_endpoint("SQL Insert Data", "POST", "/api/v1/sql", insert_sql)

    # 6. Select Data
    select_sql = {
        "query": "SELECT * FROM test_users"
    }
    test_endpoint("SQL Select Data", "POST", "/api/v1/sql", select_sql)

    # 7. Drop Table
    drop_sql = {
        "query": "DROP TABLE test_users"
    }
    test_endpoint("SQL Drop Table", "POST", "/api/v1/sql", drop_sql)

    # 8. RLM Agent (Mock run)
    agent_req = {
        "goal": "Say hello"
    }
    test_endpoint("RLM Agent", "POST", "/api/v1/agent/run", agent_req)

def run_stress_test(num_tables, rows_per_table, columns_per_table):
    log(f"Starting STRESS TEST: {num_tables} tables, {columns_per_table} columns, {rows_per_table} rows per table", "HEADER")
    
    # 1. Create Tables
    log(f"Creating {num_tables} tables...", "OKBLUE")
    start_time = time.time()
    
    for t in range(num_tables):
        table_name = f"stress_table_{t}"
        # Create columns: id INT, col_0 TEXT, col_1 TEXT, ...
        cols = ["id INT"] + [f"col_{c} TEXT" for c in range(columns_per_table)]
        create_sql = f"CREATE TABLE {table_name} ({', '.join(cols)})"
        
        resp = requests.post(f"{BASE_URL}/api/v1/sql", json={"query": create_sql})
        if resp.status_code != 200:
            log(f"Failed to create table {table_name}: {resp.text}", "FAIL")
            return

    log(f"Created {num_tables} tables in {time.time() - start_time:.2f}s", "OKGREEN")

    # 2. Insert Data
    log(f"Inserting {rows_per_table} rows into each table...", "OKBLUE")
    insert_start_time = time.time()
    
    for t in range(num_tables):
        table_name = f"stress_table_{t}"
        # Batch insert if possible, or single inserts. simplified for script length: single batch query per table if small, or chunks
        # Generating naive data
        
        # We will split into chunks of 1000 to avoid huge request bodies if rows_per_table is large
        chunk_size = 1000
        for i in range(0, rows_per_table, chunk_size):
            chunk_end = min(i + chunk_size, rows_per_table)
            values = []
            for r in range(i, chunk_end):
                # id = r, others = random string
                row_cols = [str(r)] + [f"'val_{t}_{c}_{r}'" for c in range(columns_per_table)]
                values.append(f"({', '.join(row_cols)})")
            
            insert_sql = f"INSERT INTO {table_name} VALUES {', '.join(values)}"
            resp = requests.post(f"{BASE_URL}/api/v1/sql", json={"query": insert_sql})
            if resp.status_code != 200:
                log(f"Failed insert on {table_name}: {resp.text}", "FAIL")
                # Continue anyway to stress test
    
    log(f"Inserted total {num_tables * rows_per_table} rows in {time.time() - insert_start_time:.2f}s", "OKGREEN")

    # 3. Test Join
    log(f"Testing {num_tables}-way JOIN...", "OKBLUE")
    
    # SELECT t0.id, t0.col_0, t1.col_0 ... FROM stress_table_0 t0 JOIN stress_table_1 t1 ON t0.id = t1.id ...
    select_cols = ["t0.id"] + [f"t{t}.col_0" for t in range(num_tables)] # Select one col from each
    join_clause = f"stress_table_0 t0"
    for t in range(1, num_tables):
        join_clause += f" JOIN stress_table_{t} t{t} ON t0.id = t{t}.id"
    
    query = f"SELECT {', '.join(select_cols)} FROM {join_clause} LIMIT 10"
    
    log(f"Executing: {query[:100]}...", "OKCYAN")
    join_start = time.time()
    resp = requests.post(f"{BASE_URL}/api/v1/sql", json={"query": query})
    duration = time.time() - join_start
    
    if resp.status_code == 200:
        log(f"Join query success! Duration: {duration:.4f}s", "OKGREEN")
        # print first row to verify
        data = resp.json().get("data", [])
        if data:
             log(f"First row result sample: {str(data[0])[:100]}...", "OKBLUE")
    else:
        log(f"Join query failed: {resp.text}", "FAIL")
        
    # 4. Cleanup (Optional, disabled by default to allow inspection)
    # log("Cleaning up...", "HEADER")
    # for t in range(num_tables):
    #    requests.post(f"{BASE_URL}/api/v1/sql", json={"query": f"DROP TABLE stress_table_{t}"})

def main():
    parser = argparse.ArgumentParser(description="MiracleDb Test Script")
    parser.add_argument("--stress", action="store_true", help="Run stress test")
    parser.add_argument("--tables", type=int, default=50, help="Number of tables for stress test")
    parser.add_argument("--rows", type=int, default=20000, help="Rows per table (default 20k, total 1M across 50 tables)")
    parser.add_argument("--cols", type=int, default=30, help="Columns per table")
    parser.add_argument("--skip-server-start", action="store_true", help="Do not attempt to start the server")
    args = parser.parse_args()

    server_process = None
    try:
        # Check if running
        try:
            requests.get(f"{BASE_URL}/health", timeout=1)
            log("Server already running, using existing instance.", "WARNING")
        except:
            if not args.skip_server_start:
                if not os.path.exists(SERVER_BIN):
                    log(f"Executable {SERVER_BIN} not found!", "FAIL")
                    log("Please build the project first or place the executable here.", "FAIL")
                    sys.exit(1)
                    
                log(f"Starting {SERVER_BIN}...", "HEADER")
                server_process = subprocess.Popen([SERVER_BIN, "start"])
                if not check_server_health():
                    sys.exit(1)
            else:
                 log("Server not running and --skip-server-start used. Exiting.", "FAIL")
                 sys.exit(1)

        if args.stress:
            run_stress_test(args.tables, args.rows, args.cols)
        else:
            run_standard_tests()

    except KeyboardInterrupt:
        log("Interrupted.", "WARNING")
    finally:
        if server_process:
            log("Stopping server...", "HEADER")
            server_process.terminate()
            server_process.wait()
            log("Server stopped.", "OKGREEN")

if __name__ == "__main__":
    main()
