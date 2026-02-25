
import os
import sys
import argparse
import time
import requests
import io
import csv

# You might need to install: pip install sqlalchemy pandas psycopg2-binary pymysql
try:
    import pandas as pd
    from sqlalchemy import create_engine, inspect
except ImportError:
    print("Error: Missing required libraries.")
    print("Please run: pip install pandas sqlalchemy psycopg2-binary pymysql")
    sys.exit(1)

MIRACLE_API = "http://localhost:8080/api/v1"

def sanitize_type(sql_type):
    """
    Map SQL types to MiracleDb/Arrow compatible types strings for CREATE TABLE.
    This is a basic mapper.
    """
    st = str(sql_type).upper()
    if "INT" in st: return "BIGINT" # Safe default
    if "CHAR" in st or "TEXT" in st or "CLOB" in st: return "TEXT"
    if "FLOAT" in st or "DOUBLE" in st or "REAL" in st: return "FLOAT"
    if "BOOL" in st: return "BOOLEAN"
    if "DATE" in st or "TIME" in st: return "TIMESTAMP"
    return "TEXT" # Fallback

def migrate_table(src_engine, table_name, target_table_name, chunk_size=10000):
    print(f"Migrating table: {table_name} -> {target_table_name}...")
    
    # 1. Get Schema
    inspector = inspect(src_engine)
    columns = inspector.get_columns(table_name)
    
    schema_def = []
    for col in columns:
        col_name = col['name']
        col_type = sanitize_type(col['type'])
        nullable = col.get('nullable', True)
        schema_def.append({
            "name": col_name,
            "data_type": col_type,
            "nullable": nullable
        })
    
    # 2. Create Table in MiracleDb
    create_payload = {"schema": schema_def}
    print(f"  Creating table schema in MiracleDb...")
    resp = requests.post(f"{MIRACLE_API}/tables/{target_table_name}", json=create_payload)
    if resp.status_code not in [200, 201]:
        print(f"  Failed to create table: {resp.text}")
        return
    
    # 3. Stream Data
    print(f"  Streaming data (Chunk size: {chunk_size})...")
    offset = 0
    total_rows = 0
    
    # Use pandas read_sql_query with chunksize
    query = f"SELECT * FROM {table_name}"
    
    # We use a context manager for the connection
    with src_engine.connect() as conn:
        for chunk in pd.read_sql_query(query, conn, chunksize=chunk_size):
            if chunk.empty:
                break
            
            # Convert chunk to CSV in memory
            csv_buffer = io.StringIO()
            chunk.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            # Upload CSV chunk
            files = {'file': ('chunk.csv', csv_data, 'text/csv')}
            upload_resp = requests.post(
                f"{MIRACLE_API}/tables/{target_table_name}/import",
                files=files
            )
            
            if upload_resp.status_code not in [200, 201]:
                print(f"  Failed to upload chunk starting at {offset}: {upload_resp.text}")
            else:
                rows_in_chunk = len(chunk)
                total_rows += rows_in_chunk
                print(f"    Imported {rows_in_chunk} rows (Total: {total_rows})")
            
            offset += chunk_size

    print(f"Done. Migrated {total_rows} rows.")

def main():
    parser = argparse.ArgumentParser(description="MiracleDb Migration Tool")
    parser.add_argument("--source", required=True, help="SQLAlchemy Connection String (e.g., postgresql://user:pass@localhost/db)")
    parser.add_argument("--tables", help="Comma-separated list of tables to migrate (default: all)")
    
    args = parser.parse_args()
    
    try:
        engine = create_engine(args.source)
        conn = engine.connect()
        print("Connected to source database.")
        
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        
        tables_to_migrate = args.tables.split(",") if args.tables else all_tables
        
        for table in tables_to_migrate:
            table = table.strip()
            if table in all_tables:
                migrate_table(engine, table, table)
            else:
                print(f"Warning: Table '{table}' not found in source database.")
                
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    main()
