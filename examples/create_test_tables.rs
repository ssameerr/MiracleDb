//! Example: Create test tables and data for auto-generation testing

use miracledb::engine::MiracleEngine;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("Initializing MiracleDb engine...");
    let engine = Arc::new(MiracleEngine::new().await?);

    println!("\n=== Creating Test Tables ===\n");

    // Create users table
    println!("Creating 'users' table...");
    engine.query("CREATE TABLE users (id INTEGER, name VARCHAR(100), email VARCHAR(100), age INTEGER, created_at TIMESTAMP)").await?;
    println!("✓ users table created");

    // Create products table
    println!("Creating 'products' table...");
    engine.query("CREATE TABLE products (id INTEGER, name VARCHAR(200), description VARCHAR(500), price DOUBLE, stock INTEGER, category VARCHAR(50), created_at TIMESTAMP)").await?;
    println!("✓ products table created");

    // Create orders table
    println!("Creating 'orders' table...");
    engine.query("CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER, total_price DOUBLE, status VARCHAR(20), order_date TIMESTAMP)").await?;
    println!("✓ orders table created");

    println!("\n=== Inserting Sample Data ===\n");

    // Insert users
    println!("Inserting users...");
    engine.query("INSERT INTO users (id, name, email, age) VALUES (1, 'Alice Johnson', 'alice@example.com', 28)").await?;
    engine.query("INSERT INTO users (id, name, email, age) VALUES (2, 'Bob Smith', 'bob@example.com', 34)").await?;
    engine.query("INSERT INTO users (id, name, email, age) VALUES (3, 'Carol Williams', 'carol@example.com', 42)").await?;
    println!("✓ Inserted 3 users");

    // Insert products
    println!("Inserting products...");
    engine.query("INSERT INTO products (id, name, description, price, stock, category) VALUES (1, 'Laptop Pro', 'High-performance laptop', 1299.99, 50, 'Electronics')").await?;
    engine.query("INSERT INTO products (id, name, description, price, stock, category) VALUES (2, 'Wireless Mouse', 'Ergonomic wireless mouse', 29.99, 200, 'Accessories')").await?;
    engine.query("INSERT INTO products (id, name, description, price, stock, category) VALUES (3, 'USB-C Cable', '6ft USB-C charging cable', 15.99, 500, 'Accessories')").await?;
    println!("✓ Inserted 3 products");

    // Insert orders
    println!("Inserting orders...");
    engine.query("INSERT INTO orders (id, user_id, product_id, quantity, total_price, status) VALUES (1, 1, 1, 1, 1299.99, 'completed')").await?;
    engine.query("INSERT INTO orders (id, user_id, product_id, quantity, total_price, status) VALUES (2, 2, 2, 2, 59.98, 'shipped')").await?;
    engine.query("INSERT INTO orders (id, user_id, product_id, quantity, total_price, status) VALUES (3, 1, 3, 3, 47.97, 'pending')").await?;
    println!("✓ Inserted 3 orders");

    println!("\n=== Verification ===\n");

    // List all tables
    let tables = engine.list_tables().await;
    println!("Tables in database: {:?}", tables);

    // Query each table
    for table in &tables {
        let result = engine.query(&format!("SELECT * FROM {}", table)).await?;
        let batches = result.collect().await?;
        let mut total_rows = 0;
        for batch in batches {
            total_rows += batch.num_rows();
        }
        println!("  - {}: {} rows", table, total_rows);
    }

    println!("\n✓ Test data created successfully!");
    println!("\nNow restart the server to see the auto-generated APIs.");
    println!("Or manually trigger re-discovery if the server supports it.");

    Ok(())
}
