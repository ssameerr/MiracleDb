//! Postmaster Module - Main Server Process

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use crate::tcop::TrafficCop;
use crate::config::ConfigManager;

/// Postmaster state
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PmState {
    NoBeg,
    Startup,
    Recovery,
    Run,
    Shutdown,
}

/// Postmaster
pub struct Postmaster {
    config: Arc<ConfigManager>,
    state: PmState,
    tcop: Arc<TrafficCop>,
}

impl Postmaster {
    pub fn new(config: Arc<ConfigManager>, tcop: Arc<TrafficCop>) -> Self {
        Self {
            config,
            state: PmState::NoBeg,
            tcop,
        }
    }

    /// Run the postmaster loop
    pub async fn run(&mut self) -> Result<(), String> {
        self.state = PmState::Startup;
        
        // 1. Load config
        // self.config.load().await?;

        // 2. Start subsystems
        // self.startup_subsystems().await?;

        self.state = PmState::Run;

        // 3. Listen for connections
        let addr = format!("{}:{}", 
            self.config.get_value("server.host").await.unwrap_or("127.0.0.1".to_string()),
            self.config.get_value("server.port").await.unwrap_or("5432".to_string())
        );

        let listener = TcpListener::bind(&addr).await
            .map_err(|e| format!("Failed to bind to {}: {}", addr, e))?;

        println!("Postmaster listening on {}", addr);

        // 4. Accept loop
        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((socket, addr)) => {
                            // Fork backend (spawn task)
                            let tcop = self.tcop.clone();
                            tokio::spawn(async move {
                                // Handle connection
                                // Protocol handshake
                                // Backend startup
                            });
                        }
                        Err(e) => eprintln!("Accept error: {}", e),
                    }
                }
                _ = signal::ctrl_c() => {
                    println!("Received Ctrl+C, shutting down");
                    break;
                }
            }
        }

        self.shutdown().await
    }

    async fn shutdown(&mut self) -> Result<(), String> {
        self.state = PmState::Shutdown;
        // Checkpoint
        // Kill backends
        // Flush WAL
        Ok(())
    }
}
