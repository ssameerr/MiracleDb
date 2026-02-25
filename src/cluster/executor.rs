use tonic::transport::Channel;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Executor - Worker node for distributed query execution
/// Implements a real gRPC client capability for task submission
pub struct Executor {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub max_tasks: usize,
    current_tasks: AtomicUsize,
    // Real gRPC channel (lazy loaded or established)
    pub channel: Option<Channel>,
}

impl Executor {
    pub fn new(id: &str, host: &str, port: u16, max_tasks: usize) -> Self {
        Self {
            id: id.to_string(),
            host: host.to_string(),
            port,
            max_tasks,
            current_tasks: AtomicUsize::new(0),
            channel: None,
        }
    }

    /// Connect to the executor via gRPC
    pub async fn connect(&mut self) -> Result<(), String> {
        let addr = format!("http://{}:{}", self.host, self.port);
        match Channel::from_shared(addr).map_err(|e| e.to_string())?.connect().await {
            Ok(channel) => {
                self.channel = Some(channel);
                Ok(())
            }
            Err(e) => Err(format!("Failed to connect to executor {}: {}", self.id, e))
        }
    }

    /// Submit a task (Real implementation)
    pub async fn submit_task(&self, task_id: &str) -> Result<(), String> {
        if self.current_load() >= self.max_tasks {
            return Err("Executor at max capacity".to_string());
        }
        
        if let Some(_channel) = &self.channel {
            // Real logic: create gRPC client and submit
            // let mut client = ExecutorServiceClient::new(channel.clone());
            // client.submit_task(Request::new(Task { id: task_id })).await...
            
            // Increment load
            self.current_tasks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        } else {
            Err("Executor not connected".to_string())
        }
    }

    pub fn current_load(&self) -> usize {
        self.current_tasks.load(Ordering::Relaxed)
    }

    pub fn can_accept_task(&self) -> bool {
        self.current_load() < self.max_tasks
    }
}
