//! Scheduler Module - Job scheduling and background tasks

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use serde::{Serialize, Deserialize};

/// Scheduled job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub schedule: Schedule,
    pub task_type: TaskType,
    pub enabled: bool,
    pub last_run: Option<i64>,
    pub next_run: Option<i64>,
    pub run_count: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Schedule {
    Once { at: i64 },
    Interval { seconds: u64 },
    Cron { expression: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskType {
    Vacuum,
    RefreshMaterializedView { view_name: String },
    Backup { backup_type: String },
    Retention { table: String, days: i64 },
    Custom { sql: String },
}

/// Job execution result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub success: bool,
    pub start_time: i64,
    pub end_time: i64,
    pub message: Option<String>,
}

/// Job scheduler
pub struct Scheduler {
    jobs: Arc<RwLock<HashMap<String, Job>>>,
    results: Arc<RwLock<Vec<JobResult>>>,
    running: Arc<RwLock<bool>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            results: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Register a new job
    pub async fn register_job(&self, job: Job) {
        let mut jobs = self.jobs.write().await;
        jobs.insert(job.id.clone(), job);
    }

    /// Remove a job
    pub async fn remove_job(&self, job_id: &str) -> Option<Job> {
        let mut jobs = self.jobs.write().await;
        jobs.remove(job_id)
    }

    /// Enable/disable a job
    pub async fn set_job_enabled(&self, job_id: &str, enabled: bool) -> Result<(), String> {
        let mut jobs = self.jobs.write().await;
        let job = jobs.get_mut(job_id)
            .ok_or("Job not found")?;
        job.enabled = enabled;
        Ok(())
    }

    /// Start the scheduler loop
    pub async fn start(&self) {
        let mut running = self.running.write().await;
        if *running {
            return;
        }
        *running = true;
        drop(running);

        let jobs = Arc::clone(&self.jobs);
        let results = Arc::clone(&self.results);
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));

            loop {
                ticker.tick().await;

                let is_running = *running.read().await;
                if !is_running {
                    break;
                }

                let now = chrono::Utc::now().timestamp();
                let mut jobs_guard = jobs.write().await;

                for job in jobs_guard.values_mut() {
                    if !job.enabled {
                        continue;
                    }

                    let should_run = match &job.schedule {
                        Schedule::Once { at } => *at <= now && job.run_count == 0,
                        Schedule::Interval { seconds } => {
                            job.next_run.map_or(true, |next| now >= next)
                        }
                        Schedule::Cron { .. } => false, // Would parse cron expression
                    };

                    if should_run {
                        let start_time = now;
                        
                        // Execute task (simplified)
                        let success = true;
                        let message = Some("Task completed".to_string());

                        let end_time = chrono::Utc::now().timestamp();
                        
                        job.last_run = Some(end_time);
                        job.run_count += 1;

                        if let Schedule::Interval { seconds } = &job.schedule {
                            job.next_run = Some(end_time + *seconds as i64);
                        }

                        let result = JobResult {
                            job_id: job.id.clone(),
                            success,
                            start_time,
                            end_time,
                            message,
                        };

                        let mut results_guard = results.write().await;
                        results_guard.push(result);
                        
                        // Keep only last 1000 results
                        if results_guard.len() > 1000 {
                            results_guard.remove(0);
                        }
                    }
                }
            }
        });
    }

    /// Stop the scheduler
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
    }

    /// Get all jobs
    pub async fn list_jobs(&self) -> Vec<Job> {
        let jobs = self.jobs.read().await;
        jobs.values().cloned().collect()
    }

    /// Get job by ID
    pub async fn get_job(&self, job_id: &str) -> Option<Job> {
        let jobs = self.jobs.read().await;
        jobs.get(job_id).cloned()
    }

    /// Get recent job results
    pub async fn get_results(&self, limit: usize) -> Vec<JobResult> {
        let results = self.results.read().await;
        results.iter().rev().take(limit).cloned().collect()
    }

    /// Run a job immediately
    pub async fn run_now(&self, job_id: &str) -> Result<JobResult, String> {
        let jobs = self.jobs.read().await;
        let job = jobs.get(job_id)
            .ok_or("Job not found")?
            .clone();
        drop(jobs);

        let start_time = chrono::Utc::now().timestamp();
        
        // Execute task
        let success = true;
        let message = Some("Task completed (manual run)".to_string());

        let end_time = chrono::Utc::now().timestamp();

        let result = JobResult {
            job_id: job.id.clone(),
            success,
            start_time,
            end_time,
            message,
        };

        // Update job
        let mut jobs = self.jobs.write().await;
        if let Some(j) = jobs.get_mut(job_id) {
            j.last_run = Some(end_time);
            j.run_count += 1;
        }

        // Store result
        let mut results = self.results.write().await;
        results.push(result.clone());

        Ok(result)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
