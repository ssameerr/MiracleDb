//! Retry Module - Retry and backoff utilities

use std::time::Duration;
use std::future::Future;
use serde::{Serialize, Deserialize};

/// Retry policy
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff: BackoffStrategy,
    pub jitter: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum BackoffStrategy {
    Constant,
    Linear,
    Exponential,
    Fibonacci,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff: BackoffStrategy::Exponential,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            ..Default::default()
        }
    }

    pub fn with_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn with_backoff(mut self, strategy: BackoffStrategy) -> Self {
        self.backoff = strategy;
        self
    }

    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate delay for attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay = match self.backoff {
            BackoffStrategy::Constant => self.initial_delay,
            BackoffStrategy::Linear => self.initial_delay * attempt,
            BackoffStrategy::Exponential => {
                self.initial_delay * 2_u32.saturating_pow(attempt.saturating_sub(1))
            }
            BackoffStrategy::Fibonacci => {
                let fib = Self::fibonacci(attempt as usize);
                self.initial_delay * fib as u32
            }
        };

        let delay = base_delay.min(self.max_delay);

        if self.jitter {
            let jitter = Duration::from_millis(rand::random::<u64>() % 100);
            delay + jitter
        } else {
            delay
        }
    }

    fn fibonacci(n: usize) -> usize {
        match n {
            0 => 0,
            1 => 1,
            _ => {
                let mut a = 0;
                let mut b = 1;
                for _ in 2..=n {
                    let c = a + b;
                    a = b;
                    b = c;
                }
                b
            }
        }
    }
}

/// Retry result
#[derive(Clone, Debug)]
pub struct RetryResult<T, E> {
    pub result: Result<T, E>,
    pub attempts: u32,
    pub total_duration: Duration,
}

/// Retry executor
pub struct Retry;

impl Retry {
    /// Execute with retry
    pub async fn execute<F, Fut, T, E>(policy: &RetryPolicy, mut f: F) -> RetryResult<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let start = std::time::Instant::now();
        let mut attempts = 0;
        let mut last_error = None;

        for attempt in 1..=policy.max_attempts {
            attempts = attempt;

            match f().await {
                Ok(value) => {
                    return RetryResult {
                        result: Ok(value),
                        attempts,
                        total_duration: start.elapsed(),
                    };
                }
                Err(e) => {
                    last_error = Some(e);

                    if attempt < policy.max_attempts {
                        let delay = policy.delay_for_attempt(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        RetryResult {
            result: Err(last_error.unwrap()),
            attempts,
            total_duration: start.elapsed(),
        }
    }

    /// Execute with callback on retry
    pub async fn execute_with_callback<F, Fut, T, E, C>(
        policy: &RetryPolicy,
        mut f: F,
        mut on_retry: C,
    ) -> RetryResult<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        C: FnMut(u32, &E, Duration),
    {
        let start = std::time::Instant::now();
        let mut attempts = 0;
        let mut last_error = None;

        for attempt in 1..=policy.max_attempts {
            attempts = attempt;

            match f().await {
                Ok(value) => {
                    return RetryResult {
                        result: Ok(value),
                        attempts,
                        total_duration: start.elapsed(),
                    };
                }
                Err(e) => {
                    if attempt < policy.max_attempts {
                        let delay = policy.delay_for_attempt(attempt);
                        on_retry(attempt, &e, delay);
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(e);
                }
            }
        }

        RetryResult {
            result: Err(last_error.unwrap()),
            attempts,
            total_duration: start.elapsed(),
        }
    }
}

/// Circuit breaker state
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker
pub struct CircuitBreaker {
    state: std::sync::atomic::AtomicU8,
    failure_count: std::sync::atomic::AtomicU32,
    success_count: std::sync::atomic::AtomicU32,
    last_failure: std::sync::atomic::AtomicI64,
    failure_threshold: u32,
    success_threshold: u32,
    reset_timeout: Duration,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, success_threshold: u32, reset_timeout: Duration) -> Self {
        Self {
            state: std::sync::atomic::AtomicU8::new(0), // Closed
            failure_count: std::sync::atomic::AtomicU32::new(0),
            success_count: std::sync::atomic::AtomicU32::new(0),
            last_failure: std::sync::atomic::AtomicI64::new(0),
            failure_threshold,
            success_threshold,
            reset_timeout,
        }
    }

    pub fn state(&self) -> CircuitState {
        match self.state.load(std::sync::atomic::Ordering::SeqCst) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            _ => CircuitState::HalfOpen,
        }
    }

    pub fn allow_request(&self) -> bool {
        match self.state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let last = self.last_failure.load(std::sync::atomic::Ordering::SeqCst);
                let now = chrono::Utc::now().timestamp();
                if now - last > self.reset_timeout.as_secs() as i64 {
                    self.state.store(2, std::sync::atomic::Ordering::SeqCst); // HalfOpen
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub fn record_success(&self) {
        match self.state() {
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                if count >= self.success_threshold {
                    self.state.store(0, std::sync::atomic::Ordering::SeqCst); // Closed
                    self.failure_count.store(0, std::sync::atomic::Ordering::SeqCst);
                    self.success_count.store(0, std::sync::atomic::Ordering::SeqCst);
                }
            }
            _ => {
                self.failure_count.store(0, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    pub fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        self.last_failure.store(chrono::Utc::now().timestamp(), std::sync::atomic::Ordering::SeqCst);

        if count >= self.failure_threshold {
            self.state.store(1, std::sync::atomic::Ordering::SeqCst); // Open
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(5, 3, Duration::from_secs(30))
    }
}
