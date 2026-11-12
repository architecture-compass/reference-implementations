//! A minimal circuit breaker: opens after N consecutive failures, rejects calls while
//! open, then lets calls through again after a cool-down.

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct CircuitBreaker {
    threshold: u32,
    open_for: Duration,
    state: Mutex<(u32, Option<Instant>)>, // (consecutive failures, open until)
}

impl CircuitBreaker {
    pub fn new(threshold: u32, open_for: Duration) -> Self {
        Self { threshold, open_for, state: Mutex::new((0, None)) }
    }

    pub fn allow(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.1.is_none_or(|until| Instant::now() >= until)
    }

    pub fn record_success(&self) {
        *self.state.lock().unwrap() = (0, None);
    }

    pub fn record_failure(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        if state.0 >= self.threshold {
            *state = (0, Some(Instant::now() + self.open_for));
        }
    }
}
