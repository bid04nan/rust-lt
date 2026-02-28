use super::scenario::{LoadModel, Stage};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// User injection schedule - determines when to start each virtual user
#[derive(Debug, Clone)]
pub struct InjectionSchedule {
    /// List of injection times (milliseconds from test start)
    pub injection_times: Vec<u64>,
    /// Total test duration in seconds
    pub duration: Option<u64>,
}

impl InjectionSchedule {
    /// Create an empty schedule
    pub fn new() -> Self {
        Self {
            injection_times: Vec::new(),
            duration: None,
        }
    }

    /// Add an injection time
    pub fn add(&mut self, time_ms: u64) {
        self.injection_times.push(time_ms);
    }

    /// Sort injection times
    pub fn sort(&mut self) {
        self.injection_times.sort_unstable();
    }

    /// Get total number of users to inject
    pub fn total_users(&self) -> usize {
        self.injection_times.len()
    }
}

impl Default for InjectionSchedule {
    fn default() -> Self {
        Self::new()
    }
}

/// Build an injection schedule from a load model
pub fn build_schedule(load_model: &LoadModel) -> InjectionSchedule {
    match load_model {
        LoadModel::AtOnceUsers { users } => {
            build_at_once_schedule(*users)
        }
        LoadModel::Constant { users, duration } => {
            build_constant_schedule(*users, *duration)
        }
        LoadModel::RampUp { users, duration } => {
            build_ramp_up_schedule(*users, *duration)
        }
        LoadModel::RampDown { users, duration } => {
            build_ramp_down_schedule(*users, *duration)
        }
        LoadModel::ConstantUsersPerSec { rate, duration } => {
            build_constant_rate_schedule(*rate, *duration)
        }
        LoadModel::RampUsersPerSec { start_rate, end_rate, duration } => {
            build_ramp_rate_schedule(*start_rate, *end_rate, *duration)
        }
        LoadModel::HeavisideUsers { users, ramp_duration, hold_duration } => {
            build_heaviside_schedule(*users, *ramp_duration, *hold_duration)
        }
        LoadModel::Stages { stages } => {
            build_stages_schedule(stages)
        }
    }
}

/// At once: inject all users immediately
fn build_at_once_schedule(users: usize) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    for _ in 0..users {
        schedule.add(0); // All start at time 0
    }
    schedule.duration = None; // Run until all users complete
    schedule
}

/// Constant: fixed number of users, optional duration
fn build_constant_schedule(users: usize, duration: Option<u64>) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    
    if users == 0 {
        return schedule;
    }
    
    // If duration is specified, spread users evenly across it
    // Otherwise, start all at once
    if let Some(dur_sec) = duration {
        let total_ms = dur_sec * 1000;
        let interval_ms = if users > 1 {
            total_ms / (users - 1) as u64
        } else {
            0
        };
        
        for i in 0..users {
            schedule.add(i as u64 * interval_ms);
        }
    } else {
        // No duration specified, start all at once
        for _ in 0..users {
            schedule.add(0);
        }
    }
    
    schedule.duration = duration;
    schedule
}

/// Ramp up: gradually increase from 0 to target users over duration
fn build_ramp_up_schedule(users: usize, duration: u64) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    
    if users == 0 {
        return schedule;
    }
    
    let total_ms = duration * 1000;
    let interval_ms = if users > 1 {
        total_ms / (users - 1) as u64
    } else {
        0
    };
    
    for i in 0..users {
        schedule.add(i as u64 * interval_ms);
    }
    
    schedule.duration = Some(duration);
    schedule
}

/// Ramp down: gradually decrease users (start many, then fewer)
fn build_ramp_down_schedule(users: usize, duration: u64) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    
    if users == 0 {
        return schedule;
    }
    
    let total_ms = duration * 1000;
    
    // Start with more users at the beginning, fewer at the end
    // This is a bit artificial for a load test, but we'll spread them with decreasing intervals
    for i in 0..users {
        let progress = i as f64 / users as f64;
        let time_ms = (progress * progress * total_ms as f64) as u64; // Quadratic curve
        schedule.add(time_ms);
    }
    
    schedule.duration = Some(duration);
    schedule.sort();
    schedule
}

/// Constant rate: open model, inject users at constant rate (users per second)
fn build_constant_rate_schedule(rate: f64, duration: u64) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    
    if rate <= 0.0 {
        return schedule;
    }
    
    let total_users = (rate * duration as f64) as usize;
    let interval_ms = (1000.0 / rate) as u64;
    
    for i in 0..total_users {
        let time_ms = i as u64 * interval_ms;
        if time_ms < duration * 1000 {
            schedule.add(time_ms);
        }
    }
    
    schedule.duration = Some(duration);
    schedule
}

/// Ramp rate: gradually increase injection rate from start to end
fn build_ramp_rate_schedule(start_rate: f64, end_rate: f64, duration: u64) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    
    let duration_sec = duration as f64;
    let total_ms = duration * 1000;
    
    // Calculate total users: integral of linear ramp from start_rate to end_rate
    let avg_rate = (start_rate + end_rate) / 2.0;
    let total_users = (avg_rate * duration_sec) as usize;
    
    // Distribute users with increasing rate
    let mut current_time_ms = 0.0;
    for i in 0..total_users {
        if current_time_ms >= total_ms as f64 {
            break;
        }
        
        schedule.add(current_time_ms as u64);
        
        // Calculate instantaneous rate at current time
        let progress = current_time_ms / (total_ms as f64);
        let current_rate = start_rate + (end_rate - start_rate) * progress;
        
        // Time to next user based on current rate
        let interval_ms = if current_rate > 0.0 {
            1000.0 / current_rate
        } else {
            1000.0 // Default to 1 second if rate is 0
        };
        
        current_time_ms += interval_ms;
    }
    
    schedule.duration = Some(duration);
    schedule
}

/// Heaviside: ramp to target, then hold
fn build_heaviside_schedule(users: usize, ramp_duration: u64, hold_duration: u64) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    
    if users == 0 {
        return schedule;
    }
    
    let ramp_ms = ramp_duration * 1000;
    let interval_ms = if users > 1 {
        ramp_ms / (users - 1) as u64
    } else {
        0
    };
    
    // Inject all users during ramp period
    for i in 0..users {
        schedule.add(i as u64 * interval_ms);
    }
    
    schedule.duration = Some(ramp_duration + hold_duration);
    schedule
}

/// Stages: multi-stage custom profile
fn build_stages_schedule(stages: &[Stage]) -> InjectionSchedule {
    let mut schedule = InjectionSchedule::new();
    let mut current_time_sec = 0.0;
    
    for stage in stages {
        let stage_duration_sec = stage.duration as f64;
        let rate = stage.rate;
        
        if rate > 0.0 {
            let users_in_stage = (rate * stage_duration_sec) as usize;
            let interval_ms = (1000.0 / rate) as u64;
            
            for i in 0..users_in_stage {
                let time_in_stage_ms = i as u64 * interval_ms;
                let absolute_time_ms = (current_time_sec * 1000.0) as u64 + time_in_stage_ms;
                
                if time_in_stage_ms < (stage_duration_sec * 1000.0) as u64 {
                    schedule.add(absolute_time_ms);
                }
            }
        }
        
        current_time_sec += stage_duration_sec;
    }
    
    let total_duration = stages.iter().map(|s| s.duration).sum();
    schedule.duration = Some(total_duration);
    schedule
}

/// User injector that spawns virtual users according to schedule
pub struct UserInjector {
    schedule: InjectionSchedule,
    start_time: Option<Instant>,
}

impl UserInjector {
    /// Create a new injector with the given schedule
    pub fn new(schedule: InjectionSchedule) -> Self {
        Self {
            schedule,
            start_time: None,
        }
    }

    /// Start the injector
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Get the next user ID and delay until injection
    /// Returns (user_id, delay_ms) or None if all users injected
    pub async fn next_user(&mut self, current_user: usize) -> Option<(usize, u64)> {
        if current_user >= self.schedule.total_users() {
            return None;
        }

        let injection_time_ms = self.schedule.injection_times[current_user];
        
        if let Some(start) = self.start_time {
            let elapsed_ms = start.elapsed().as_millis() as u64;
            
            if injection_time_ms > elapsed_ms {
                let delay_ms = injection_time_ms - elapsed_ms;
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }
        
        Some((current_user + 1, injection_time_ms))
    }

    /// Get total number of users to inject
    pub fn total_users(&self) -> usize {
        self.schedule.total_users()
    }

    /// Get test duration
    pub fn duration(&self) -> Option<u64> {
        self.schedule.duration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_at_once_schedule() {
        let schedule = build_at_once_schedule(10);
        assert_eq!(schedule.total_users(), 10);
        assert!(schedule.injection_times.iter().all(|&t| t == 0));
        assert_eq!(schedule.duration, None);
    }

    #[test]
    fn test_ramp_up_schedule() {
        let schedule = build_ramp_up_schedule(5, 10);
        assert_eq!(schedule.total_users(), 5);
        assert_eq!(schedule.duration, Some(10));
        
        // Check evenly distributed
        assert_eq!(schedule.injection_times[0], 0);
        assert_eq!(schedule.injection_times[4], 10000);
    }

    #[test]
    fn test_constant_rate_schedule() {
        let schedule = build_constant_rate_schedule(10.0, 5);
        
        // 10 users/sec * 5 sec = 50 users
        assert_eq!(schedule.total_users(), 50);
        assert_eq!(schedule.duration, Some(5));
        
        // Interval should be 100ms between users
        assert_eq!(schedule.injection_times[0], 0);
        assert_eq!(schedule.injection_times[1], 100);
        assert_eq!(schedule.injection_times[9], 900);
    }

    #[test]
    fn test_ramp_rate_schedule() {
        let schedule = build_ramp_rate_schedule(1.0, 10.0, 10);
        
        // Average rate: (1 + 10) / 2 = 5.5, over 10 sec = ~55 users
        assert!(schedule.total_users() >= 50 && schedule.total_users() <= 60);
        assert_eq!(schedule.duration, Some(10));
        
        // Times should be increasing
        for i in 1..schedule.injection_times.len() {
            assert!(schedule.injection_times[i] > schedule.injection_times[i - 1]);
        }
    }

    #[test]
    fn test_heaviside_schedule() {
        let schedule = build_heaviside_schedule(10, 5, 15);
        assert_eq!(schedule.total_users(), 10);
        assert_eq!(schedule.duration, Some(20)); // 5 + 15
        
        // All users should be injected during ramp period
        assert!(schedule.injection_times.iter().all(|&t| t <= 5000));
    }

    #[test]
    fn test_stages_schedule() {
        let stages = vec![
            Stage { duration: 10, rate: 5.0 },
            Stage { duration: 10, rate: 10.0 },
        ];
        
        let schedule = build_stages_schedule(&stages);
        
        // Stage 1: 5 users/sec * 10 sec = 50
        // Stage 2: 10 users/sec * 10 sec = 100
        // Total: 150 users
        assert_eq!(schedule.total_users(), 150);
        assert_eq!(schedule.duration, Some(20));
    }

    #[test]
    fn test_constant_with_duration() {
        let schedule = build_constant_schedule(10, Some(20));
        assert_eq!(schedule.total_users(), 10);
        assert_eq!(schedule.duration, Some(20));
        
        // Users should be spread across 20 seconds
        assert_eq!(schedule.injection_times[0], 0);
        // Last user should be close to 20000ms (allowing for rounding)
        assert!(schedule.injection_times[9] >= 19998 && schedule.injection_times[9] <= 20000);
    }

    #[test]
    fn test_constant_without_duration() {
        let schedule = build_constant_schedule(10, None);
        assert_eq!(schedule.total_users(), 10);
        assert_eq!(schedule.duration, None);
        
        // All should start at once when no duration
        assert!(schedule.injection_times.iter().all(|&t| t == 0));
    }

    #[tokio::test]
    async fn test_user_injector() {
        let mut schedule = InjectionSchedule::new();
        schedule.add(0);
        schedule.add(100);
        schedule.add(200);
        
        let mut injector = UserInjector::new(schedule);
        injector.start();
        
        let start = Instant::now();
        
        // First user should be immediate
        let result = injector.next_user(0).await;
        assert!(result.is_some());
        let (user_id, _) = result.unwrap();
        assert_eq!(user_id, 1);
        
        // Second user after ~100ms
        let result = injector.next_user(1).await;
        assert!(result.is_some());
        let elapsed = start.elapsed().as_millis() as u64;
        assert!(elapsed >= 100 && elapsed < 200);
        
        // Third user after ~200ms total
        let result = injector.next_user(2).await;
        assert!(result.is_some());
        let elapsed = start.elapsed().as_millis() as u64;
        assert!(elapsed >= 200);
        
        // No more users
        let result = injector.next_user(3).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_build_schedule_integration() {
        // Test that build_schedule works for all load model types
        let models = vec![
            LoadModel::AtOnceUsers { users: 10 },
            LoadModel::Constant { users: 10, duration: Some(60) },
            LoadModel::RampUp { users: 20, duration: 120 },
            LoadModel::ConstantUsersPerSec { rate: 5.0, duration: 60 },
            LoadModel::HeavisideUsers { users: 50, ramp_duration: 30, hold_duration: 60 },
        ];
        
        for model in models {
            let schedule = build_schedule(&model);
            assert!(schedule.total_users() > 0, "Schedule should have users for {:?}", model);
        }
    }
}
