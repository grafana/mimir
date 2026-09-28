//! Read-path protection like the Go ingester's `StartReadRequest`: a CPU and memory utilization
//! based limiter and the read circuit breaker, applied to every `/cortex.Ingester/*` gRPC call,
//! plus the `cortex_request_duration_seconds` instrumentation with gRPC status codes.

use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::Result;
use http_body::{Body, Frame, SizeHint};
use tonic::Status;

use crate::limits::parse_duration_ms;
use crate::metrics;

pub const TOO_BUSY_MESSAGE: &str =
    "ingester is currently too busy to process queries, try again later";
pub const READ_REQUEST_TYPE: &str = "read";
pub const PUSH_REQUEST_TYPE: &str = "push";
const ONLY_REPLICA_HEADER: &str = "__only_replica__";

#[derive(clap::Args, Clone, Debug)]
pub struct ProtectionArgs {
    #[arg(long = "ingester.read-circuit-breaker.enabled", default_value_t = false, action = clap::ArgAction::Set)]
    pub circuit_breaker_enabled: bool,
    #[arg(
        long = "ingester.read-circuit-breaker.failure-threshold-percentage",
        default_value_t = 10
    )]
    pub failure_threshold_percentage: u32,
    #[arg(
        long = "ingester.read-circuit-breaker.failure-execution-threshold",
        default_value_t = 100
    )]
    pub failure_execution_threshold: u32,
    #[arg(
        long = "ingester.read-circuit-breaker.thresholding-period",
        default_value = "1m"
    )]
    pub thresholding_period: String,
    #[arg(
        long = "ingester.read-circuit-breaker.cooldown-period",
        default_value = "10s"
    )]
    pub cooldown_period: String,
    #[arg(
        long = "ingester.read-circuit-breaker.initial-delay",
        default_value = "0"
    )]
    pub initial_delay: String,
    #[arg(
        long = "ingester.read-circuit-breaker.request-timeout",
        default_value = "30s"
    )]
    pub request_timeout: String,
    /// The push circuit breaker, which the Go ingester applies to each head append of records.
    #[arg(long = "ingester.push-circuit-breaker.enabled", default_value_t = false, action = clap::ArgAction::Set)]
    pub push_circuit_breaker_enabled: bool,
    #[arg(
        long = "ingester.push-circuit-breaker.failure-threshold-percentage",
        default_value_t = 10
    )]
    pub push_failure_threshold_percentage: u32,
    #[arg(
        long = "ingester.push-circuit-breaker.failure-execution-threshold",
        default_value_t = 100
    )]
    pub push_failure_execution_threshold: u32,
    #[arg(
        long = "ingester.push-circuit-breaker.thresholding-period",
        default_value = "1m"
    )]
    pub push_thresholding_period: String,
    #[arg(
        long = "ingester.push-circuit-breaker.cooldown-period",
        default_value = "10s"
    )]
    pub push_cooldown_period: String,
    #[arg(
        long = "ingester.push-circuit-breaker.initial-delay",
        default_value = "0"
    )]
    pub push_initial_delay: String,
    #[arg(
        long = "ingester.push-circuit-breaker.request-timeout",
        default_value = "2s"
    )]
    pub push_request_timeout: String,
    /// CPU cores; 0 disables the CPU part of the utilization limiter.
    #[arg(
        long = "ingester.read-path-cpu-utilization-limit",
        default_value_t = 0.0
    )]
    pub cpu_utilization_limit: f64,
    /// Bytes of allocated memory; 0 disables the memory part of the utilization limiter.
    #[arg(
        long = "ingester.read-path-memory-utilization-limit",
        default_value_t = 0
    )]
    pub memory_utilization_limit: u64,
    #[arg(long = "server.grpc-max-concurrent-streams", default_value_t = 100)]
    pub grpc_max_concurrent_streams: u32,
}

#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    pub failure_threshold_percentage: u32,
    pub failure_execution_threshold: u32,
    pub thresholding_period: Duration,
    pub cooldown_period: Duration,
    pub initial_delay: Duration,
    pub request_timeout: Duration,
}

fn breaker_config(
    enabled: bool,
    failure_threshold_percentage: u32,
    failure_execution_threshold: u32,
    periods: [&str; 4],
) -> Result<Option<CircuitBreakerConfig>> {
    if !enabled {
        return Ok(None);
    }
    let duration = |value: &str| -> Result<Duration> {
        Ok(Duration::from_millis(parse_duration_ms(value)? as u64))
    };
    let [thresholding, cooldown, initial_delay, request_timeout] = periods;
    Ok(Some(CircuitBreakerConfig {
        failure_threshold_percentage,
        failure_execution_threshold,
        thresholding_period: duration(thresholding)?,
        cooldown_period: duration(cooldown)?,
        initial_delay: duration(initial_delay)?,
        request_timeout: duration(request_timeout)?,
    }))
}

impl ProtectionArgs {
    pub fn circuit_breaker(&self) -> Result<Option<CircuitBreakerConfig>> {
        breaker_config(
            self.circuit_breaker_enabled,
            self.failure_threshold_percentage,
            self.failure_execution_threshold,
            [
                &self.thresholding_period,
                &self.cooldown_period,
                &self.initial_delay,
                &self.request_timeout,
            ],
        )
    }

    pub fn push_circuit_breaker(&self) -> Result<Option<CircuitBreakerConfig>> {
        breaker_config(
            self.push_circuit_breaker_enabled,
            self.push_failure_threshold_percentage,
            self.push_failure_execution_threshold,
            [
                &self.push_thresholding_period,
                &self.push_cooldown_period,
                &self.push_initial_delay,
                &self.push_request_timeout,
            ],
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerState {
    fn label(self) -> &'static str {
        match self {
            BreakerState::Closed => "closed",
            BreakerState::Open => "open",
            BreakerState::HalfOpen => "half-open",
        }
    }
}

const BUCKETS: usize = 10;

struct BreakerInner {
    state: BreakerState,
    opened_at: Instant,
    // Closed state: executions and failures in time buckets covering the thresholding period.
    buckets: [(Instant, u32, u32); BUCKETS],
    // Half-open state: permits handed out and results recorded.
    half_open_permits: u32,
    half_open_executions: u32,
    half_open_failures: u32,
}

/// A time-based failure-rate circuit breaker with the behavior of failsafe-go's, as configured by
/// the Go ingester: only requests that time out count as failures.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    request_type: &'static str,
    // 0 inactive, 1 pending the initial delay, 2 active.
    activation: AtomicU8,
    activated_at: Mutex<Option<Instant>>,
    inner: Mutex<BreakerInner>,
}

#[derive(Clone, Debug)]
pub struct Permit {
    started: Instant,
    half_open: bool,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig, request_type: &'static str) -> Self {
        let now = Instant::now();
        for state in [
            BreakerState::Closed,
            BreakerState::Open,
            BreakerState::HalfOpen,
        ] {
            metrics::CIRCUIT_BREAKER_CURRENT_STATE
                .with_label_values(&[request_type, state.label()])
                .set(i64::from(state == BreakerState::Closed));
            metrics::CIRCUIT_BREAKER_TRANSITIONS.with_label_values(&[request_type, state.label()]);
        }
        for result in ["success", "error", "circuit_breaker_open"] {
            metrics::CIRCUIT_BREAKER_RESULTS.with_label_values(&[request_type, result]);
        }
        Self {
            config,
            request_type,
            activation: AtomicU8::new(0),
            activated_at: Mutex::new(None),
            inner: Mutex::new(BreakerInner {
                state: BreakerState::Closed,
                opened_at: now,
                buckets: [(now, 0, 0); BUCKETS],
                half_open_permits: 0,
                half_open_executions: 0,
                half_open_failures: 0,
            }),
        }
    }

    /// Called once the ingester serves reads; the breaker starts counting after the initial delay.
    pub fn activate(&self) {
        *self.activated_at.lock().expect("breaker lock poisoned") = Some(Instant::now());
        self.activation.store(1, Ordering::Release);
    }

    fn is_active(&self) -> bool {
        match self.activation.load(Ordering::Acquire) {
            2 => true,
            1 => {
                let activated_at = *self.activated_at.lock().expect("breaker lock poisoned");
                if activated_at.is_some_and(|at| at.elapsed() >= self.config.initial_delay) {
                    self.activation.store(2, Ordering::Release);
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn state(&self) -> BreakerState {
        self.inner.lock().expect("breaker lock poisoned").state
    }

    fn transition(&self, inner: &mut BreakerInner, state: BreakerState, now: Instant) {
        metrics::CIRCUIT_BREAKER_CURRENT_STATE
            .with_label_values(&[self.request_type, inner.state.label()])
            .set(0);
        metrics::CIRCUIT_BREAKER_CURRENT_STATE
            .with_label_values(&[self.request_type, state.label()])
            .set(1);
        metrics::CIRCUIT_BREAKER_TRANSITIONS
            .with_label_values(&[self.request_type, state.label()])
            .inc();
        eprintln!(
            "phase=circuit_breaker previous={} current={}",
            inner.state.label(),
            state.label()
        );
        inner.state = state;
        inner.opened_at = now;
        inner.buckets = [(now, 0, 0); BUCKETS];
        inner.half_open_permits = 0;
        inner.half_open_executions = 0;
        inner.half_open_failures = 0;
    }

    /// `Ok(None)` when the breaker is not active yet; `Err` with the remaining delay when open.
    pub fn try_acquire(&self) -> Result<Option<Permit>, Duration> {
        self.try_acquire_at(Instant::now())
    }

    fn try_acquire_at(&self, now: Instant) -> Result<Option<Permit>, Duration> {
        if !self.is_active() {
            return Ok(None);
        }
        let mut inner = self.inner.lock().expect("breaker lock poisoned");
        if inner.state == BreakerState::Open {
            let elapsed = now.saturating_duration_since(inner.opened_at);
            if elapsed < self.config.cooldown_period {
                metrics::CIRCUIT_BREAKER_RESULTS
                    .with_label_values(&[self.request_type, "circuit_breaker_open"])
                    .inc();
                return Err(self.config.cooldown_period - elapsed);
            }
            self.transition(&mut inner, BreakerState::HalfOpen, now);
        }
        if inner.state == BreakerState::HalfOpen {
            if inner.half_open_permits >= self.config.failure_execution_threshold.max(1) {
                metrics::CIRCUIT_BREAKER_RESULTS
                    .with_label_values(&[self.request_type, "circuit_breaker_open"])
                    .inc();
                return Err(Duration::ZERO);
            }
            inner.half_open_permits += 1;
            return Ok(Some(Permit {
                started: now,
                half_open: true,
            }));
        }
        Ok(Some(Permit {
            started: now,
            half_open: false,
        }))
    }

    /// Records the end of a request; `deadline_exceeded` is its gRPC status.
    pub fn finish(&self, permit: Permit, deadline_exceeded: bool) {
        self.finish_at(permit, deadline_exceeded, Instant::now());
    }

    /// Records `executions` requests that ran together under `permit`, like the head appends of
    /// the Go ingester's flushes for one batch of records.
    pub fn finish_all(&self, permit: Permit, executions: usize) {
        let now = Instant::now();
        for _ in 0..executions.max(1) {
            self.finish_at(permit.clone(), false, now);
        }
    }

    fn finish_at(&self, permit: Permit, deadline_exceeded: bool, now: Instant) {
        let failed = deadline_exceeded
            || now.saturating_duration_since(permit.started) > self.config.request_timeout;
        if failed {
            metrics::CIRCUIT_BREAKER_REQUEST_TIMEOUTS
                .with_label_values(&[self.request_type])
                .inc();
        }
        metrics::CIRCUIT_BREAKER_RESULTS
            .with_label_values(&[self.request_type, if failed { "error" } else { "success" }])
            .inc();
        let mut inner = self.inner.lock().expect("breaker lock poisoned");
        let threshold = f64::from(self.config.failure_threshold_percentage) / 100.0;
        match inner.state {
            BreakerState::Open => {}
            BreakerState::HalfOpen if permit.half_open => {
                inner.half_open_executions += 1;
                inner.half_open_failures += u32::from(failed);
                let capacity = self.config.failure_execution_threshold.max(1);
                let failures = f64::from(inner.half_open_failures);
                if failures / f64::from(capacity) >= threshold && failures > 0.0 {
                    self.transition(&mut inner, BreakerState::Open, now);
                } else if inner.half_open_executions >= capacity {
                    self.transition(&mut inner, BreakerState::Closed, now);
                }
            }
            BreakerState::HalfOpen => {}
            BreakerState::Closed => {
                let bucket_width = (self.config.thresholding_period / BUCKETS as u32)
                    .max(Duration::from_millis(1));
                let since_open = now.saturating_duration_since(inner.opened_at);
                let index = (since_open.as_nanos() / bucket_width.as_nanos()) as usize;
                let bucket_start = inner.opened_at + bucket_width * index as u32;
                let slot = &mut inner.buckets[index % BUCKETS];
                if slot.0 != bucket_start {
                    *slot = (bucket_start, 0, 0);
                }
                slot.1 += 1;
                slot.2 += u32::from(failed);
                let window_start = now
                    .checked_sub(self.config.thresholding_period)
                    .unwrap_or(inner.opened_at);
                let (executions, failures) = inner
                    .buckets
                    .iter()
                    .filter(|(start, _, _)| *start + bucket_width > window_start)
                    .fold((0, 0), |(executions, failures), (_, e, f)| {
                        (executions + e, failures + f)
                    });
                if executions >= self.config.failure_execution_threshold
                    && executions > 0
                    && f64::from(failures) / f64::from(executions) >= threshold
                {
                    self.transition(&mut inner, BreakerState::Open, now);
                }
            }
        }
    }
}

/// Reads process CPU time in seconds and memory in bytes.
pub trait UtilizationScanner: Send + 'static {
    fn scan(&mut self) -> Option<(f64, u64)>;
}

pub struct ProcessScanner;

impl UtilizationScanner for ProcessScanner {
    fn scan(&mut self) -> Option<(f64, u64)> {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        // Fields after the parenthesized command name: utime and stime are the 12th and 13th.
        let fields = stat
            .rsplit_once(')')?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        let ticks = fields.get(11)?.parse::<f64>().ok()? + fields.get(12)?.parse::<f64>().ok()?;
        Some((ticks / 100.0, allocated_bytes()?))
    }
}

// Go limits on heap object bytes; the allocator's allocated bytes are the equivalent.
#[cfg(all(target_os = "linux", feature = "jemalloc"))]
fn allocated_bytes() -> Option<u64> {
    tikv_jemalloc_ctl::epoch::advance().ok()?;
    tikv_jemalloc_ctl::stats::allocated::read()
        .ok()
        .map(|bytes| bytes as u64)
}

#[cfg(not(all(target_os = "linux", feature = "jemalloc")))]
fn allocated_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("RssAnon:"))?;
    Some(line.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
}

const UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const SLIDING_WINDOW: Duration = Duration::from_secs(60);

/// Go's `UtilizationBasedLimiter`: a one-minute moving average of CPU cores, sampled every second
/// and reported after a one-minute warmup, and the current memory use.
pub struct UtilizationLimiter {
    cpu_limit: f64,
    memory_limit: u64,
    alpha: f64,
    rate: Option<f64>,
    last: Option<(Instant, f64)>,
    first: Option<Instant>,
    reason: Arc<Mutex<&'static str>>,
}

impl UtilizationLimiter {
    pub fn new(cpu_limit: f64, memory_limit: u64) -> Self {
        Self {
            cpu_limit,
            memory_limit,
            alpha: 2.0 / (SLIDING_WINDOW.as_secs_f64() / UPDATE_INTERVAL.as_secs_f64() + 1.0),
            rate: None,
            last: None,
            first: None,
            reason: Arc::new(Mutex::new("")),
        }
    }

    pub fn reason_handle(&self) -> Arc<Mutex<&'static str>> {
        Arc::clone(&self.reason)
    }

    /// One update tick; returns the CPU utilization and memory it computed.
    pub fn compute(&mut self, now: Instant, sample: Option<(f64, u64)>) -> (f64, u64) {
        let Some((cpu_time, memory)) = sample else {
            *self.reason.lock().expect("limiter lock poisoned") = "";
            return (0.0, 0);
        };
        match self.last {
            Some((previous, previous_cpu)) => {
                let elapsed = now.saturating_duration_since(previous);
                if elapsed > UPDATE_INTERVAL / 2 {
                    let instant = (cpu_time - previous_cpu) / elapsed.as_secs_f64();
                    // Go's EWMA counts whole hundredths of a core.
                    let instant = (instant * 100.0).trunc();
                    self.rate = Some(match self.rate {
                        Some(rate) => rate + self.alpha * (instant - rate),
                        None => instant,
                    });
                    self.last = Some((now, cpu_time));
                }
            }
            None => self.last = Some((now, cpu_time)),
        }
        let mut cpu = 0.0;
        match self.first {
            None => self.first = Some(now),
            Some(first) if now.saturating_duration_since(first) >= SLIDING_WINDOW => {
                cpu = self.rate.unwrap_or(0.0) / 100.0;
            }
            Some(_) => {}
        }
        let reason = if self.memory_limit > 0 && memory >= self.memory_limit {
            "memory"
        } else if self.cpu_limit > 0.0 && cpu >= self.cpu_limit {
            "cpu"
        } else {
            ""
        };
        let mut current = self.reason.lock().expect("limiter lock poisoned");
        if (reason.is_empty()) != (current.is_empty()) {
            eprintln!(
                "phase=utilization_limiting enabled={} reason={reason} cpu={cpu:.2} memory_bytes={memory}",
                !reason.is_empty()
            );
        }
        *current = reason;
        (cpu, memory)
    }

    pub async fn run(mut self, mut scanner: impl UtilizationScanner) {
        let mut ticker = tokio::time::interval(UPDATE_INTERVAL);
        loop {
            ticker.tick().await;
            let (cpu, memory) = self.compute(Instant::now(), scanner.scan());
            metrics::UTILIZATION_CPU.set(cpu);
            metrics::UTILIZATION_MEMORY.set(memory as f64);
        }
    }
}

/// The read-path checks shared by every gRPC call.
#[derive(Clone, Default)]
pub struct ReadProtection {
    pub limiting_reason: Option<Arc<Mutex<&'static str>>>,
    pub circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl ReadProtection {
    fn start(&self, only_replica: bool) -> Result<Option<Permit>, Status> {
        if let Some(reason) = &self.limiting_reason {
            let reason = *reason.lock().expect("limiter lock poisoned");
            if !reason.is_empty() {
                metrics::UTILIZATION_LIMITED_READS
                    .with_label_values(&[reason])
                    .inc();
                return Err(Status::unavailable(TOO_BUSY_MESSAGE));
            }
        }
        // A request no other replica can serve is never rejected for overload.
        if only_replica {
            return Ok(None);
        }
        match &self.circuit_breaker {
            Some(breaker) => breaker.try_acquire().map_err(|remaining| {
                Status::unavailable(format!(
                    "circuit breaker open on read request type with remaining delay {remaining:?}"
                ))
            }),
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct ProtectionLayer {
    protection: ReadProtection,
}

impl ProtectionLayer {
    pub fn new(protection: ReadProtection) -> Self {
        Self { protection }
    }
}

impl<S> tower::Layer<S> for ProtectionLayer {
    type Service = ProtectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProtectionService {
            inner,
            protection: self.protection.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ProtectionService<S> {
    inner: S,
    protection: ReadProtection,
}

type BoxFuture<T> = Pin<Box<dyn std::future::Future<Output = T> + Send>>;

impl<S, B> tower::Service<http::Request<B>> for ProtectionService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<tonic::body::Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let route = request.uri().path().to_owned();
        let only_replica = request
            .headers()
            .get(ONLY_REPLICA_HEADER)
            .is_some_and(|value| value == "true");
        let read = route.starts_with("/cortex.Ingester/");
        // Keep the service that was polled ready and leave a fresh clone in its place.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let protection = self.protection.clone();
        Box::pin(async move {
            let tracker = RequestTracker::new(route);
            let permit = if read {
                match protection.start(only_replica) {
                    Ok(permit) => permit,
                    Err(status) => {
                        tracker.finish(status.code() as i32);
                        return Ok(status.into_http());
                    }
                }
            } else {
                None
            };
            let finish = Finish {
                tracker: Some(tracker),
                permit: permit.map(|permit| (permit, protection.circuit_breaker.clone())),
            };
            let response = inner.call(request).await?;
            let header_status = grpc_status(response.headers());
            let (parts, body) = response.into_parts();
            let body = InstrumentedBody {
                inner: body,
                status: header_status,
                finish: Some(finish),
            };
            Ok(http::Response::from_parts(
                parts,
                tonic::body::Body::new(body),
            ))
        })
    }
}

fn grpc_status(headers: &http::HeaderMap) -> Option<i32> {
    headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

struct RequestTracker {
    route: String,
    started: Instant,
}

impl RequestTracker {
    fn new(route: String) -> Self {
        metrics::INFLIGHT_REQUESTS
            .with_label_values(&["gRPC", &route])
            .inc();
        Self {
            route,
            started: Instant::now(),
        }
    }

    fn finish(&self, code: i32) {
        metrics::INFLIGHT_REQUESTS
            .with_label_values(&["gRPC", &self.route])
            .dec();
        metrics::REQUEST_DURATION
            .with_label_values(&["gRPC", &self.route, grpc_code_name(code), "false"])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

struct Finish {
    tracker: Option<RequestTracker>,
    permit: Option<(Permit, Option<Arc<CircuitBreaker>>)>,
}

impl Finish {
    fn complete(mut self, code: i32) {
        if let Some(tracker) = self.tracker.take() {
            tracker.finish(code);
        }
        if let Some((permit, Some(breaker))) = self.permit.take() {
            breaker.finish(permit, code == tonic::Code::DeadlineExceeded as i32);
        }
    }
}

/// Watches the response body for the gRPC status in its trailers, so streaming calls are timed and
/// classified when they end; a body dropped before its end counts as canceled.
struct InstrumentedBody {
    inner: tonic::body::Body,
    status: Option<i32>,
    finish: Option<Finish>,
}

impl Drop for InstrumentedBody {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish.complete(self.status.unwrap_or(tonic::Code::Cancelled as i32));
        }
    }
}

impl Body for InstrumentedBody {
    type Data = bytes::Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(context);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref()
                    && let Some(code) = grpc_status(trailers)
                {
                    self.status = Some(code);
                }
            }
            Poll::Ready(Some(Err(_))) => {
                self.status.get_or_insert(tonic::Code::Internal as i32);
            }
            Poll::Ready(None) => {
                if let Some(finish) = self.finish.take() {
                    finish.complete(self.status.unwrap_or(0));
                }
            }
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// gRPC code names as Go's `codes.Code.String()` prints them.
pub fn grpc_code_name(code: i32) -> &'static str {
    match code {
        0 => "OK",
        1 => "Canceled",
        2 => "Unknown",
        3 => "InvalidArgument",
        4 => "DeadlineExceeded",
        5 => "NotFound",
        6 => "AlreadyExists",
        7 => "PermissionDenied",
        8 => "ResourceExhausted",
        9 => "FailedPrecondition",
        10 => "Aborted",
        11 => "OutOfRange",
        12 => "Unimplemented",
        13 => "Internal",
        14 => "Unavailable",
        15 => "DataLoss",
        16 => "Unauthenticated",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold_percentage: 20,
            failure_execution_threshold: 10,
            thresholding_period: Duration::from_secs(60),
            cooldown_period: Duration::from_secs(10),
            initial_delay: Duration::ZERO,
            request_timeout: Duration::from_secs(30),
        }
    }

    fn run(breaker: &CircuitBreaker, at: Instant, failed: bool) {
        let permit = breaker.try_acquire_at(at).unwrap().unwrap();
        breaker.finish_at(permit, failed, at);
    }

    #[test]
    fn circuit_breaker_is_inactive_until_activated() {
        let breaker = CircuitBreaker::new(config(), READ_REQUEST_TYPE);
        assert!(breaker.try_acquire().unwrap().is_none());
        let delayed = CircuitBreaker::new(
            CircuitBreakerConfig {
                initial_delay: Duration::from_secs(3600),
                ..config()
            },
            READ_REQUEST_TYPE,
        );
        delayed.activate();
        assert!(delayed.try_acquire().unwrap().is_none());
    }

    #[test]
    fn push_circuit_breaker_counts_every_head_append_of_a_batch() {
        let breaker = CircuitBreaker::new(
            CircuitBreakerConfig {
                request_timeout: Duration::ZERO,
                ..config()
            },
            PUSH_REQUEST_TYPE,
        );
        breaker.activate();
        let errors = || {
            metrics::CIRCUIT_BREAKER_RESULTS
                .with_label_values(&[PUSH_REQUEST_TYPE, "error"])
                .get()
        };
        let before = errors();
        let permit = breaker.try_acquire().unwrap().unwrap();
        std::thread::sleep(Duration::from_millis(2));
        // One slow batch of ten flushes reaches the execution threshold on its own.
        breaker.finish_all(permit, 10);
        assert_eq!(errors() - before, 10);
        assert_eq!(breaker.state(), BreakerState::Open);
    }

    #[test]
    fn circuit_breaker_opens_on_timeouts_and_recovers_through_half_open() {
        let breaker = CircuitBreaker::new(config(), READ_REQUEST_TYPE);
        breaker.activate();
        let start = Instant::now();
        // Below the execution threshold nothing opens, even with failures.
        for _ in 0..5 {
            run(&breaker, start, true);
        }
        assert_eq!(breaker.state(), BreakerState::Closed);
        for _ in 0..4 {
            run(&breaker, start, false);
        }
        assert_eq!(breaker.state(), BreakerState::Closed);
        run(&breaker, start, false);
        assert_eq!(breaker.state(), BreakerState::Open);
        let remaining = breaker
            .try_acquire_at(start + Duration::from_secs(4))
            .unwrap_err();
        assert_eq!(remaining, Duration::from_secs(6));
        // After the cooldown a half-open trial of successes closes it again.
        let later = start + Duration::from_secs(11);
        let permits = (0..10)
            .map(|_| breaker.try_acquire_at(later).unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(breaker.state(), BreakerState::HalfOpen);
        assert!(breaker.try_acquire_at(later).is_err());
        for permit in permits {
            breaker.finish_at(permit, false, later);
        }
        assert_eq!(breaker.state(), BreakerState::Closed);
    }

    #[test]
    fn slow_requests_count_as_failures_and_old_ones_expire() {
        let breaker = CircuitBreaker::new(config(), READ_REQUEST_TYPE);
        breaker.activate();
        let start = Instant::now();
        for _ in 0..3 {
            let permit = breaker.try_acquire_at(start).unwrap().unwrap();
            breaker.finish_at(permit, false, start + Duration::from_secs(31));
        }
        // The slow requests fall out of the one-minute window before enough executions arrive.
        let later = start + Duration::from_secs(120);
        for _ in 0..10 {
            run(&breaker, later, false);
        }
        assert_eq!(breaker.state(), BreakerState::Closed);
        // 3 slow requests out of 13 cross the 20% threshold; 2 out of 12 do not.
        for slow in 1..=3 {
            let permit = breaker.try_acquire_at(later).unwrap().unwrap();
            breaker.finish_at(permit, false, later + Duration::from_secs(31));
            let expected = if slow == 3 {
                BreakerState::Open
            } else {
                BreakerState::Closed
            };
            assert_eq!(breaker.state(), expected, "slow request {slow}");
        }
    }

    #[test]
    fn utilization_limiter_limits_on_memory_at_once_and_cpu_after_warmup() {
        let mut limiter = UtilizationLimiter::new(2.0, 1000);
        let reason = limiter.reason_handle();
        let start = Instant::now();
        limiter.compute(start, Some((0.0, 2000)));
        assert_eq!(*reason.lock().unwrap(), "memory");
        // Four cores busy: the CPU average only counts after the one-minute warmup.
        let mut cpu = 0.0;
        for second in 1..=59 {
            cpu += 4.0;
            limiter.compute(start + Duration::from_secs(second), Some((cpu, 0)));
            assert_eq!(*reason.lock().unwrap(), "", "second {second}");
        }
        cpu += 4.0;
        let (load, _) = limiter.compute(start + Duration::from_secs(60), Some((cpu, 0)));
        assert!((load - 4.0).abs() < 1e-9, "{load}");
        assert_eq!(*reason.lock().unwrap(), "cpu");
        // Without stats nothing is limited.
        limiter.compute(start + Duration::from_secs(61), None);
        assert_eq!(*reason.lock().unwrap(), "");
    }

    #[test]
    fn protection_rejects_when_too_busy_and_skips_the_breaker_for_only_replica_requests() {
        let breaker = Arc::new(CircuitBreaker::new(config(), READ_REQUEST_TYPE));
        breaker.activate();
        let reason = Arc::new(Mutex::new("cpu"));
        let protection = ReadProtection {
            limiting_reason: Some(Arc::clone(&reason)),
            circuit_breaker: Some(Arc::clone(&breaker)),
        };
        let status = protection.start(false).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), TOO_BUSY_MESSAGE);
        *reason.lock().unwrap() = "";
        assert!(protection.start(false).unwrap().is_some());
        assert!(protection.start(true).unwrap().is_none());
    }

    fn request(route: &str, only_replica: bool) -> http::Request<tonic::body::Body> {
        let mut request = http::Request::builder().uri(route);
        if only_replica {
            request = request.header(ONLY_REPLICA_HEADER, "true");
        }
        request.body(tonic::body::Body::empty()).unwrap()
    }

    fn observed(route: &str, code: &str) -> u64 {
        metrics::REQUEST_DURATION
            .with_label_values(&["gRPC", route, code, "false"])
            .get_sample_count()
    }

    // A streaming response whose status arrives in trailers, like tonic's server streams.
    fn streaming(code: tonic::Code) -> http::Response<tonic::body::Body> {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", (code as i32).to_string().parse().unwrap());
        let frames = futures::stream::iter([
            Ok::<_, Status>(Frame::data(bytes::Bytes::from_static(b"x"))),
            Ok(Frame::trailers(trailers)),
        ]);
        http::Response::new(tonic::body::Body::new(http_body_util::StreamBody::new(
            frames,
        )))
    }

    #[tokio::test]
    async fn layer_rejects_overloaded_reads_and_instruments_streams_by_status() {
        use http_body_util::BodyExt;
        use tower::{Layer, ServiceExt};

        let breaker = Arc::new(CircuitBreaker::new(config(), READ_REQUEST_TYPE));
        breaker.activate();
        let reason = Arc::new(Mutex::new("memory"));
        let layer = ProtectionLayer::new(ReadProtection {
            limiting_reason: Some(Arc::clone(&reason)),
            circuit_breaker: Some(Arc::clone(&breaker)),
        });
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let inner_calls = Arc::clone(&calls);
        let service = layer.layer(tower::service_fn(
            move |request: http::Request<tonic::body::Body>| {
                inner_calls.fetch_add(1, Ordering::Relaxed);
                let code = if request.uri().path().ends_with("Slow") {
                    tonic::Code::DeadlineExceeded
                } else {
                    tonic::Code::Ok
                };
                async move { Ok::<_, std::convert::Infallible>(streaming(code)) }
            },
        ));

        let route = "/cortex.Ingester/LayerTestBusy";
        let response = service
            .clone()
            .oneshot(request(route, false))
            .await
            .unwrap();
        assert_eq!(
            grpc_status(response.headers()),
            Some(tonic::Code::Unavailable as i32)
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(observed(route, "Unavailable"), 1);

        *reason.lock().unwrap() = "";
        let route = "/cortex.Ingester/LayerTestSlow";
        let timeouts = || {
            metrics::CIRCUIT_BREAKER_REQUEST_TIMEOUTS
                .with_label_values(&[READ_REQUEST_TYPE])
                .get()
        };
        let before = timeouts();
        let response = service
            .clone()
            .oneshot(request(route, false))
            .await
            .unwrap();
        assert_eq!(observed(route, "DeadlineExceeded"), 0);
        response.into_body().collect().await.unwrap();
        assert_eq!(observed(route, "DeadlineExceeded"), 1);
        assert!(timeouts() > before);

        // Requests only this replica can serve skip the breaker but are still instrumented.
        let route = "/cortex.Ingester/LayerTestOnlyReplica";
        let response = service.clone().oneshot(request(route, true)).await.unwrap();
        response.into_body().collect().await.unwrap();
        assert_eq!(observed(route, "OK"), 1);

        // A body dropped before its end, like a canceled stream.
        let route = "/cortex.Ingester/LayerTestCanceled";
        drop(
            service
                .clone()
                .oneshot(request(route, false))
                .await
                .unwrap(),
        );
        assert_eq!(observed(route, "Canceled"), 1);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}
