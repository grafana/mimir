use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Status};

use crate::kafka::{OffsetAt, PartitionClient};

pub struct Consistency {
    partition: i32,
    read_compartment: i32,
    consumed: Vec<AtomicI64>,
    high_watermark: Vec<AtomicI64>,
    last_consumed_timestamp_ms: Vec<AtomicI64>,
    clients: RwLock<Vec<Option<Arc<PartitionClient>>>>,
    changed: tokio::sync::Notify,
    timeout: Duration,
}

impl Consistency {
    pub fn new(partition: i32, read_compartment: i32, clusters: usize, timeout: Duration) -> Self {
        Self {
            partition,
            read_compartment,
            consumed: (0..clusters).map(|_| AtomicI64::new(-1)).collect(),
            high_watermark: (0..clusters).map(|_| AtomicI64::new(-1)).collect(),
            last_consumed_timestamp_ms: (0..clusters).map(|_| AtomicI64::new(0)).collect(),
            clients: RwLock::new((0..clusters).map(|_| None).collect()),
            changed: tokio::sync::Notify::new(),
            timeout,
        }
    }

    pub fn consumed(&self, cluster: usize, offset: i64, high_watermark: i64, timestamp_ms: i64) {
        self.consumed[cluster].store(offset, Ordering::Release);
        self.last_consumed_timestamp_ms[cluster].fetch_max(timestamp_ms, Ordering::AcqRel);
        self.high_watermark(cluster, high_watermark);
    }

    pub fn high_watermark(&self, cluster: usize, high_watermark: i64) {
        self.high_watermark[cluster].fetch_max(high_watermark.saturating_sub(1), Ordering::AcqRel);
        self.changed.notify_waiters();
    }

    pub fn set_client(&self, cluster: usize, client: Arc<PartitionClient>) {
        self.clients.write().expect("client lock poisoned")[cluster] = Some(client);
    }

    pub async fn enforce<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let level = request
            .metadata()
            .get("__consistency_level__")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("eventual");
        if level != "strong" {
            return self.enforce_max_delay(request);
        }
        let encoded_targets = request
            .metadata()
            .get("__consistency_offsets__")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| offsets_for(value, self.read_compartment, self.partition))
            .filter(|offsets| offsets.len() == self.consumed.len());
        let targets = if let Some(targets) = encoded_targets {
            targets
        } else {
            let clients = self.clients.read().expect("client lock poisoned").clone();
            if clients.iter().all(Option::is_some) {
                let mut targets = Vec::with_capacity(clients.len());
                for client in clients.into_iter().flatten() {
                    targets.push(
                        client
                            .get_offset(OffsetAt::Latest)
                            .await
                            .map_err(|error| Status::unavailable(error.to_string()))?
                            .saturating_sub(1),
                    );
                }
                targets
            } else {
                self.high_watermark
                    .iter()
                    .map(|offset| offset.load(Ordering::Acquire))
                    .collect()
            }
        };
        if self.reached(&targets) {
            return Ok(());
        }
        tokio::time::timeout(self.timeout, async {
            loop {
                let notified = self.changed.notified();
                if self.reached(&targets) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| Status::deadline_exceeded("waiting for ingest-storage read consistency"))
    }

    fn reached(&self, targets: &[i64]) -> bool {
        self.consumed
            .iter()
            .zip(targets)
            .all(|(consumed, target)| *target < 0 || consumed.load(Ordering::Acquire) >= *target)
    }

    fn enforce_max_delay<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(max_delay) = request
            .metadata()
            .get("__consistency_max_delay__")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_go_duration)
        else {
            return Ok(());
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as i64);
        let delayed = self
            .consumed
            .iter()
            .zip(&self.high_watermark)
            .zip(&self.last_consumed_timestamp_ms)
            .any(|((consumed, high), timestamp)| {
                consumed.load(Ordering::Acquire) < high.load(Ordering::Acquire)
                    && timestamp.load(Ordering::Acquire) > 0
                    && now.saturating_sub(timestamp.load(Ordering::Acquire))
                        > max_delay.as_millis() as i64
            });
        if delayed {
            Err(Status::unavailable(
                "partition reader exceeds the allowed read delay",
            ))
        } else {
            Ok(())
        }
    }
}

fn parse_go_duration(value: &str) -> Option<Duration> {
    let units = [
        ("ms", 1_000_000_u64),
        ("us", 1_000),
        ("µs", 1_000),
        ("ns", 1),
        ("h", 3_600_000_000_000),
        ("m", 60_000_000_000),
        ("s", 1_000_000_000),
    ];
    let mut rest = value;
    let mut total = 0_u64;
    while !rest.is_empty() {
        let number_end = rest
            .find(|character: char| !character.is_ascii_digit() && character != '.')
            .unwrap_or(rest.len());
        if number_end == 0 {
            return None;
        }
        let number: f64 = rest[..number_end].parse().ok()?;
        rest = &rest[number_end..];
        let (unit, nanos) = units.iter().find(|(unit, _)| rest.starts_with(unit))?;
        total = total.checked_add((number * *nanos as f64) as u64)?;
        rest = &rest[unit.len()..];
    }
    Some(Duration::from_nanos(total))
}

fn offsets_for(encoded: &str, read_compartment: i32, partition: i32) -> Option<Vec<i64>> {
    if let Some(entries) = encoded.strip_prefix("v1=") {
        return find_offset(entries, &format!("{partition}:")).map(|offset| vec![offset]);
    }
    let entries = encoded.strip_prefix("v2=")?;
    let offsets = find_value(entries, &format!("{read_compartment}/{partition}:"))?;
    offsets
        .split(';')
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()
}

fn find_offset(entries: &str, key: &str) -> Option<i64> {
    find_value(entries, key)?.parse().ok()
}

fn find_value<'a>(entries: &'a str, key: &str) -> Option<&'a str> {
    entries.split(',').find_map(|entry| entry.strip_prefix(key))
}

#[cfg(test)]
mod tests {
    use super::offsets_for;

    #[test]
    fn reads_mimir_offset_encodings() {
        assert_eq!(Some(vec![42]), offsets_for("v1=1:7,3:42", 0, 3));
        assert_eq!(Some(vec![42, 90]), offsets_for("v2=0/1:7,2/3:42;90", 2, 3));
        assert_eq!(None, offsets_for("v2=0/3:42", 2, 3));
    }
}
