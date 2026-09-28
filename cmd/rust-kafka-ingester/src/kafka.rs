use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, Message};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;

use crate::record::{DecodedRequest, decode_record};

#[derive(Clone, Copy)]
pub enum OffsetAt {
    Earliest,
    Latest,
}

#[derive(Clone, Copy)]
pub enum StartOffset {
    Earliest,
    Latest,
    At(i64),
}

pub struct Record {
    pub timestamp_ms: i64,
    pub tenant: String,
    pub request: Option<Result<DecodedRequest>>,
}

pub struct RecordAndOffset {
    pub offset: i64,
    pub record: Record,
}

/// A fetched record whose payload has not been decoded yet.
pub struct RawRecord {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub tenant: String,
    pub version: u32,
    pub payload: Option<Vec<u8>>,
}

impl RawRecord {
    pub fn decode(&self) -> Option<Result<DecodedRequest>> {
        self.payload
            .as_deref()
            .map(|payload| decode_record(self.version, payload))
    }
}

pub struct PartitionClient {
    consumer: StreamConsumer<KafkaContext>,
    topic: String,
    partition: i32,
    high_watermark: AtomicI64,
}

struct KafkaContext {
    topic: String,
    partition: i32,
    debug: bool,
}

impl ClientContext for KafkaContext {
    fn log(&self, level: RDKafkaLogLevel, facility: &str, message: &str) {
        if self.debug || (level as u8) <= (RDKafkaLogLevel::Warning as u8) {
            eprintln!(
                "phase=kafka_client_log partition={} level={level:?} facility={facility} message={message}",
                self.partition
            );
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        eprintln!(
            "phase=kafka_client_error partition={} error={error} reason={reason}",
            self.partition
        );
    }

    fn stats(&self, stats: Statistics) {
        if let Some(partition) = stats
            .topics
            .get(&self.topic)
            .and_then(|topic| topic.partitions.get(&self.partition))
        {
            eprintln!(
                "phase=kafka_client_stats partition={} state={} next_offset={} high_watermark={} fetch_queue={} rx_messages={} requests_sent={} responses_received={}",
                self.partition,
                partition.fetch_state,
                partition.next_offset,
                partition.hi_offset,
                partition.fetchq_cnt,
                stats.rxmsgs,
                stats.tx,
                stats.rx
            );
        }
    }
}

impl ConsumerContext for KafkaContext {
    fn commit_callback(&self, result: rdkafka::error::KafkaResult<()>, _: &TopicPartitionList) {
        if let Err(error) = result {
            eprintln!(
                "phase=kafka_commit_failed partition={} error={error}",
                self.partition
            );
        }
    }
}

impl PartitionClient {
    pub fn connect(
        brokers: &str,
        topic: &str,
        partition: i32,
        kafka_tls: bool,
        sasl_username: Option<&str>,
        sasl_password: Option<&str>,
        sasl_mechanism: &str,
    ) -> Result<Arc<Self>> {
        let mut config = consumer_config(
            brokers,
            &std::env::var("MIMIR_KAFKA_CLIENT_ID")
                .unwrap_or_else(|_| "rust-kafka-ingester".to_owned()),
            &std::env::var("MIMIR_KAFKA_FETCH_MAX_BYTES").unwrap_or_else(|_| "8388608".to_owned()),
        );
        match (sasl_username, sasl_password) {
            (Some(username), Some(password)) => {
                let mechanism = match sasl_mechanism {
                    "plain" => "PLAIN",
                    "scram-sha-256" => "SCRAM-SHA-256",
                    "scram-sha-512" => "SCRAM-SHA-512",
                    value => bail!("unsupported SASL mechanism {value}"),
                };
                config
                    .set(
                        "security.protocol",
                        if kafka_tls {
                            "SASL_SSL"
                        } else {
                            "SASL_PLAINTEXT"
                        },
                    )
                    .set("sasl.mechanism", mechanism)
                    .set("sasl.username", username)
                    .set("sasl.password", password);
            }
            (None, None) => {
                config.set(
                    "security.protocol",
                    if kafka_tls { "SSL" } else { "PLAINTEXT" },
                );
            }
            _ => bail!("both SASL username and password are required"),
        }
        let debug = std::env::var("MIMIR_KAFKA_DEBUG").is_ok_and(|value| value == "true");
        config.set("statistics.interval.ms", "30000");
        if debug {
            config.set("debug", "broker,fetch");
            config.set_log_level(RDKafkaLogLevel::Debug);
        }
        let consumer = config
            .create_with_context(KafkaContext {
                topic: topic.to_owned(),
                partition,
                debug,
            })
            .context("create Kafka consumer")?;
        Ok(Arc::new(Self {
            consumer,
            topic: topic.to_owned(),
            partition,
            high_watermark: AtomicI64::new(-1),
        }))
    }

    /// Commits the next offset to consume for this partition's consumer group. It only serves lag
    /// monitoring: restarts resume from the local segment checkpoint.
    pub fn commit(&self, next_offset: i64) -> Result<()> {
        let mut partitions = TopicPartitionList::new();
        partitions
            .add_partition_offset(&self.topic, self.partition, Offset::Offset(next_offset))
            .context("set Kafka commit offset")?;
        self.consumer
            .commit(&partitions, CommitMode::Async)
            .context("commit Kafka offset")
    }

    pub fn assign(&self, offset: i64) -> Result<()> {
        let mut partitions = TopicPartitionList::new();
        partitions
            .add_partition_offset(&self.topic, self.partition, Offset::Offset(offset))
            .context("set Kafka partition offset")?;
        self.consumer
            .assign(&partitions)
            .context("assign Kafka partition")
    }

    /// Returns the first offset whose record timestamp is at or after `timestamp_ms`, or `None`
    /// when every record is older.
    pub async fn offset_for_time(self: &Arc<Self>, timestamp_ms: i64) -> Result<Option<i64>> {
        let client = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut partitions = TopicPartitionList::new();
            partitions
                .add_partition_offset(
                    &client.topic,
                    client.partition,
                    Offset::Offset(timestamp_ms),
                )
                .context("set Kafka lookup timestamp")?;
            let result = client
                .consumer
                .offsets_for_times(partitions, Timeout::After(Duration::from_secs(30)))
                .context("look up Kafka offset for timestamp")?;
            let element = result
                .find_partition(&client.topic, client.partition)
                .context("Kafka offset lookup returned no partition")?;
            element
                .error()
                .context("Kafka offset lookup failed for partition")?;
            Ok(match element.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            })
        })
        .await
        .context("join Kafka offset lookup")?
    }

    pub async fn get_offset(self: &Arc<Self>, at: OffsetAt) -> Result<i64> {
        let client = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let (earliest, latest) = client
                .consumer
                .fetch_watermarks(
                    &client.topic,
                    client.partition,
                    Timeout::After(Duration::from_secs(10)),
                )
                .context("fetch Kafka watermarks")?;
            client.high_watermark.store(latest, Ordering::Release);
            Ok(match at {
                OffsetAt::Earliest => earliest,
                OffsetAt::Latest => latest,
            })
        })
        .await
        .context("join Kafka watermark request")?
    }

    pub async fn next_raw(&self) -> Option<Result<(RawRecord, i64)>> {
        let message = match self.consumer.recv().await {
            Ok(message) => message,
            Err(error) => return Some(Err(error.into())),
        };
        let offset = message.offset();
        let record = RawRecord {
            offset,
            timestamp_ms: message.timestamp().to_millis().unwrap_or_default(),
            tenant: message
                .key()
                .map(|key| String::from_utf8_lossy(key).into_owned())
                .unwrap_or_default(),
            version: record_version(message.headers()),
            payload: message.payload().map(<[u8]>::to_vec),
        };
        let high_watermark = self
            .high_watermark
            .load(Ordering::Acquire)
            .max(offset.saturating_add(1));
        Some(Ok((record, high_watermark)))
    }

    pub async fn next(&self) -> Option<Result<(RecordAndOffset, i64)>> {
        let message = match self.consumer.recv().await {
            Ok(message) => message,
            Err(error) => return Some(Err(error.into())),
        };
        let offset = message.offset();
        let version = record_version(message.headers());
        let record = Record {
            timestamp_ms: message.timestamp().to_millis().unwrap_or_default(),
            tenant: message
                .key()
                .map(|key| String::from_utf8_lossy(key).into_owned())
                .unwrap_or_default(),
            request: message
                .payload()
                .map(|payload| decode_record(version, payload)),
        };
        let high_watermark = self
            .high_watermark
            .load(Ordering::Acquire)
            .max(offset.saturating_add(1));
        Some(Ok((RecordAndOffset { offset, record }, high_watermark)))
    }
}

fn consumer_config(brokers: &str, client_id: &str, fetch_max_bytes: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("client.id", client_id)
        .set("group.id", "rust-kafka-ingester")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "error")
        .set("isolation.level", "read_uncommitted")
        .set("fetch.max.bytes", fetch_max_bytes)
        // librdkafka's 1 MiB per-partition default otherwise caps each single-partition fetch at a
        // few records, making catch-up bound by broker round trips.
        .set("fetch.message.max.bytes", fetch_max_bytes)
        .set("queued.max.messages.kbytes", "32768")
        .set("fetch.wait.max.ms", "500");
    config
}

fn record_version<H: Headers>(headers: Option<&H>) -> u32 {
    let Some(headers) = headers else { return 0 };
    for index in 0..headers.count() {
        let header = headers.get(index);
        if header.key == "Version" {
            // Go's ParseRecordVersion uses the first matching header.
            return header
                .value
                .filter(|value| value.len() == 4)
                .map_or(0, |value| {
                    u32::from_be_bytes(value.try_into().expect("length checked"))
                });
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use rdkafka::message::{Header, OwnedHeaders};

    use super::{consumer_config, record_version};

    #[test]
    fn single_partition_fetches_are_not_capped_below_the_fetch_size() {
        let config = consumer_config("broker:9092", "client", "16777216");
        assert_eq!(config.get("fetch.max.bytes"), Some("16777216"));
        assert_eq!(config.get("fetch.message.max.bytes"), Some("16777216"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn first_version_header_matches_go() {
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "Other",
                value: Some(&b"ignored"[..]),
            })
            .insert(Header {
                key: "Version",
                value: Some(&1_u32.to_be_bytes()[..]),
            })
            .insert(Header {
                key: "Version",
                value: Some(&2_u32.to_be_bytes()[..]),
            });
        assert_eq!(record_version(Some(&headers)), 1);
    }
}
