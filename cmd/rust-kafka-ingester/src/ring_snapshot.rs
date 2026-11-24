// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use prost::Message;
use serde::{Deserialize, Serialize};

const TCP_STREAM: u8 = 2;
const PUSH_PULL: u8 = 6;
const USER_MESSAGE: u8 = 8;

pub(crate) const INSTANCE_ACTIVE: i32 = 0;
pub(crate) const INSTANCE_LEFT: i32 = 4;
pub(crate) const PARTITION_PENDING: i32 = 1;
pub(crate) const PARTITION_ACTIVE: i32 = 2;
pub(crate) const PARTITION_INACTIVE: i32 = 3;
pub(crate) const PARTITION_DELETED: i32 = 4;
pub(crate) const OWNER_ACTIVE: i32 = 1;
pub(crate) const OWNER_DELETED: i32 = 2;
const COMPRESSED: u8 = 9;
const REMOTE_ERROR: u8 = 13;
const LABEL: u8 = 244;
const MAX_PAYLOAD: usize = 20 * 1024 * 1024;
const MAX_NODES: u32 = 100_000;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
struct PushPullHeader {
    nodes: u32,
    user_state_len: usize,
    join: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CompressedPayload {
    algo: u8,
    buf: serde_bytes::ByteBuf,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RemoteError {
    error: String,
}

#[derive(Message)]
struct KeyValuePair {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(bytes, tag = "2")]
    value: Vec<u8>,
    #[prost(string, tag = "3")]
    codec: String,
    #[prost(bool, tag = "4")]
    deleted: bool,
    #[prost(int64, tag = "5")]
    update_time_millis: i64,
}

#[derive(Message)]
struct InstanceRing {
    #[prost(map = "string, message", tag = "1")]
    ingesters: HashMap<String, Instance>,
}

#[derive(Clone, Message, PartialEq, Serialize)]
pub struct Instance {
    #[prost(string, tag = "1")]
    pub addr: String,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
    #[prost(int32, tag = "3")]
    pub state: i32,
    #[prost(string, tag = "7")]
    pub zone: String,
    #[prost(int64, tag = "8")]
    pub registered_timestamp: i64,
    #[prost(string, tag = "9")]
    pub id: String,
}

#[derive(Message)]
struct PartitionRing {
    #[prost(map = "int32, message", tag = "1")]
    partitions: HashMap<i32, Partition>,
    #[prost(map = "string, message", tag = "2")]
    owners: HashMap<String, Owner>,
}

#[derive(Clone, Message, PartialEq, Serialize)]
pub struct Partition {
    #[prost(uint32, repeated, tag = "1")]
    pub tokens: Vec<u32>,
    #[prost(int32, tag = "2")]
    pub state: i32,
    #[prost(int64, tag = "3")]
    pub state_timestamp: i64,
    #[prost(int32, tag = "7")]
    pub token_scheme: i32,
}

#[derive(Clone, Message, PartialEq, Serialize)]
pub struct Owner {
    #[prost(int32, tag = "1")]
    pub owned_partition: i32,
    #[prost(int32, tag = "2")]
    pub state: i32,
    #[prost(int64, tag = "3")]
    pub updated_timestamp: i64,
}

#[derive(Clone, Serialize)]
pub struct RingSnapshot {
    pub instances: HashMap<String, Instance>,
    pub partitions: HashMap<i32, Partition>,
    pub owners: HashMap<String, Owner>,
}

/// Read the rings directly from a Go peer without advertising a member or writing KV state.
pub fn fetch(
    peer: SocketAddr,
    label: &str,
    instance_key: &str,
    partition_key: &str,
) -> Result<RingSnapshot> {
    let (mut stream, mut request) = connect(peer, label)?;
    request.push(PUSH_PULL);
    // Go responds with its snapshot before merging ours. An empty, non-join
    // exchange keeps the probe out of the membership table and KV store.
    rmp_serde::encode::write_named(
        &mut request,
        &PushPullHeader {
            nodes: 0,
            user_state_len: 0,
            join: false,
        },
    )?;
    stream.write_all(&request)?;

    let state = read_state(&mut BufReader::new(stream).take(MAX_PAYLOAD as u64))
        .with_context(|| format!("read memberlist snapshot from {peer}"))?;
    decode_rings(&state, instance_key, partition_key)
}

fn connect(peer: SocketAddr, label: &str) -> Result<(TcpStream, Vec<u8>)> {
    let label_len = u8::try_from(label.len()).context("memberlist label is too long")?;
    let timeout = Duration::from_secs(3);
    let stream = TcpStream::connect_timeout(&peer, timeout)
        .with_context(|| format!("connect to memberlist peer {peer}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let mut request = vec![TCP_STREAM];
    if label_len != 0 {
        request.extend_from_slice(&[LABEL, label_len]);
        request.extend_from_slice(label.as_bytes());
    }
    Ok((stream, request))
}

/// Only advertise changes to this ingester's records. Go peers merge and retransmit
/// them through dskit's normal KV gossip, including tombstones.
pub(crate) fn publish(
    peer: SocketAddr,
    label: &str,
    instance_key: &str,
    instance: Option<&Instance>,
    partition_key: &str,
    owner: Option<(&str, &Owner)>,
) -> Result<()> {
    let encoder = &mut snap::raw::Encoder::new();
    if let Some(instance) = instance {
        let ring = InstanceRing {
            ingesters: HashMap::from([(instance.id.clone(), instance.clone())]),
        };
        send_pair(
            peer,
            label,
            instance_key,
            "ringDesc",
            encoder.compress_vec(&ring.encode_to_vec())?,
            instance.timestamp,
        )?;
    }
    if let Some((id, owner)) = owner {
        let ring = PartitionRing {
            partitions: HashMap::new(),
            owners: HashMap::from([(id.to_owned(), owner.clone())]),
        };
        send_pair(
            peer,
            label,
            partition_key,
            "partitionRingDesc",
            encoder.compress_vec(&ring.encode_to_vec())?,
            owner.updated_timestamp,
        )?;
    }
    Ok(())
}

fn send_pair(
    peer: SocketAddr,
    label: &str,
    key: &str,
    codec: &str,
    value: Vec<u8>,
    seconds: i64,
) -> Result<()> {
    #[derive(Serialize)]
    #[serde(rename_all = "PascalCase")]
    struct Header {
        user_msg_len: usize,
    }

    let payload = KeyValuePair {
        key: key.to_owned(),
        value,
        codec: codec.to_owned(),
        deleted: false,
        update_time_millis: seconds * 1000,
    }
    .encode_to_vec();
    let (mut stream, mut request) = connect(peer, label)?;
    request.push(USER_MESSAGE);
    rmp_serde::encode::write_named(
        &mut request,
        &Header {
            user_msg_len: payload.len(),
        },
    )?;
    request.extend(payload);
    stream.write_all(&request)?;
    // The stream has no application-level reply. Readiness is based on the next
    // observed ring state, since delivery alone does not mean the merge succeeded.
    stream.shutdown(std::net::Shutdown::Write)?;
    ensure!(
        stream.read(&mut [0])? == 0,
        "unexpected response to memberlist KV update"
    );
    Ok(())
}

fn read_state(reader: &mut impl Read) -> Result<Vec<u8>> {
    match read_tag(reader)? {
        PUSH_PULL => read_push_pull(reader),
        COMPRESSED => {
            let payload: CompressedPayload = rmp_serde::from_read(reader)?;
            let decoded = match payload.algo {
                0 => decode_lzw(&payload.buf)?,
                1 => decode_snappy(&payload.buf)?,
                other => bail!("unsupported memberlist compression {other}"),
            };
            let mut decoded = &decoded[..];
            ensure!(
                read_tag(&mut decoded)? == PUSH_PULL,
                "expected compressed push/pull"
            );
            read_push_pull(&mut decoded)
        }
        REMOTE_ERROR => {
            let remote: RemoteError = rmp_serde::from_read(reader)?;
            bail!(
                "memberlist peer rejected snapshot request: {}",
                remote.error
            )
        }
        tag => bail!("unexpected memberlist stream message {tag}"),
    }
}

fn read_tag(reader: &mut impl Read) -> Result<u8> {
    let mut byte = [0];
    reader.read_exact(&mut byte)?;
    Ok(byte[0])
}

fn read_push_pull(reader: &mut impl Read) -> Result<Vec<u8>> {
    let header: PushPullHeader = rmp_serde::from_read(&mut *reader)?;
    ensure!(header.nodes <= MAX_NODES, "too many memberlist nodes");
    ensure!(
        header.user_state_len <= MAX_PAYLOAD,
        "memberlist KV state too large"
    );
    for _ in 0..header.nodes {
        let _: serde::de::IgnoredAny = rmp_serde::from_read(&mut *reader)?;
    }
    let mut state = vec![0; header.user_state_len];
    reader.read_exact(&mut state)?;
    Ok(state)
}

fn decode_lzw(mut encoded: &[u8]) -> Result<Vec<u8>> {
    use weezl::LzwStatus;

    let mut decoder = weezl::decode::Decoder::new(weezl::BitOrder::Lsb, 8);
    let mut decoded = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let result = decoder.decode_bytes(encoded, &mut chunk);
        ensure!(
            decoded.len() + result.consumed_out <= MAX_PAYLOAD,
            "memberlist LZW payload too large"
        );
        decoded.extend_from_slice(&chunk[..result.consumed_out]);
        encoded = &encoded[result.consumed_in..];
        match result.status? {
            LzwStatus::Done => return Ok(decoded),
            LzwStatus::Ok => {}
            LzwStatus::NoProgress => bail!("truncated memberlist LZW payload"),
        }
    }
}

fn decode_snappy(encoded: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        snap::raw::decompress_len(encoded)? <= MAX_PAYLOAD,
        "Snappy payload too large"
    );
    Ok(snap::raw::Decoder::new().decompress_vec(encoded)?)
}

fn decode_rings(mut state: &[u8], instance_key: &str, partition_key: &str) -> Result<RingSnapshot> {
    let mut instances = None;
    let mut partitions = None;
    while !state.is_empty() {
        // A full dskit KV snapshot prefixes each Protobuf KV pair with its size.
        let (len, rest) = state
            .split_first_chunk::<4>()
            .context("missing KV pair length")?;
        let (pair, rest) = rest
            .split_at_checked(u32::from_be_bytes(*len) as usize)
            .context("truncated KV pair")?;
        state = rest;
        let entry = KeyValuePair::decode(pair)?;
        if entry.deleted {
            continue;
        }
        if entry.key == instance_key {
            ensure!(entry.codec == "ringDesc", "unexpected instance ring codec");
            instances = Some(InstanceRing::decode(
                decode_snappy(&entry.value)?.as_slice(),
            )?);
        }
        if entry.key == partition_key {
            ensure!(
                entry.codec == "partitionRingDesc",
                "unexpected partition ring codec"
            );
            partitions = Some(PartitionRing::decode(
                decode_snappy(&entry.value)?.as_slice(),
            )?);
        }
    }
    let partitions =
        partitions.with_context(|| format!("missing partition ring {partition_key}"))?;
    Ok(RingSnapshot {
        // The first ingester creates its private instance ring after replay.
        instances: instances.map(|ring| ring.ingesters).unwrap_or_default(),
        partitions: partitions.partitions,
        owners: partitions.owners,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_bootstrap_without_an_instance_ring() {
        let entry = KeyValuePair {
            key: "partitions".into(),
            codec: "partitionRingDesc".into(),
            value: snap::raw::Encoder::new()
                .compress_vec(
                    &PartitionRing {
                        partitions: HashMap::from([(0, Partition::default())]),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .unwrap(),
            ..Default::default()
        }
        .encode_to_vec();
        let mut state = (entry.len() as u32).to_be_bytes().to_vec();
        state.extend_from_slice(&entry);

        let snapshot = decode_rings(&state, "instances", "partitions").unwrap();
        assert!(snapshot.instances.is_empty());
        assert!(snapshot.partitions.contains_key(&0));
    }

    #[test]
    fn still_requires_the_shared_partition_ring() {
        let error = decode_rings(&[], "instances", "partitions").err().unwrap();
        assert_eq!(error.to_string(), "missing partition ring partitions");
    }
}
