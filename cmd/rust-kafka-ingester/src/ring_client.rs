//! Reads the partition ring through the ring sidecar, which already watches it over memberlist:
//! the active partition count for global limits and each tenant's token ranges for owned series.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use http_body_util::{BodyExt, Empty, Full};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::limits::Overrides;
use crate::store::Store;

/// Keeps `overrides` informed of the active partitions in the partition ring, which the ring
/// sidecar serves as a plain number, so global limits convert to this partition's share.
pub async fn poll_active_partitions(url: String, overrides: Arc<Overrides>, period: Duration) {
    let client: Client<_, Empty<bytes::Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let mut ticker = tokio::time::interval(period);
    let mut last = None;
    loop {
        ticker.tick().await;
        let fetched = async {
            let response = tokio::time::timeout(
                Duration::from_secs(5),
                client.get(url.parse::<hyper::Uri>()?),
            )
            .await??;
            if !response.status().is_success() {
                bail!("{url} returned {}", response.status());
            }
            let body = response.into_body().collect().await?.to_bytes();
            Ok(std::str::from_utf8(&body)?.trim().parse::<u64>()?)
        }
        .await;
        match fetched {
            Ok(partitions) => {
                overrides.set_active_partitions(partitions);
                if last != Some(partitions) {
                    eprintln!("phase=active_partitions count={partitions}");
                    last = Some(partitions);
                }
            }
            Err(error) => eprintln!("phase=active_partitions_error error={error:#}"),
        }
    }
}

/// Every `period`, asks the sidecar for this partition's token ranges in each stored tenant's
/// shuffle shard, like Mimir's owned series service with the partition ring strategy.
pub async fn poll_owned_ranges(url: String, store: Arc<Store>, period: Duration) {
    let client: Client<_, Full<bytes::Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let mut ticker = tokio::time::interval(period);
    loop {
        ticker.tick().await;
        let tenants = store
            .tenant_ids()
            .into_iter()
            .map(|tenant| {
                let shard_size = store
                    .overrides()
                    .tenant(&tenant)
                    .limits
                    .ingestion_partitions_tenant_shard_size;
                (tenant, shard_size)
            })
            .collect::<HashMap<_, _>>();
        let fetched = async {
            let body = serde_json::to_vec(&serde_json::json!({ "tenants": tenants }))?;
            let request = hyper::Request::post(&url)
                .header("Content-Type", "application/json")
                .body(Full::new(bytes::Bytes::from(body)))?;
            let response =
                tokio::time::timeout(Duration::from_secs(5), client.request(request)).await??;
            if !response.status().is_success() {
                bail!("{url} returned {}", response.status());
            }
            let body = response.into_body().collect().await?.to_bytes();
            parse_owned_ranges(&body)
        }
        .await;
        match fetched {
            Ok(ranges) => store.set_owned_ranges(ranges),
            Err(error) => eprintln!("phase=owned_ranges_error error={error:#}"),
        }
    }
}

/// `{"tenant": [start, end, ...] | null}`, null when the tenant's shard skips this partition.
pub fn parse_owned_ranges(body: &[u8]) -> Result<HashMap<String, Option<Vec<u32>>>> {
    Ok(serde_json::from_slice(body)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ranges_and_tenants_outside_the_shard() {
        let ranges = parse_owned_ranges(br#"{"a":[0,10,20,4294967295],"b":null}"#).unwrap();
        assert_eq!(ranges["a"], Some(vec![0, 10, 20, u32::MAX]));
        assert_eq!(ranges["b"], None);
        assert!(parse_owned_ranges(b"[]").is_err());
    }
}
