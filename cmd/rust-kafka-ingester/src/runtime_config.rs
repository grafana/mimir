//! Loads Mimir's runtime config (`-runtime-config.file`): a comma-separated list of YAML or JSON
//! files and HTTP URLs, merged left to right like dskit's runtimeconfig manager, and reloaded
//! periodically.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use http_body_util::{BodyExt, Empty};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{Map, Value};

use crate::limits::Overrides;
use crate::metrics;

#[derive(clap::Args, Clone, Debug)]
pub struct RuntimeConfigArgs {
    /// Comma-separated YAML or JSON files and HTTP URLs, merged from left to right.
    #[arg(long = "runtime-config.file", default_value = "")]
    pub file: String,
    #[arg(long = "runtime-config.reload-period", default_value = "10s")]
    pub reload_period: String,
    #[arg(long = "runtime-config.http-client-timeout", default_value = "30s")]
    pub http_client_timeout: String,
    /// Sent as `X-Cluster` when fetching URLs, for servers that validate the cluster label.
    #[arg(long = "common.client-cluster-validation.label", default_value = "")]
    pub cluster_validation_label: String,
}

pub struct RuntimeConfig {
    sources: Vec<String>,
    timeout: Duration,
    cluster_label: String,
    client: Client<hyper_util::client::legacy::connect::HttpConnector, Empty<bytes::Bytes>>,
    last: Option<Vec<u8>>,
}

impl RuntimeConfig {
    pub fn new(args: &RuntimeConfigArgs) -> Result<Self> {
        Ok(Self {
            sources: args
                .file
                .split(',')
                .map(str::trim)
                .filter(|source| !source.is_empty())
                .map(str::to_owned)
                .collect(),
            timeout: Duration::from_millis(crate::limits::parse_duration_ms(
                &args.http_client_timeout,
            )? as u64),
            cluster_label: args.cluster_validation_label.clone(),
            client: Client::builder(TokioExecutor::new()).build_http(),
            last: None,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    async fn read(&self, source: &str) -> Result<Vec<u8>> {
        if source.starts_with("http://") || source.starts_with("https://") {
            if source.starts_with("https://") {
                bail!("https runtime config URLs are not supported");
            }
            let mut request =
                hyper::Request::get(source).header("User-Agent", "dskit-runtimeconfig");
            if !self.cluster_label.is_empty() {
                request = request.header("X-Cluster", &self.cluster_label);
            }
            let request = request.body(Empty::new())?;
            let started = std::time::Instant::now();
            let response = tokio::time::timeout(self.timeout, async {
                let response = self.client.request(request).await?;
                let status = response.status();
                let body = response.into_body().collect().await?.to_bytes();
                anyhow::Ok((status, body))
            })
            .await
            .context("timed out")??;
            metrics::observe_runtime_config_request(source, response.0.as_u16(), started.elapsed());
            if !response.0.is_success() {
                bail!("{source} returned {}", response.0);
            }
            Ok(response.1.to_vec())
        } else {
            tokio::fs::read(source)
                .await
                .with_context(|| format!("read {source}"))
        }
    }

    /// Reads every source and applies the merged config if any changed.
    pub async fn load(&mut self, overrides: &Overrides) -> Result<bool> {
        let mut raw = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            raw.push(
                self.read(source)
                    .await
                    .with_context(|| format!("read {source:?}"))?,
            );
        }
        let fingerprint = raw.join(&0_u8);
        if self.last.as_ref() == Some(&fingerprint) {
            return Ok(false);
        }
        let mut merged = Map::new();
        for (source, data) in self.sources.iter().zip(&raw) {
            let document = parse_document(data).with_context(|| format!("unmarshal {source:?}"))?;
            merged = merge_maps(merged, document, "").with_context(|| {
                format!("can't merge {source:?} on top of the previous providers")
            })?;
        }
        overrides.apply_runtime_config(&merged)?;
        metrics::set_runtime_config_hash(fingerprint_hash(&fingerprint));
        self.last = Some(fingerprint);
        Ok(true)
    }

    /// Reloads forever; like dskit, failures keep the previous config.
    pub async fn run(mut self, overrides: Arc<Overrides>, period: Duration) {
        let mut ticker = tokio::time::interval(period);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match self.load(&overrides).await {
                Ok(changed) => {
                    metrics::set_runtime_config_success(true);
                    if changed {
                        eprintln!("phase=runtime_config_reloaded");
                    }
                }
                Err(error) => {
                    metrics::set_runtime_config_success(false);
                    eprintln!("phase=runtime_config_error error={error:#}");
                }
            }
        }
    }
}

// Stands in for dskit's SHA-256 in the `cortex_runtime_config_hash` label; only changes matter.
fn fingerprint_hash(bytes: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

fn parse_document(data: &[u8]) -> Result<Map<String, Value>> {
    let trimmed = data.trim_ascii_start();
    let value: Value = if trimmed.first() == Some(&b'{') {
        match serde_json::from_slice(trimmed) {
            Ok(value) => value,
            Err(_) => serde_yaml::from_slice(data)?,
        }
    } else if trimmed.is_empty() {
        Value::Object(Map::new())
    } else {
        serde_yaml::from_slice(data)?
    };
    match value {
        Value::Object(map) => Ok(map),
        Value::Null => Ok(Map::new()),
        other => bail!("runtime config must be a map, got {other}"),
    }
}

/// Deep-merges `b` over `a` like dskit's `mergeConfigMaps`: maps merge key by key, anything else
/// is replaced, and a null on either side of a map counts as an empty map.
pub fn merge_maps(
    a: Map<String, Value>,
    b: Map<String, Value>,
    path: &str,
) -> Result<Map<String, Value>> {
    let mut out = a;
    for (key, value) in b {
        let key_path = format!("{path}.{key}");
        let existing = out.remove(&key);
        let merged = match (existing, value) {
            (Some(Value::Object(left)), Value::Object(right)) => {
                Value::Object(merge_maps(left, right, &key_path)?)
            }
            (Some(Value::Object(left)), Value::Null) => Value::Object(left),
            (Some(Value::Null), Value::Object(right)) => Value::Object(right),
            (Some(Value::Object(_)), other) => {
                bail!("conflicting types for {key_path:?}: map != {other}")
            }
            (Some(left), Value::Object(_)) if !left.is_null() => {
                bail!("conflicting types for {key_path:?}: {left} != map")
            }
            (_, value) => value,
        };
        out.insert(key, merged);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(yaml: &str) -> Map<String, Value> {
        parse_document(yaml.as_bytes()).unwrap()
    }

    #[test]
    fn merges_like_dskit() {
        let merged = merge_maps(
            map("overrides:\n  a:\n    x: 1\n    y: 2\n  b: {z: 1}\nother: 1\n"),
            map(r#"{"overrides": {"a": {"y": 3}, "c": {"w": 1}}}"#),
            "",
        )
        .unwrap();
        assert_eq!(
            Value::Object(merged),
            serde_json::json!({
                "overrides": {"a": {"x": 1, "y": 3}, "b": {"z": 1}, "c": {"w": 1}},
                "other": 1,
            })
        );
        let nulls = merge_maps(map("overrides:\n"), map("overrides:\n  a: {x: 1}\n"), "").unwrap();
        assert_eq!(
            Value::Object(nulls),
            serde_json::json!({"overrides": {"a": {"x": 1}}})
        );
        assert!(merge_maps(map("a: 1\n"), map("a: {b: 1}\n"), "").is_err());
    }

    #[tokio::test]
    async fn loads_files_and_urls_left_to_right_and_skips_unchanged() {
        let directory =
            std::env::temp_dir().join(format!("runtime-config-test-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let first = directory.join("overrides.yaml");
        std::fs::write(
            &first,
            "overrides:\n  t:\n    max_global_exemplars_per_user: 5\n    max_global_metadata_per_user: 7\n",
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen_cluster = Arc::new(std::sync::Mutex::new(None));
        let seen = Arc::clone(&seen_cluster);
        tokio::spawn(async move {
            let app = axum::Router::new().route(
                "/overrides",
                axum::routing::get(move |headers: axum::http::HeaderMap| {
                    let seen = Arc::clone(&seen);
                    async move {
                        *seen.lock().unwrap() = headers
                            .get("X-Cluster")
                            .map(|value| value.to_str().unwrap().to_owned());
                        r#"{"overrides":{"t":{"max_global_exemplars_per_user":9}}}"#
                    }
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let mut config = RuntimeConfig::new(&RuntimeConfigArgs {
            file: format!("{}, http://{address}/overrides", first.display()),
            reload_period: "10s".into(),
            http_client_timeout: "5s".into(),
            cluster_validation_label: "cell".into(),
        })
        .unwrap();
        let overrides = Overrides::default();
        assert!(config.load(&overrides).await.unwrap());
        let limits = overrides.tenant("t");
        assert_eq!(limits.limits.max_global_exemplars_per_user, 9);
        assert_eq!(limits.limits.max_global_metadata_per_user, 7);
        assert_eq!(seen_cluster.lock().unwrap().as_deref(), Some("cell"));
        assert!(!config.load(&overrides).await.unwrap());
        // A broken source fails the load and keeps the previous overrides.
        std::fs::write(&first, "overrides: [").unwrap();
        assert!(config.load(&overrides).await.is_err());
        assert_eq!(
            overrides.tenant("t").limits.max_global_exemplars_per_user,
            9
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
