# Development Scripts

This directory contains demo and utility scripts for the monolithic mode development environment.

## OTLP Resource Attributes Demo

The `otlp-resource-attrs-demo.sh` script demonstrates how Mimir persists OTel resource attributes from OTLP metrics and makes them queryable via the `/api/v1/resources` endpoint.

### Overview

When Mimir receives metrics via OTLP, each resource contains attributes that describe the source of the metrics (service.name, host.name, etc.). This demo shows how these attributes are:

1. Ingested from OTLP metrics
2. Stored per-series in the ingester's TSDB head (in-memory)
3. Persisted to Parquet files when blocks are flushed
4. Retrieved from both ingesters and store-gateways
5. Exposed via the `/api/v1/resources` API with version history

### Prerequisites

- Mimir running in monolithic mode
- `curl` and `jq` installed

### Running the Demo

1. Start Mimir:
   ```bash
   cd development/mimir-monolithic-mode
   ./compose-up.sh
   ```

2. Wait for Mimir to be ready (check http://localhost:8101/ready)

3. Run the demo:
   ```bash
   ./scripts/otlp-resource-attrs-demo.sh
   ```

### Configuration

The demo requires `otel_persist_resource_attributes: true` to be set in the runtime configuration. This is already configured in `config/runtime.yaml` for the anonymous tenant.

### Demo Phases

1. **Send OTLP Metrics**: Sends metrics from multiple services with diverse resource attributes
2. **Query from Head**: Shows resource attributes stored in-memory in the ingester
3. **Flush to Blocks**: Triggers ingester flush to persist data to blocks
4. **Query from Blocks**: Shows resource attributes retrieved from store-gateways
5. **Service Migration**: Demonstrates how descriptive attributes can change over time while identifying attributes remain constant
6. **Versioned Attributes**: Shows the version history with time ranges

### Resource Attributes

The demo uses these OTel resource attributes:

**Identifying Attributes** (constant for a series, used for correlation):
- `service.name` - The logical name of the service
- `service.namespace` - The namespace/environment
- `service.instance.id` - Unique instance identifier

**Descriptive Attributes** (can change over time):
- `host.name` - Hostname of the service
- `cloud.region` - Cloud provider region
- `deployment.environment` - Deployment environment
- `k8s.pod.name` - Kubernetes pod name

### API Reference

Query resource attributes:
```bash
curl 'http://localhost:8101/prometheus/api/v1/resources?match[]={__name__=~".+"}'
```

Send OTLP metrics:
```bash
curl -X POST 'http://localhost:8101/otlp/v1/metrics' \
  -H 'Content-Type: application/json' \
  -d '{"resourceMetrics": [...]}'
```
