# Development Scripts

This directory contains demo and utility scripts for the monolithic mode development environment.

## OTLP Resource Attributes Demo

The `otlp-resource-attrs-demo.sh` script demonstrates how Mimir persists OTel resource attributes from OTLP metrics and makes them queryable via the `/api/v1/resources` endpoint and the `info()` PromQL function.

This is a faithful port of the Prometheus demo at `documentation/examples/otlp-resource-attributes/main.go`.

### Overview

When Mimir receives metrics via OTLP, each resource contains attributes that describe the source of the metrics (service.name, host.name, etc.). This demo shows how these attributes are:

1. Ingested from OTLP metrics
2. Stored per-series in the ingester's TSDB head (in-memory)
3. Persisted to Parquet files during block compaction
4. Retrieved from both ingesters and store-gateways
5. Exposed via the `/api/v1/resources` API
6. Enriched into PromQL queries via the `info()` function

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

### Cloud Mode

The demo can also run against a remote Mimir deployment (e.g., a cloud dev environment) by setting the `MIMIR_TENANT_ID` environment variable:

```bash
MIMIR_URL=https://mimir.dev.example.com MIMIR_TENANT_ID=my-tenant MIMIR_API_TOKEN=glc_... ./scripts/otlp-resource-attrs-demo.sh
```

When `MIMIR_TENANT_ID` is set, cloud mode is activated:

- An `X-Scope-OrgID` header is added to all requests for tenant routing
- The readiness check uses an OTLP probe instead of `/ready` (which is typically not exposed)
- **Phase 3** (WAL replay) is skipped — requires docker-compose restart
- **Phase 4** (flush to blocks) is skipped — `/ingester/flush` is not exposed
- `--start-stack` and `--stop-stack` flags are ignored with a warning

All other phases (1, 2, 5–9) run normally against the remote endpoint.

| Variable | Description | Default |
|---|---|---|
| `MIMIR_URL` | Mimir base URL | `http://localhost:8101` |
| `MIMIR_TENANT_ID` | Tenant ID; enables cloud mode when set | (unset) |
| `MIMIR_API_TOKEN` | API token (basic auth with tenant ID as username) | (unset) |

### Configuration

The demo requires `otel_persist_resource_attributes: true` to be set in the blocks storage TSDB config. This is already configured in `config/mimir.yaml` under `blocks_storage.tsdb`.

### Demo Phases

1. **Send OTLP Metrics**: Sends metrics from multiple services with diverse resource attributes and entity_refs
2. **Query from Head**: Shows resource attributes stored in-memory in the ingester
3. **Compact to Disk**: Triggers ingester flush to persist data to Parquet block files
4. **Query from Blocks**: Shows resource attributes retrieved from store-gateways
5. **Descriptive Attributes Changing**: Demonstrates how non-identifying (descriptive) attributes can change over time while identifying attributes remain constant
6. **Query with info()**: Shows how the `info()` function enriches metrics with time-appropriate resource attributes
7. **API Response Format**: Displays the full `/api/v1/resources` JSON response structure
8. **Summary**: Summarizes the key concepts demonstrated

### Resource Attributes

The demo uses these OTel resource attributes:

**Identifying Attributes** (constant for a series, used for correlation):

- `service.name` - The logical name of the service
- `service.namespace` - The namespace/environment
- `service.instance.id` - Unique instance identifier

These attributes uniquely identify the resource and remain constant throughout the lifetime of a series. They enable correlation with traces and logs.

**Descriptive Attributes** (can change over time):

- `host.name` - Hostname of the service (can change during migration)
- `cloud.region` - Cloud provider region (can change during migration)
- `deployment.environment` - Deployment environment
- `k8s.pod.name` - Kubernetes pod name (changes on pod restart)

These attributes describe the current state of the resource and may change over time as infrastructure evolves (e.g., during migrations, scaling, restarts).

### Entity Refs

The demo demonstrates OTel entity_refs which structure resources into typed entities:

```json
{
  "resource": {
    "attributes": [...],
    "entityRefs": [
      {
        "type": "service",
        "idKeys": ["service.name", "service.namespace", "service.instance.id"],
        "descriptionKeys": ["deployment.environment"]
      },
      {
        "type": "host",
        "idKeys": ["host.name"],
        "descriptionKeys": ["cloud.region"]
      }
    ]
  }
}
```

### Architecture

```
OTLP Metrics                 TSDB Head              Parquet Block
┌─────────────────┐         ┌────────────┐         ┌────────────┐
│ ResourceMetrics │ ──────► │ In-memory  │ ──────► │ series_    │
│   └─ Resource   │ Ingest  │ storage    │ Compact │ metadata.  │
│      └─ Attrs   │         │            │         │ parquet    │
│      └─ Entities│         │            │         │            │
└─────────────────┘         └────────────┘         └────────────┘
                                   │                      │
                                   ▼                      ▼
                            ┌─────────────────────────────────┐
                            │       /api/v1/resources          │
                            │   (combined head + blocks)       │
                            └─────────────────────────────────┘
                                          │
                                          ▼
                            ┌─────────────────────────────────┐
                            │     info() PromQL function       │
                            │  (enriches metrics at query time)│
                            └─────────────────────────────────┘
```

### API Reference

Query resource attributes:

```bash
curl 'http://localhost:8101/prometheus/api/v1/resources?match[]={__name__=~".+"}'
```

Query with info() function:

```bash
curl 'http://localhost:8101/prometheus/api/v1/query?query=info(http_requests_total)&time=1234567890'
```

Send OTLP metrics with entity_refs:

```bash
curl -X POST 'http://localhost:8101/otlp/v1/metrics' \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceMetrics": [{
      "resource": {
        "attributes": [...],
        "entityRefs": [...]
      },
      "scopeMetrics": [...]
    }]
  }'
```

### Use Cases

- **Trace-to-Metrics Correlation**: Use service.name, service.namespace, and service.instance.id to correlate metrics with distributed traces
- **Resource Discovery**: Query what resources have reported metrics
- **Historical Analysis**: Understand which services were active during time ranges
- **Infrastructure Tracking**: Track changes in descriptive attributes (host migrations, region changes) over time
