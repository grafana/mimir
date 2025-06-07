# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Grafana Mimir is a horizontally scalable, highly available, multi-tenant time series database for Prometheus. It provides long-term storage for Prometheus metrics with features like massive scalability (up to 1 billion active time series), global metric views, and cost-effective object storage.

## Development Commands

### Building and Testing

```bash
# Build all binaries
make exes

# Run unit tests
make test

# Run tests with race detection
make test-with-race

# Run integration tests (requires Docker)
make integration-tests

# Run specific test
go test ./pkg/querier/... -run TestSpecificFunction

# Build Docker images
make images

# Lint code (comprehensive linting suite)
make lint

# Generate documentation
make doc
```

### Local Development Environment

```bash
# Start monolithic mode (easiest for development)
cd development/mimir-monolithic-mode
make compose-up

# Start microservices mode (full architecture)
cd development/mimir-microservices-mode
make compose-up

# Start with ingest storage (newer architecture)
cd development/mimir-ingest-storage
make compose-up

# Stop environments
make compose-down
```

### Code Generation

```bash
# Generate protobuf code
make clean-protos BUILD_IN_CONTAINER=false
make protos BUILD_IN_CONTAINER=false

# Generate configuration documentation
make reference-help
```

## Architecture Overview

Mimir follows a microservices architecture with these core components:

### Data Path Components

- **Distributor** (`pkg/distributor/`) - Receives and routes incoming metrics to ingesters
- **Ingester** (`pkg/ingester/`) - Handles metric ingestion, maintains in-memory time series, and writes blocks to storage
- **Store Gateway** (`pkg/storegateway/`) - Serves queries from long-term object storage blocks
- **Compactor** (`pkg/compactor/`) - Compacts and optimizes storage blocks in object storage

### Query Path Components

- **Query Frontend** (`pkg/frontend/`) - Provides query API, caching, and request splitting
- **Query Scheduler** (`pkg/scheduler/`) - Schedules and load-balances queries across queriers
- **Querier** (`pkg/querier/`) - Executes queries against ingesters and store gateways

### Additional Services

- **Ruler** (`pkg/ruler/`) - Evaluates recording and alerting rules
- **Alertmanager** (`pkg/alertmanager/`) - Routes and manages alerts
- **Block Builder** (`pkg/blockbuilder/`) - Builds blocks in ingest storage architecture

### Storage Architecture

- **Short-term storage**: In-memory in ingesters, with WAL and periodic flushing
- **Long-term storage**: Immutable blocks in object storage (S3, GCS, Azure, Swift)
- **Metadata**: Ring-based service discovery, with optional external storage for metadata

## Key Entry Points

- `cmd/mimir/main.go` - Main Mimir binary (supports all deployment modes)
- `cmd/mimirtool/main.go` - CLI tool for operations and management
- `cmd/query-tee/main.go` - Query comparison tool for testing
- `pkg/mimir/mimir.go` - Core service orchestration and module management

## Testing Patterns

### Unit Tests

- Place test files alongside source code as `*_test.go`
- Use table-driven tests for multiple scenarios
- Mock external dependencies using interfaces

### Integration Tests

- Located in `integration/` directory
- Use `e2emimir` package for creating test clusters
- Test complete user workflows end-to-end

### Key Test Utilities

- `integration/e2emimir/` - Framework for spinning up test clusters
- `pkg/*/testutil/` - Component-specific test utilities
- Use `testify` for assertions and test suites

## Configuration System

Mimir uses a hierarchical configuration system:

- YAML configuration files with embedded defaults
- Runtime configuration for tenant-specific overrides (`pkg/mimir/runtime_config.go`)
- Feature flags and gradual rollouts supported
- Configuration validation and documentation generation

## Multi-tenancy Design

Everything in Mimir is designed around tenant isolation:

- Tenant ID extracted from HTTP headers or JWT tokens
- Per-tenant limits and configurations
- Data isolation at storage and query levels
- Cost attribution and usage tracking per tenant

## Development Tips

### Working with Rings

Most services use hash rings for sharding and service discovery. Key concepts:

- Ring state stored in KV store (etcd, Consul, or memberlist gossip)
- Consistent hashing for data distribution
- Replication factors for availability

### Storage Operations

- Blocks are immutable once written to object storage
- Use tools in `tools/` directory for debugging storage issues
- `tools/listblocks` and `tools/mark-blocks` are particularly useful

### Debugging Queries

- Enable query stats in configuration for detailed query analysis
- Use `pkg/querier/stats_renderer.go` for understanding query execution
- Query-tee tool useful for comparing query results between versions

### Common Gotchas

- Services must be started in correct order in microservices mode
- Ring propagation can take time - allow for eventual consistency
- Object storage operations have different consistency guarantees
- Ingester WAL replay can take significant time on restart

## Code Quality Standards

- All new code requires unit tests
- Integration tests for user-facing changes
- Protobuf changes require careful consideration of backward compatibility
- Use existing patterns for metrics, logging, and configuration
- Follow existing package organization and naming conventions
