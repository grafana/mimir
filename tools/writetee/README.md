# Write-Tee

Write-tee is a reverse proxy that fans out Prometheus remote write requests to multiple backend Mimir clusters. It enables testing, migration, and validation scenarios where writes need to be duplicated across multiple clusters.

## Features

### Core Functionality

- **Request Fan-Out**: Receives write requests and forwards them to multiple backend clusters in parallel
- **Backend Types**:
  - **Mirrored backends** (`-backend.mirrored-endpoints`): Receive unmodified write requests (1:1 traffic mirroring)
  - **Amplified backends** (`-backend.amplified-endpoints`): Receive amplified write requests with duplicated time series
- **Write Amplification**: Duplicate time series based on configurable amplification factor
  - Factor 1.0: No amplification (pass-through)
  - Factor 2.0: Each time series duplicated once (2x total series)
  - Factor 3.5: Each time series gets 3 full copies + 50% probability of 4th copy (3.5x average)
  - Amplified copies get unique `__amplified__="<replica_number>"` label
- **Protocol Support**: Prometheus Remote Write 1.0 and 2.0
- **Preferred Backend**: Required preferred backend whose response is returned to clients
- **Fire-and-Forget**: Non-preferred backends receive requests asynchronously without blocking the response
- **Backpressure Handling**: Bounded concurrent in-flight requests per non-preferred backend; requests dropped when at capacity
- **Authentication**: Supports basic auth forwarding and per-backend auth override

## Example Usage

Amplify traffic to test cluster scalability:

```bash
write-tee \
  -backend.mirrored-endpoints=http://prod-mimir:8080 \
  -backend.amplified-endpoints=http://test-mimir:8080 \
  -backend.amplification-factor=10.0 \
  -backend.preferred=prod-mimir \
  -server.http-listen-port=8080
```

Production cluster (`prod-mimir`) is the preferred backend - it receives normal traffic (1x), and its response is returned to clients immediately. Test cluster (`test-mimir`) receives 10x amplified traffic via fire-and-forget.

## Architecture

### Request Flow

```
Client → Write-Tee → [Preferred Backend] ← Response returned immediately
                   ↘ [Non-Preferred Backends] (fire-and-forget, bounded concurrency)
```

1. **Receive**: Client sends Prometheus remote write request to write-tee
2. **Buffer**: Entire request body is buffered in memory (writes cannot be streamed)
3. **Dispatch to Non-Preferred**: A goroutine is spawned for async delivery to each non-preferred backend (fire-and-forget)
4. **Send to Preferred**: Request sent synchronously to preferred backend
5. **Return**: Preferred backend's response returned to client immediately (does not wait for non-preferred backends)
6. **Async Processing**: Spawned goroutines send requests to non-preferred backends concurrently (bounded by max-in-flight limit)
7. **Metrics**: Request duration, errors, dropped requests, and response status recorded per backend

### Amplification Process

For each time series in the incoming request:

1. **Original series**: Forwarded as-is (no label changes)
2. **Amplified copies**: Duplicated with `__amplified__="1"`, `__amplified__="2"`, etc.
3. **Fractional amplification**: If factor is 3.5, each series gets 3 guaranteed copies + 50% probability of 4th

**Remote Write 1.0** (with embedded label strings):
```
Original: {__name__="http_requests", method="GET"}
Copy 1:   {__name__="http_requests", method="GET", __amplified__="1"}
Copy 2:   {__name__="http_requests", method="GET", __amplified__="2"}
```

**Remote Write 2.0** (with symbol table):
```
Symbol table: ["", "__name__", "http_requests", "method", "GET", "__amplified__", "1", "2"]
Original:     labels_refs=[1,2,3,4]  (references to symbol table indices)
Copy 1:       labels_refs=[1,2,3,4,5,6]  (adds __amplified__ + "1")
Copy 2:       labels_refs=[1,2,3,4,5,7]  (adds __amplified__ + "2")
```

The RW 2.0 approach only adds a few strings to the symbol table and duplicates small uint32 arrays, avoiding massive memory expansion.

## Configuration

### Backend Endpoints

```bash
-backend.mirrored-endpoints string
    Comma-separated list of backend endpoints to mirror writes to (without amplification).
    Example: http://mimir-1:8080,http://mimir-2:8080

-backend.amplified-endpoints string
    Comma-separated list of backend endpoints to send amplified writes to.
    Example: http://loadtest-mimir:8080

-backend.amplification-factor float
    The factor by which to amplify writes to amplified backends.
    Default: 1.0 (no amplification)
    Example: 3.5 (each series duplicated 3.5 times on average)
    Note: Only applies to backends in -backend.amplified-endpoints

-backend.preferred string (REQUIRED)
    The hostname of the preferred backend. This backend's response is always
    returned to the client. Non-preferred backends receive fire-and-forget requests.
    Example: mimir-1

-backend.async-max-in-flight int
    Maximum concurrent in-flight requests per non-preferred backend (async fire-and-forget).
    Requests are dropped silently when at capacity.
    Default: 1000
```
