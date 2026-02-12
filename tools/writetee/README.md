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
  - Amplified copies have all label values (except `__name__`) suffixed with `_amp{N}` where N is the replica number
- **Protocol Support**: Prometheus Remote Write 1.0 and 2.0
- **Preferred Backend**: Required preferred backend whose response is returned to clients (must be a mirrored backend, not amplified)
- **Request Splitting**: Optionally split large amplified requests into multiple smaller requests to stay under series limits
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

1. **Original series**: Forwarded as-is (considered replica 1, no label changes)
2. **Amplified copies**: Duplicated with all label values (except `__name__`) suffixed with `_amp2`, `_amp3`, etc.
3. **Fractional amplification**: If factor is 3.5, each series gets 3 guaranteed copies + 50% probability of 4th

**Remote Write 1.0** (with embedded label strings):

```
Original: {__name__="http_requests", method="GET"}
Copy 1:   {__name__="http_requests", method="GET_amp2"}
Copy 2:   {__name__="http_requests", method="GET_amp3"}
```

**Remote Write 2.0** (with symbol table):

```
Symbol table: ["", "__name__", "http_requests", "method", "GET", "GET_amp2", "GET_amp3"]
Original:     labels_refs=[1,2,3,4]  (references to symbol table indices)
Copy 1:       labels_refs=[1,2,3,5]  (method value changed to GET_amp2)
Copy 2:       labels_refs=[1,2,3,6]  (method value changed to GET_amp3)
```

The RW 2.0 approach only adds suffixed value strings to the symbol table and duplicates small uint32 arrays, avoiding massive memory expansion.

### Request Splitting

When amplification results in a large number of series, write-tee can split the amplified request into multiple smaller requests. This is controlled by `-backend.amplified-max-series-per-request`.

Splitting is done at **replica boundaries** for efficiency:

```
Original request: 100 series
Amplification factor: 5x (5 replicas)
Max series per request: 250

Resulting requests:
- Request 1: Original (100) + Replica 2 (100) = 200 series
- Request 2: Replica 3 (100) + Replica 4 (100) = 200 series
- Request 3: Replica 5 (100) = 100 series
```

Benefits of replica-boundary splitting:
- Each request has a minimal symbol table (RW 2.0) with only the suffixes it needs
- No series are duplicated across requests
- Predictable request sizes

**Note**: Splitting only applies to amplified backends (fire-and-forget). The preferred backend never receives amplified or split requests.

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
    IMPORTANT: The preferred backend must be in -backend.mirrored-endpoints, not
    -backend.amplified-endpoints. This ensures predictable responses to clients.
    Example: mimir-1

-backend.amplified-max-series-per-request int
    Maximum series per request when sending amplified writes to non-preferred backends.
    If amplification results in more series, the request is split into multiple smaller
    requests at replica boundaries. Set to 0 to disable splitting.
    Default: 2000
    Example: -backend.amplified-max-series-per-request=1000

-backend.async-max-in-flight int
    Maximum concurrent in-flight requests per non-preferred backend (async fire-and-forget).
    Requests are dropped silently when at capacity.
    Default: 1000
```
