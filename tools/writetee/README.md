# Write-Tee

Write-tee is a reverse proxy that sits in front of a single backend endpoint (typically a cell's own distributors) and amplifies Prometheus remote write traffic to it. It enables load-testing and validation scenarios where a cell needs to receive a multiple of its real write traffic.

## Features

### Core Functionality

- **Single Endpoint** (`-backend.endpoint`): All traffic is sent to one backend endpoint
- **Synchronous Original**: The original (unmodified) request is sent synchronously and its response is returned to the client (this is replica 1)
- **Write Amplification**: When `-backend.amplification-factor` > 1, additional suffixed copies are sent to the same endpoint fire-and-forget
  - Factor 1.0: No amplification — only the original request is sent (1x total series)
  - Factor 2.0: Original + one suffixed copy `_amp2` (2x total series)
  - Factor 3.5: Original + copies `_amp2`, `_amp3` + a 50%-sampled copy `_amp4` (3.5x average)
  - Amplified copies have all label values (except `__name__`) suffixed with `_amp{N}` where N is the replica number (starting at 2)
- **Protocol Support**: Prometheus Remote Write 1.0 and 2.0
- **Fire-and-Forget**: Amplified copies are dispatched asynchronously without blocking the client response
- **Backpressure Handling**: Bounded concurrent in-flight amplified requests; requests dropped (and counted) when at capacity
- **Authentication**: Supports basic auth forwarding and per-endpoint auth override

## Example Usage

Amplify a cell's incoming write traffic 10x onto its own distributors:

```bash
write-tee \
  -backend.endpoint=http://distributor:8080 \
  -backend.amplification-factor=10.0 \
  -server.http-listen-port=8080
```

The endpoint receives the original request synchronously (its response is returned to the client immediately) plus 9 additional suffixed copies (`_amp2`..`_amp10`) via fire-and-forget, for 10x total series.

## Architecture

### Request Flow

```
Client → Write-Tee → [Backend Endpoint] (original, synchronous) ← Response returned immediately
                   ↘ [Backend Endpoint] (amplified copies, fire-and-forget, bounded concurrency)
```

1. **Receive**: Client sends Prometheus remote write request to write-tee
2. **Buffer**: Entire request body is buffered in memory (writes cannot be streamed)
3. **Dispatch Amplified Copies**: When factor > 1, a goroutine is spawned per suffixed copy for async delivery to the endpoint (fire-and-forget)
4. **Send Original**: Original request sent synchronously to the endpoint
5. **Return**: The endpoint's response to the original request is returned to the client immediately (does not wait for amplified copies)
6. **Async Processing**: Spawned goroutines send the amplified copies concurrently (bounded by max-in-flight limit)
7. **Metrics**: Request duration, errors, dropped requests, and response status recorded per backend

### Amplification Process

For each time series in the incoming request, with amplification factor F:

1. **Original series**: Sent synchronously as-is (replica 1, no label changes)
2. **Full copies**: `floor(F)-1` copies with all label values (except `__name__`) suffixed `_amp2`, `_amp3`, …, `_amp{floor(F)}`
3. **Fractional copy**: If `F` has a fractional part (e.g. 3.5), one more copy is sampled to that fraction and suffixed `_amp{floor(F)+1}`

So the endpoint receives `floor(F) + (frac>0 ? 1 : 0)` requests total.

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

## Configuration

### Backend Endpoint

```bash
-backend.endpoint string (REQUIRED)
    The backend endpoint to send writes to. The original (unmodified) request is sent
    synchronously and its response is returned to the client; when amplification-factor > 1,
    additional suffixed copies are sent asynchronously (fire-and-forget).
    Supported schemes: http, https (HTTP), and dns (HTTPgRPC).
    Example: http://distributor:8080

-backend.amplification-factor float
    The factor by which to amplify writes to the backend. Must be >= 1.0.
    Default: 1.0 (no amplification — only the original request is sent)
    Example: 3.5 (each series sent 3.5 times on average)
    Amplified copies have all label values (except __name__) suffixed with _amp{N}
    where N is the replica number (starting at 2).

-backend.async-max-in-flight int
    Maximum concurrent in-flight amplified requests (async fire-and-forget).
    Requests are dropped (and counted) when at capacity.
    Default: 1000
```
