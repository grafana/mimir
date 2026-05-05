# Async Prefetch for BucketReader

## Context

`BucketReader` (in `pkg/storage/indexheader/encoding/bucket_reader.go`) currently fills its buffer synchronously: when a `Peek` or `Read` finds the buffer empty, it issues a single `GetRange` call sized to fill the rest of the buffer (default 1 MiB) and the consumer blocks until the entire range arrives.

Two problems with this:
1. **Time-to-first-byte is high.** A consumer that only needs a few bytes still waits for the entire 1 MiB GetRange to complete before any data is available.
2. **No pipelining.** While the consumer is processing buffered bytes, the reader is idle. The next fetch only starts once the consumer drains the buffer.

This is especially painful in the hot index-header binary-search paths (`symbols.go` `Lookup`, `postings.go` `PostingsOffset`) which call `ResetAt` ~O(log N) times per query, each followed by a few small `Peek`/`Read` calls.

**Goal:** A background goroutine in `BucketReader` that asynchronously prefetches the buffer in chunks of `len(buf)/8` (≈128 KiB for the default 1 MiB buffer). After the first chunk lands, small `Peek`/`Read` calls can return immediately; subsequent chunks arrive in the background while the consumer processes earlier ones. `Peek(n)`/`Read(n)` block only if `n` exceeds what is currently buffered.

## Design Overview

- One prefetch goroutine per BucketBufReader, started when its buffer is attached and stopped on `Close`.
- All buffer state (`bufR`, `bufW`, `off`, `lastErr`, `gen`, `pendingFetch`) is protected by a mutex (`mu`).
- Two condition variables: `dataAvailable` (consumer waits, goroutine signals after a successful fetch) and `spaceAvailable` (goroutine waits, consumer signals after `Read`/`Discard`/`Reset`).
- Goroutine fetches one chunk at a time and writes into `buf[bufW:bufW+chunkSize]`. It only releases the mutex during the actual `GetRange`/`io.ReadFull` call.
- **Goroutine never compacts.** It only appends past `bufW`. This guarantees that bytes returned by `Peek` (which live in `buf[bufR:bufW]`) are never overwritten by an in-flight fetch — slice safety preserved without changing the existing "valid until next Read" contract.
- **Consumer compacts reactively** in `Read`/`Discard` when it advances `bufR` and the goroutine needs more space. Per the bufio contract, any prior `Peek` slice is invalidated by `Read`/`Discard`, so compaction here is safe.
- **Reset aborts in-flight fetches** via a per-fetch context. A `gen` counter is incremented on every `Reset`/`Seek`; the goroutine checks `gen` after the fetch returns and discards the result if it does not match.

The unbuffered path (`buf == nil`, used by `bucket_factory.go` for the 4-byte length prefix) is unchanged: it still routes through `readDirect` with no locking and no goroutine.

## State Changes to `BucketReader`

File: `pkg/storage/indexheader/encoding/bucket_reader.go`

Add to the struct:

```go
chunkSize int

mu             sync.Mutex
dataAvailable  *sync.Cond  // sync.NewCond(&mu)
spaceAvailable *sync.Cond  // sync.NewCond(&mu)
gen            uint64      // bumped on Reset/Seek to invalidate in-flight fetches
pendingFetch   bool        // true while the goroutine has released mu mid-fetch
closed         bool

workerCtx    context.Context
workerCancel context.CancelFunc
fetchCtx     context.Context     // child of workerCtx, cancelled on Reset
fetchCancel  context.CancelFunc
workerWG     sync.WaitGroup
```

`buf` / `bufR` / `bufW` / `off` / `lastErr` already exist.

## Goroutine Lifecycle

Add (unexported) `startPrefetch(chunkSize int)` on `BucketReader`. Called from `newBucketBufReader` after the buffer is attached. It:
1. Stores `chunkSize`, sets up cond vars, derives `workerCtx` from `r.ctx`, derives initial `fetchCtx` from `workerCtx`.
2. Adds 1 to `workerWG` and launches `prefetchLoop` in a goroutine.

Add `stopPrefetch()`. Called from `Close`. It:
1. Acquires `mu`, sets `closed = true`, broadcasts both cond vars, calls `workerCancel`, releases `mu`.
2. `workerWG.Wait()` — guarantees the goroutine has exited before `Close` returns the buffer to the pool.

## The `prefetchLoop`

Pseudocode:

```
for {
    mu.Lock()
    for !shouldFetch() {           // see below
        if closed || workerCtx.Err() != nil { mu.Unlock(); return }
        spaceAvailable.Wait()
    }
    toFetch  := min(chunkSize, len(buf)-bufW, length-off)
    sourceOff := base + off
    bufStart := bufW
    ourGen   := gen
    fetchCtx := r.fetchCtx          // capture under lock
    pendingFetch = true
    mu.Unlock()

    rc, err := bkt.GetRange(fetchCtx, name, sourceOff, toFetch)
    var n int
    if err == nil {
        n, err = io.ReadFull(rc, buf[bufStart:bufStart+toFetch])
        rc.Close()
        if errors.Is(err, io.ErrUnexpectedEOF) { err = io.EOF }
    }

    mu.Lock()
    pendingFetch = false
    spaceAvailable.Signal()         // unblock any consumer waiting on the flag
    if closed { mu.Unlock(); return }
    if gen != ourGen {              // Reset raced with this fetch
        mu.Unlock()
        continue                    // discard result, replan from new state
    }
    if errors.Is(err, context.Canceled) {
        // fetchCtx was cancelled by Reset; gen check above usually catches this,
        // but a cancellation could land first. Replan.
        mu.Unlock()
        continue
    }
    if err != nil {
        lastErr = err
        dataAvailable.Broadcast()
        mu.Unlock()
        continue                    // wait on Reset to clear sticky error
    }
    bufW = bufStart + n
    off += n
    if off >= length { lastErr = io.EOF }
    dataAvailable.Broadcast()
    mu.Unlock()
}
```

`shouldFetch` (called with `mu` held):

```go
func (r *BucketReader) shouldFetch() bool {
    if r.lastErr != nil { return false }
    if r.off >= r.length { return false }
    free := len(r.buf) - r.bufW
    remaining := r.length - r.off
    // Last partial chunk: fetch whatever fits even if < chunkSize.
    if remaining < r.chunkSize { return free >= remaining }
    return free >= r.chunkSize
}
```

This delivers the user's "fill in chunks" semantics: each fetch is bounded by `chunkSize`; the goroutine pipelines as long as the buffer has room for a full chunk.

## Method Changes

All buffered-path methods acquire `r.mu` for the duration. Unbuffered path (`r.buf == nil`) is unchanged.

### `Peek(n int) ([]byte, error)`

```
mu.Lock(); defer mu.Unlock()
for bufW-bufR < n && bufW-bufR < len(buf) && lastErr == nil && !closed {
    dataAvailable.Wait()
}
if avail := bufW - bufR; n > avail {
    n = avail
    if lastErr != nil { err = readErr() }
    else              { err = errBufferFull }
}
return buf[bufR : bufR+n], err
```

### `Read(p []byte) (int, error)`

```
if buf == nil { return readDirect(p) }
mu.Lock(); defer mu.Unlock()
for bufR == bufW && lastErr == nil && !closed {
    dataAvailable.Wait()
}
if bufR == bufW { return 0, readErr() }
n := copy(p, buf[bufR:bufW])
bufR += n
maybeCompact()
spaceAvailable.Signal()
return n, nil
```

### `Discard(n int) (int, error)` — same loop as today, but waits on `dataAvailable` instead of calling `fill()`. Calls `maybeCompact()` and signals `spaceAvailable` after advancing `bufR`.

### `maybeCompact()` (called with `mu` held, never while `pendingFetch == true`)

```go
func (r *BucketReader) maybeCompact() {
    if r.pendingFetch { return }
    if r.bufR == 0 { return }
    if len(r.buf) - r.bufW >= r.chunkSize { return } // room already
    copy(r.buf, r.buf[r.bufR:r.bufW])
    r.bufW -= r.bufR
    r.bufR = 0
}
```

Compaction is the consumer's responsibility precisely so that it never races with an in-flight fetch's writes to `buf[bufW:]`.

### `Reset(off int)`

```
mu.Lock(); defer mu.Unlock()
fetchCancel()                        // abort any in-flight GetRange
fetchCtx, fetchCancel = context.WithCancel(workerCtx)
gen++
r.off = off
bufR, bufW = 0, 0
lastErr = nil
spaceAvailable.Broadcast()
dataAvailable.Broadcast()
```

### `Seek(offset, whence)` — same as `Reset` plus the whence check.

### `Buffered() int`, `Size() int` — acquire `mu` and return the field.

### `fill()` is deleted. The goroutine is the only filler.

## `BucketBufReader` Changes

File: same.

- `newBucketBufReader` calls `reader.startPrefetch(len(reader.buf) / 8)` after attaching the buffer (with a floor so very small test buffers still work, e.g. `max(len/8, 1)`). The chunk size is plumbed through so tests can override it.
- `Close` calls `reader.stopPrefetch()` before returning the buffer to the pool.
- Other methods (`Peek`, `Read`, `ReadInto`, `Skip`, `ResetAt`, `Buffered`, `Size`, `Len`, `Offset`) are unchanged — they delegate to the now-async `BucketReader` methods.
- The existing `ResetAt` skip-ahead optimization (`dist > 0 && dist < bbr.Buffered()` → `Skip(dist)`) is preserved and continues to avoid a Reset when seeking forward within the buffered range.

## Concurrency Invariants

These invariants justify why the design is race-free:

1. **`bufR`, `bufW`, `off`, `lastErr`, `gen`, `pendingFetch`, `closed` are read/written only under `mu`.** Trivial.
2. **Bytes in `buf[bufR:bufW]` are stable while `mu` is not held.** The goroutine only writes to `buf[bufW:bufW+toFetch]` (strictly past the valid range it captured under the lock). Since `bufW` only grows monotonically between Resets, and consumer-side `bufR` advances move only forward, the slice returned by `Peek` (which lives in `buf[bufR:bufR+n] ⊂ buf[bufR:bufW]`) is not touched by the goroutine.
3. **Compaction never overlaps an in-flight fetch.** `maybeCompact` early-returns if `pendingFetch == true`. The goroutine sets `pendingFetch = true` under `mu` before releasing the lock and clears it under `mu` after the fetch.
4. **Reset cleanly invalidates an in-flight fetch.** `fetchCancel` is called under `mu` before bumping `gen`. The goroutine, upon reacquiring `mu`, observes `gen != ourGen` (or `errors.Is(err, context.Canceled)`) and discards the result without touching shared state.
5. **`buf` slice header is never reassigned while the goroutine is running.** `buf` is only set to `nil` in `Close`, which calls `stopPrefetch` (which `Wait`s for the goroutine to exit) before clearing.

## Test Changes

File: `pkg/storage/indexheader/encoding/bucket_reader_test.go`

- `newBucketBufReader` test helper grows a `chunkSize` parameter. Existing tests pass `chunkSize = testBufPoolSize` (= 16) so each fetch fills the whole tiny test buffer in one call, preserving today's GetRange-count assertions in `TestBucketBufReader_GetRangeCalls_Buffering` and `TestBucketBufReader_GetRangeCalls_ResetRefetches`.
- Add new tests:
  - `TestBucketReader_AsyncPrefetch_ChunkedFills`: tracking bucket sees N calls of `chunkSize` bytes when a small reader (chunkSize < bufSize < contentLen) is read sequentially.
  - `TestBucketReader_AsyncPrefetch_FirstByteEarly`: consumer's first `Peek(1)` returns after only the first chunk has arrived (use a `failingBucket`-style wrapper that gates `GetRange` returns).
  - `TestBucketReader_AsyncPrefetch_ResetCancelsInFlight`: gate the bucket so a `GetRange` is in-flight, call `ResetAt(newOff)`, verify the goroutine starts a new `GetRange` from `newOff` (and the old fetch's bytes are not visible).
  - `TestBucketReader_AsyncPrefetch_BlocksUntilEnoughBuffered`: `Peek(n)` where `n` > current `Buffered()` blocks until the goroutine fetches enough.
  - `TestBucketReader_AsyncPrefetch_CloseStopsGoroutine`: with race detector, ensure no goroutine leak / no writes after `Close`.
- Run the full package with `-race`.

## Files to Modify

- `pkg/storage/indexheader/encoding/bucket_reader.go` — main changes (add async fields, `prefetchLoop`, `startPrefetch`/`stopPrefetch`, locking on all buffered methods, delete `fill`).
- `pkg/storage/indexheader/encoding/bucket_reader_test.go` — extend `newBucketBufReader` helper, add new tests.

`bucket_factory.go`, `encoding.go`, `symbols.go`, `postings.go` need no changes — the public surface of `BucketBufReader` is preserved.

## Verification

1. `go test -race -count=1 ./pkg/storage/indexheader/encoding/...` — unit tests including new async ones, race detector clean.
2. `go test -race -count=1 ./pkg/storage/indexheader/...` — broader smoke test for symbols/postings paths that rely on `BucketBufReader`.
3. Spot-check a benchmark from the existing `benchout-*.txt` files in the repo root (e.g. `LabelValuesOffsetsWithPrefix`) to confirm async prefetch does not regress throughput on the binary-search hot path.
