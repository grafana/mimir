# Mimir Query Engine (MQE) conventions

## Pooled `HPoint` slices must not share `FloatHistogram` instances

Reusing `*histogram.FloatHistogram` instances (via `FloatHistogram.CopyTo`) is an intentional performance optimisation: it avoids re-allocating the histogram bucket slices on every copy. `HPointSlicePool` is configured with `clearOnGet=false`, and consumers such as `types.AppendHPointCopies` deliberately reuse the `FloatHistogram` instances already present in a slice obtained from the pool.

The invariant that makes this safe: **any code that returns an `HPoint` slice to `HPointSlicePool` must ensure that no two points in the slice reference the same `FloatHistogram` instance, and that those instances are not still referenced by any other live slice.** Otherwise, the next caller to reuse the slice from the pool will alias those instances via `CopyTo` and corrupt unrelated results.

In practice, when you move or append points out of a slice and then return the source slice to the pool — for example `pool.AppendToSlice(dest, ..., src[a:b]...)` followed by `HPointSlicePool.Put(&src, ...)` — you must `clear()` the transferred range of `src` first, because `AppendToSlice` copies the `HPoint` structs and therefore shares their `FloatHistogram` pointers with `dest`.

Do **not** work around an aliasing bug by making `AppendHPointCopies` (or similar) always deep-copy: that would discard the optimisation. Fix the producer that violates the invariant instead.
