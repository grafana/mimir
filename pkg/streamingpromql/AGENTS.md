# Memory pooling

This package makes extensive use of memory pooling.

It is preferred to not return slices to pools after errors.
This makes the code simpler and clearer. Errors are expected to be rare and therefore have minimal impact on the effectiveness of pooling.

# Memory consumption estimate

This package makes use of memory consumption limiting through the memory consumption estimate maintained by `limiter.MemoryConsumptionTracker`.

It is preferred to not reduce the memory consumption estimate after errors.
This makes the code simpler and clearer. Once an error occurs, the query is expected to stop.
In the worst case scenario, the query would continue with the elevated memory consumption estimate and later fail due to exceeding the memory limit.
