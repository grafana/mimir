`benchmark-query-engine` runs the streaming PromQL engine benchmarks and captures peak memory utilisation of the benchmark process.

Each benchmark is run in a separate process to provide some kind of guarantee that the memory utilisation is not affected by other benchmarks.

An ingester is started in the `benchmark-query-engine` process (ie. not the benchmark process) to ensure the TSDB does not skew results.

Peak memory utilisation is the high-water mark of the benchmark process' resident set size (RSS), reported in the last column of each benchmark line as `B`. On macOS this is taken from the `Rusage` returned when the benchmark process exits. On Linux that value would include the peak RSS of the `benchmark-query-engine` process itself (including the ingester's data), so the benchmark process reports its own peak RSS from `/proc/self/status` instead. This is the peak over all iterations of the benchmark, and a benchmark runs more iterations when each query is faster. To compare the peak memory utilisation of two versions whose queries take different times, run both with the same number of iterations, for example with `-benchtime=10x`.

For the Mimir engine, each benchmark line also includes `estimated-peak-B/op`: the mean peak memory consumption that the memory consumption tracker estimates for each query. This is the value that per-query memory limits use. It does not include memory that the tracker does not count, so it can be much lower than the peak RSS.

Results from `benchmark-query-engine` can be summarised with `benchstat`, as well as [`compare.sh`](./compare.sh).

Usage:

- `go run .`: run all benchmarks once and capture peak memory utilisation
- `go run . -list`: print all available benchmarks
- `go run . -bench=abc`: run all benchmarks with names matching regex `abc`
- `go run . -count=X`: run all benchmarks X times
- `go run . -bench=abc -count=X`: run all benchmarks with names matching regex `abc` X times
- `go run . -start-ingester`: start ingester and wait (run no benchmarks)
- `go run . -use-existing-ingester=localhost:1234`: use existing ingester started with `-start-ingester` to reduce startup time
