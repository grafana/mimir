`benchmark-query-engine` runs the streaming PromQL engine benchmarks and captures peak memory utilisation of the benchmark process.

Each benchmark is run in a separate process to provide some kind of guarantee that the memory utilisation is not affected by other benchmarks.

An ingester is started in the `benchmark-query-engine` process (ie. not the benchmark process) to ensure the TSDB does not skew results.

Results from `benchmark-query-engine` can be summarised with `benchstat`, as well as [`compare.sh`](./compare.sh).

Usage:

- `go run .`: run all benchmarks once and capture peak memory utilisation
- `go run . -list`: print all available benchmarks
- `go run . -bench=abc`: run all benchmarks with names matching regex `abc`
- `go run . -count=X`: run all benchmarks X times
- `go run . -bench=abc -count=X`: run all benchmarks with names matching regex `abc` X times
- `go run . -start-ingester`: start ingester and wait (run no benchmarks)
- `go run . -use-existing-ingester=localhost:1234`: use existing ingester started with `-start-ingester` to reduce startup time
