#! /usr/bin/env bash

set -euo pipefail

go test -run=XXX -bench="BenchmarkQuery" -count=6 -benchmem -timeout=1h ../test
