#! /usr/bin/env bash

set -euo pipefail

mkdir -p profiles

go test -run=XXX -bench="BenchmarkQuery/.*/streaming" -count=1 -benchmem -cpuprofile profiles/cpu.prof -memprofile profiles/mem.prof .
