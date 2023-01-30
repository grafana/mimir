#! /usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/benchmark-results"

codecs=(
  "original JSON"
  "gzipped original JSON"
  "snappy compressed original JSON"
  "original query middleware protobuf"
  "snappy compressed original query middleware protobuf"
  "uninterned protobuf"
  "interned protobuf"
  "snappy compressed interned protobuf"
  "interned protobuf with single string symbol table"
  "packed interned protobuf"
  "packed interned protobuf with single string symbol table"
  "packed interned protobuf with relative timestamps"
  "snappy compressed packed interned protobuf with relative timestamps"
  "Arrow"
  "snappy compressed Arrow"
)

mkdir -p "$OUTPUT_DIR"

for codec in "${codecs[@]}"; do
  output_file="$OUTPUT_DIR/$codec"

  if [ -f "$output_file" ]; then
    echo "Skipping re-running benchmarks for $codec."
    echo
    continue
  fi

  echo "Running benchmarks for $codec..."

  {
    cd "$SCRIPT_DIR"
    CODEC="$codec" go test -run=XXX -bench='.' -benchmem -count 5 . | tee "$output_file"
  }

  echo
done

echo "Benchmarking complete, summarising results..."

{
  cd "$OUTPUT_DIR"
  benchstat "${codecs[@]}"
}
