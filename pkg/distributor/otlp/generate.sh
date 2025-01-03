#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

command -v gopatch >/dev/null 2>&1 || { echo "Please install gopatch. Run 'go install github.com/uber-go/gopatch@latest' or visit https://github.com/uber-go/gopatch for more info."; exit 1; }
command -v goimports >/dev/null 2>&1 || { echo "Please install goimports. Run 'go install golang.org/x/tools/cmd/goimports@latest' or visit https://pkg.go.dev/golang.org/x/tools/cmd/goimports for more info."; exit 1; }

# Use GNU sed on MacOS falling back to `sed` everywhere else
SED="sed"
type gsed >/dev/null 2>&1 && SED=gsed

FILES=$(find ../../../vendor/github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite -name '*.go' ! -name timeseries.go ! -name "*_test.go")

for SRC in $FILES
do
  BASENAME=$(basename "$SRC")
  DST="${BASENAME%%.go}_generated.go"

  rm -f "$DST"
  echo "Processing $SRC to $DST"
  printf "// Code generated from Prometheus sources - DO NOT EDIT.\n\n" >"$DST"
  cat "$SRC" >> "$DST"

  gopatch -p mimirpb.patch "$DST"

  $SED -i "s/PrometheusConverter/MimirConverter/g" "$DST"
  $SED -i "s/Prometheus remote write format/Mimir remote write format/g" "$DST"
  goimports -w -local github.com/grafana/mimir "$DST"
done
