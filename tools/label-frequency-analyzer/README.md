# What is this?

A simple hacky script to download label names and values from ingesters and analyze their frequencies. This tool is similar to the tool for dumping chunks from ingesters: [grpcurl-query-ingesters](../grpcurl-query-ingesters).

# How to use it

1. Edit `pkg/ingester/client/ingester.proto` and change the `import "github.com/grafana/mimir/pkg/mimirpb/mimir.proto"` statement to `import "pkg/mimirpb/mimir.proto"`
2. Edit `download-labels-from-ingesters.sh` with the configuration about the Kubernetes namespace and Mimir tenant to query.
3. Run `bash ./download-labels-from-ingesters.sh` from this directory to download the labels.
4. Once you've got the dump (1 file per ingester), run `go run . label-names-and-values-dump/*` to analyze the label frequencies.

## Configuration

Required flags:
- `--ingester-addresses`: Comma-separated list of ingester addresses

## Purpose

The tool helps identify:
- Most frequently used label values across all tenants
- Potential cardinality issues
- Common patterns in label usage
- Opportunities for label optimization

## Configuration

The tool accepts the following flags:
- `--top-n`: Number of top label values to show (default: 100)
- `--min-frequency`: Minimum frequency to include a label value (default: 1)
- `--output-format`: Output format (json, yaml, text) (default: text)
- `--timeout`: Timeout for ingester operations (default: 30s)

## Usage

```bash
# Basic usage
./label-frequency-analyzer --ingester-addresses=ingester-1:9090,ingester-2:9090

# With specific options
./label-frequency-analyzer \
  --ingester-addresses=ingester-1:9090,ingester-2:9090 \
  --top-n=100 \
  --min-frequency=10 \
  --output-format=json
```

## Output

The tool outputs a list of the most frequently used label values, sorted by frequency. For each label value, it shows:
- The label name
- The label value
- The frequency count
- The percentage of total labels

## Building

```bash
go build -o label-frequency-analyzer ./tools/label-frequency-analyzer
``` 