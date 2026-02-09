# tsdb-index-header

CLI tool to analyze TSDB index and index-header files, providing statistics about their structure and contents.

## Usage

```bash
go run ./tools/tsdb-index-header <block-directory>
```

The tool automatically detects whether the block directory contains a full `index` file or an `index-header` file (preferring the full index if both are present).

To analyze specific label value distributions:

```bash
go run ./tools/tsdb-index-header -analyze-labels=__name__,instance <block-directory>
```

The block directory must be located at `<dir>/<block-id>/` where `<block-id>` is a valid ULID.

**Note:** Only TSDB index V2 format is supported. V1 format indexes will be rejected.

## Analyses

The tool performs the following analyses:

1. **Index Info** - Index file structure including size, version, and section sizes (symbols, postings offset table for index-headers).
2. **Symbols Analysis** - Symbol table statistics including count, total size, length distribution histogram, longest symbols, and validation checks (sort order, duplicates).
3. **Label Cardinality Analysis** - Statistics about label names and their value counts, with cardinality distribution histogram and top highest-cardinality labels.
4. **Label Value Distribution** (optional, via `-analyze-labels`) - Detailed analysis of values for specific labels, including length distribution and sample values.
5. **Metric Name Statistics** (optional, via `-analyze-labels`, full index only) - For each analyzed label, shows how many unique metric names (`__name__`) have that label and the top 10 metric names by series count.

## Full Index vs Index-Header

Some features are only available when analyzing a full index file:

| Feature | Full Index | Index-Header |
|---------|------------|--------------|
| Symbols Analysis | Yes | Yes |
| Label Cardinality Analysis | Yes | Yes |
| Label Value Distribution | Yes | Yes |
| Metric Name Statistics | Yes | No |

Metric name statistics require iterating over series postings, which are not available in index-header files.

## Example Output

When running with `-analyze-labels=pod` on a full index:

```
=== Label Value Distribution: pod ===
Total values:              22357
Total bytes:               773331 (0.74 MB)
Average value length:      34.59 bytes
...

Metric names with this label: 170 unique
Top 10 metric names by series count:
   1. [   15384 series] kube_pod_status_phase
   2. [    8153 series] kube_pod_container_resource_requests
   3. [    7263 series] asserts:resource
   ...
```
