# TSDB to Parquet Converter

A standalone tool for converting existing TSDB blocks to Parquet format.

## Usage

```bash
go run main.go [flags]
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-block-dir` | **required** | Path to the TSDB block directory to convert |
| `-output` | same as input parent | Output directory for converted Parquet files |
| `-user` | `converted-user` | User ID for the converted Parquet block |
| `-compression` | `true` | Enable compression for Parquet data |
| `-sort-by` | `""` | Comma-separated list of fields to sort by in Parquet data |
| `-verbose` | `false` | Verbose logging |
| `-sizes` | `true` | Report file sizes after conversion |

## Examples

### Basic conversion
```bash
go run main.go -block-dir ./benchmark-data/benchmark-user/01K4F5TR4ZH4B3T5870W1Z4M7P
```

### Conversion with custom output directory
```bash
go run main.go \
  -block-dir ./tsdb-blocks/01K4F5TR4ZH4B3T5870W1Z4M7P \
  -output ./parquet-blocks \
  -user my-tenant
```

### Conversion with custom sorting and verbose output
```bash
go run main.go \
  -block-dir ./tsdb-blocks/01K4F5TR4ZH4B3T5870W1Z4M7P \
  -sort-by "__name__,instance,region" \
  -verbose
```

### Conversion without compression
```bash
go run main.go \
  -block-dir ./tsdb-blocks/01K4F5TR4ZH4B3T5870W1Z4M7P \
  -compression=false \
  -verbose
```

## Input Requirements

The tool expects a valid TSDB block directory containing:
- `index` - TSDB index file
- `meta.json` - Block metadata file
- `chunks/` - Directory containing chunk files
- Block directory name must be a valid ULID

## Output

The tool creates a Parquet block with:
- `0.labels.parquet` - Series labels in Parquet format
- `0.chunks.parquet` - Sample data in Parquet format (if chunks exist)
- `parquet-conversion-mark.json` - Conversion metadata

## File Size Reporting

By default (unless `-sizes=false`), the tool reports the sizes of generated files:

**Verbose output example:**
```
=== File Size Report ===
  0.labels.parquet              :     9151 bytes (Parquet labels file)
    - labels footer             :     1060 bytes (Parquet footer metadata)
  0.chunks.parquet              :   124567 bytes (Parquet chunks file)
    - chunks footer             :     1234 bytes (Parquet footer metadata)
  parquet-conversion-mark.json  :       89 bytes (Conversion mark file)
  Total                         :   133807 bytes
========================================
```

**Concise output example:**
```
0.labels.parquet: 9151 bytes
0.labels.parquet footer: 1060 bytes
0.chunks.parquet: 124567 bytes
0.chunks.parquet footer: 1234 bytes
parquet-conversion-mark.json: 89 bytes
```

## Error Handling

The tool validates:
- Block directory exists and is accessible
- Required files (`index`, `meta.json`, `chunks/`) are present
- Block directory name is a valid ULID
- Output directory is writable

## Integration with Grafana Mimir

The converted Parquet blocks are compatible with Grafana Mimir's store-gateway and can be used alongside TSDB blocks for:
- Performance comparisons
- Storage format migration
- Benchmarking different query patterns

## Comparison with blockgen

| Tool | Purpose | Input | Output |
|------|---------|--------|-------|
| `blockgen` | Generate new test data | Configurable dimensions | TSDB blocks + Parquet blocks |
| `tsdb2parquet` | Convert existing blocks | Existing TSDB block | Parquet block |

Use `tsdb2parquet` when you have existing TSDB blocks that you want to convert to Parquet format for analysis or migration purposes.