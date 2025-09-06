# Index Header Generator

A standalone tool for generating `index-header` and `sparse-index-header` files from existing TSDB blocks.

## Overview

This tool generates the optimization files that Grafana Mimir uses for efficient block querying:
- **`index-header`**: Contains symbols and posting offset tables extracted from the TSDB index
- **`sparse-index-header`**: A compressed version of the index-header for memory optimization

## Usage

```bash
go run main.go [flags]
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-block-dir` | **required** | Path to the TSDB block directory |
| `-user` | `default-user` | User ID for bucket organization |
| `-verbose` | `false` | Verbose logging with detailed progress |
| `-sizes` | `true` | Report file sizes and compression ratios |
| `-force` | `false` | Force regeneration even if files exist |

## Examples

### Basic usage
```bash
go run main.go -block-dir ./benchmark-data/benchmark-user/01K4FS5F4VMFQFCJM5BVFD20Z0
```

### Verbose output with size analysis
```bash
go run main.go \
  -block-dir ./tsdb-blocks/01K4FS5F4VMFQFCJM5BVFD20Z0 \
  -verbose \
  -user my-tenant
```

### Force regeneration of existing headers
```bash
go run main.go \
  -block-dir ./tsdb-blocks/01K4FS5F4VMFQFCJM5BVFD20Z0 \
  -force \
  -verbose
```

### Generate without size reporting
```bash
go run main.go \
  -block-dir ./tsdb-blocks/01K4FS5F4VMFQFCJM5BVFD20Z0 \
  -sizes=false
```

## Input Requirements

The tool expects a valid TSDB block directory containing:
- `index` - TSDB index file (required)
- `meta.json` - Block metadata file (required)
- Block directory name must be a valid ULID

## Output

The tool creates two optimization files in the block directory:
- `index-header` - Optimized header containing symbols and posting offsets
- `sparse-index-header` - Compressed sparse version for memory efficiency

## File Size Reporting

By default, the tool reports file sizes and compression ratios:

**Verbose output example:**
```
=== File Size Report ===
  index               :    49246 bytes (TSDB index file)
  index-header        :      980 bytes (Optimized index header)
  sparse-index-header :      254 bytes (Sparse index header)
  --- Ratios ---
  header reduction    :     98.0% (vs index)
  sparse reduction    :     99.5% (vs index)
  sparse saving       :     74.1% (vs header)
  Total               :    50480 bytes
========================
```

**Concise output example:**
```
index: 49246 bytes
index-header: 980 bytes
sparse-index-header: 254 bytes
```

## Smart Generation

The tool intelligently handles existing files:
- **No existing files**: Generates both index-header and sparse-index-header
- **Index-header exists**: Generates only sparse-index-header
- **Both exist**: Reports sizes only (unless `-force` is used)
- **Sparse exists but not index-header**: Generates index-header first, then sparse

## Performance Benefits

The generated files provide significant performance improvements:

**Index-header benefits:**
- Avoids reading the full TSDB index for queries
- Contains only essential symbols and posting offset information
- Typically 95-99% smaller than the full index

**Sparse-index-header benefits:**
- Further memory optimization using sampling
- Stores only 1/32 symbols in memory by default
- Additional 70-80% reduction over regular index-header
- Ideal for store-gateway memory optimization

## Error Handling

The tool validates:
- Block directory exists and is accessible
- Required files (`index`, `meta.json`) are present
- Block directory name is a valid ULID
- Output files can be created successfully

## Use Cases

**Development:**
- Generate headers for test blocks
- Recreate headers after index modifications
- Compare header sizes across different block configurations

**Operations:**
- Regenerate corrupted header files
- Create headers for imported TSDB blocks
- Optimize existing blocks for better query performance

**Analysis:**
- Measure header compression ratios
- Understand storage overhead of optimization layers
- Size modeling for different block configurations

## Integration

The generated files are compatible with:
- Grafana Mimir store-gateway
- Thanos store-gateway
- Any system using the Thanos block format with index headers

## Comparison with Other Tools

| Tool | Purpose | Input | Output |
|------|---------|-------|---------|
| `indexheadergen` | Generate headers from existing blocks | TSDB block | index-header + sparse-index-header |
| `blockgen` | Generate new test data | Configuration | Complete blocks with headers |
| `tsdb2parquet` | Convert block formats | TSDB block | Parquet block |