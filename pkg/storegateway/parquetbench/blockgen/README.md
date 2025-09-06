# Block Generator Tool

A standalone tool for generating benchmark data blocks with configurable label cardinality dimensions and realistic time series data.

## Usage

```bash
go run main.go [flags]
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-output` | `./benchmark-data` | Output directory for generated blocks |
| `-user` | `benchmark-user` | User ID for the generated blocks |
| `-compression` | `true` | Enable compression for parquet data |
| `-sort-by` | `""` | Comma-separated list of fields to sort by in parquet data |
| `-store` | `both` | Store type to generate: 'parquet', 'tsdb', or 'both' |
| `-metrics` | `5` | Number of different metrics to generate |
| `-instances` | `100` | Number of different instances to generate |
| `-regions` | `5` | Number of different regions to generate |
| `-zones` | `10` | Number of different zones to generate |
| `-services` | `20` | Number of different services to generate |
| `-environments` | `3` | Number of different environments to generate |
| `-sample-value` | `0` | Fixed sample value (0 = random) |
| `-samples` | `10` | Number of samples per series |
| `-time-range` | `2` | Time range in hours for the generated block |
| `-verbose` | `false` | Verbose logging |

## Examples

### Generate 1.5M series (default dimensions: 5×100×5×10×20×3)
```bash
go run main.go -verbose
```

### Generate 15M series by scaling instances
```bash
go run main.go -instances 1000 -verbose
```

### High cardinality for specific dimensions
```bash
# 10M series with many instances and services
go run main.go -instances 500 -services 100 -verbose
```

### Low cardinality test scenario
```bash
# Only 60 series with minimal dimensions
go run main.go -metrics 2 -instances 2 -regions 3 -zones 5 -services 1 -environments 1 -verbose
```

### Generate only parquet blocks
```bash
go run main.go -store parquet -verbose
```

### Generate only TSDB blocks
```bash
go run main.go -store tsdb -verbose
```

### Generate with custom sorting
```bash
go run main.go -sort-by "__name__,instance,region" -verbose
```

### Generate with fixed sample values (for deterministic testing)
```bash
go run main.go -sample-value 42.0 -verbose
```

### Generate with custom time series density
```bash
# Generate 100 samples per series over 6 hours
go run main.go -samples 100 -time-range 6 -verbose
```

## Fixed Issues

The tool now correctly generates blocks with:
- **Proper timestamps**: Uses realistic time ranges instead of all samples at timestamp 0
- **Multiple samples per series**: Configurable number of samples distributed over the time range
- **Correct metadata**: Blocks contain proper minTime/maxTime and sample counts
- **TSDB components**: Generated TSDB blocks include index files and chunk files with actual data

## Output

The tool creates:
- TSDB blocks (if `-store tsdb` or `-store both`)
  - `index` file with series index
  - `chunks/000001` file with actual sample data  
  - `meta.json` with block metadata
  - `index-header` file for optimized queries (automatically generated)
  - `sparse-index-header` file for memory optimization (automatically generated)
- Parquet blocks (if `-store parquet` or `-store both`) 
  - `0.labels.parquet` with series labels
  - `0.chunks.parquet` with sample data
  - `parquet-conversion-mark.json`
- Bucket index files
- All data organized under the specified user ID

## Series Calculation

Total series = metrics × instances × regions × zones × services × environments

**Default dimensions:**
- 5 metrics × 100 instances × 5 regions × 10 zones × 20 services × 3 environments = **1,500,000 series**

**Examples:**
- **60 series**: `-metrics 2 -instances 2 -regions 3 -zones 5 -services 1 -environments 1`  
- **15M series**: `-instances 1000` (keeping other defaults)
- **10M series**: `-instances 500 -services 100` (with defaults: zones=10, regions=5, environments=3)

**Cardinality Control:**
- High cardinality dimensions (instances, services) → More realistic distribution patterns
- Low cardinality dimensions (regions, environments) → Smaller label sets
- Use different combinations to test various query selectivity scenarios

## Time Series Data

Each series gets multiple samples distributed evenly across the specified time range:
- Default: 10 samples over 2 hours = 1 sample every 13.3 minutes
- Custom: `-samples 60 -time-range 6` = 60 samples over 6 hours = 1 sample every 6 minutes

Sample values are random (0-100) unless `-sample-value` is specified.

## Size Modeling

When generating TSDB blocks, the tool automatically creates both `index-header` and `sparse-index-header` files. These files are important for:

- **Performance**: Index headers optimize query performance by avoiding full index reads
- **Memory usage**: Sparse headers further optimize memory consumption in the store-gateway
- **Size analysis**: Compare file sizes to understand storage overhead of different optimization layers

Example file sizes for a small test case (2 series, 10 samples each):
- `index`: 585 bytes (full TSDB index)
- `index-header`: 398 bytes (optimized header)
- `sparse-index-header`: 209 bytes (sparse optimization)