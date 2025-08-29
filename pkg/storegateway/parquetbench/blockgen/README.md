# Block Generator Tool

A standalone tool for generating benchmark data blocks with configurable series counts and realistic time series data.

## Usage

```bash
go run main.go [flags]
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-output` | `./benchmark-data` | Output directory for generated blocks |
| `-user` | `test-user` | User ID for the generated blocks |
| `-series` | `1500000` | Total number of series to generate |
| `-compression` | `true` | Enable compression for parquet data |
| `-sort-by` | `""` | Comma-separated list of fields to sort by in parquet data |
| `-store` | `both` | Store type to generate: 'parquet', 'tsdb', or 'both' |
| `-metrics` | `5` | Number of different metrics to generate |
| `-sample-value` | `0` | Fixed sample value (0 = random) |
| `-samples` | `10` | Number of samples per series |
| `-time-range` | `2` | Time range in hours for the generated block |
| `-verbose` | `false` | Verbose logging |

## Examples

### Generate 1.5M series (default)
```bash
go run main.go -verbose
```

### Generate 15M series
```bash
go run main.go -series 15000000 -verbose
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
- Parquet blocks (if `-store parquet` or `-store both`) 
  - `0.labels.parquet` with series labels
  - `0.chunks.parquet` with sample data
  - `parquet-conversion-mark.json`
- Bucket index files
- All data organized under the specified user ID

## Series Distribution

The tool calculates dimensions to achieve the target series count:
- Metrics: Configurable via `-metrics` flag
- Instances, regions, zones, services, environments: Automatically calculated

For 1.5M series with 5 metrics:
- 5 metrics × 100 instances × 5 regions × 10 zones × 20 services × 3 environments = 1,500,000 series

For 15M series with 5 metrics:
- 5 metrics × 1000 instances × 5 regions × 10 zones × 20 services × 3 environments = 15,000,000 series

## Time Series Data

Each series gets multiple samples distributed evenly across the specified time range:
- Default: 10 samples over 2 hours = 1 sample every 13.3 minutes
- Custom: `-samples 60 -time-range 6` = 60 samples over 6 hours = 1 sample every 6 minutes

Sample values are random (0-100) unless `-sample-value` is specified.