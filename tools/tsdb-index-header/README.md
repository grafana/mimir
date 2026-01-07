# tsdb-index-header

CLI tool to analyze TSDB index-header files, providing statistics about their structure and contents.

## Usage

```bash
go run ./tools/tsdb-index-header <path-to-index-header>
```

To analyze specific label value distributions:

```bash
go run ./tools/tsdb-index-header -analyze-labels=__name__,instance <path-to-index-header>
```

The index-header file must be located at `<dir>/<block-id>/index-header` where `<block-id>` is a valid ULID.

**Note:** Only TSDB index V2 format is supported. V1 format index-headers will be rejected.

## Analyses

The tool performs the following analyses:

1. **TOC Info** - Index-header file structure including size, version, and section sizes (symbols, postings offset table).
2. **Symbols Analysis** - Symbol table statistics including count, total size, length distribution histogram, longest symbols, and validation checks (sort order, duplicates).
3. **Label Cardinality Analysis** - Statistics about label names and their value counts, with cardinality distribution histogram and top highest-cardinality labels.
4. **Label Value Distribution** (optional, via `-analyze-labels`) - Detailed analysis of values for specific labels, including length distribution and sample values.
