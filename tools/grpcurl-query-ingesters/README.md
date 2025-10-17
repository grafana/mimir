# What is this?

A tool to query chunks from ingesters and immediately dump their content. This tool connects directly to Kubernetes ingesters using gRPC and outputs parsed samples in real-time, without creating intermediate JSON files.

# How to use it

1. Edit `download-chunks-from-ingesters-query.json` with the label matchers and time range to query.
1. Run `go run . <k8s-context> <k8s-namespace> <tenant-id> download-chunks-from-ingesters-query.json` from this directory to query ingesters and dump chunk content directly.

## Examples

Query ingesters and dump chunk content:
```bash
go run . my-k8s-context mimir-namespace my-tenant-id download-chunks-from-ingesters-query.json
```

Get help:
```bash
go run . --help
```
