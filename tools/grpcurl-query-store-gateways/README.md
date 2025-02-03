# What is this?

A simple tool to download chunks from store-gateways and dump their content. This tool is similar to the tool for dumping chunks from ingesters: [grpcurl-query-ingesters](../grpcurl-query-ingesters).

# How to use it

1. Edit `download-chunks-from-store-gateways-query.json` with the label matchers and time range to query.
2. Edit `download-chunks-from-store-gateway.sh` with:
    - The Kubernetes context
    - The Kubernetes namespace
    - The Mimir tenant ID to query
3. Run `bash ./download-chunks-from-store-gateways.sh` from this directory to download chunks from all store-gateways. This will create one file per store-gateway pod in the `chunks-dump` directory.
4. Once you've got the dumps, run `go run . chunks-dump/*` to parse and print the content of the dump files.
