# What is this?

A simple tool to download chunks from store-gateways and dump their content. This tool is similar to the tool for dumping chunks from ingesters: [grpcurl-query-ingesters](../grpcurl-query-ingesters).

# How to use it

1. Edit `.proto` files in `pkg/storegateway` and change the `import "github.com/grafana/mimir/pkg/` statements to `import "pkg/`. You can use the following script from the root of the repository to do this:
   ```bash
   find pkg/storegateway -type f -name "*.proto" -exec sed -i '' 's#import "github.com/grafana/mimir/pkg/#import "pkg/#g' {} +
   ```
2. Edit `download-chunks-from-store-gateways-query.json` with the label matchers and time range to query.
3. Edit `download-chunks-from-store-gateway.sh` with:
    - The Kubernetes context
    - The Kubernetes namespace
    - The Mimir tenant ID to query
4. Run `bash ./download-chunks-from-store-gateways.sh` from this directory to download chunks from all store-gateways. This will create one file per store-gateway pod in the `chunks-dump` directory.
5. Once you've got the dumps, run `go run . chunks-dump/*` to parse and print the content of the dump files.
