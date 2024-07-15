# What is this?

A simple hacky script + tool to download chunks from ingesters and dump their content.

# How to use it

1. Edit `pkg/ingester/client/ingester.proto` and change the `import "github.com/grafana/mimir/pkg/mimirpb/mimir.proto"` statement to `import "pkg/mimirpb/mimir.proto"`
1. Edit `download-chunks-from-ingesters-query.json` with the label matchers and time range to query.
1. Edit `download-chunks-from-ingesters.sh` with the configuration about the Kubernetes namespace and Mimir tenant to query.
1. Run `bash ./download-chunks-from-ingesters.sh` from this directory to download the chunks.
1. Once you've got the dump (1 file per ingester), run `go run . chunks-dump/*` to print the dump content of 1+ files.
