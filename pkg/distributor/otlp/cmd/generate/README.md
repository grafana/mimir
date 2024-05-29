# OTLP translation code generator

This command line utility ("generate") generates code for translating from an OTLP write request to a Mimir remote write request.
It does so by transforming corresponding vendored Prometheus Go files through the [gopatch](https://github.com/uber-go/gopatch) tool.
