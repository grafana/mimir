# OTLP translation code generator

This command line utility ("generate") generates code for translating from an OTLP write request to a Mimir remote write request.
It does so by downloading corresponding mimir-prometheus Go files, and transforming them through the
[gopatch](https://github.com/uber-go/gopatch) tool.

The utility maintains the source mimir-prometheus Git reference hash, from which the current sources were generated, in the file .source-ref in this directory.
If the mimir-prometheus version currently depended on hasn't changed from that, no source code is re-generated.
