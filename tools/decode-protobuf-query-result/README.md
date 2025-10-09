# Purpose

To decode PromQL query results encoded in Protobuf format.

Use case: to query and print native histograms in their raw format instead
of the simplified format in the default JSON response. In particular to get
schema number, reset hint, spans, etc, in other words the raw samples that
PromQL produced.

# How to use

Make a query using curl (or some other tool) and make sure to:

- use the header `-H "Accept: application/vnd.mimir.queryresponse+protobuf"`,
- optionally use `-H "Cache-Control: no-store"` to avoid caches,
- save the result in a file,
- verify that the HTTP response was 2xx.

Run this command with the filename as the argument and enjoy.
