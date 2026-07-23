# dupts — find `sample_duplicate_timestamp` discards in a distributor pcap

`dupts` reads a packet capture of write traffic into a Mimir distributor and
reports which series carry samples that the distributor would discard with the
`sample_duplicate_timestamp` reason (i.e. that increment
`cortex_discarded_samples_total{reason="sample_duplicate_timestamp"}`).

It reproduces exactly what `pkg/distributor.validateSamples` /
`validateHistograms` do: **within a single timeseries entry of a single
WriteRequest**, when two or more samples (or histograms) share the same
timestamp, the first is kept and every later duplicate is dropped and counted.
Duplicates across separate requests or separate series are *not* what this
reason counts.

## What it decodes

Distributor writes arrive on gRPC port `9095` via two paths, both handled:

| `:path` | payload | tenant source |
| --- | --- | --- |
| `/distributor.Distributor/Push` | native gRPC — the message **is** a `mimirpb.WriteRequest` | `x-scope-orgid` gRPC metadata header |
| `/httpgrpc.HTTP/Handle` → `/api/v1/push` | dskit httpgrpc wrapping remote-write (snappy + `WriteRequest`) | wrapped `X-Scope-OrgID` header |
| `/httpgrpc.HTTP/Handle` → `/otlp/v1/metrics` | dskit httpgrpc wrapping OTLP (gzip/zstd/lz4 + protobuf/JSON) | wrapped `X-Scope-OrgID` header |

OTLP is converted to Prometheus series with Mimir's own converter
(`prometheusremotewrite` + `otlpappender.MimirAppender`), so the detected series
and timestamps match what the distributor validates. Because a single OTLP
histogram data point becomes several Prometheus series (`_bucket`/`_sum`/
`_count`), one duplicated OTLP data point is reported as duplicates on each of
those series — exactly as the distributor would discard them.

## Requirements

- `tshark` (Wireshark CLI). Default path is the macOS bundle
  `/Applications/Wireshark.app/Contents/MacOS/tshark`; override with `-tshark`.
- The capture must be **plaintext h2c** (no TLS). Mimir intra-cluster gRPC on
  9095 is plaintext.

## Usage

```bash
# Everything, all tenants:
go run ./tools/dupts -pcap /path/to/dump.pcap

# Only a specific tenant (recommended when chasing one tenant's discards):
go run ./tools/dupts -pcap /path/to/dump.pcap -tenant 1316763

# Print the per-occurrence lines as well as the aggregated report:
go run ./tools/dupts -pcap /path/to/dump.pcap -tenant 1316763 -all
```

Useful flags:

- `-tenant <id>` — only consider requests whose `X-Scope-OrgID` matches.
- `-push` / `-otlp` (default true) — enable/disable each path.
- `-otlp-add-suffixes` (default true), `-otlp-nhcb`, `-otlp-promote-scope` —
  OTLP conversion knobs matching per-tenant limits. They affect metric *names*,
  not the duplicate-detection grouping.
- `-recover-unknown` — best-effort decode of messages on streams whose HEADERS
  frame was not captured (connection predates the capture start). Tenant is
  unknown for these, so they are skipped when `-tenant` is set.
- `-urls` — print a histogram of the request `:path`s seen (diagnostic).
- `-hexfile <file>` — decode a single gRPC message from a hex dump (debug),
  e.g. `tshark ... -e grpc.message_data` piped to a file.

## Printing sample values

The aggregated report shows the offending series and their duplicated
timestamps, but not the sample values. To see values, use `-dump`, which prints
each series followed by its samples as `t=<ms> (<UTC>) v=<value>`. A duplicated
timestamp appears as the **same `t=` repeated with different values** (the first
is kept, the rest dropped).

Since `-dump` prints every sample of every series, the focused workflow is to
dump a single offending request found via `-all`:

```bash
TENANT=1320425
TSHARK=/Applications/Wireshark.app/Contents/MacOS/tshark
PCAP=/path/to/dump.pcap

# 1) find a request (frame) that has duplicates for the tenant
go run ./tools/dupts -pcap "$PCAP" -tenant "$TENANT" -all | grep '^frame=' | head

# 2) extract that gRPC message and dump its series WITH values (replace FRAME)
"$TSHARK" -r "$PCAP" -Y 'frame.number==FRAME' -T fields -e grpc.message_data | tr -d '\n:' > /tmp/msg.hex
go run ./tools/dupts -hexfile /tmp/msg.hex -dump
```

`dupts -hexfile` auto-detects native `/distributor.Distributor/Push`, httpgrpc
`/api/v1/push`, and httpgrpc `/otlp/v1/metrics` — no extra flags needed. Grep the
dump to isolate one series, e.g. `... -dump | grep -A20 procstat_cpu_usage`.

Example output for a duplicated series:

```
[rw1 tenant=1320425] {__name__="procstat_cpu_usage", host="ip-10-20-13-182.ec2.internal", job="sarlacc", ...}
    sample t=1784796120000 (2026-07-23 08:42:00.000 UTC) v=3.1     <- kept
    sample t=1784796120000 (2026-07-23 08:42:00.000 UTC) v=0.7     <- dropped (sample_duplicate_timestamp)
```

For a low-volume tenant you can skip the frame extraction and dump everything
for it directly — `go run ./tools/dupts -pcap "$PCAP" -tenant "$TENANT" -dump` —
but for a high-volume tenant that prints a lot, so prefer the single-frame
approach above.

## Capturing the right window

`sample_duplicate_timestamp` is per-request, so the capture must include the
request(s) that actually triggered the discards. Capture on the pod during a
window where
`cortex_discarded_samples_total{user="<tenant>", reason="sample_duplicate_timestamp"}`
is actively increasing. A capture that starts mid-connection will leave some
streams unclassifiable (`<none>` in the `-urls` histogram); `-recover-unknown`
recovers most of them at the cost of losing tenant attribution.

## Companion: otlplist

`tools/otlplist` decodes a single OTLP `httpgrpc.HTTPRequest` (from a
`-hexfile`) and lists its metrics and any duplicate-timestamp data points —
handy for inspecting one request in isolation.
