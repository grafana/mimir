# tsdb-gaps

This program searches for and analyzes gaps in TSDB blocks. It writes
the analyzed output as JSON to the standard output.

## Usage

```
Usage: tsdb-gaps [flags] -select string blockID1 blockID2 ...

required argument:
  blockIDs: ULID corresponding to the top-level directory of the TSDB block

required flag:
  -select string
        PromQL metric selector

optional flags:
  -max-single-gap-time int
        Maximum gap time in seconds for a single gap (default 86400)
  -max-single-missed-samples int
        Maximum missed samples in a single gap (default 10000)
  -max-total-gap-time int
        Maximum total gap time in seconds for a series (default 86400)
  -max-total-missed-samples int
        Maximum total missed samples in a series (default 1000000)
  -maxt value
        Maximum timestamp to consider (default: block max time)
  -min-single-gap-time int
        Minimum gap time in seconds for a single gap (default 1)
  -min-single-missed-samples int
        Minimum missed samples in a single gap (default 1)
  -min-total-gap-time int
        Minimum total gap time in seconds for a series (default 1)
  -min-total-missed-samples int
        Minimum total missed samples in a series (default 1)
  -mint value
        Minimum timestamp to consider (default: block min time)
  -scrape-interval int
        Threshold for gap detection in seconds, set to 0 for automatic interval detection)
```

The `tsdb-gaps` program runs against a local filesystem containing one or more TSDB blocks.
Besides the block IDs, the only required flag is `-select string`, to narrow down the number of
series to check. The program processes every sample of every series matched by the `-select` PromQL
metric selector. For large blocks this could take a while, so it makes sense to restrict the
number of series to check if possible.

Likewise the rest of the flags have the effect of restricting the total search space in order
to reduce the size of the output to what is necessary.

## Examples

### Find gaps in a single metric

```yaml
./tsdb-gaps -select '{__name__="etcd_requests_total"}' 01J0SSQT75APKRZ2ZX8FYZ08TA | jq
level=debug msg="using matchers" matcher="__name__=\"etcd_requests_total\""
{
  "01J0SSQT75APKRZ2ZX8FYZ08TA": {
    "blockID": "01J0SSQT75APKRZ2ZX8FYZ08TA",
    "minTime": 1718841600134,
    "maxTime": 1718848800000,
    "totalSeries": 398711,
    "totalMatchedSeries": 2487,
    "totalSeriesWithGaps": 0,
    "totalSamples": 195206875,
    "totalMissedSamples": 0,
    "gapStats": null
  }
}
```

The `totalSeriesWithGaps` and `"gapStats": null` show there were no gaps in the metric.

### Using regex and limiting by time range

```yaml
./tsdb-gaps -select '{__name__="minio_node_drive_latency_us", api=~"storage.(Read|Write).*"}' -mint "2024-06-20T00:30:00Z" -maxt "2024-06-20T01:00:00Z" 01J0SSQT75APKRZ2ZX8FYZ08TA | jq
  level=debug msg="using matchers" matcher="__name__=\"minio_node_drive_latency_us\"" matcher="api=~\"storage.(Read|Write).*\""
{
  "01J0SSQT75APKRZ2ZX8FYZ08TA": {
    "blockID": "01J0SSQT75APKRZ2ZX8FYZ08TA",
    "minTime": 1718841600134,
    "maxTime": 1718848800000,
    "totalSeries": 398711,
    "totalMatchedSeries": 6,
    "totalSeriesWithGaps": 1,
    "totalSamples": 195206875,
    "totalMissedSamples": 38,
    "gapStats": [
      {
        "seriesLabels": "{__name__=\"minio_node_drive_latency_us\", api=\"storage.ReadVersion\", drive=\"/var/db/minio\", instance=\"minio\", job=\"minio-job\", server=\"127.0.0.1:9000\"}",
        "minTime": 1718843532624,
        "maxTime": 1718845677624,
        "totalSamples": 10,
        "missedSamples": 38,
        "minIntervalDiffMillis": 15000,
        "maxIntervalDiffMillis": 870000,
        "mostCommonIntervalSeconds": 15,
        "gapThreshold": 22500,
        "gaps": [
          {
            "start": 1718843592624,
            "end": 1718844462624,
            "intervals": 38
          }
        ]
      }
    ]
  }
}
```

The metric selector limits by a regex on a label value, and restricts the total time range to a 30 minute window.
One series with one gap was found, with the gap representing 38 missed samples. This assumes a 15 minute scrape interval. The
scrape interval can be configured with `-scrape-interval`.
