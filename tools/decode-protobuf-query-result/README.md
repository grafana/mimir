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

For example

```sh
$ curl -G -H "Accept: application/vnd.mimir.queryresponse+protobuf" -H "X-Scope-OrgId: 1" http://localhost:8080/prometheus/api/v1/query --data-urlencode 'query=foo{bar="baz"}[6m]' --data-urlencode 'time=1759932075' -o out.proto
 % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  1315  100  1315    0     0  17870      0 --:--:-- --:--:-- --:--:-- 18013

$ go run . out.proto
Decoding out.proto as matrix
Stream 0, labels foo{bar="baz"}:
  Timestamp: 1759931725000, Histogram: &FloatHistogram{Count:0,Sum:0,Schema:0,ZeroThreshold:0,ZeroCount:0,NegativeSpans:[]BucketSpan{},NegativeBuckets:[],PositiveSpans:[]BucketSpan{},PositiveBuckets:[],CounterResetHint:0,CustomValues:[],}
  Timestamp: 1759931740000, Histogram: &FloatHistogram{Count:2,Sum:2.2885222e+07,Schema:3,ZeroThreshold:1e-128,ZeroCount:0,NegativeSpans:[]BucketSpan{},NegativeBuckets:[],PositiveSpans:[]BucketSpan{BucketSpan{Offset:187,Length:3,},},PositiveBuckets:[1 1 0],CounterResetHint:0,CustomValues:[],}
  Timestamp: 1759931935000, Histogram: &FloatHistogram{Count:0,Sum:0,Schema:0,ZeroThreshold:0,ZeroCount:0,NegativeSpans:[]BucketSpan{},NegativeBuckets:[],PositiveSpans:[]BucketSpan{},PositiveBuckets:[],CounterResetHint:0,CustomValues:[],}
  Timestamp: 1759931950000, Histogram: &FloatHistogram{Count:2,Sum:2.4272461e+07,Schema:3,ZeroThreshold:1e-128,ZeroCount:0,NegativeSpans:[]BucketSpan{},NegativeBuckets:[],PositiveSpans:[]BucketSpan{BucketSpan{Offset:186,Length:1,},BucketSpan{Offset:3,Length:2,},},PositiveBuckets:[1 1 0],CounterResetHint:0,CustomValues:[],}
```

Run this command with the filename as the argument and enjoy.
