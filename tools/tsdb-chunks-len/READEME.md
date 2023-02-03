this tool helps to understand how bug chunks are; in particular how big the last chunk of a series is
compared to the rest of the chunks in a series. It is useful because it's difficult to know the length of the last
chunk of a series, so the best we can do is estiamte it; this tool helps with this estimation;

example usage

```bash
go run tools/tsdb-chunks-len/main.go dev-us-central1-cortex-tsdb-dev/9960/01GPYS8C08C4D75VP3WW3A4QXK/index
```

example output
```
263 0.900685
261 0.912587
206 0.768657
next segment file
195 0.819328
212 0.861789
206 0.757353
next segment file
one series with chunks in multiple segment files
210 0.709459
210 0.709459
210 0.709459
one chunk
5m23.0650165s
```

explained

* `263 0.900685` - the last chunk of a timeseries was 263; The last chunk was 90% of the size of the largest chunk from the rest 
* `next segment file` - this is logged once for every segment file after the first one
* `one series with chunks in multiple segment files` - self-explanatory; this should be rare

one chunk

```
