this tool helps to understand how bug chunks are; in particular how big the last chunk of a series is
compared to the rest of the chunks in a series. It is useful because it's difficult to know the length of the last
chunk of a series, so the best we can do is estiamte it; this tool helps with this estimation;

### Example usage

```bash
go run tools/tsdb-chunks-len/main.go dev-us-central1-cortex-tsdb-dev/9960/01GPYS8C08C4D75VP3WW3A4QXK/index
```

Output

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
one chunk 279
```

Explained

* `263 0.900685` - the last chunk of a timeseries was 263 bytes; The last chunk was 90% of the size of the largest chunk from the rest 
* `next segment file` - this is logged once for every segment file after the first one
* `one chunk 279` - a series has only one chunk and its size is 279 bytes
* `one series with chunks in multiple segment files` - self-explanatory; this should be rare

### Helper commands

Run it for a list of blocks in a bucket

```bash
x=( '01F4X9CCYRQ2M9EFDNN72S6EPG' '01FZ9N452CCW3YZK2HKMFNJERD' '01FZ9NZBR4204BQFV9SQ835G11' )
for b in "${x[@]}"; do 
  go run tools/tsdb-chunks-len/main.go dev-us-central1-cortex-tsdb-dev/9960/$b/index > cortex-dev-01-$b.raw;
done
```

Some useful plots and analytics 
```bash
for f in $(find . -name '*.raw'); do 
  echo $f
  
  # Get only the numbers from the output files
  cat $f | grep -E '[0-9]+ [0-9]+\.[0-9]+' > $f.txt
  
  # Plot the last chunk size in a histogram
  cat $f.txt | awk '{print $1}' | promfreq -mode exponential -count 13 -factor 2
  
  # Generate png files with plots of the chunk size of the largest chunk (excl last) and the ratio of last chunk to max chunk
  cat $f.txt | awk '{if ($2 > 0) { print $1/$2, $2 }}' > $f.max-chunk-vs-last.txt
  gnuplot -e 'set term png size 2560, 1440; set xlabel "max chunk size (excl last)"; set ylabel "ratio of last chunk to max chunk size (excl last)"; set xrange [:4096]; set logscale x; set xtics 1,2,4096; set output "'$f.max-chunk-vs-last.txt.png'"; plot "'$f.max-chunk-vs-last.txt'", 1, 0.1, 0.2, 0.25, 0.5;'
  
  # Generate png files with plots of the chunk size of the largest chunk (excl last) and last chunk of a series
  cat $f.txt | awk '{if ($2 > 0) { print $1/$2, $1 }}' > $f.max-chunk-vs-last-sizes.txt
  gnuplot -e 'set term png size 2560, 1440; set xlabel "max chunk size (excl last)"; set ylabel "last chunk"; set xrange [:4096]; set logscale x; set xtics 1,2,4096; set yrange [:4096]; set logscale y; set ytics 1,2,4096; set output "'$f.max-chunk-vs-last-sizes.txt.png'"; plot "'$f.max-chunk-vs-last-sizes.txt'", 1, 0.1, 0.2, 0.25, 0.5;'
done
```
