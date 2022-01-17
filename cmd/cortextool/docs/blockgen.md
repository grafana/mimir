# Blockgen

The `blockgen` is a tool which generates Prometheus blocks of mock data. These
blocks can be uploaded to an object store used by Cortex, which may be useful
to profile and benchmark Cortex components when querying or compacting the 
generated blocks.

## Config file

The blockgen command requires a config file to be specified. 
It uses the same syntax to define what metrics should be generated like the
`benchtool` which is documented [here](benchtool.md). 
 
This is an example config file:

```yaml
replicas: 3
block_gen:
  interval: 15s
  block_size: 2h
  block_dir: /tmp/mock_blocks
  max_t: 1623422958000
  min_t: 1623336558000
series:
  - labels:
      - name: label_01
        unique_values: 100
        value_prefix: label_value_01
      - name: label_02
        unique_values: 10
        value_prefix: label_value_02
    name: metric_gauge_random_01
    static_labels:
        static: "true"
    type: gauge-random
  - labels:
      - name: label_01
        unique_values: 100
        value_prefix: label_value_01
      - name: label_02
        unique_values: 10
        value_prefix: label_value_02
    name: metric_gauge_zero_01
    static_labels:
        static: "true"
    type: gauge-zero
```


## Running it

Assuming the config is in a file named `blockgen.conf` the command could 
be run like this:

```
$ ./blockgen  --config.file=./blockgen.conf
level=info msg="Generating data" minT=1623336558000 maxT=1623422958000 interval=15000
level=info msg="starting new block" block_id=225463 blocks_left=13
level=info msg="starting new block" block_id=225464 blocks_left=12
level=info msg="starting new block" block_id=225465 blocks_left=11
level=info msg="starting new block" block_id=225466 blocks_left=10
level=info msg="starting new block" block_id=225467 blocks_left=9
level=info msg="starting new block" block_id=225468 blocks_left=8
level=info msg="starting new block" block_id=225469 blocks_left=7
level=info msg="starting new block" block_id=225470 blocks_left=6
level=info msg="starting new block" block_id=225471 blocks_left=5
level=info msg="starting new block" block_id=225472 blocks_left=4
level=info msg="starting new block" block_id=225473 blocks_left=3
level=info msg="starting new block" block_id=225474 blocks_left=2
level=info msg="starting new block" block_id=225475 blocks_left=1
level=info msg=finished block_dir=/tmp/mock_blocks
$
```