# Naming conventions

Compartments and their per-compartment resources are named with a `wc-<id>` / `rc-<id>` suffix, where
`<id>` is the zero-based compartment index.

| Resource                             | Convention            | Example            |
| ------------------------------------ | --------------------- | ------------------ |
| Write compartment                    | `wc-<id>`             | `wc-0`, `wc-1`     |
| Read compartment                     | `rc-<id>`             | `rc-0`, `rc-1`     |
| Distributors of a write compartment  | `distributor-wc-<id>` | `distributor-wc-0` |
| Kafka cluster of a write compartment | `kafka-wc-<id>`       | `kafka-wc-0`       |
| Ingest topic of a read compartment   | `ingest-rc-<id>`      | `ingest-rc-0`      |
