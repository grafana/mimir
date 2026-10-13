# Ingester query-tee

Experimental gRPC proxy for comparing primary and shadow ingesters that own the same Kafka partition.
Callers always receive the primary response; the shadow is queried only for asynchronous comparison.

## Recommended deployment: sidecar on the primary

Deploy one tee sidecar in each primary ingester pod in the zone being validated.
Keep the primary ingester's ring identity, availability zone, and partition ownership.
Configure the ingester to listen on `9095` and advertise the tee's port:

```text
-server.grpc-listen-port=9095
-ingester.ring.instance-port=9096
```

Configure the tee sidecar for that pod's matching shadow partition:

```text
-primary-address=127.0.0.1:9095
-shadow-address=ingester-shadow-0.ingester-shadow:9095
-server.grpc-listen-port=9096
-server.http-listen-port=9900
-skip-recent-samples=30s
```

Queriers reach the primary pod at its advertised port `9096`; the tee forwards to the primary over localhost.
Repeat the pairing for every pod in the zone, using the correct shadow pod for each partition.
Use a specific shadow pod address, because a zone-wide load-balanced Service can select the wrong partition owner.
Keep the shadow zone isolated from normal query selection during validation.

Expose `9096` in the relevant Services and NetworkPolicies and `9900` for metrics and readiness.
Retain the primary ingester's existing readiness probe; the tee's `/ready` checks startup only.
Configure incoming and backend TLS when the deployment uses TLS or mTLS.
Kafka consumption continues directly in the ingester.

If a spot eviction removes the primary pod, both the ingester and tee disappear.
Mimir's other serving zones provide availability; the tee never falls back to the shadow, even when the primary fails.
Unavailable backends and timeouts produce `result="unavailable"` metrics without outage logs.

After validation, promote the replacement zone through the normal ring and rollout process.
The tee does not register ring entries, discover partitions, or perform the cutover.

## Build

```bash
go build -o ingester-query-tee ./cmd/ingester-query-tee
```

See the [full documentation](../../docs/sources/mimir/manage/tools/ingester-query-tee.md) for comparison semantics, metrics, load limits, and TLS options.
