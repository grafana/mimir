# grpc-tee in the microservices compose setup

This compose setup shows the store-gateway tee.
The querier sends the calls for the zone-a store-gateways through grpc-tee.
grpc-tee sends each call for `store-gateway-1` to the mirror of `store-gateway-1`, `store-gateway-5`.
Then grpc-tee compares the two responses.

## Start and stop

Run these commands in `development/mimir-microservices-mode`.

1. Make sure that `enable_grpc_tee` is `true` in `docker-compose.jsonnet`.
2. Run `./compose-up.sh -d`.
3. To stop the containers, run `./compose-down.sh`.

`compose-up.sh` makes `docker-compose.yml` from `docker-compose.jsonnet` again before it starts the containers.

## Test the setup

Do these steps after `./compose-up.sh -d`.
Run the commands in `development/mimir-microservices-mode`.

### Step 1: Wait for the blocks

The ingesters cut a block every 2 minutes and ship it within 30 seconds.
The querier sends only the queries for data older than 10 minutes to the store-gateways
(`query_store_after` in `config/mimir.yaml`).

1. Wait about 15 minutes after the start.
2. Open http://localhost:8011/store-gateway/tenants.
3. Make sure that the tenant `anonymous` has blocks.

Multitenancy is disabled in this setup, so all of the data is in the tenant `anonymous`.

### Step 2: Compare the block ownership

| Store-gateway | Blocks page |
|---|---|
| `store-gateway-1` | http://localhost:8011/store-gateway/tenant/anonymous/blocks?loaded_only=on |
| `store-gateway-5` (the mirror) | http://localhost:8015/store-gateway/tenant/anonymous/blocks?loaded_only=on |

1. Open the two pages in two tabs.
2. Make sure that the line "This store-gateway loaded N blocks of this tenant." has the same N on both pages.
3. Make sure that the two block lists are the same.

If you remove `?loaded_only=on`, each page shows all of the blocks in the bucket for the tenant.

Each store-gateway syncs its blocks every 30 seconds, at a different time.
So a new block can show on one page before it shows on the other page.
If the two lists are different, reload both pages after 30 seconds.

### Step 3: Look at the rings

The ring pages show the tokens and an ownership percentage for each instance.
They do not show blocks.

| Page | Ring that it shows | What to look for |
|---|---|---|
| http://localhost:8011/store-gateway/ring | the store-gateway ring | `store-gateway-1` to `store-gateway-4`, two in zone-a and two in zone-b |
| http://localhost:8015/store-gateway/ring | the mirror ring | one entry: ID `store-gateway-1` at `store-gateway-5:9015` |
| http://localhost:8095/ring | the grpc-tee ring | `grpc-tee-1` and `grpc-tee-2`, both in zone-a |
| http://localhost:8095/backend/store-gateway-mirror/ring | the mirror ring, as grpc-tee reads it | the same entry as the page on port 8015 |

The page on port 8015 shows the ring that the mirror joined.
The mirror does not use that ring to pick its blocks.
It picks its blocks from the store-gateway ring, the ring on the page on port 8011.

### Step 4: Send queries

Send a range query for the last hour to the query-frontend:

```bash
curl -s -G http://localhost:8007/prometheus/api/v1/query_range \
  -H 'Cache-Control: no-store' \
  --data-urlencode 'query=up' \
  --data-urlencode "start=$(( $(date +%s) - 3600 ))" \
  --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'step=60'
```

1. Run the command 5 times or more.
   The querier picks the store-gateways at random, so each run can use different paths.
2. Make sure that the response has `"status":"success"`.

Do not remove the `Cache-Control: no-store` header.
Without it, the query-frontend can answer from the results cache, and the store-gateways get no calls.

### Step 5: Read the grpc-tee logs

```bash
docker compose logs grpc-tee-1 grpc-tee-2 | grep 'proxied call'
```

1. Make sure that the lines with `target_id=store-gateway-1` have `result=match`.
2. Make sure that the lines with `target_id=store-gateway-2` have no `result` field.

A compared call has a log line with these fields, and more:

```
msg="proxied call" target_id=store-gateway-1 primary_backend=store-gateway primary_addr=store-gateway-1:9011 secondary_backend=store-gateway-mirror secondary_addr=store-gateway-5:9015 result=match
```

The `*_backend` fields have the name of the configured backend.
The metrics use the same names, so the label has a small set of values.
The `*_addr` fields have the address of the instance that received the call.

If a line has `result=mismatch`, the line also has a `reason` field.
grpc-tee writes these lines at the warn level.

### Step 6: Read the metrics

```bash
curl -s http://localhost:8095/metrics | grep cortex_grpctee_responses_compared_total
curl -s http://localhost:8096/metrics | grep cortex_grpctee_responses_compared_total
curl -s http://localhost:8005/metrics | grep cortex_querier_store_gateway_proxy_direct_calls_total
```

1. Make sure that `cortex_grpctee_responses_compared_total` has values with `result="match"`.
2. Make sure that `cortex_grpctee_responses_compared_total` has no values with `result="mismatch"`.
3. Make sure that `cortex_querier_store_gateway_proxy_direct_calls_total` is 0.

Prometheus also scrapes these metrics, so you can see them in Grafana at http://localhost:3000.

## The containers

| Container | Zone | Ring | Role |
|---|---|---|---|
| `querier` | none | reads the store-gateway ring and the grpc-tee ring | It picks the store-gateways and sends the zone-a calls through a grpc-tee. |
| `store-gateway-1`, `store-gateway-2` | zone-a | joins the store-gateway ring | The normal zone-a store-gateways. |
| `store-gateway-3`, `store-gateway-4` | zone-b | joins the store-gateway ring | The normal zone-b store-gateways. |
| `store-gateway-5` | zone-a | joins the mirror ring with ID `store-gateway-1` | The mirror of `store-gateway-1`. |
| `grpc-tee-1`, `grpc-tee-2` | zone-a | joins the grpc-tee ring | They forward the zone-a calls and compare the responses. |

## The rings

| Ring | KV prefix and key | Members | Readers |
|---|---|---|---|
| store-gateway ring | `collectors/store-gateway` | `store-gateway-1` to `store-gateway-4` | the querier, `store-gateway-5`, and grpc-tee (backend `store-gateway`) |
| mirror ring | `secondary/store-gateway` | `store-gateway-5`, with ID `store-gateway-1` | grpc-tee (backend `store-gateway-mirror`) |
| grpc-tee ring | `collectors/grpc-tee` | `grpc-tee-1` and `grpc-tee-2`, in zone-a | the querier |

The store-gateway ring has RF 2 and zone awareness (`config/mimir.yaml`).
So each block has one owner in zone-a and one owner in zone-b.

The querier does not read the mirror ring,
so it never sends calls to `store-gateway-5`.

## What one query does

1. The querier reads the store-gateway ring.
   Each block has two owners: one in zone-a, one in zone-b.
2. The querier picks one owner at random for each block.
   The compose setup sets no preferred zone.
3. If the owner is in zone-b, the querier sends the call direct.
4. If the owner is in zone-a, the querier sends the call to `grpc-tee-1` or `grpc-tee-2` at random.
   The querier flag `-querier.store-gateway-proxy.zones=zone-a` selects this zone.
   The call has two gRPC metadata keys:
   - `x-grpctee-target-addr` has the store-gateway address, for example `store-gateway-1:9011`.
   - `x-grpctee-target-id` has the instance ID, for example `store-gateway-1`.
5. grpc-tee removes the two keys from the metadata.
   Then it sends the call to the target address.
6. grpc-tee looks up the target ID in the mirror ring:
   - If the target is `store-gateway-1`, the mirror ring has that ID at `store-gateway-5:9015`.
     grpc-tee sends the same call there and compares the two responses.
   - If the target is `store-gateway-2`, the mirror ring does not have that ID.
     grpc-tee sends no second call and does no compare.

If no grpc-tee in zone-a is healthy, the querier sends the call direct.
Then `cortex_querier_store_gateway_proxy_direct_calls_total` increases.

## How the mirror loads the same blocks

`store-gateway-5` has these flags:

```
-store-gateway.sharding-ring.prefix=secondary/
-store-gateway.sharding-ring.instance-id=store-gateway-1
-store-gateway.sharding-ring.instance-availability-zone=zone-a
-store-gateway.mirror.enabled=true
-store-gateway.mirror.ring-kvstore-prefix=collectors/
```

- It joins the mirror ring with the ID `store-gateway-1`.
  This entry lets grpc-tee find the mirror by ID.
- It reads the store-gateway ring live, with the ID `store-gateway-1`.
  So it owns and loads the same blocks as `store-gateway-1`.
- It copies no tokens.
  The ownership of the blocks comes only from the store-gateway ring.

The querier puts the block IDs in the request.
Both store-gateways get the same request bytes,
so they read the same blocks and the responses match.

## How the calls divide

| Path | Approximate share of calls | Compared |
|---|---|---|
| zone-b, direct | 50% | no |
| zone-a, `store-gateway-2` through a grpc-tee | 25% | no |
| zone-a, `store-gateway-1` through a grpc-tee, teed to `store-gateway-5` | 25% | yes |

The querier sends one call to each store-gateway for each query,
so the real split changes from query to query.

## Limits

- Only `store-gateway-1` has a mirror.
  The calls to `store-gateway-2` go through grpc-tee, but grpc-tee does not compare them.
- grpc-tee does not close the connections to the addresses that it does not use anymore.
- The flags `-backend.primary.address` and `-backend.secondary.address` set fixed addresses.
  grpc-tee uses them only for the calls without a target.
  The querier always sends a target, so these addresses get no querier calls.
