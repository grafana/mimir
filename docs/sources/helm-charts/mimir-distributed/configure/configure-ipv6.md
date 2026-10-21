---
title: "Configure Grafana Mimir on an IPv6 only cluster"
menuTitle: "IPv6 only clusters"
description: "Learn how to run the mimir-distributed Helm chart on an IPv6 only Kubernetes cluster."
weight: 40
---

# Configure Grafana Mimir on an IPv6 only cluster

On an IPv6 only Kubernetes cluster, every Pod and Service address is IPv6. Grafana Mimir does not
detect this by itself: each component has to be told to advertise an IPv6 address in its hash ring,
and the memberlist transport and the HTTP and gRPC listeners have to bind to the IPv6 wildcard
address.

The chart ships an [`ipv6-only`](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/ipv6-only.yaml)
preset that does all of that. Install it as an extra values file, before your own values:

```bash
helm install mimir grafana/mimir-distributed \
  -f https://raw.githubusercontent.com/grafana/mimir/main/operations/helm/charts/mimir-distributed/ipv6-only.yaml \
  -f my-values.yaml
```

## What the preset sets

- `instance_enable_ipv6: true` on the ring of every component that has one, so each instance
  advertises its IPv6 address instead of failing to find a usable IPv4 one. Note that the
  query-frontend ring is configured under `frontend`, not under `query_frontend`.
- `memberlist.bind_addr: ["::"]`, so the gossip ring binds to all IPv6 interfaces.
- `server.http_listen_address` and `server.grpc_listen_address` set to `::`.
- `KAFKA_LISTENERS` on the bundled Kafka set to `PLAINTEXT://[::]:9092,CONTROLLER://[::]:9093`.
  Skip this part of the preset if you run your own Kafka.

The nginx gateway already listens on both address families, so it needs no change.

## Why the addresses have no brackets

The Mimir addresses are written as `::` and not as `[::]`. Mimir passes the server listen addresses
through `net.JoinHostPort`, which adds the brackets itself, and the memberlist bind address through
`net.ParseIP`, which rejects a bracketed value.

Using `[::]` produces an address such as `[[::]]:8080`, and Mimir fails to start with a misleading
error:

```
listen tcp: address [[::]]:8080: missing port in address
```

The bundled Kafka is the exception, because Kafka parses each listener as a URI, and in a URI the
host part of an IPv6 address is bracketed.

## Try it locally

[kind](https://kind.sigs.k8s.io/) can create an IPv6 only cluster:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: mimir-ipv6
networking:
  ipFamily: ipv6
  apiServerAddress: "127.0.0.1"
nodes:
  - role: control-plane
```

`apiServerAddress` is IPv4 so that `kubectl` can reach the API server from the host. Everything
inside the cluster is IPv6 only. Note that pulling images from an IPv6 only node requires NAT64 and
DNS64 on the container runtime, which Docker Desktop provides.

After installing the chart with the preset, the Pod and Service addresses are all IPv6:

```bash
kubectl -n mimir get pod -o wide
kubectl -n mimir get svc
```

The components log the listen addresses at startup, bracketed by `net.JoinHostPort`:

```
level=info msg="server listening on addresses" http=[::]:8080 grpc=[::]:9095
```

The memberlist status page at `/memberlist` on any component lists the members of the gossip ring,
which should all be IPv6 addresses.
