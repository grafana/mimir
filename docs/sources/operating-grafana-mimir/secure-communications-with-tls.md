---
title: "Secure communications with TLS"
description: "How to configure TLS between Grafana Mimir microservices."
weight: 60
---

# Secure communications with TLS

Grafana Mimir is a distributed system with significant traffic between its services.
To allow for secure communication, Grafana Mimir supports TLS between all its
components. This guide describes the process of setting up TLS.

### Generation of certs to configure TLS

The first step to securing inter-service communication in Grafana Mimir with TLS is
generating certificates. A Certificate Authority (CA) will be used for this
purpose which should be private to the organization, as any certificates signed
by this CA will have permissions to communicate with the cluster.

We will use the following script to generate self signed certs for the cluster:

```
# keys
openssl genrsa -out root.key
openssl genrsa -out client.key
openssl genrsa -out server.key

# root cert / certifying authority
openssl req -x509 -new -nodes -key root.key -subj "/C=US/ST=KY/O=Org/CN=root" -sha256 -days 100000 -out root.crt

# csrs - certificate signing requests
openssl req -new -sha256 -key client.key -subj "/C=US/ST=KY/O=Org/CN=client" -out client.csr
openssl req -new -sha256 -key server.key -subj "/C=US/ST=KY/O=Org/CN=localhost" -out server.csr

# certificates
openssl x509 -req -in client.csr -CA root.crt -CAkey root.key -CAcreateserial -out client.crt -days 100000 -sha256
openssl x509 -req -in server.csr -CA root.crt -CAkey root.key -CAcreateserial -out server.crt -days 100000 -sha256
```

Note that the above script generates certificates that are valid for 100000 days.
This can be changed by adjusting the `-days` option in the above commands.
It is recommended that the certs be replaced atleast once every 2 years.

The above script generates keys `client.key, server.key` and certs
`client.crt, server.crt` for both the client and server. The CA cert is
generated as `root.crt`.

### Load certs into the HTTP/GRPC server/client

Every HTTP/GRPC link between Grafana Mimir components supports TLS configuration
through the following config parameters:

#### Server flags

```
    # Path to the TLS Cert for the HTTP Server
    -server.http-tls-cert-path=/path/to/server.crt

    # Path to the TLS Key for the HTTP Server
    -server.http-tls-key-path=/path/to/server.key

    # Type of Client Auth for the HTTP Server
    -server.http-tls-client-auth="RequireAndVerifyClientCert"

    # Path to the Client CA Cert for the HTTP Server
    -server.http-tls-ca-path="/path/to/root.crt"

    # Path to the TLS Cert for the GRPC Server
    -server.grpc-tls-cert-path=/path/to/server.crt

    # Path to the TLS Key for the GRPC Server
    -server.grpc-tls-key-path=/path/to/server.key

    # Type of Client Auth for the GRPC Server
    -server.grpc-tls-client-auth="RequireAndVerifyClientCert"

    # Path to the Client CA Cert for the GRPC Server
    -server.grpc-tls-ca-path=/path/to/root.crt
```
Both of the client authorization flags, `-server.http-tls-client-auth` and `-server.grpc-tls-client-auth`, are shown with the strictest option, "RequiredAndVerifyClientCert". However the flags support other values as well:
- NoClientCert
- RequestClientCert
- RequireClientCert
- VerifyClientCertIfGiven
- RequireAndVerifyClientCert

#### Client flags

Client flags are component specific.

For a GRPC client in the Querier:

```
    # Path to the TLS Cert for the GRPC Client
    -querier.frontend-client.tls-cert-path=/path/to/client.crt

    # Path to the TLS Key for the GRPC Client
    -querier.frontend-client.tls-key-path=/path/to/client.key

    # Path to the TLS CA for the GRPC Client
    -querier.frontend-client.tls-ca-path=/path/to/root.crt
```

Similarly, for the GRPC Ingester Client:

```
    # Path to the TLS Cert for the GRPC Client
    -ingester.client.tls-cert-path=/path/to/client.crt

    # Path to the TLS Key for the GRPC Client
    -ingester.client.tls-key-path=/path/to/client.key

    # Path to the TLS CA for the GRPC Client
    -ingester.client.tls-ca-path=/path/to/root.crt
```

TLS keys, certificates, and certificate authorities can be configured in a similar fashion for other HTTP/GRPC clients in Grafana Mimir.

To enable TLS for a given component, use the client flag that is suffixed with `*.tls-enabled=true`, (e.g. `-querier.fontend-client.tls-enabled=true`). The following Grafana Mimir components support TLS for inter-communication, shown with their corresponding configuration flag prefixes:
- query scheduler GRPC client (`-query-scheduler.grpc-client-config.*`)
- store-gateway client (`-querier.store-gateway-client.*`)
- query frontend GRPC client (`-query-frontend.grpc-client-config.*`)
- ruler client (`-ruler.client.*`)
- alertmanager (`-alertmanager.alertmanager-client.*`)
- ingester client (`-ingester.client.*`)
- querier frontend client (`-querier.frontend-client.*`)
- etcd client (`-<prefix>.etcd.*`)
- memberlist client (`-memberlist.`)

Each of these components supports the following TLS configuration options, shown with their corresponding flag suffixes:

- `*.tls-enabled=<boolean>`: Enable TLS in the client.
- `*.tls-server-name=<string>`: Override the expected name on the server certificate.
- `*.tls-insecure-skip-verify=<boolean>`: Skip validating server certificate.

