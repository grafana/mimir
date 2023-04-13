---
title: "Configure Grafana Mimir to receive secrets via Hashicorp Vault Agent"
menuTitle: "Vault Agent"
description: "Learn how to configure Grafana Mimir to allow Vault Agent to inject secrets into pods"
---

# Configure Grafana Mimir to receive secrets via Hashicorp Vault Agent

When enabled, the pod annotations for TLS configurable pods will be updated. Upon deployment, the Vault Agent will intercept the deployment, fetch the relevant secrets from the Vault and mount them to the pod. Note: Vault and Vault Agent are required to be running already.

Example values file:

```yaml
vaultAgent:
  enabled: true
  roleName: "test-role"
  clientCertPath: "client/cert/path"
  clientKeyPath: "client/key/path"
  serverCertPath: "server/cert/path"
  serverKeyPath: "server/key/path"
  caCertPath: "ca/cert/path"
```

Example deployment yaml:

```yaml
spec:
  template:
    metadata:
      annotations:
        vault.hashicorp.com/agent-inject: "true"
        vault.hashicorp.com/role: "test-role"
        vault.hashicorp.com/agent-inject-secret-client.crt: "client/cert/path"
        vault.hashicorp.com/agent-inject-secret-client.key: "client/key/path"
        vault.hashicorp.com/agent-inject-secret-server.crt: "server/cert/path"
        vault.hashicorp.com/agent-inject-secret-server.key: "server/key/path"
        vault.hashicorp.com/agent-inject-secret-root.crt: "ca/cert/path"
```

`vault.hashicorp.com/agent-inject-secret-<FILENAME>: '<PATH>'` tells the Vault Agent where to find the secret, and the name of the file to write the secret to. For example: `vault.hashicorp.com/agent-inject-secret-client.crt: 'client/cert/path'` will look for the secret at the path `client/cert/path` within Vault, and mount this secret to the pod as `client.crt` in the `/vault/secrets/` directory.

For more information about Vault and Vault Agent, see: https://www.hashicorp.com/blog/injecting-vault-secrets-into-kubernetes-pods-via-a-sidecar

To configure TLS in Mimir, see: https://grafana.com/docs/mimir/latest/operators-guide/secure/securing-communications-with-tls/
