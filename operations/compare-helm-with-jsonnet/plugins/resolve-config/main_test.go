package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

func TestResolveConfigs(t *testing.T) {

	t.Run("ResolveConfigs annotates values that match defaults", func(t *testing.T) {
		querierDep, err := yaml.Parse(`
apiVersion: v1
kind: Deployment
metadata:
  name: querier
  namespace: default
spec:
  template:
    spec:
      containers:
        - name: mimir
          image: grafana/mimir:latest
          args:
          - -target=querier
`)
		require.NoError(t, err)

		mapper := NewConfigExtractor([]*yaml.RNode{querierDep})
		configs, err := mapper.ResolveConfigs()
		require.Len(t, configs, 1)
		require.NoError(t, err)

		require.Equal(t, "querier", configs[0].GetName(), "The MimirConfig metadata.name is set to the configured target")

		target, err := configs[0].GetFieldValue("config.target")
		require.NoError(t, err)
		require.Equal(t, "querier", target, "The MimirConfig should have its target set to the configured target")

		bindPort, err := configs[0].GetFieldValue("config.memberlist.bind_port")
		require.NoError(t, err)
		require.Equal(t, "7946 (default)", bindPort, "The MimirConfig should have its memberlist bind port set to the default value, with the text 'default'")
	})

	t.Run("ResolveConfigs sets configuration parameters from cli arguments", func(t *testing.T) {
		querierDep, err := yaml.Parse(`
apiVersion: v1
kind: Deployment
metadata:
  name: querier
  namespace: default
spec:
  template:
    spec:
      containers:
        - name: mimir
          image: grafana/mimir:latest
          args:
          - -target=querier
          - -memberlist.bind-port=1234
`)
		require.NoError(t, err)

		mapper := NewConfigExtractor([]*yaml.RNode{querierDep})
		configs, err := mapper.ResolveConfigs()
		require.Len(t, configs, 1)
		require.NoError(t, err)

		require.Equal(t, "querier", configs[0].GetName(), "The MimirConfig metadata.name is set to the configured target")

		target, err := configs[0].GetFieldValue("config.target")
		require.NoError(t, err)
		require.Equal(t, "querier", target, "The MimirConfig should have its target set to the configured target")

		bindPort, err := configs[0].GetFieldValue("config.memberlist.bind_port")
		require.NoError(t, err)
		require.Equal(t, 1234, bindPort, "The MimirConfig should have its memberlist bind port set to the configured value")
	})

	t.Run("ResolveConfigs sets configuration parameters from mounted ConfigMaps", func(t *testing.T) {
		querierDep, err := yaml.Parse(`
apiVersion: v1
kind: Deployment
metadata:
  name: querier
  namespace: default
spec:
  template:
    spec:
      containers:
        - name: mimir
          image: grafana/mimir:latest
          args:
          - -target=querier
          - -config.file=/etc/mimir/mimir.yaml
          volumeMounts:
          - name: mimir-config
            mountPath: /etc/mimir
      volumes:
        - name: mimir-config
          configMap:
            name: config
`)
		require.NoError(t, err)
		configMap, err := yaml.Parse(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: config
  namespace: default
data:
  mimir.yaml: |
    memberlist:
      bind_port: 1234
`)
		require.NoError(t, err)

		mapper := NewConfigExtractor([]*yaml.RNode{querierDep, configMap})
		configs, err := mapper.ResolveConfigs()
		require.Len(t, configs, 1)
		require.NoError(t, err)

		require.Equal(t, "querier", configs[0].GetName(), "The MimirConfig metadata.name is set to the configured target")

		target, err := configs[0].GetFieldValue("config.target")
		require.NoError(t, err)
		require.Equal(t, "querier", target, "The MimirConfig should have its target set to the configured target")

		bindPort, err := configs[0].GetFieldValue("config.memberlist.bind_port")
		require.NoError(t, err)
		require.Equal(t, 1234, bindPort, "The MimirConfig should have its memberlist bind port set to the configured value")
	})

	t.Run("ResolveConfigs sets configuration parameters from mounted Secrets", func(t *testing.T) {
		querierDep, err := yaml.Parse(`
apiVersion: v1
kind: Deployment
metadata:
  name: querier
  namespace: default
spec:
  template:
    spec:
      containers:
        - name: mimir
          image: grafana/mimir:latest
          args:
          - -target=querier
          - -config.file=/etc/mimir/mimir.yaml
          volumeMounts:
          - name: mimir-config
            mountPath: /etc/mimir
      volumes:
        - name: mimir-config
          secret:
            secretName: config
`)
		require.NoError(t, err)
		configMap, err := yaml.Parse(`
apiVersion: v1
kind: Secret
metadata:
  name: config
  namespace: default
data:
  mimir.yaml: bWVtYmVybGlzdDoKICBiaW5kX3BvcnQ6IDEyMzQK
`)
		require.NoError(t, err)

		mapper := NewConfigExtractor([]*yaml.RNode{querierDep, configMap})
		configs, err := mapper.ResolveConfigs()
		require.Len(t, configs, 1)
		require.NoError(t, err)

		require.Equal(t, "querier", configs[0].GetName(), "The MimirConfig metadata.name is set to the configured target")

		target, err := configs[0].GetFieldValue("config.target")
		require.NoError(t, err)
		require.Equal(t, "querier", target, "The MimirConfig should have its target set to the configured target")

		bindPort, err := configs[0].GetFieldValue("config.memberlist.bind_port")
		require.NoError(t, err)
		require.Equal(t, 1234, bindPort, "The MimirConfig should have its memberlist bind port set to the configured value")
	})
}
