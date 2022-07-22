// SPDX-License-Identifier: AGPL-3.0-only

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

	type testCase struct {
		name    string
		objects []string
	}

	var testCases = []testCase{
		{
			name: "ResolveConfigs sets configuration parameters from cli arguments",
			objects: []string{`
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
`},
		},
		{
			name: "ResolveConfigs sets configuration parameters from mounted ConfigMaps",
			objects: []string{`
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
`,
				`
apiVersion: v1
kind: ConfigMap
metadata:
  name: config
  namespace: default
data:
  mimir.yaml: |
    memberlist:
      bind_port: 1234
`,
			},
		},
		{
			name: "ResolveConfigs sets configuration parameters from mounted Secrets",
			objects: []string{
				`
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
`,
				`
apiVersion: v1
kind: Secret
metadata:
  name: config
  namespace: default
data:
  mimir.yaml: bWVtYmVybGlzdDoKICBiaW5kX3BvcnQ6IDEyMzQK
`,
			}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var objects []*yaml.RNode
			for _, obj := range tc.objects {
				rnode, err := yaml.Parse(obj)
				require.NoError(t, err)
				objects = append(objects, rnode)
			}

			mapper := NewConfigExtractor(objects)
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
}
