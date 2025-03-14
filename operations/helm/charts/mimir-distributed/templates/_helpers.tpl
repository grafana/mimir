{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "mimir.name" -}}
{{- default ( include "mimir.infixName" . ) .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "mimir.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default ( include "mimir.infixName" . ) .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Calculate the infix for naming
*/}}
{{- define "mimir.infixName" -}}
{{- if and .Values.enterprise.enabled .Values.enterprise.legacyLabels -}}enterprise-metrics{{- else -}}mimir{{- end -}}
{{- end -}}

{{/*
Calculate the gateway url
*/}}
{{- define "mimir.gatewayUrl" -}}
{{- if eq (include "mimir.gateway.isEnabled" . ) "true" -}}
http://{{ include "mimir.gateway.service.name" . }}.{{ .Release.Namespace }}.svc:{{ .Values.gateway.service.port | default (include "mimir.serverHttpListenPort" . ) }}
{{- else -}}
http://{{ template "mimir.fullname" . }}-nginx.{{ .Release.Namespace }}.svc:{{ .Values.nginx.service.port }}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "mimir.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Build mimir image reference based on whether enterprise features are requested. The component local values always take precedence.
Params:
  ctx = . context
  component = component name
*/}}
{{- define "mimir.imageReference" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $image := $componentSection.image | default dict -}}
{{- if .ctx.Values.enterprise.enabled -}}
  {{- $image = mustMerge $image .ctx.Values.enterprise.image -}}
{{- else -}}
  {{- $image = mustMerge $image .ctx.Values.image -}}
{{- end -}}
{{ $image.repository }}:{{ $image.tag }}
{{- end -}}

{{/*
For compatibility and to support upgrade from enterprise-metrics chart calculate minio bucket name
*/}}
{{- define "mimir.minioBucketPrefix" -}}
{{- if .Values.enterprise.legacyLabels -}}enterprise-metrics{{- else -}}mimir{{- end -}}
{{- end -}}

{{/*
Create the name of the general service account
*/}}
{{- define "mimir.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "mimir.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Create the name of the ruler service account
*/}}
{{- define "mimir.ruler.serviceAccountName" -}}
{{- if and .Values.ruler.serviceAccount.create (eq .Values.ruler.serviceAccount.name "") -}}
{{- $sa := default (include "mimir.fullname" .) .Values.serviceAccount.name }}
{{- printf "%s-ruler" $sa }}
{{- else if and .Values.ruler.serviceAccount.create (not (eq .Values.ruler.serviceAccount.name "")) -}}
{{- .Values.ruler.serviceAccount.name -}}
{{- else -}}
{{- include "mimir.serviceAccountName" . -}}
{{- end -}}
{{- end -}}

{{/*
Create the name of the alertmanager service account
*/}}
{{- define "mimir.alertmanager.serviceAccountName" -}}
{{- if and .Values.alertmanager.serviceAccount.create (eq .Values.alertmanager.serviceAccount.name "") -}}
{{- $sa := default (include "mimir.fullname" .) .Values.serviceAccount.name }}
{{- printf "%s-alertmanager" $sa }}
{{- else if and .Values.alertmanager.serviceAccount.create (not (eq .Values.alertmanager.serviceAccount.name "")) -}}
{{- .Values.alertmanager.serviceAccount.name -}}
{{- else -}}
{{- include "mimir.serviceAccountName" . -}}
{{- end -}}
{{- end -}}

{{/*
Create the app name for clients. Defaults to the same logic as "mimir.fullname", and default client expects "prometheus".
*/}}
{{- define "client.name" -}}
{{- if .Values.client.name -}}
{{- .Values.client.name -}}
{{- else if .Values.client.fullnameOverride -}}
{{- .Values.client.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "prometheus" .Values.client.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Calculate the config from structured and unstructured text input
*/}}
{{- define "mimir.calculatedConfig" -}}
{{ tpl (mergeOverwrite (include "mimir.unstructuredConfig" . | fromYaml) .Values.mimir.structuredConfig | toYaml) . }}
{{- end -}}

{{/*
Calculate the config from the unstructured text input
*/}}
{{- define "mimir.unstructuredConfig" -}}
{{ include (print $.Template.BasePath "/_config-render.tpl") . }}
{{- end -}}

{{/*
The volume to mount for mimir configuration
*/}}
{{- define "mimir.configVolume" -}}
{{- if eq .Values.configStorageType "Secret" -}}
secret:
  secretName: {{ tpl .Values.externalConfigSecretName . }}
{{- else if eq .Values.configStorageType "ConfigMap" -}}
configMap:
  name: {{ tpl .Values.externalConfigSecretName . }}
  items:
    - key: "mimir.yaml"
      path: "mimir.yaml"
{{- end -}}
{{- end -}}

{{/*
Internal servers http listen port - derived from Mimir default
*/}}
{{- define "mimir.serverHttpListenPort" -}}
{{ (((.Values.mimir).structuredConfig).server).http_listen_port | default "8080" }}
{{- end -}}

{{/*
Internal servers grpc listen port - derived from Mimir default
*/}}
{{- define "mimir.serverGrpcListenPort" -}}
{{ (((.Values.mimir).structuredConfig).server).grpc_listen_port | default "9095" }}
{{- end -}}

{{/*
Alertmanager cluster bind address
*/}}
{{- define "mimir.alertmanagerClusterBindAddress" -}}
{{- if (include "mimir.calculatedConfig" . | fromYaml).alertmanager -}}
{{ (include "mimir.calculatedConfig" . | fromYaml).alertmanager.cluster_bind_address | default "" }}
{{- end -}}
{{- end -}}

{{- define "mimir.chunksCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-chunks-cache.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}:{{ (index .Values "chunks-cache").port }}
{{- end -}}

{{- define "mimir.indexCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-index-cache.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}:{{ (index .Values "index-cache").port }}
{{- end -}}

{{- define "mimir.metadataCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-metadata-cache.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}:{{ (index .Values "metadata-cache").port }}
{{- end -}}

{{- define "mimir.resultsCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-results-cache.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}:{{ (index .Values "results-cache").port }}
{{- end -}}

{{- define "mimir.adminCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-admin-cache.{{ .Release.Namespace }}.svc.{{ .Values.global.clusterDomain }}:{{ (index .Values "admin-cache").port }}
{{- end -}}

{{/*
Memberlist bind port
*/}}
{{- define "mimir.memberlistBindPort" -}}
{{ (((.Values.mimir).structuredConfig).memberlist).bind_port | default "7946" }}
{{- end -}}

{{/*
Resource name template
Params:
  ctx = . context
  component = component name (optional)
  rolloutZoneName = rollout zone name (optional)
*/}}
{{- define "mimir.resourceName" -}}
{{- $resourceName := include "mimir.fullname" .ctx -}}
{{- if .component -}}{{- $resourceName = printf "%s-%s" $resourceName .component -}}{{- end -}}
{{- if and (not .component) .rolloutZoneName -}}{{- printf "Component name cannot be empty if rolloutZoneName (%s) is set" .rolloutZoneName | fail -}}{{- end -}}
{{- if .rolloutZoneName -}}{{- $resourceName = printf "%s-%s" $resourceName .rolloutZoneName -}}{{- end -}}
{{- if gt (len $resourceName) 253 -}}{{- printf "Resource name (%s) exceeds kubernetes limit of 253 character. To fix: shorten release name if this will be a fresh install or shorten zone names (e.g. \"a\" instead of \"zone-a\") if using zone-awareness." $resourceName | fail -}}{{- end -}}
{{- $resourceName -}}
{{- end -}}

{{/*
Resource labels
Params:
  ctx = . context
  component = component name (optional)
  rolloutZoneName = rollout zone name (optional)
*/}}
{{- define "mimir.labels" -}}
{{- if .ctx.Values.enterprise.legacyLabels }}
{{- if .component -}}
app: {{ include "mimir.name" .ctx }}-{{ .component }}
{{- else -}}
app: {{ include "mimir.name" .ctx }}
{{- end }}
chart: {{ template "mimir.chart" .ctx }}
heritage: {{ .ctx.Release.Service }}
release: {{ .ctx.Release.Name }}

{{- else -}}

helm.sh/chart: {{ include "mimir.chart" .ctx }}
app.kubernetes.io/name: {{ include "mimir.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
{{- if .component }}
app.kubernetes.io/component: {{ .component }}
{{- end }}
{{- if .memberlist }}
app.kubernetes.io/part-of: memberlist
{{- end }}
{{- if .ctx.Chart.AppVersion }}
app.kubernetes.io/version: {{ .ctx.Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .ctx.Release.Service }}
{{- end }}
{{- if .rolloutZoneName }}
{{-   if not .component }}
{{-     printf "Component name cannot be empty if rolloutZoneName (%s) is set" .rolloutZoneName | fail }}
{{-   end }}
name: "{{ .component }}-{{ .rolloutZoneName }}" {{- /* Currently required for rollout-operator. https://github.com/grafana/rollout-operator/issues/15 */}}
rollout-group: {{ .component }}
zone: {{ .rolloutZoneName }}
{{- end }}
{{- end -}}

{{/*
POD labels
Params:
  ctx = . context
  component = name of the component
  memberlist = true if part of memberlist gossip ring
  rolloutZoneName = rollout zone name (optional)
*/}}
{{- define "mimir.podLabels" -}}
{{ with .ctx.Values.global.podLabels -}}
{{ toYaml . }}
{{ end }}
{{- if .ctx.Values.enterprise.legacyLabels }}
{{- if .component -}}
app: {{ include "mimir.name" .ctx }}-{{ .component }}
{{- if not .rolloutZoneName }}
name: {{ .component }}
{{- end }}
{{- end }}
{{- if .memberlist }}
gossip_ring_member: "true"
{{- end -}}
{{- if .component }}
target: {{ .component }}
release: {{ .ctx.Release.Name }}
{{- end }}
{{- else -}}
helm.sh/chart: {{ include "mimir.chart" .ctx }}
app.kubernetes.io/name: {{ include "mimir.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/version: {{ .ctx.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .ctx.Release.Service }}
{{- if .component }}
app.kubernetes.io/component: {{ .component }}
{{- end }}
{{- if .memberlist }}
app.kubernetes.io/part-of: memberlist
{{- end }}
{{- end }}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{- with ($componentSection).podLabels }}
{{ toYaml . }}
{{- end }}
{{- if .rolloutZoneName }}
{{-   if not .component }}
{{-     printf "Component name cannot be empty if rolloutZoneName (%s) is set" .rolloutZoneName | fail }}
{{-   end }}
name: "{{ .component }}-{{ .rolloutZoneName }}" {{- /* Currently required for rollout-operator. https://github.com/grafana/rollout-operator/issues/15 */}}
rollout-group: {{ .component }}
zone: {{ .rolloutZoneName }}
{{- end }}
{{- end -}}

{{/*
POD annotations
Params:
  ctx = . context
  component = name of the component
*/}}
{{- define "mimir.podAnnotations" -}}
{{- if .ctx.Values.useExternalConfig }}
checksum/config: {{ .ctx.Values.externalConfigVersion | quote }}
{{- else -}}
checksum/config: {{ include (print .ctx.Template.BasePath "/mimir-config.yaml") .ctx | sha256sum }}
{{- end }}
{{- with .ctx.Values.global.podAnnotations }}
{{ toYaml . }}
{{- end }}
{{- if .component }}
{{- if .ctx.Values.vaultAgent.enabled }}
{{- include "mimir.vaultAgent.annotations" (dict "ctx" .ctx "component" .component) }}
{{- end }}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml }}
{{- with ($componentSection).podAnnotations }}
{{ toYaml . }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Service selector labels
Params:
  ctx = . context
  component = name of the component
  rolloutZoneName = rollout zone name (optional)
*/}}
{{- define "mimir.selectorLabels" -}}
{{- if .ctx.Values.enterprise.legacyLabels }}
{{- if .component -}}
app: {{ include "mimir.name" .ctx }}-{{ .component }}
{{- end }}
release: {{ .ctx.Release.Name }}
{{- else -}}
app.kubernetes.io/name: {{ include "mimir.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
{{- if .component }}
app.kubernetes.io/component: {{ .component }}
{{- end }}
{{- end -}}
{{- if .rolloutZoneName }}
{{-   if not .component }}
{{-     printf "Component name cannot be empty if rolloutZoneName (%s) is set" .rolloutZoneName | fail }}
{{-   end }}
rollout-group: {{ .component }}
zone: {{ .rolloutZoneName }}
{{- end }}
{{- end -}}


{{/*
Alertmanager http prefix
*/}}
{{- define "mimir.alertmanagerHttpPrefix" -}}
{{- if (include "mimir.calculatedConfig" . | fromYaml).api }}
{{- (include "mimir.calculatedConfig" . | fromYaml).api.alertmanager_http_prefix | default "/alertmanager" -}}
{{- else -}}
{{- print "/alertmanager" -}}
{{- end -}}
{{- end -}}


{{/*
Prometheus http prefix
*/}}
{{- define "mimir.prometheusHttpPrefix" -}}
{{- if (include "mimir.calculatedConfig" . | fromYaml).api }}
{{- (include "mimir.calculatedConfig" . | fromYaml).api.prometheus_http_prefix | default "/prometheus" -}}
{{- else -}}
{{- print "/prometheus" -}}
{{- end -}}
{{- end -}}

{{/*
KEDA Autoscaling Prometheus address
*/}}
{{- define "mimir.kedaPrometheusAddress" -}}
{{- if not .ctx.Values.kedaAutoscaling.prometheusAddress -}}
{{ include "mimir.metaMonitoring.metrics.remoteReadUrl" . }}
{{- else -}}
{{ .ctx.Values.kedaAutoscaling.prometheusAddress }}
{{- end -}}
{{- end -}}


{{/*
Cluster name that shows up in dashboard metrics
*/}}
{{- define "mimir.clusterName" -}}
{{ (include "mimir.calculatedConfig" . | fromYaml).cluster_name | default .Release.Name }}
{{- end -}}

{{/* Allow KubeVersion to be overridden. */}}
{{- define "mimir.kubeVersion" -}}
  {{- default .Capabilities.KubeVersion.Version .Values.kubeVersionOverride -}}
{{- end -}}

{{/* Get API Versions */}}
{{- define "mimir.podDisruptionBudget.apiVersion" -}}
  {{- if semverCompare ">= 1.21-0" (include "mimir.kubeVersion" .) -}}
    {{- print "policy/v1" -}}
  {{- else -}}
    {{- print "policy/v1beta1" -}}
  {{- end -}}
{{- end -}}

{{/*
mimir.componentSectionFromName returns the sections from the user .Values in YAML
that corresponds to the requested component. mimir.componentSectionFromName takes two arguments
  .ctx = the root context of the chart
  .component = the name of the component. mimir.componentSectionFromName uses an internal mapping to know
                which component lives where in the values.yaml
Examples:
  $componentSection := include "mimir.componentSectionFromName" (dict "ctx" . "component" "store-gateway") | fromYaml
  $componentSection.podLabels ...
*/}}
{{- define "mimir.componentSectionFromName" -}}
{{- $componentsMap := dict
  "admin-api" "admin_api"
  "admin-cache" "admin-cache"
  "alertmanager" "alertmanager"
  "chunks-cache" "chunks-cache"
  "compactor" "compactor"
  "continuous-test" "continuous_test"
  "distributor" "distributor"
  "provisioner" "provisioner"
  "federation-frontend" "federation_frontend"
  "gateway" "gateway"
  "gr-aggr-cache" "gr-aggr-cache"
  "gr-metricname-cache" "gr-metricname-cache"
  "graphite-querier" "graphite.querier"
  "graphite-write-proxy" "graphite.write_proxy"
  "index-cache" "index-cache"
  "ingester" "ingester"
  "memcached" "memcached"
  "meta-monitoring" "metaMonitoring.grafanaAgent"
  "metadata-cache" "metadata-cache"
  "nginx" "nginx"
  "overrides-exporter" "overrides_exporter"
  "querier" "querier"
  "query-frontend" "query_frontend"
  "query-scheduler" "query_scheduler"
  "results-cache" "results-cache"
  "ruler" "ruler"
  "ruler-querier" "ruler_querier"
  "ruler-query-frontend" "ruler_query_frontend"
  "ruler-query-scheduler" "ruler_query_scheduler"
  "smoke-test" "smoke_test"
  "store-gateway" "store_gateway"
  "tokengen" "tokengenJob"
-}}
{{- $componentSection := index $componentsMap .component -}}
{{- if not $componentSection -}}{{- printf "No component section mapping for %s not found in values; submit a bug report if you are a user, edit mimir.componentSectionFromName if you are a contributor" .component | fail -}}{{- end -}}
{{- $section := .ctx.Values -}}
{{- range regexSplit "\\." $componentSection -1 -}}
  {{- $section = index $section . -}}
  {{- if not $section -}}{{- printf "Component section %s not found in values; values: %s" . ($.ctx.Values | toJson | abbrev 100) | fail -}}{{- end -}}
{{- end -}}
{{- $section | toYaml -}}
{{- end -}}

{{/*
Return the Vault Agent pod annotations if enabled and required by the component
mimir.vaultAgent.annotations takes 2 arguments
  .ctx = the root context of the chart
  .component = the name of the component
*/}}
{{- define "mimir.vaultAgent.annotations" -}}
{{- $vaultEnabledComponents := dict
  "admin-api" true
  "alertmanager" true
  "compactor" true
  "distributor" true
  "gateway" true
  "ingester" true
  "overrides-exporter" true
  "querier" true
  "query-frontend" true
  "query-scheduler" true
  "ruler" true
  "store-gateway" true
-}}
{{- if hasKey $vaultEnabledComponents .component }}
vault.hashicorp.com/agent-inject: 'true'
vault.hashicorp.com/role: '{{ .ctx.Values.vaultAgent.roleName }}'
vault.hashicorp.com/agent-inject-secret-client.crt: '{{ .ctx.Values.vaultAgent.clientCertPath }}'
vault.hashicorp.com/agent-inject-secret-client.key: '{{ .ctx.Values.vaultAgent.clientKeyPath }}'
vault.hashicorp.com/agent-inject-secret-server.crt: '{{ .ctx.Values.vaultAgent.serverCertPath }}'
vault.hashicorp.com/agent-inject-secret-server.key: '{{ .ctx.Values.vaultAgent.serverKeyPath }}'
vault.hashicorp.com/agent-inject-secret-root.crt: '{{ .ctx.Values.vaultAgent.caCertPath }}'
{{- end}}
{{- end -}}

{{/*
Get the no_auth_tenant from the configuration
*/}}
{{- define "mimir.noAuthTenant" -}}
{{- (include "mimir.calculatedConfig" . | fromYaml).no_auth_tenant | default "anonymous" -}}
{{- end -}}

{{/*
Return if we should create a PodSecurityPolicy. Takes into account user values and supported kubernetes versions.
*/}}
{{- define "mimir.rbac.usePodSecurityPolicy" -}}
{{- and
      (
        or (semverCompare "< 1.24-0" (include "mimir.kubeVersion" .))
           (and (semverCompare "< 1.25-0" (include "mimir.kubeVersion" .)) .Values.rbac.forcePSPOnKubernetes124)
      )
      (and .Values.rbac.create (eq .Values.rbac.type "psp"))
-}}
{{- end -}}

{{/*
Return if we should create a SecurityContextConstraints. Takes into account user values and supported openshift versions.
*/}}
{{- define "mimir.rbac.useSecurityContextConstraints" -}}
{{- and .Values.rbac.create (eq .Values.rbac.type "scc") -}}
{{- end -}}

{{- define "mimir.remoteWriteUrl.inCluster" -}}
{{- if or (eq (include "mimir.gateway.isEnabled" . ) "true") .Values.nginx.enabled -}}
{{ include "mimir.gatewayUrl" . }}/api/v1/push
{{- else -}}
http://{{ template "mimir.fullname" . }}-distributor-headless.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}/api/v1/push
{{- end -}}
{{- end -}}

{{- define "mimir.remoteReadUrl.inCluster" -}}
{{- if or (eq (include "mimir.gateway.isEnabled" . ) "true") .Values.nginx.enabled -}}
{{ include "mimir.gatewayUrl" . }}{{ include "mimir.prometheusHttpPrefix" . }}
{{- else -}}
http://{{ template "mimir.fullname" . }}-query-frontend.{{ .Release.Namespace }}.svc:{{ include "mimir.serverHttpListenPort" . }}{{ include "mimir.prometheusHttpPrefix" . }}
{{- end -}}
{{- end -}}

{{/*
Creates dict for zone-aware replication configuration
Params:
  ctx = . context
  component = component name
Return value:
  {
    zoneName: {
      affinity: <affinity>,
      nodeSelector: <nodeSelector>,
      replicas: <N>,
      storageClass: <S>
    },
    ...
  }
During migration there is a special case where an extra "zone" is generated with zonaName == "" empty string.
The empty string evaluates to false in boolean expressions so it is treated as the default (non zone-aware) zone,
which allows us to keep generating everything for the default zone.
*/}}
{{- define "mimir.zoneAwareReplicationMap" -}}
{{- $zonesMap := (dict) -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $defaultZone := (dict "affinity" $componentSection.affinity "nodeSelector" $componentSection.nodeSelector "replicas" $componentSection.replicas "storageClass" $componentSection.storageClass) -}}

{{- if $componentSection.zoneAwareReplication.enabled -}}
{{- $numberOfZones := len $componentSection.zoneAwareReplication.zones -}}
{{- if lt $numberOfZones 3 -}}
{{- fail "When zone-awareness is enabled, you must have at least 3 zones defined." -}}
{{- end -}}

{{- $requestedReplicas := $componentSection.replicas -}}
{{- if and (has .component (list "ingester" "alertmanager")) $componentSection.zoneAwareReplication.migration.enabled (not $componentSection.zoneAwareReplication.migration.writePath) -}}
{{- $requestedReplicas = $componentSection.zoneAwareReplication.migration.replicas }}
{{- end -}}
{{- $replicaPerZone := div (add $requestedReplicas $numberOfZones -1) $numberOfZones -}}

{{- range $idx, $rolloutZone := $componentSection.zoneAwareReplication.zones -}}
{{- $_ := set $zonesMap $rolloutZone.name (dict
  "affinity" (($rolloutZone.extraAffinity | default (dict)) | mergeOverwrite (include "mimir.zoneAntiAffinity" (dict "component" $.component "rolloutZoneName" $rolloutZone.name "topologyKey" $componentSection.zoneAwareReplication.topologyKey ) | fromYaml ) )
  "nodeSelector" ($rolloutZone.nodeSelector | default (dict) )
  "replicas" $replicaPerZone
  "storageClass" $rolloutZone.storageClass
  ) -}}
{{- end -}}
{{- if $componentSection.zoneAwareReplication.migration.enabled -}}
{{- if $componentSection.zoneAwareReplication.migration.scaleDownDefaultZone -}}
{{- $_ := set $defaultZone "replicas" 0 -}}
{{- end -}}
{{- $_ := set $zonesMap "" $defaultZone -}}
{{- end -}}

{{- else -}}
{{- $_ := set $zonesMap "" $defaultZone -}}
{{- end -}}
{{- $zonesMap | toYaml }}

{{- end -}}

{{/*
Calculate anti-affinity for a zone
Params:
  component = component name
  rolloutZoneName = name of the rollout zone
  topologyKey = topology key
*/}}
{{- define "mimir.zoneAntiAffinity" -}}
{{- if .topologyKey -}}
podAntiAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchExpressions:
          - key: rollout-group
            operator: In
            values:
              - {{ .component }}
          - key: zone
            operator: NotIn
            values:
              - {{ .rolloutZoneName }}
      topologyKey: {{ .topologyKey | quote }}
{{- else -}}
{}
{{- end -}}
{{- end -}}

{{/*
Calculate annotations with zone-awareness
Params:
  ctx = . context
  component = component name
  rolloutZoneName = rollout zone name (optional)
*/}}
{{- define "mimir.componentAnnotations" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- if and (or $componentSection.zoneAwareReplication.enabled $componentSection.zoneAwareReplication.migration.enabled) .rolloutZoneName }}
{{- $map := dict "rollout-max-unavailable" ($componentSection.zoneAwareReplication.maxUnavailable | toString) -}}
{{- toYaml (deepCopy $map | mergeOverwrite $componentSection.annotations) }}
{{- else -}}
{{ toYaml $componentSection.annotations }}
{{- end -}}
{{- end -}}


{{- define "mimir.var_dump" -}}
{{- . | mustToPrettyJson | printf "\nThe JSON output of the dumped var is: \n%s" | fail }}
{{- end -}}


{{/*
siToBytes is used to convert Kubernetes byte units to bytes.
Works for a sub set of SI suffixes: m, k, M, G, T, and their power-of-two equivalents: Ki, Mi, Gi, Ti.

mimir.siToBytes takes 1 argument
  .value = the input value with SI unit
*/}}
{{- define "mimir.siToBytes" -}}
    {{- if (hasSuffix "Ki" .value) -}}
        {{- trimSuffix "Ki" .value | float64 | mulf 1024 | ceil | int64 -}}
    {{- else if (hasSuffix "Mi" .value) -}}
        {{- trimSuffix "Mi" .value | float64 | mulf 1048576 | ceil | int64 -}}
    {{- else if (hasSuffix "Gi" .value) -}}
        {{- trimSuffix "Gi" .value | float64 | mulf 1073741824 | ceil | int64 -}}
    {{- else if (hasSuffix "Ti" .value) -}}
        {{- trimSuffix "Ti" .value | float64 | mulf 1099511627776 | ceil | int64 -}}
    {{- else if (hasSuffix "k" .value) -}}
        {{- trimSuffix "k" .value | float64 | mulf 1000 | ceil | int64 -}}
    {{- else if (hasSuffix "M" .value) -}}
        {{- trimSuffix "M" .value | float64 | mulf 1000000 | ceil | int64 -}}
    {{- else if (hasSuffix "G" .value) -}}
        {{- trimSuffix "G" .value | float64 | mulf 1000000000 | ceil | int64 -}}
    {{- else if (hasSuffix "T" .value) -}}
        {{- trimSuffix "T" .value | float64 | mulf 1000000000000 | ceil | int64 -}}
    {{- else if (hasSuffix "m" .value) -}}
        {{- trimSuffix "m" .value | float64 | mulf 0.001 | ceil | int64 -}}
    {{- else -}}
        {{- .value }}
    {{- end -}}
{{- end -}}

{{/*
parseCPU is used to convert Kubernetes CPU units to the corresponding float value of CPU cores.
The returned value is a string representation. If you need to do any math on it, please parse the string first.

mimir.parseCPU takes 1 argument
  .value = the Kubernetes CPU request value
*/}}
{{- define "mimir.parseCPU" -}}
    {{- $value_string := .value | toString -}}
    {{- if (hasSuffix "m" $value_string) -}}
        {{ trimSuffix "m" $value_string | float64 | mulf 0.001 -}}
    {{- else -}}
        {{- $value_string }}
    {{- end -}}
{{- end -}}

{{/*
cpuToMilliCPU is used to convert Kubernetes CPU units to MilliCPU.
The returned value is a string representation. If you need to do any math on it, please parse the string first.

mimir.cpuToMilliCPU takes 1 argument
  .value = the Kubernetes CPU request value
*/}}
{{- define "mimir.cpuToMilliCPU" -}}
    {{- $value_string := .value | toString -}}
    {{- if (hasSuffix "m" $value_string) -}}
        {{ trimSuffix "m" $value_string -}}
    {{- else -}}
        {{- $value_string | float64 | mulf 1000 | toString }}
    {{- end -}}
{{- end -}}

{{/*
kubectl image reference
*/}}
{{- define "mimir.kubectlImage" -}}
{{ .Values.kubectlImage.repository }}:{{ .Values.kubectlImage.tag }}
{{- end -}}
