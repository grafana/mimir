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
Calculate image name based on whether enterprise features are requested
*/}}
{{- define "mimir.imageReference" -}}
{{- if .Values.enterprise.enabled -}}{{ .Values.enterprise.image.repository }}:{{ .Values.enterprise.image.tag }}{{- else -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end -}}
{{- end -}}

{{/*
For compatiblity and to support upgrade from enterprise-metrics chart calculate minio bucket name
*/}}
{{- define "mimir.minioBucketPrefix" -}}
{{- if .Values.enterprise.legacyLabels -}}enterprise-metrics{{- else -}}mimir{{- end -}}
{{- end -}}

{{/*
Create the name of the service account
*/}}
{{- define "mimir.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "mimir.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
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
Calculate the config from structured and unstructred text input
*/}}
{{- define "mimir.calculatedConfig" -}}
{{ tpl (mergeOverwrite (include "mimir.unstructuredConfig" . | fromYaml) .Values.mimir.structuredConfig | toYaml) . }}
{{- end -}}

{{/*
Calculate the config from the unstructred text input
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
dns+{{ template "mimir.fullname" . }}-chunks-cache.{{ .Release.Namespace }}.svc:{{ (index .Values "chunks-cache").port }}
{{- end -}}

{{- define "mimir.indexCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-index-cache.{{ .Release.Namespace }}.svc:{{ (index .Values "index-cache").port }}
{{- end -}}

{{- define "mimir.metadataCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-metadata-cache.{{ .Release.Namespace }}.svc:{{ (index .Values "metadata-cache").port }}
{{- end -}}

{{- define "mimir.resultsCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-results-cache.{{ .Release.Namespace }}.svc:{{ (index .Values "results-cache").port }}
{{- end -}}

{{- define "mimir.adminCacheAddress" -}}
dns+{{ template "mimir.fullname" . }}-admin-cache.{{ .Release.Namespace }}.svc:{{ (index .Values "admin-cache").port }}
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
checksum/config: {{ .ctx.Values.externalConfigVersion }}
{{- else -}}
checksum/config: {{ include (print .ctx.Template.BasePath "/mimir-config.yaml") .ctx | sha256sum }}
{{- end }}
{{- with .ctx.Values.global.podAnnotations }}
{{ toYaml . }}
{{- end }}
{{- if .component }}
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
  "gateway" "gateway"
  "gr-aggr-cache" "gr-aggr-cache"
  "gr-metricname-cache" "gr-metricname-cache"
  "graphite-querier" "graphite.querier"
  "graphite-web" "graphite.web"
  "graphite-write-proxy" "graphite.write_proxy"
  "index-cache" "index-cache"
  "ingester" "ingester"
  "memcached" "memcached"
  "metadata-cache" "metadata-cache"
  "nginx" "nginx"
  "overrides-exporter" "overrides_exporter"
  "querier" "querier"
  "query-frontend" "query_frontend"
  "query-scheduler" "query_scheduler"
  "results-cache" "results-cache"
  "ruler" "ruler"
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
Get the no_auth_tenant from the configuration
*/}}
{{- define "mimir.noAuthTenant" -}}
{{- (include "mimir.calculatedConfig" . | fromYaml).no_auth_tenant | default "anonymous" -}}
{{- end -}}

{{/*
Return if we should create a PodSecurityPoliPodSecurityPolicycy. Takes into account user values and supported kubernetes versions.
*/}}
{{- define "mimir.rbac.usePodSecurityPolicy" -}}
{{- and (semverCompare "< 1.25-0" (include "mimir.kubeVersion" .)) (and .Values.rbac.create (eq .Values.rbac.type "psp")) -}}
{{- end -}}

{{/*
Return if we should create a SecurityContextConstraints. Takes into account user values and supported openshift versions.
*/}}
{{- define "mimir.rbac.useSecurityContextConstraints" -}}
{{- and .Values.rbac.create (eq .Values.rbac.type "scc") -}}
{{- end -}}

{{- define "mimir.remoteWriteUrl.inCluster" -}}
{{ include "mimir.gatewayUrl" . }}/api/v1/push
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
      replicas: <N>
    },
    ...
  }
During migration there is a special case where an extra "zone" is generated with zonaName == "" empty string.
The empty string evaulates to false in boolean expressions so it is treated as the default (non zone-aware) zone,
which allows us to keep generating everything for the default zone.
*/}}
{{- define "mimir.zoneAwareReplicationMap" -}}
{{- $zonesMap := (dict) -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $defaultZone := (dict "affinity" $componentSection.affinity "nodeSelector" $componentSection.nodeSelector "replicas" $componentSection.replicas) -}}

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
          - key: app.kubernetes.io/component
            operator: NotIn
            values:
              - {{ .component }}-{{ .rolloutZoneName }}
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
