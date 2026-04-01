{{/*
Kafka container environment variables with merge precedence.
Builds the Kafka required default env vars and merges user-provided overrides by name,
preserving the order of the defaults list (critical: _POD_NAME must appear before
KAFKA_ADVERTISED_LISTENERS so Kubernetes resolves the reference at pod creation time).

The order of precedence is as follows (high to low):
1. kafka.env (component-specific overrides, matched by name)
2. global.extraEnv
3. The computed Kafka defaults

Outputs the complete env/envFrom section for the Kafka container manifest.
Params:
  ctx      = root (.) context of the chart
  replicas = number of Kafka replicas (int)
*/}}
{{- define "mimir.kafka.containerEnv" -}}
{{- $ctx             := .ctx -}}
{{- $replicas        := .replicas -}}
{{- $kafkaSection    := $ctx.Values.kafka -}}
{{- $kafkaName       := include "mimir.resourceName" (dict "ctx" $ctx "component" "kafka") -}}
{{- $clusterDomain   := $ctx.Values.global.clusterDomain -}}
{{- $namespace       := $ctx.Release.Namespace -}}
{{- /* Build quorum-voters string. Kafka fails to resolve DNS names with a trailing dot,
     e.g. "cluster.local.", so trimSuffix is applied for the clusterDomain. */ -}}
{{- $quorumVoters := list -}}
{{- range $i := until (int $replicas) -}}
  {{- $quorumVoters = append $quorumVoters (printf "%d@%s-%d.%s-headless.%s.svc.%s:9093" $i $kafkaName $i $kafkaName $namespace (trimSuffix "." $clusterDomain)) -}}
{{- end -}}
{{- /* Default Kafka env vars in their required order.
     "_POD_NAME" must be first so Kubernetes resolves it before KAFKA_ADVERTISED_LISTENERS
     references it via $(_POD_NAME).
     The REPLICATION_FACTOR vars must be present so brokers create the offsets topic;
     otherwise Mimir ingesters cannot start. */ -}}
{{- $defaultEnv := list
    (dict "name" "_POD_NAME" "valueFrom" (dict "fieldRef" (dict "fieldPath" "metadata.name")))
    (dict "name" "KAFKA_CLUSTER_ID" "value" ($kafkaSection.clusterId | default ""))
    (dict "name" "KAFKA_NODE_ID" "valueFrom" (dict "fieldRef" (dict "fieldPath" "metadata.labels['apps.kubernetes.io/pod-index']")))
    (dict "name" "KAFKA_PROCESS_ROLES" "value" "broker,controller")
    (dict "name" "KAFKA_LISTENERS" "value" "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
    (dict "name" "KAFKA_ADVERTISED_LISTENERS" "value" (printf "PLAINTEXT://$(_POD_NAME).%s-headless.%s.svc.%s:9092" $kafkaName $namespace $clusterDomain))
    (dict "name" "KAFKA_CONTROLLER_QUORUM_VOTERS" "value" (join "," $quorumVoters))
    (dict "name" "KAFKA_CONTROLLER_LISTENER_NAMES" "value" "CONTROLLER")
    (dict "name" "KAFKA_INTER_BROKER_LISTENER_NAME" "value" "PLAINTEXT")
    (dict "name" "KAFKA_LOG_DIRS" "value" "/var/lib/kafka/data")
    (dict "name" "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" "value" (min 3 $replicas | toString))
    (dict "name" "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" "value" (min 3 $replicas | toString))
    (dict "name" "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR" "value" (min 2 $replicas | toString))
    (dict "name" "KAFKA_LOG_RETENTION_HOURS" "value" ($kafkaSection.logRetentionHours | default 24 | toString))
-}}
{{- /* Build a name→entry lookup from user overrides.
     global.extraEnv is applied first, kafka.env second so kafka.env wins (highest precedence). */ -}}
{{- $overridesKV := dict -}}
{{- range ($ctx.Values.global.extraEnv | default list) -}}
  {{- $_ := set $overridesKV .name . -}}
{{- end -}}
{{- range ($kafkaSection.env | default list) -}}
  {{- $_ := set $overridesKV .name . -}}
{{- end -}}
{{- /* Walk defaults in their original order so the required ordering is preserved.
     Apply the user override for a given name in-place rather than appending at the end. */ -}}
{{- $defaultNames := dict -}}
{{- $envList := list -}}
{{- range $defaultEnv -}}
  {{- $_ := set $defaultNames .name true -}}
  {{- if hasKey $overridesKV .name -}}
    {{- $envList = append $envList (index $overridesKV .name) -}}
  {{- else -}}
    {{- $envList = append $envList . -}}
  {{- end -}}
{{- end -}}
{{- /* Append user-defined vars that are not overrides of an existing default. */ -}}
{{- range $name, $envVar := $overridesKV -}}
  {{- if not (hasKey $defaultNames $name) -}}
    {{- $envList = append $envList $envVar -}}
  {{- end -}}
{{- end -}}
env:
  {{- toYaml $envList | nindent 2 }}
{{- if or $ctx.Values.global.extraEnvFrom $kafkaSection.extraEnvFrom }}
envFrom:
{{- with $ctx.Values.global.extraEnvFrom }}
  {{- toYaml . | nindent 2 -}}
{{- end }}
{{- with $kafkaSection.extraEnvFrom }}
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
{{- end -}}
