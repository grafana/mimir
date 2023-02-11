{{/*
Mimir common HorizontalPodAutoscaler definition
Params:
  ctx = . context
  component = name of the component
  kind = Kind of object to be scaled
  zoneName = name of the zone, e.g. zone-a (optional, StatefulSets only)
  rolloutZone = used to get replicas per zone when using zone-aware replication (optional, StatefulSets only)
  zoneAware = if the sts uses zone-aware replication (optional, StatefulSets only)
*/}}
{{- define "mimir.lib.horizontalPodAutoscaler" -}}
{{- $componentSection := include "mimir.componentSectionFromName" . | fromYaml -}}
{{- $hpa := $componentSection.hpa -}}
{{- $args := dict "ctx" $.ctx "component" $.component -}}
{{- $name := (and (eq ($.zoneAware | toString) "true") (ne ($.zoneName | toString) "")) | ternary
  (printf "%s-%s" (include "mimir.resourceName" $args) $.zoneName)
  (include "mimir.resourceName" $args)
-}}

{{- if $hpa.enabled -}}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ $name }}-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: {{ $.kind }}
    name: {{ $name }}
  {{- if and $.zoneAware (ne ($.zoneName | toString) "") }}
  minReplicas: {{ $.rolloutZone.replicas }}
  {{- else }}
  minReplicas: {{ $hpa.minReplicas }}
  {{- end }}
  maxReplicas: {{ $hpa.maxReplicas }}
  metrics:
    {{- if $hpa.averageMemoryUtilization }}
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: {{ $hpa.averageMemoryUtilization }}
    {{- end }}
    {{- if $hpa.averageCpuUtilization }}
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ $hpa.averageCpuUtilization }}
    {{- end }}
  behavior:
    {{- toYaml $hpa.behavior | nindent 4 -}}
{{- end -}}
{{- end -}}
